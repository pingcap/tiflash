#include <Poco/File.h>

#include <DataTypes/IDataType.h>
#include <IO/ReadBufferFromFile.h>
#include <Storages/DeltaMerge/File/DMFileReader.h>
#include <Storages/Page/PageUtil.h>

namespace DB
{
namespace DM
{

DMFileReader::Stream::Stream(DMFileReader & reader, ColId col_id, size_t aio_threshold, size_t max_read_buffer_size, Logger * log)
    : avg_size_hint(reader.dmfile->getColumnStat(col_id).avg_size)
{
    String mark_path = reader.dmfile->colMarkPath(col_id);
    String data_path = reader.dmfile->colDataPath(col_id);

    auto mark_load = [&]() -> MarksInCompressedFilePtr {
        auto res = std::make_shared<MarksInCompressedFile>(reader.dmfile->getChunks());
        if (res->empty()) // 0 rows.
            return res;
        size_t size = sizeof(MarkInCompressedFile) * reader.dmfile->getChunks();
        auto   fd   = PageUtil::openFile<true>(mark_path);
        SCOPE_EXIT({ ::close(fd); });
        PageUtil::readFile(fd, 0, reinterpret_cast<char *>(res->data()), size, mark_path);

        return res;
    };

    if (reader.mark_cache)
        marks = reader.mark_cache->getOrSet(MarkCache::hash(mark_path, reader.hash_salt), mark_load);
    else
        marks = mark_load();

    size_t data_file_size = Poco::File(data_path).getSize();
    size_t chunks         = reader.dmfile->getChunks();
    size_t buffer_size    = 0;
    size_t estimated_size = 0;
    for (size_t i = 0; i < chunks;)
    {
        if (!reader.use_chunks[i])
        {
            ++i;
            continue;
        }
        size_t cur_offset_in_file = (*marks)[i].offset_in_compressed_file;
        size_t end                = i + 1;
        // First find the end of current available range.
        while (end < chunks && reader.use_chunks[end])
            ++end;

        // Second If the end of range is inside the block, we will need to read it too.
        if (end < chunks)
        {
            size_t last_offset_in_file = (*marks)[end].offset_in_compressed_file;
            if ((*marks)[end].offset_in_decompressed_block > 0)
            {
                while (end < chunks && (*marks)[end].offset_in_compressed_file == last_offset_in_file)
                    ++end;
            }
        }

        size_t range_end_in_file = (end == chunks) ? data_file_size : (*marks)[end].offset_in_compressed_file;

        size_t range = range_end_in_file - cur_offset_in_file;
        buffer_size  = std::max(buffer_size, range);

        estimated_size += range;
        i = end;
    }

    buffer_size = std::min(buffer_size, max_read_buffer_size);

    LOG_TRACE(log,
              "file size: " << data_file_size << ", estimated read size: " << estimated_size << ", buffer_size: " << buffer_size
                            << " (aio_threshold: " << aio_threshold << ", max_read_buffer_size: " << max_read_buffer_size << ")");

    for (size_t i = 0; i < chunks; ++i)
    {
        if (reader.use_chunks[i])
        {
            auto start = (*marks)[i].offset_in_compressed_file;
            auto end   = (i == chunks - 1) ? data_file_size : (*marks)[i + 1].offset_in_compressed_file;
            estimated_size += end - start;
        }
    }

    buf = std::make_unique<CompressedReadBufferFromFile>(data_path, estimated_size, aio_threshold, buffer_size);
}

DMFileReader::DMFileReader(bool                  enable_clean_read_,
                           const DMFilePtr &     dmfile_,
                           const ColumnDefines & read_columns_,
                           const HandleRange &   handle_range_,
                           const RSOperatorPtr & filter_,
                           const IdSetPtr &      read_chunks_,
                           MarkCache *           mark_cache_,
                           MinMaxIndexCache *    index_cache_,
                           UInt64                hash_salt_,
                           size_t                aio_threshold,
                           size_t                max_read_buffer_size,
                           size_t                rows_threshold_per_read_)
    : enable_clean_read(enable_clean_read_),
      dmfile(dmfile_),
      read_columns(read_columns_),
      handle_range(handle_range_),
      mark_cache(mark_cache_),
      hash_salt(hash_salt_),
      rows_threshold_per_read(rows_threshold_per_read_),
      chunk_filter(dmfile_, index_cache_, hash_salt_, handle_range_, filter_, read_chunks_),
      handle_res(chunk_filter.getHandleRes()),
      use_chunks(chunk_filter.getUseChunks()),
      skip_chunks_by_column(read_columns.size(), 0),
      log(&Logger::get("DMFileReader"))
{
    if (dmfile->getStatus() != DMFile::Status::READABLE)
        throw Exception("DMFile [" + DB::toString(dmfile->fileId())
                        + "] is expected to be in READABLE status, but: " + DMFile::statusString(dmfile->getStatus()));

    for (auto & cd : read_columns)
    {
        column_streams.emplace(cd.id, std::make_unique<Stream>(*this, cd.id, aio_threshold, max_read_buffer_size, log));
    }
}

bool DMFileReader::shouldSeek(size_t chunk_id)
{
    // If current chunk is the first one, or we just finished reading the last chunk, then no need to seek.
    return chunk_id != 0 && !use_chunks[chunk_id - 1];
}

bool DMFileReader::getSkippedRows(size_t & skip_rows)
{
    skip_rows = 0;
    for (; next_chunk_id < use_chunks.size() && !use_chunks[next_chunk_id]; ++next_chunk_id)
    {
        skip_rows += dmfile->getSplit()[next_chunk_id];
    }
    return next_chunk_id < use_chunks.size();
}

bool isExtraColumn(const ColumnDefine & cd)
{
    return cd.id == EXTRA_HANDLE_COLUMN_ID || cd.id == VERSION_COLUMN_ID || cd.id == TAG_COLUMN_ID;
}

Block DMFileReader::read()
{
    // Go to next available chunk.
    for (; next_chunk_id < use_chunks.size() && !use_chunks[next_chunk_id]; ++next_chunk_id) {}

    if (next_chunk_id >= use_chunks.size())
        return {};

    // Find max continuing rows we can read.
    size_t start_chunk_id = next_chunk_id;

    auto & split          = dmfile->getSplit();
    auto & not_clean      = dmfile->getNotClean();
    size_t read_rows      = 0;
    size_t not_clean_rows = 0;

    RSResult expected_handle_res = handle_res[next_chunk_id];
    for (; next_chunk_id < use_chunks.size() && use_chunks[next_chunk_id] && read_rows < rows_threshold_per_read; ++next_chunk_id)
    {
        if (enable_clean_read && handle_res[next_chunk_id] != expected_handle_res)
            break;

        read_rows += split[next_chunk_id];
        not_clean_rows += not_clean[next_chunk_id];
    }

    if (!read_rows)
        return {};

    Block res;

    size_t read_chunks   = next_chunk_id - start_chunk_id;
    bool   do_clean_read = enable_clean_read && expected_handle_res == All && !not_clean_rows;
    for (size_t i = 0; i < read_columns.size(); ++i)
    {
        auto & cd = read_columns[i];
        if (do_clean_read && isExtraColumn(cd))
        {
            ColumnPtr column;
            if (cd.id == EXTRA_HANDLE_COLUMN_ID)
            {
                // Return the first row's handle
                Handle min_handle = chunk_filter.getMinHandle(start_chunk_id);
                column            = cd.type->createColumnConst(read_rows, Field(min_handle));
            }
            else
            {
                column = cd.type->createColumnConstWithDefaultValue(read_rows);
            }

            res.insert(ColumnWithTypeAndName{column, cd.type, cd.name, cd.id});

            skip_chunks_by_column[i] = read_chunks;
        }
        else
        {
            auto & stream = column_streams.at(cd.id);
            if (shouldSeek(start_chunk_id) || skip_chunks_by_column[i] > 0)
            {
                auto & mark = (*stream->marks)[start_chunk_id];
                stream->buf->seek(mark.offset_in_compressed_file, mark.offset_in_decompressed_block);
            }

            auto column = cd.type->createColumn();
            cd.type->deserializeBinaryBulkWithMultipleStreams(*column, //
                                                              [&](const IDataType::SubstreamPath &) { return stream->buf.get(); },
                                                              read_rows,
                                                              stream->avg_size_hint,
                                                              true,
                                                              {});
            IDataType::updateAvgValueSizeHint(*column, stream->avg_size_hint);

            res.insert(ColumnWithTypeAndName{std::move(column), cd.type, cd.name, cd.id});

            skip_chunks_by_column[i] = 0;
        }
    }

    return res;
}

} // namespace DM
} // namespace DB
