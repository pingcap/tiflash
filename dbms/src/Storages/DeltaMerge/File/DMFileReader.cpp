#include <Poco/File.h>

#include <DataTypes/IDataType.h>
#include <IO/ReadBufferFromFile.h>
#include <Storages/DeltaMerge/File/DMFileReader.h>
#include <Storages/Page/PageUtil.h>

namespace DB
{
namespace DM
{

static const size_t READ_ROWS_THRESHOLD = DEFAULT_MERGE_BLOCK_SIZE * 3;

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
        marks = reader.mark_cache->getOrSet(MarkCache::hash(mark_path), mark_load);
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

DMFileReader::DMFileReader(const DMFilePtr &     dmfile_,
                           const ColumnDefines & read_columns_,
                           const RSOperatorPtr & filter,
                           MarkCache *           mark_cache_,
                           MinMaxIndexCache *    index_cache_,
                           size_t                aio_threshold,
                           size_t                max_read_buffer_size)
    : dmfile(dmfile_),
      read_columns(read_columns_),
      mark_cache(mark_cache_),
      index_cache(index_cache_),
      use_chunks(dmfile->getChunks(), 1),
      log(&Logger::get("DMFileReader"))
{
    if (dmfile->getStatus() != DMFile::Status::READABLE)
        throw Exception("DMFile [" + DB::toString(dmfile->fileId())
                        + "] is expected to be in READABLE status, but: " + DMFile::statusString(dmfile->getStatus()));
    if (filter)
    {
        // Currently we only load handle's index.
        RSCheckParam param;
        loadIndex(param, EXTRA_HANDLE_COLUMN_ID);

        for (size_t i = 0; i < dmfile->getChunks(); ++i)
            use_chunks[i] = filter->roughCheck(i, param) != None;
    }

    for (auto & cd : read_columns)
    {
        column_streams.emplace(cd.id, std::make_unique<Stream>(*this, cd.id, aio_threshold, max_read_buffer_size, log));
    }
}

void DMFileReader::loadIndex(RSCheckParam & param, const ColId col_id)
{
    auto & type = dmfile->getColumnStat(col_id).type;
    auto   load = [&]() {
        auto index_buf = openForRead(dmfile->colIndexPath(col_id));
        return MinMaxIndex::read(*type, index_buf);
    };
    MinMaxIndexPtr minmax_index;
    if (index_cache)
    {
        auto key     = MinMaxIndexCache::hash(dmfile->colIndexPath(col_id));
        minmax_index = index_cache->getOrSet(key, load);
    }
    else
    {
        minmax_index = load();
    }

    param.indexes.emplace(col_id, RSIndex(type, minmax_index));
}

bool DMFileReader::shouldSeek(size_t chunk_id)
{
    // If current chunk is the first one, or we just finished reading the last chunk, then no need to seek.
    return chunk_id != 0 && !use_chunks[chunk_id - 1];
}

size_t DMFileReader::getSkippedRows()
{
    size_t skip_rows = 0;
    for (; next_chunk_id < use_chunks.size() && !use_chunks[next_chunk_id]; ++next_chunk_id)
    {
        skip_rows += dmfile->getSplit()[next_chunk_id];
    }
    return skip_rows;
}

Block DMFileReader::read()
{
    // Go to next avaliable chunk.
    for (; next_chunk_id < use_chunks.size() && !use_chunks[next_chunk_id]; ++next_chunk_id) {}

    size_t start_chunk_id = next_chunk_id;

    // Find max continuing rows we can read.
    size_t read_rows = 0;
    for (; next_chunk_id < use_chunks.size() && use_chunks[next_chunk_id] && read_rows < READ_ROWS_THRESHOLD; ++next_chunk_id)
        read_rows += dmfile->getSplit()[next_chunk_id];

    if (!read_rows)
        return {};

    Block res;
    for (auto & cd : read_columns)
    {
        auto & stream = column_streams.at(cd.id);
        if (shouldSeek(start_chunk_id))
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
    }

    return res;
}

} // namespace DM
} // namespace DB
