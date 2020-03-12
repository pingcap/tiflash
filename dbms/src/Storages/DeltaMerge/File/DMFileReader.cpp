#include <Poco/File.h>

#include <DataTypes/IDataType.h>
#include <IO/ReadBufferFromFile.h>
#include <Storages/DeltaMerge/File/DMFileReader.h>
#include <Storages/Page/PageUtil.h>

namespace DB
{
namespace DM
{

DMFileReader::Stream::Stream(DMFileReader & reader, //
                             ColId          col_id,
                             const String & file_name_base,
                             size_t         aio_threshold,
                             size_t         max_read_buffer_size,
                             Logger *       log)
    : avg_size_hint(reader.dmfile->getColumnStat(col_id).avg_size)
{
    String mark_path = reader.dmfile->colMarkPath(file_name_base);
    String data_path = reader.dmfile->colDataPath(file_name_base);

    auto mark_load = [&]() -> MarksInCompressedFilePtr {
        auto res = std::make_shared<MarksInCompressedFile>(reader.dmfile->getPacks());
        if (res->empty()) // 0 rows.
            return res;
        size_t size = sizeof(MarkInCompressedFile) * reader.dmfile->getPacks();
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
    size_t packs         = reader.dmfile->getPacks();
    size_t buffer_size    = 0;
    size_t estimated_size = 0;
    for (size_t i = 0; i < packs;)
    {
        if (!reader.use_packs[i])
        {
            ++i;
            continue;
        }
        size_t cur_offset_in_file = (*marks)[i].offset_in_compressed_file;
        size_t end                = i + 1;
        // First find the end of current available range.
        while (end < packs && reader.use_packs[end])
            ++end;

        // Second If the end of range is inside the block, we will need to read it too.
        if (end < packs)
        {
            size_t last_offset_in_file = (*marks)[end].offset_in_compressed_file;
            if ((*marks)[end].offset_in_decompressed_block > 0)
            {
                while (end < packs && (*marks)[end].offset_in_compressed_file == last_offset_in_file)
                    ++end;
            }
        }

        size_t range_end_in_file = (end == packs) ? data_file_size : (*marks)[end].offset_in_compressed_file;

        size_t range = range_end_in_file - cur_offset_in_file;
        buffer_size  = std::max(buffer_size, range);

        estimated_size += range;
        i = end;
    }

    buffer_size = std::min(buffer_size, max_read_buffer_size);

    LOG_TRACE(log,
              "file size: " << data_file_size << ", estimated read size: " << estimated_size << ", buffer_size: " << buffer_size
                            << " (aio_threshold: " << aio_threshold << ", max_read_buffer_size: " << max_read_buffer_size << ")");

    for (size_t i = 0; i < packs; ++i)
    {
        if (reader.use_packs[i])
        {
            auto start = (*marks)[i].offset_in_compressed_file;
            auto end   = (i == packs - 1) ? data_file_size : (*marks)[i + 1].offset_in_compressed_file;
            estimated_size += end - start;
        }
    }

    buf = std::make_unique<CompressedReadBufferFromFile>(data_path, estimated_size, aio_threshold, buffer_size);
}

DMFileReader::DMFileReader(bool                  enable_clean_read_,
                           UInt64                max_data_version_,
                           const DMFilePtr &     dmfile_,
                           const ColumnDefines & read_columns_,
                           const HandleRange &   handle_range_,
                           const RSOperatorPtr & filter_,
                           const IdSetPtr &      read_packs_,
                           MarkCache *           mark_cache_,
                           MinMaxIndexCache *    index_cache_,
                           UInt64                hash_salt_,
                           size_t                aio_threshold,
                           size_t                max_read_buffer_size,
                           size_t                rows_threshold_per_read_)
    : enable_clean_read(enable_clean_read_),
      max_data_version(max_data_version_),
      dmfile(dmfile_),
      read_columns(read_columns_),
      handle_range(handle_range_),
      mark_cache(mark_cache_),
      hash_salt(hash_salt_),
      rows_threshold_per_read(rows_threshold_per_read_),
      pack_filter(dmfile_, index_cache_, hash_salt_, handle_range_, filter_, read_packs_),
      handle_res(pack_filter.getHandleRes()),
      use_packs(pack_filter.getUsePacks()),
      skip_packs_by_column(read_columns.size(), 0),
      log(&Logger::get("DMFileReader"))
{
    if (dmfile->getStatus() != DMFile::Status::READABLE)
        throw Exception("DMFile [" + DB::toString(dmfile->fileId())
                        + "] is expected to be in READABLE status, but: " + DMFile::statusString(dmfile->getStatus()));

    for (auto & cd : read_columns)
    {
        auto callback = [&](const IDataType::SubstreamPath & substream) {
            String stream_name = DMFile::getFileNameBase(cd.id, substream);
            auto   stream      = std::make_unique<Stream>(*this, //
                                                   cd.id,
                                                   stream_name,
                                                   aio_threshold,
                                                   max_read_buffer_size,
                                                   log);
            column_streams.emplace(stream_name, std::move(stream));
        };
        cd.type->enumerateStreams(callback, {});
    }
}

bool DMFileReader::shouldSeek(size_t pack_id)
{
    // If current pack is the first one, or we just finished reading the last pack, then no need to seek.
    return pack_id != 0 && !use_packs[pack_id - 1];
}

bool DMFileReader::getSkippedRows(size_t & skip_rows)
{
    skip_rows = 0;
    for (; next_pack_id < use_packs.size() && !use_packs[next_pack_id]; ++next_pack_id)
    {
        skip_rows += dmfile->getPackStat(next_pack_id).rows;
    }
    return next_pack_id < use_packs.size();
}

bool isExtraColumn(const ColumnDefine & cd)
{
    return cd.id == EXTRA_HANDLE_COLUMN_ID || cd.id == VERSION_COLUMN_ID || cd.id == TAG_COLUMN_ID;
}

Block DMFileReader::read()
{
    // Go to next available pack.
    size_t skip_rows;
    getSkippedRows(skip_rows);

    if (next_pack_id >= use_packs.size())
        return {};

    // Find max continuing rows we can read.
    size_t start_pack_id = next_pack_id;

    auto & pack_stats    = dmfile->getPackStats();
    size_t read_rows      = 0;
    size_t not_clean_rows = 0;

    RSResult expected_handle_res = handle_res[next_pack_id];
    for (; next_pack_id < use_packs.size() && use_packs[next_pack_id] && read_rows < rows_threshold_per_read; ++next_pack_id)
    {
        if (enable_clean_read && handle_res[next_pack_id] != expected_handle_res)
            break;

        read_rows += pack_stats[next_pack_id].rows;
        not_clean_rows += pack_stats[next_pack_id].not_clean;
    }

    if (!read_rows)
        return {};

    Block res;

    size_t read_packs = next_pack_id - start_pack_id;

    // TODO: this will need better algorithm: we should separate those packs which can and can not do clean read.
    bool do_clean_read = enable_clean_read && expected_handle_res == All && !not_clean_rows;
    if (do_clean_read)
    {
        UInt64 max_version = 0;
        for (size_t pack_id = start_pack_id; pack_id < next_pack_id; ++pack_id)
            max_version = std::max(pack_filter.getMaxVersion(pack_id), max_version);
        do_clean_read = max_version <= max_data_version;
    }

    for (size_t i = 0; i < read_columns.size(); ++i)
    {
        auto & cd = read_columns[i];
        if (do_clean_read && isExtraColumn(cd))
        {
            ColumnPtr column;
            if (cd.id == EXTRA_HANDLE_COLUMN_ID)
            {
                // Return the first row's handle
                Handle min_handle = pack_filter.getMinHandle(start_pack_id);
                column            = cd.type->createColumnConst(read_rows, Field(min_handle));
            }
            else if (cd.id == VERSION_COLUMN_ID)
            {
                column = cd.type->createColumnConst(read_rows, Field(pack_stats[start_pack_id].first_version));
            }
            else if (cd.id == TAG_COLUMN_ID)
            {
                column = cd.type->createColumnConst(read_rows, Field((UInt64)(pack_stats[start_pack_id].first_tag)));
            }

            res.insert(ColumnWithTypeAndName{column, cd.type, cd.name, cd.id});

            skip_packs_by_column[i] = read_packs;
        }
        else
        {
            String stream_name = DMFile::getFileNameBase(cd.id);
            auto & stream      = column_streams.at(stream_name);
            if (shouldSeek(start_pack_id) || skip_packs_by_column[i] > 0)
            {
                auto & mark = (*stream->marks)[start_pack_id];
                stream->buf->seek(mark.offset_in_compressed_file, mark.offset_in_decompressed_block);
            }

            auto column = cd.type->createColumn();
            cd.type->deserializeBinaryBulkWithMultipleStreams(*column, //
                                                              [&](const IDataType::SubstreamPath & substream) {
                                                                  String name   = DMFile::getFileNameBase(cd.id, substream);
                                                                  auto & stream = column_streams.at(name);
                                                                  return stream->buf.get();
                                                              },
                                                              read_rows,
                                                              stream->avg_size_hint,
                                                              true,
                                                              {});
            IDataType::updateAvgValueSizeHint(*column, stream->avg_size_hint);

            res.insert(ColumnWithTypeAndName{std::move(column), cd.type, cd.name, cd.id});

            skip_packs_by_column[i] = 0;
        }
    }

    return res;
}

} // namespace DM
} // namespace DB
