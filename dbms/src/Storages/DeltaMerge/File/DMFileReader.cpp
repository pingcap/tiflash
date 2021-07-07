#include <Common/CurrentMetrics.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadBufferFromFile.h>
#include <Poco/File.h>
#include <Storages/DeltaMerge/File/DMFileReader.h>
#include <Storages/DeltaMerge/convertColumnTypeHelpers.h>
#include <Storages/Page/PageUtil.h>

namespace CurrentMetrics
{
extern const Metric OpenFileForRead;
}

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
    const String mark_path = reader.dmfile->colMarkPath(file_name_base);
    const String data_path = reader.dmfile->colDataPath(file_name_base);

    auto mark_load = [&]() -> MarksInCompressedFilePtr {
        auto res = std::make_shared<MarksInCompressedFile>(reader.dmfile->getPacks());
        if (res->empty()) // 0 rows.
            return res;
        size_t size = sizeof(MarkInCompressedFile) * reader.dmfile->getPacks();
        auto   file = reader.file_provider->newRandomAccessFile(mark_path, reader.dmfile->encryptionMarkPath(file_name_base));

        PageUtil::readFile(file, 0, reinterpret_cast<char *>(res->data()), size);

        return res;
    };

    if (reader.mark_cache)
        marks = reader.mark_cache->getOrSet(mark_path, mark_load);
    else
        marks = mark_load();

    size_t data_file_size = Poco::File(data_path).getSize();
    size_t packs          = reader.dmfile->getPacks();
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

    buf = std::make_unique<CompressedReadBufferFromFileProvider>(
        reader.file_provider, data_path, reader.dmfile->encryptionDataPath(file_name_base), estimated_size, aio_threshold, buffer_size);
}

DMFileReader::DMFileReader(const DMFilePtr &     dmfile_,
                           const ColumnDefines & read_columns_,
                           // clean read
                           bool   enable_clean_read_,
                           UInt64 max_read_version_,
                           // filters
                           const HandleRange &   handle_range_,
                           const RSOperatorPtr & filter_,
                           const IdSetPtr &      read_packs_,
                           // caches
                           MarkCache *             mark_cache_,
                           MinMaxIndexCache *      index_cache_,
                           bool                    enable_column_cache_,
                           ColumnCachePtr &        column_cache_,
                           size_t                  aio_threshold,
                           size_t                  max_read_buffer_size,
                           const FileProviderPtr & file_provider_,
                           size_t                  rows_threshold_per_read_)
    : dmfile(dmfile_),
      read_columns(read_columns_),
      enable_clean_read(enable_clean_read_),
      max_read_version(max_read_version_),
      pack_filter(dmfile_, index_cache_, handle_range_, filter_, read_packs_, file_provider_),
      handle_res(pack_filter.getHandleRes()),
      use_packs(pack_filter.getUsePacks()),
      skip_packs_by_column(read_columns.size(), 0),
      mark_cache(mark_cache_),
      enable_column_cache(enable_column_cache_),
      column_cache(column_cache_),
      rows_threshold_per_read(rows_threshold_per_read_),
      file_provider(file_provider_),
      log(&Logger::get("DMFileReader"))
{
    if (dmfile->getStatus() != DMFile::Status::READABLE)
        throw Exception("DMFile [" + DB::toString(dmfile->fileId())
                        + "] is expected to be in READABLE status, but: " + DMFile::statusString(dmfile->getStatus()));

    for (const auto & cd : read_columns)
    {
        // New inserted column, fill them with default value later
        if (!dmfile->isColumnExist(cd.id))
            continue;

        auto callback = [&](const IDataType::SubstreamPath & substream) {
            String stream_name = DMFile::getFileNameBase(cd.id, substream);
            auto   stream      = std::make_unique<Stream>( //
                *this,
                cd.id,
                stream_name,
                aio_threshold,
                max_read_buffer_size,
                log);
            column_streams.emplace(stream_name, std::move(stream));
        };

        // Load stream according to DataType in disk
        const auto data_type = dmfile->getColumnStat(cd.id).type;
        data_type->enumerateStreams(callback, {});
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

inline bool isExtraColumn(const ColumnDefine & cd)
{
    return cd.id == EXTRA_HANDLE_COLUMN_ID || cd.id == VERSION_COLUMN_ID || cd.id == TAG_COLUMN_ID;
}

inline bool isCacheableColumn(const ColumnDefine & cd)
{
    return cd.id == EXTRA_HANDLE_COLUMN_ID || cd.id == VERSION_COLUMN_ID;
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

    auto & pack_stats     = dmfile->getPackStats();
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

    if (read_rows == 0)
        return {};

    Block res;

    size_t read_packs = next_pack_id - start_pack_id;

    // TODO: this will need better algorithm: we should separate those packs which can and can not do clean read.
    bool do_clean_read = enable_clean_read && expected_handle_res == All && not_clean_rows == 0;
    if (do_clean_read)
    {
        UInt64 max_version = 0;
        for (size_t pack_id = start_pack_id; pack_id < next_pack_id; ++pack_id)
            max_version = std::max(pack_filter.getMaxVersion(pack_id), max_version);
        do_clean_read = max_version <= max_read_version;
    }

    for (size_t i = 0; i < read_columns.size(); ++i)
    {
        try
        {
            // For clean read of column pk, version, tag, instead of loading data from disk, just create placeholder column is OK.
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
                const String stream_name = DMFile::getFileNameBase(cd.id);
                if (auto iter = column_streams.find(stream_name); iter != column_streams.end())
                {
                    if (enable_column_cache && isCacheableColumn(cd))
                    {
                        auto read_strategy = column_cache->getReadStrategy(start_pack_id, read_packs, cd.id);

                        auto data_type = dmfile->getColumnStat(cd.id).type;
                        auto column    = data_type->createColumn();
                        column->reserve(read_rows);
                        for (auto & [range, strategy] : read_strategy)
                        {
                            if (strategy == ColumnCache::Strategy::Memory)
                            {
                                for (size_t cursor = range.first; cursor < range.second; cursor++)
                                {
                                    auto cache_element = column_cache->getColumn(cursor, cd.id);
                                    column->insertRangeFrom(
                                        *(cache_element.first), cache_element.second.first, cache_element.second.second);
                                }
                                skip_packs_by_column[i] += (range.second - range.first);
                            }
                            else if (strategy == ColumnCache::Strategy::Disk)
                            {
                                size_t rows_count = 0;
                                for (size_t cursor = range.first; cursor < range.second; cursor++)
                                {
                                    rows_count += pack_stats[cursor].rows;
                                }
                                readFromDisk(cd, column, range.first, rows_count, skip_packs_by_column[i]);
                                skip_packs_by_column[i] = 0;
                            }
                            else
                            {
                                throw Exception("Unknown strategy", ErrorCodes::LOGICAL_ERROR);
                            }
                        }
                        ColumnPtr result_column = std::move(column);
                        size_t    rows_offset   = 0;
                        for (size_t cursor = start_pack_id; cursor < start_pack_id + read_packs; cursor++)
                        {
                            column_cache->tryPutColumn(cursor, cd.id, result_column, rows_offset, pack_stats[cursor].rows);
                            rows_offset += pack_stats[cursor].rows;
                        }
                        // Cast column's data from DataType in disk to what we need now
                        auto converted_column = convertColumnByColumnDefineIfNeed(data_type, std::move(result_column), cd);
                        res.insert(ColumnWithTypeAndName{converted_column, cd.type, cd.name, cd.id});
                    }
                    else
                    {
                        auto data_type = dmfile->getColumnStat(cd.id).type;
                        auto column    = data_type->createColumn();
                        readFromDisk(cd, column, start_pack_id, read_rows, skip_packs_by_column[i]);
                        auto converted_column = convertColumnByColumnDefineIfNeed(data_type, std::move(column), cd);
                        res.insert(ColumnWithTypeAndName{std::move(converted_column), cd.type, cd.name, cd.id});
                        skip_packs_by_column[i] = 0;
                    }
                }
                else
                {
                    LOG_TRACE(log,
                              "Column [id:" << cd.id << ",name:" << cd.name << ",type:" << cd.type->getName()
                                            << "] not found, use default value. DMFile: " << dmfile->path());
                    // New column after ddl is not exist in this DMFile, fill with default value
                    ColumnPtr column = createColumnWithDefaultValue(cd, read_rows);
                    res.insert(ColumnWithTypeAndName{std::move(column), cd.type, cd.name, cd.id});
                    skip_packs_by_column[i] = 0;
                }
            }
        }
        catch (DB::Exception & e)
        {
            e.addMessage("(while reading from DTFile: " + this->dmfile->path() + ")");
            e.rethrow();
        }
    }

    return res;
}

void DMFileReader::readFromDisk(
    ColumnDefine & column_define, MutableColumnPtr & column, size_t start_pack_id, size_t read_rows, size_t skip_packs)
{
    const String stream_name = DMFile::getFileNameBase(column_define.id);
    if (auto iter = column_streams.find(stream_name); iter != column_streams.end())
    {
        auto & top_stream  = iter->second;
        bool   should_seek = shouldSeek(start_pack_id) || skip_packs > 0;

        auto data_type = dmfile->getColumnStat(column_define.id).type;
        data_type->deserializeBinaryBulkWithMultipleStreams( //
            *column,
            [&](const IDataType::SubstreamPath & substream_path) {
                String substream_name = DMFile::getFileNameBase(column_define.id, substream_path);
                auto & sub_stream     = column_streams.at(substream_name);

                if (should_seek)
                {
                    auto & mark = (*sub_stream->marks)[start_pack_id];
                    sub_stream->buf->seek(mark.offset_in_compressed_file, mark.offset_in_decompressed_block);
                }

                return sub_stream->buf.get();
            },
            read_rows,
            top_stream->avg_size_hint,
            true,
            {});
        IDataType::updateAvgValueSizeHint(*column, top_stream->avg_size_hint);
    }
}

} // namespace DM
} // namespace DB
