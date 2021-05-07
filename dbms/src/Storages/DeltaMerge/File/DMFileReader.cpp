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
    : single_file_mode(reader.single_file_mode), avg_size_hint(reader.dmfile->getColumnStat(col_id).avg_size)
{
    // load mark data
    if (reader.single_file_mode)
    {
        auto mark_with_size_load = [&]() -> MarkWithSizesInCompressedFilePtr {
            auto res = std::make_shared<MarkWithSizesInCompressedFile>(reader.dmfile->getPacks());
            if (res->empty()) // 0 rows.
                return res;
            size_t size        = sizeof(MarkWithSizeInCompressedFile) * reader.dmfile->getPacks();
            auto   file        = reader.file_provider->newRandomAccessFile(reader.dmfile->colMarkPath(file_name_base),
                                                                  reader.dmfile->encryptionMarkPath(file_name_base));
            auto   mark_size   = reader.dmfile->colMarkSize(file_name_base);
            auto   mark_offset = reader.dmfile->colMarkOffset(file_name_base);
            if (unlikely(mark_size != size))
            {
                throw DB::TiFlashException("Bad DMFile format, expected mark file content size: " + std::to_string(size)
                                               + " vs. actual: " + std::to_string(mark_size),
                                           Errors::DeltaTree::Internal);
            }
            PageUtil::readFile(file, mark_offset, reinterpret_cast<char *>(res->data()), size);

            return res;
        };
        mark_with_sizes = mark_with_size_load();
    }
    else
    {
        auto mark_load = [&]() -> MarksInCompressedFilePtr {
            auto res = std::make_shared<MarksInCompressedFile>(reader.dmfile->getPacks());
            if (res->empty()) // 0 rows.
                return res;
            size_t size = sizeof(MarkInCompressedFile) * reader.dmfile->getPacks();
            auto   file = reader.file_provider->newRandomAccessFile(reader.dmfile->colMarkPath(file_name_base),
                                                                  reader.dmfile->encryptionMarkPath(file_name_base));
            PageUtil::readFile(file, 0, reinterpret_cast<char *>(res->data()), size);

            return res;
        };
        if (reader.mark_cache)
            marks
                = reader.mark_cache->getOrSet(reader.dmfile->colMarkCacheKey(file_name_base), mark_load);
        else
            marks = mark_load();
    }

    const String data_path      = reader.dmfile->colDataPath(file_name_base);
    size_t       data_file_size = reader.dmfile->colDataSize(file_name_base);
    size_t       packs          = reader.dmfile->getPacks();
    size_t       buffer_size    = 0;
    size_t       estimated_size = 0;

    if (reader.single_file_mode)
    {
        for (size_t i = 0; i < packs; i++)
        {
            if (!reader.use_packs[i])
            {
                continue;
            }
            buffer_size = std::max(buffer_size, (*mark_with_sizes)[i].mark_size);
            estimated_size += (*mark_with_sizes)[i].mark_size;
        }
    }
    else
    {
        for (size_t i = 0; i < packs;)
        {
            if (!reader.use_packs[i])
            {
                ++i;
                continue;
            }
            size_t cur_offset_in_file = getOffsetInFile(i);
            size_t end                = i + 1;
            // First find the end of current available range.
            while (end < packs && reader.use_packs[end])
                ++end;

            // Second If the end of range is inside the block, we will need to read it too.
            if (end < packs)
            {
                size_t last_offset_in_file = getOffsetInFile(end);
                if (getOffsetInDecompressedBlock(end) > 0)
                {
                    while (end < packs && getOffsetInFile(end) == last_offset_in_file)
                        ++end;
                }
            }

            size_t range_end_in_file = (end == packs) ? data_file_size : getOffsetInFile(end);

            size_t range = range_end_in_file - cur_offset_in_file;
            buffer_size  = std::max(buffer_size, range);

            estimated_size += range;
            i = end;
        }
    }

    buffer_size = std::min(buffer_size, max_read_buffer_size);

    LOG_TRACE(log,
              "file size: " << data_file_size << ", estimated read size: " << estimated_size << ", buffer_size: " << buffer_size
                            << " (aio_threshold: " << aio_threshold << ", max_read_buffer_size: " << max_read_buffer_size << ")");

    buf = std::make_unique<CompressedReadBufferFromFileProvider>(reader.file_provider,
                                                                 reader.dmfile->colDataPath(file_name_base),
                                                                 reader.dmfile->encryptionDataPath(file_name_base),
                                                                 estimated_size,
                                                                 aio_threshold,
                                                                 buffer_size);
}

DMFileReader::DMFileReader(const DMFilePtr &     dmfile_,
                           const ColumnDefines & read_columns_,
                           // clean read
                           bool   enable_clean_read_,
                           UInt64 max_read_version_,
                           // filters
                           const RowKeyRange &   rowkey_range_,
                           const RSOperatorPtr & filter_,
                           const IdSetPtr &      read_packs_,
                           // caches
                           UInt64                      hash_salt_,
                           const MarkCachePtr &        mark_cache_,
                           const MinMaxIndexCachePtr & index_cache_,
                           bool                        enable_column_cache_,
                           const ColumnCachePtr &      column_cache_,
                           size_t                      aio_threshold,
                           size_t                      max_read_buffer_size,
                           const FileProviderPtr &     file_provider_,
                           size_t                      rows_threshold_per_read_,
                           bool                        read_one_pack_every_time_)
    : dmfile(dmfile_),
      read_columns(read_columns_),
      enable_clean_read(enable_clean_read_),
      max_read_version(max_read_version_),
      pack_filter(dmfile_, index_cache_, hash_salt_, rowkey_range_, filter_, read_packs_, file_provider_),
      handle_res(pack_filter.getHandleRes()),
      use_packs(pack_filter.getUsePacks()),
      is_common_handle(rowkey_range_.is_common_handle),
      skip_packs_by_column(read_columns.size(), 0),
      hash_salt(hash_salt_),
      mark_cache(mark_cache_),
      enable_column_cache(enable_column_cache_ && column_cache_),
      column_cache(column_cache_),
      rows_threshold_per_read(rows_threshold_per_read_),
      file_provider(file_provider_),
      read_one_pack_every_time(read_one_pack_every_time_),
      single_file_mode(dmfile_->isSingleFileMode()),
      log(&Logger::get("DMFileReader"))
{
    if (dmfile->getStatus() != DMFile::Status::READABLE)
        throw Exception("DMFile [" + DB::toString(dmfile->fileId())
                        + "] is expected to be in READABLE status, but: " + DMFile::statusString(dmfile->getStatus()));

    pack_filter.init();

    for (const auto & cd : read_columns)
    {
        // New inserted column, will be filled with default value later
        if (!dmfile->isColumnExist(cd.id))
            continue;

        // Load stream for existing columns according to DataType in disk
        auto callback = [&](const IDataType::SubstreamPath & substream) {
            const auto stream_name = DMFile::getFileNameBase(cd.id, substream);
            auto       stream      = std::make_unique<Stream>( //
                *this,
                cd.id,
                stream_name,
                aio_threshold,
                max_read_buffer_size,
                log);
            column_streams.emplace(stream_name, std::move(stream));
        };
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
    skip_rows               = 0;
    const auto & pack_stats = dmfile->getPackStats();
    for (; next_pack_id < use_packs.size() && !use_packs[next_pack_id]; ++next_pack_id)
    {
        skip_rows += pack_stats[next_pack_id].rows;
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
    // When single_file_mode is true, or read_one_pack_every_time is true, we can just read one pack every time.
    // 0 means no limit
    size_t read_pack_limit = (single_file_mode || read_one_pack_every_time) ? 1 : 0;

    auto & pack_stats     = dmfile->getPackStats();
    size_t read_rows      = 0;
    size_t not_clean_rows = 0;

    RSResult expected_handle_res = handle_res[next_pack_id];
    for (; next_pack_id < use_packs.size() && use_packs[next_pack_id] && read_rows < rows_threshold_per_read; ++next_pack_id)
    {
        if (read_pack_limit != 0 && next_pack_id - start_pack_id >= read_pack_limit)
            break;
        if (enable_clean_read && handle_res[next_pack_id] != expected_handle_res)
            break;

        read_rows += pack_stats[next_pack_id].rows;
        not_clean_rows += pack_stats[next_pack_id].not_clean;
    }

    if (read_rows == 0)
        return {};

    Block res;

    size_t read_packs = next_pack_id - start_pack_id;

    if (single_file_mode && read_packs != 1)
    {
        throw DB::TiFlashException("read_packs must be one when single_file_mode is true.", Errors::DeltaTree::Internal);
    }

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
                    if (is_common_handle)
                    {
                        StringRef min_handle = pack_filter.getMinStringHandle(start_pack_id);
                        column               = cd.type->createColumnConst(read_rows, Field(min_handle.data, min_handle.size));
                    }
                    else
                    {
                        Handle min_handle = pack_filter.getMinHandle(start_pack_id);
                        column            = cd.type->createColumnConst(read_rows, Field(min_handle));
                    }
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
                const auto stream_name = DMFile::getFileNameBase(cd.id);
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
                                readFromDisk(cd, column, range.first, rows_count, skip_packs_by_column[i], single_file_mode);
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
                        readFromDisk(cd, column, start_pack_id, read_rows, skip_packs_by_column[i], single_file_mode);
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
    ColumnDefine & column_define, MutableColumnPtr & column, size_t start_pack_id, size_t read_rows, size_t skip_packs, bool force_seek)
{
    const auto stream_name = DMFile::getFileNameBase(column_define.id);
    if (auto iter = column_streams.find(stream_name); iter != column_streams.end())
    {
        auto & top_stream  = iter->second;
        bool   should_seek = force_seek || shouldSeek(start_pack_id) || skip_packs > 0;

        auto data_type = dmfile->getColumnStat(column_define.id).type;
        data_type->deserializeBinaryBulkWithMultipleStreams( //
            *column,
            [&](const IDataType::SubstreamPath & substream_path) {
                const auto substream_name = DMFile::getFileNameBase(column_define.id, substream_path);
                auto &     sub_stream     = column_streams.at(substream_name);

                if (should_seek)
                {
                    sub_stream->buf->seek(sub_stream->getOffsetInFile(start_pack_id),
                                          sub_stream->getOffsetInDecompressedBlock(start_pack_id));
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
