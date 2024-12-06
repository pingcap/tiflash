// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Columns/countBytesInFilter.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/Stopwatch.h>
#include <Common/escapeForFileName.h>
#include <DataTypes/IDataType.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/File/ColumnCacheLongTerm.h>
#include <Storages/DeltaMerge/File/DMFileReader.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/DeltaMerge/convertColumnTypeHelpers.h>
#include <Storages/KVStore/Types.h>
#include <common/logger_useful.h>
#include <fmt/format.h>

#include <ranges>


namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace DB::ErrorCodes

namespace DB::DM
{

DMFileReader::DMFileReader(
    const DMFilePtr & dmfile_,
    const ColumnDefines & read_columns_,
    bool is_common_handle_,
    // clean read
    bool enable_handle_clean_read_,
    bool enable_del_clean_read_,
    bool is_fast_scan_,
    UInt64 max_read_version_,
    // filters
    DMFilePackFilter && pack_filter_,
    // caches
    const MarkCachePtr & mark_cache_,
    bool enable_column_cache_,
    const ColumnCachePtr & column_cache_,
    size_t max_read_buffer_size,
    const FileProviderPtr & file_provider_,
    const ReadLimiterPtr & read_limiter,
    size_t rows_threshold_per_read_,
    bool read_one_pack_every_time_,
    const String & tracing_id_,
    size_t max_sharing_column_bytes_,
    const ScanContextPtr & scan_context_,
    const ReadTag read_tag_)
    : dmfile(dmfile_)
    , read_columns(read_columns_)
    , is_common_handle(is_common_handle_)
    , read_one_pack_every_time(read_one_pack_every_time_)
    , enable_handle_clean_read(enable_handle_clean_read_)
    , enable_del_clean_read(enable_del_clean_read_)
    , is_fast_scan(is_fast_scan_)
    , enable_column_cache(enable_column_cache_ && column_cache_)
    , max_read_version(max_read_version_)
    , pack_filter(std::move(pack_filter_))
    , mark_cache(mark_cache_)
    , column_cache(column_cache_)
    , scan_context(scan_context_)
    , read_tag(read_tag_)
    , rows_threshold_per_read(rows_threshold_per_read_)
    , max_sharing_column_bytes(max_sharing_column_bytes_)
    , file_provider(file_provider_)
    , log(Logger::get(tracing_id_))
{
    for (const auto & cd : read_columns)
    {
        // New inserted column, will be filled with default value later
        if (!dmfile->isColumnExist(cd.id))
            continue;

        // Load stream for existing columns according to DataType in disk
        auto callback = [&](const IDataType::SubstreamPath & substream) {
            const auto stream_name = DMFile::getFileNameBase(cd.id, substream);
            auto stream = std::make_unique<ColumnReadStream>( //
                *this,
                cd.id,
                stream_name,
                max_read_buffer_size,
                log,
                read_limiter);
            column_streams.emplace(stream_name, std::move(stream));
        };
        const auto data_type = dmfile->getColumnStat(cd.id).type;
        data_type->enumerateStreams(callback, {});
    }
    if (max_sharing_column_bytes > 0)
    {
        data_sharing_col_data_cache = std::make_unique<ColumnCache>(ColumnCacheType::DataSharingCache);
    }

    // Initialize pack_id_to_offset
    {
        const auto & pack_stats = dmfile->getPackStats();
        {
            size_t offset = 0;
            for (size_t i = 0; i < pack_stats.size(); ++i)
            {
                pack_id_to_offset[i] = offset;
                offset += pack_stats[i].rows;
            }
        }
    }
    // Initialize read_block_infos
    initReadBlockInfos();
}

bool DMFileReader::getSkippedRows(size_t & skip_rows)
{
    skip_rows = 0;
    const auto & pack_stats = dmfile->getPackStats();
    const auto end_pack_id = read_block_infos.empty() ? pack_stats.size() : std::get<0>(read_block_infos.front());
    for (; next_pack_id < end_pack_id; ++next_pack_id)
        skip_rows += pack_stats[next_pack_id].rows;
    addSkippedRows(skip_rows);

    return !read_block_infos.empty();
}

// Skip the block which should be returned by next read()
size_t DMFileReader::skipNextBlock()
{
    if (size_t skip_rows = 0; !getSkippedRows(skip_rows))
        return 0;

    const auto [start_pack_id, pack_count, rs_result, read_rows] = read_block_infos.front();
    read_block_infos.pop_front();
    next_pack_id = start_pack_id + pack_count;
    addSkippedRows(read_rows);
    return read_rows;
}

Block DMFileReader::readWithFilter(const IColumn::Filter & filter)
{
    // do not getSkippedRows here, record the skipped rows in read().
    // if (size_t skip_rows = 0; !getSkippedRows(skip_rows))
    //     return {};

    /// 1. Get start_pack_id, rs_result, read_rows

    if (read_block_infos.empty())
        return {};

    const auto [start_pack_id, pack_count, rs_result, read_rows] = read_block_infos.front();
    read_block_infos.pop_front();
    // do not update next_pack_id here, it will be updated in read().
    // next_pack_id = start_pack_id + pack_count;
    const size_t start_row_offset = pack_id_to_offset[start_pack_id];
    RUNTIME_CHECK(read_rows == filter.size(), read_rows, filter.size());

    /// 2. update read_block_infos

    const auto num_read = updateReadBlockInfos(filter, start_pack_id, start_pack_id + pack_count);

    /// 3. Read and filter packs

    MutableColumns columns;
    columns.reserve(read_columns.size());
    size_t total_passed_count = countBytesInFilter(filter);
    for (const auto & cd : read_columns)
    {
        auto col = cd.type->createColumn();
        col->reserve(total_passed_count);
        columns.emplace_back(std::move(col));
    }

    for (size_t i = 0; i < num_read; ++i)
    {
        RUNTIME_CHECK(!read_block_infos.empty());

        const auto [new_start_pack_id, new_pack_count, new_rs_result, new_read_rows] = read_block_infos.front();
        const size_t new_start_row_offset = pack_id_to_offset[new_start_pack_id];

        IColumn::Filter block_filter{
            filter.cbegin() + new_start_row_offset - start_row_offset,
            filter.cbegin() + new_start_row_offset - start_row_offset + new_read_rows,
        };

        Block block = read(&block_filter);
        size_t passed_count = countBytesInFilter(block_filter);

        for (size_t i = 0; i < block.columns(); ++i)
        {
            auto column = block.getByPosition(i).column;
            // Some columns may only deserialize the passed rows.
            if (column->size() != passed_count)
                column = column->filter(block_filter, passed_count);
            columns[i]->insertRangeFrom(*column, 0, passed_count);
        }
    }

    Block res = getHeader().cloneWithColumns(std::move(columns));
    res.setStartOffset(start_row_offset);
    res.setRSResult(rs_result);
    return res;
}

bool DMFileReader::isCacheableColumn(const ColumnDefine & cd)
{
    return cd.id == EXTRA_HANDLE_COLUMN_ID || cd.id == VERSION_COLUMN_ID;
}

Block DMFileReader::read(const IColumn::Filter * filter)
{
    Stopwatch watch;
    SCOPE_EXIT(scan_context->total_dmfile_read_time_ns += watch.elapsed(););

    /// 1. Find the rows can be read.

    if (size_t skip_rows = 0; !getSkippedRows(skip_rows))
        return {};

    const auto [start_pack_id, pack_count, rs_result, read_rows] = read_block_infos.front();
    read_block_infos.pop_front();
    next_pack_id = start_pack_id + pack_count;
    const size_t start_row_offset = pack_id_to_offset[start_pack_id];
    addScannedRows(read_rows);

    /// 2. Find packs can do clean read.

    const auto & pack_stats = dmfile->getPackStats();
    const auto & pack_properties = dmfile->getPackProperties();
    const auto & handle_res = pack_filter.getHandleRes(); // alias of handle_res in pack_filter
    std::vector<size_t> handle_column_clean_read_packs;
    std::vector<size_t> del_column_clean_read_packs;
    std::vector<size_t> version_column_clean_read_packs;
    if (is_fast_scan)
    {
        if (enable_del_clean_read)
        {
            del_column_clean_read_packs.reserve(pack_count);
            for (size_t i = start_pack_id; i < next_pack_id; ++i)
            {
                // If delete rows is 0, we do not need to read del column.
                if (static_cast<size_t>(pack_properties.property_size()) > i
                    && pack_properties.property(i).has_deleted_rows()
                    && (pack_properties.property(i).deleted_rows() == 0
                        || pack_properties.property(i).deleted_rows() == pack_stats[i].rows))
                {
                    del_column_clean_read_packs.push_back(i);
                }
            }
        }
        if (enable_handle_clean_read)
        {
            handle_column_clean_read_packs.reserve(pack_count);
            for (size_t i = start_pack_id; i < next_pack_id; ++i)
            {
                // If all handle in a pack are in the given range, and del column do clean read, we do not need to read handle column.
                if (handle_res[i] == RSResult::All
                    && std::find(del_column_clean_read_packs.cbegin(), del_column_clean_read_packs.cend(), i)
                        != del_column_clean_read_packs.cend())
                {
                    handle_column_clean_read_packs.push_back(i);
                }
                // If all handle in a pack are in the given range, but disable del clean read, we do not need to read handle column.
                else if (!enable_del_clean_read && handle_res[i] == RSResult::All)
                {
                    handle_column_clean_read_packs.push_back(i);
                }
            }
        }
    }
    else if (enable_handle_clean_read)
    {
        handle_column_clean_read_packs.reserve(pack_count);
        version_column_clean_read_packs.reserve(pack_count);
        del_column_clean_read_packs.reserve(pack_count);
        for (size_t i = start_pack_id; i < next_pack_id; ++i)
        {
            // If all handle in a pack are in the given range, no not_clean rows, and max version <= max_read_version,
            // we do not need to read handle column.
            if (handle_res[i] == RSResult::All && pack_stats[i].not_clean == 0
                && pack_filter.getMaxVersion(i) <= max_read_version)
            {
                handle_column_clean_read_packs.push_back(i);
                version_column_clean_read_packs.push_back(i);
                del_column_clean_read_packs.push_back(i);
            }
        }
    }

    /// 3. Read columns.

    ColumnsWithTypeAndName columns;
    columns.reserve(read_columns.size());

    size_t passed_count = filter ? countBytesInFilter(filter->data(), 0, read_rows) : read_rows;
    for (auto & cd : read_columns)
    {
        try
        {
            ColumnPtr col;
            // For handle, tag and version column, we can try to do clean read.
            switch (cd.id)
            {
            case EXTRA_HANDLE_COLUMN_ID:
                col = readExtraColumn(
                    cd,
                    start_pack_id,
                    pack_count,
                    read_rows,
                    handle_column_clean_read_packs,
                    filter,
                    passed_count);
                break;
            case TAG_COLUMN_ID:
                col = readExtraColumn(
                    cd,
                    start_pack_id,
                    pack_count,
                    read_rows,
                    del_column_clean_read_packs,
                    filter,
                    passed_count);
                break;
            case VERSION_COLUMN_ID:
                col = readExtraColumn(
                    cd,
                    start_pack_id,
                    pack_count,
                    read_rows,
                    version_column_clean_read_packs,
                    filter,
                    passed_count);
                break;
            default:
                col = readColumn(cd, start_pack_id, pack_count, read_rows, filter, passed_count);
                break;
            }
            columns.emplace_back(std::move(col), cd.type, cd.name, cd.id);
        }
        catch (DB::Exception & e)
        {
            e.addMessage(
                fmt::format("(while reading from DTFile: {}, column: {}:{})", this->dmfile->path(), cd.id, cd.name));
            e.rethrow();
        }
    }

    Block res(std::move(columns));
    res.setStartOffset(start_row_offset);
    res.setRSResult(rs_result);
    return res;
}

ColumnPtr DMFileReader::cleanRead(
    const ColumnDefine & cd,
    size_t rows_count,
    std::pair<size_t, size_t> range,
    const DMFileMeta::PackStats & pack_stats)
{
    switch (cd.id)
    {
    case EXTRA_HANDLE_COLUMN_ID:
    {
        if (is_common_handle)
        {
            StringRef min_handle = pack_filter.getMinStringHandle(range.first);
            return cd.type->createColumnConst(rows_count, Field(min_handle.data, min_handle.size));
        }
        else
        {
            Handle min_handle = pack_filter.getMinHandle(range.first);
            return cd.type->createColumnConst(rows_count, Field(min_handle));
        }
    }
    case TAG_COLUMN_ID:
    {
        return cd.type->createColumnConst(rows_count, Field(static_cast<UInt64>(pack_stats[range.first].first_tag)));
    }
    case VERSION_COLUMN_ID:
    {
        return cd.type->createColumnConst(
            rows_count,
            Field(static_cast<UInt64>(pack_stats[range.first].first_version)));
    }
    default:
    {
        // This should not happen
        throw Exception("Unknown column id", ErrorCodes::LOGICAL_ERROR);
    }
    }
}

/**
  * Read the hidden column (handle, tag, version).
  */
ColumnPtr DMFileReader::readExtraColumn(
    const ColumnDefine & cd,
    size_t start_pack_id,
    size_t pack_count,
    size_t read_rows,
    const std::vector<size_t> & clean_read_packs,
    const IColumn::Filter * filter,
    size_t passed_count)
{
    assert(cd.id == EXTRA_HANDLE_COLUMN_ID || cd.id == TAG_COLUMN_ID || cd.id == VERSION_COLUMN_ID);

    const auto & pack_stats = dmfile->getPackStats();
    auto read_strategy = ColumnCache::getCleanReadStrategy(start_pack_id, pack_count, clean_read_packs);
    if (read_strategy.size() != 1 && cd.id == EXTRA_HANDLE_COLUMN_ID)
    {
        // If size of read_strategy is not 1, handle can not do clean read.
        read_strategy.clear();
        read_strategy.emplace_back(
            std::make_pair(start_pack_id, start_pack_id + pack_count),
            ColumnCache::Strategy::Disk);
    }
    auto column = cd.type->createColumn();
    column->reserve(read_rows);
    for (const auto & [range, strategy] : read_strategy)
    {
        size_t rows_count = 0;
        for (size_t cursor = range.first; cursor < range.second; cursor++)
        {
            rows_count += pack_stats[cursor].rows;
        }
        // TODO: this create a temp `src_col` then copy the data into `column`.
        //       we can try to elimiate the copying
        ColumnPtr src_col;
        switch (strategy)
        {
        case ColumnCache::Strategy::Memory:
        {
            src_col = cleanRead(cd, rows_count, range, pack_stats);
            break;
        }
        case ColumnCache::Strategy::Disk:
        {
            src_col = readColumn(cd, range.first, range.second - range.first, rows_count, nullptr, rows_count);
            break;
        }
        default:
            throw Exception("Unknown strategy", ErrorCodes::LOGICAL_ERROR);
        }
        if (read_strategy.size() == 1)
            return src_col;
        if (src_col->isColumnConst())
        {
            // The src_col get from `cleanRead` may be a ColumnConst, fill the `column`
            // with the value of ColumnConst
            auto v = (*src_col)[0];
            column->insertMany(v, src_col->size());
        }
        else
        {
            column->insertRangeFrom(*src_col, 0, src_col->size());
        }
    }

    if (filter)
        return column->filter(*filter, passed_count);
    return column;
}

ColumnPtr DMFileReader::readColumn(
    const ColumnDefine & cd,
    size_t start_pack_id,
    size_t pack_count,
    size_t read_rows,
    const IColumn::Filter * filter,
    size_t passed_count)
{
    // New column after ddl is not exist in this DMFile, fill with default value
    if (!column_streams.contains(DMFile::getFileNameBase(cd.id)))
        return createColumnWithDefaultValue(cd, read_rows);

    auto type_on_disk = dmfile->getColumnStat(cd.id).type;
    ColumnPtr column;
    // try read PK column from ColumnCacheLongTerm
    if (column_cache_long_term && cd.id == pk_col_id && ColumnCacheLongTerm::isCacheableColumn(cd))
    {
        // ColumnCacheLongTerm only caches user assigned PrimaryKey column.
        auto column_all_data
            = column_cache_long_term->get(dmfile->parentPath(), dmfile->fileId(), cd.id, [&]() -> IColumn::Ptr {
                  // Always read all packs when filling cache, and always set filter to nullptr
                  auto rows = dmfile->getRows();
                  return readFromDiskOrSharingCache(cd, type_on_disk, 0, dmfile->getPacks(), rows, nullptr, rows);
              });

        auto mut_column = type_on_disk->createColumn();
        mut_column->insertRangeFrom(*column_all_data, pack_id_to_offset[start_pack_id], read_rows);
        column = std::move(mut_column);
    }
    // Not cached
    else if (!enable_column_cache || !isCacheableColumn(cd))
    {
        column
            = readFromDiskOrSharingCache(cd, type_on_disk, start_pack_id, pack_count, read_rows, filter, passed_count);
        return convertColumnByColumnDefineIfNeed(type_on_disk, std::move(column), cd);
    }
    // enable_column_cache && isCacheableColumn(cd)
    else
    {
        // try to get column from cache
        column = getColumnFromCache(
            column_cache,
            cd,
            type_on_disk,
            start_pack_id,
            pack_count,
            read_rows,
            [&](const ColumnDefine & cd,
                const DataTypePtr & type_on_disk,
                size_t start_pack_id,
                size_t pack_count,
                size_t read_rows) {
                // read from cache, always set filter to nullptr
                return readFromDiskOrSharingCache(
                    cd,
                    type_on_disk,
                    start_pack_id,
                    pack_count,
                    read_rows,
                    nullptr,
                    read_rows);
            });
        // add column to cache
        addColumnToCache(column_cache, cd.id, start_pack_id, pack_count, column);
    }
    // filter column
    if (filter)
        column = column->filter(*filter, passed_count);
    // Cast column's data from DataType in disk to what we need now
    return convertColumnByColumnDefineIfNeed(type_on_disk, std::move(column), cd);
}

ColumnPtr DMFileReader::readFromDisk(
    const ColumnDefine & cd,
    const DataTypePtr & type_on_disk,
    size_t start_pack_id,
    size_t read_rows,
    const IColumn::Filter * filter)
{
    const auto stream_name = DMFile::getFileNameBase(cd.id);
    auto iter = column_streams.find(stream_name);
#ifndef NDEBUG
    RUNTIME_CHECK_MSG(
        iter != column_streams.end(),
        "Can not find column_stream, column_id={} stream_name={}",
        cd.id,
        stream_name);
#endif
    auto & top_stream = iter->second;
    auto mutable_col = type_on_disk->createColumn();
    type_on_disk->deserializeBinaryBulkWithMultipleStreams( //
        *mutable_col,
        [&](const IDataType::SubstreamPath & substream_path) {
            const auto substream_name = DMFile::getFileNameBase(cd.id, substream_path);
            auto & sub_stream = column_streams.at(substream_name);
            sub_stream->buf->seek(
                sub_stream->getOffsetInFile(start_pack_id),
                sub_stream->getOffsetInDecompressedBlock(start_pack_id));
            return sub_stream->buf.get();
        },
        read_rows,
        top_stream->avg_size_hint,
        true,
        {},
        filter);
    IDataType::updateAvgValueSizeHint(*mutable_col, top_stream->avg_size_hint);
    return mutable_col;
}

ColumnPtr DMFileReader::readFromDiskOrSharingCache(
    const ColumnDefine & cd,
    const DataTypePtr & type_on_disk,
    size_t start_pack_id,
    size_t pack_count,
    size_t read_rows,
    const IColumn::Filter * filter,
    size_t passed_count)
{
    bool has_concurrent_reader = DMFileReaderPool::instance().hasConcurrentReader(*this);
    bool reach_sharing_column_memory_limit = shared_column_data_mem_tracker != nullptr
        && std::cmp_greater_equal(shared_column_data_mem_tracker->get(), max_sharing_column_bytes);
    if (reach_sharing_column_memory_limit)
    {
        GET_METRIC(tiflash_storage_read_thread_counter, type_add_cache_total_bytes_limit).Increment();
    }
    bool enable_sharing_column = has_concurrent_reader && !reach_sharing_column_memory_limit;
    if (enable_sharing_column)
    {
        auto column = getColumnFromCache(
            data_sharing_col_data_cache,
            cd,
            type_on_disk,
            start_pack_id,
            pack_count,
            read_rows,
            [&](const ColumnDefine & cd,
                const DataTypePtr & type_on_disk,
                size_t start_pack_id,
                size_t /*pack_count*/,
                size_t read_rows) {
                // If there are concurrent read requests, this data is likely to be shared.
                // So the allocation and deallocation of this data may not be in the same MemoryTracker.
                // This can lead to inaccurate memory statistics of MemoryTracker.
                // To solve this problem, we use a independent global memory tracker to trace the shared column data in the data_sharing_col_data_cache.
                MemoryTrackerSetter mem_tracker_guard(true, nullptr);
                // If there are concurrent read requests, we should read the column from disk and share it, always set filter to nullptr.
                return readFromDisk(cd, type_on_disk, start_pack_id, read_rows, nullptr);
            });
        // Set the column to DMFileReaderPool to share the column data.
        DMFileReaderPool::instance().set(*this, cd.id, start_pack_id, pack_count, column);
        // Delete column from local cache since it is not used anymore.
        data_sharing_col_data_cache->delColumn(cd.id, std::get<0>(read_block_infos.front()));
        if (filter)
            column = column->filter(*filter, passed_count);
        return column;
    }

    return readFromDisk(cd, type_on_disk, start_pack_id, read_rows, filter);
}

void DMFileReader::addColumnToCache(
    const ColumnCachePtr & data_cache,
    ColId col_id,
    size_t start_pack_id,
    size_t pack_count,
    ColumnPtr & col)
{
    if (data_cache == nullptr)
        return;

    const auto & pack_stats = dmfile->getPackStats();
    size_t rows_offset = 0;
    for (size_t cursor = start_pack_id; cursor < start_pack_id + pack_count; ++cursor)
    {
        data_cache->tryPutColumn(cursor, col_id, col, rows_offset, pack_stats[cursor].rows);
        rows_offset += pack_stats[cursor].rows;
    }
}

ColumnPtr DMFileReader::getColumnFromCache(
    const ColumnCachePtr & data_cache,
    const ColumnDefine & cd,
    const DataTypePtr & type_on_disk,
    size_t start_pack_id,
    size_t pack_count,
    size_t read_rows,
    // column_define, type_on_disk, start_pack_id, pack_count, read_rows
    std::function<ColumnPtr(const ColumnDefine &, const DataTypePtr &, size_t, size_t, size_t)> on_cache_miss)
{
    RUNTIME_CHECK(data_cache != nullptr);

    const auto col_id = cd.id;
    auto read_strategy = data_cache->getReadStrategy(start_pack_id, pack_count, col_id);
    const auto & pack_stats = dmfile->getPackStats();
    auto mutable_col = type_on_disk->createColumn();
    mutable_col->reserve(read_rows);
    for (auto & [range, strategy] : read_strategy)
    {
        if (strategy == ColumnCache::Strategy::Memory)
        {
            for (size_t cursor = range.first; cursor < range.second; ++cursor)
            {
                auto cache_element = data_cache->getColumn(cursor, col_id);
                mutable_col->insertRangeFrom(
                    *(cache_element.first),
                    cache_element.second.first,
                    cache_element.second.second);
            }
        }
        else if (strategy == ColumnCache::Strategy::Disk)
        {
            size_t rows_count = 0;
            for (size_t cursor = range.first; cursor < range.second; cursor++)
            {
                rows_count += pack_stats[cursor].rows;
            }
            auto sub_col = on_cache_miss(cd, type_on_disk, range.first, range.second - range.first, rows_count);
            mutable_col->insertRangeFrom(*sub_col, 0, sub_col->size());
        }
        else
        {
            throw Exception("Unknown strategy", ErrorCodes::LOGICAL_ERROR);
        }
    }
    return mutable_col;
}

void DMFileReader::addScannedRows(UInt64 rows)
{
    switch (read_tag)
    {
    case ReadTag::Query:
        scan_context->dmfile_data_scanned_rows += rows;
        break;
    case ReadTag::MVCC:
        scan_context->dmfile_mvcc_scanned_rows += rows;
        break;
    case ReadTag::LMFilter:
        scan_context->dmfile_lm_filter_scanned_rows += rows;
        break;
    default:
        break;
    }
}

void DMFileReader::addSkippedRows(UInt64 rows)
{
    switch (read_tag)
    {
    case ReadTag::Query:
        scan_context->dmfile_data_skipped_rows += rows;
        break;
    case ReadTag::MVCC:
        scan_context->dmfile_mvcc_skipped_rows += rows;
        break;
    case ReadTag::LMFilter:
        scan_context->dmfile_lm_filter_skipped_rows += rows;
        break;
    default:
        break;
    }
}

void DMFileReader::initReadBlockInfos()
{
    const auto & pack_res = pack_filter.getPackResConst();
    const auto & pack_stats = dmfile->getPackStats();

    const size_t read_pack_limit = read_one_pack_every_time ? 1 : std::numeric_limits<size_t>::max();
    size_t start_pack_id = 0;
    size_t read_rows = 0;
    auto last_pack_res = RSResult::All;
    for (size_t pack_id = 0; pack_id < pack_res.size(); ++pack_id)
    {
        bool is_use = pack_res[pack_id].isUse();
        bool reach_limit = pack_id - start_pack_id >= read_pack_limit || read_rows >= rows_threshold_per_read;
        bool break_all_match = !pack_res[pack_id].allMatch() && read_rows >= rows_threshold_per_read / 2;

        if (!is_use)
        {
            if (read_rows > 0)
                read_block_infos.emplace_back(start_pack_id, pack_id - start_pack_id, last_pack_res, read_rows);
            start_pack_id = pack_id + 1;
            read_rows = 0;
            last_pack_res = RSResult::All;
        }
        else if (reach_limit || break_all_match)
        {
            if (read_rows > 0)
                read_block_infos.emplace_back(start_pack_id, pack_id - start_pack_id, last_pack_res, read_rows);
            start_pack_id = pack_id;
            read_rows = pack_stats[pack_id].rows;
            last_pack_res = pack_res[pack_id];
        }
        else
        {
            last_pack_res = last_pack_res && pack_res[pack_id];
            read_rows += pack_stats[pack_id].rows;
        }
    }
    if (read_rows > 0)
        read_block_infos.emplace_back(start_pack_id, pack_res.size() - start_pack_id, last_pack_res, read_rows);
}

size_t DMFileReader::updateReadBlockInfos(const IColumn::Filter & filter, size_t pack_start, size_t pack_end)
{
    const auto & pack_stats = dmfile->getPackStats();
    std::vector<std::tuple<size_t, size_t, RSResult, size_t>> new_read_block_infos;
    new_read_block_infos.reserve(pack_end - pack_start);
    size_t start_pack_id = pack_start;
    const size_t start_row_offset = pack_id_to_offset[pack_start];
    size_t read_rows = 0;
    for (size_t pack_id = pack_start; pack_id < pack_end; ++pack_id)
    {
        if (countBytesInFilter(filter, pack_id_to_offset[pack_id] - start_row_offset, pack_stats[pack_id].rows) == 0)
        {
            if (read_rows > 0)
                // rs_result is not important here, we can use RSResult::All
                new_read_block_infos.emplace_back(start_pack_id, pack_id - start_pack_id, RSResult::All, read_rows);
            start_pack_id = pack_id + 1;
            read_rows = 0;
        }
        else
        {
            read_rows += pack_stats[pack_id].rows;
        }
    }
    if (read_rows > 0)
        new_read_block_infos.emplace_back(start_pack_id, pack_end - start_pack_id, RSResult::All, read_rows);
    size_t size = new_read_block_infos.size();
    for (auto & new_read_block_info : std::ranges::reverse_view(new_read_block_infos))
        read_block_infos.emplace_front(std::move(new_read_block_info));
    return size;
}

} // namespace DB::DM
