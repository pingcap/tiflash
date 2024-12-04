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

#include <Columns/ColumnsCommon.h>
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
    initAllMatchBlockInfo();
}

bool DMFileReader::getSkippedRows(size_t & skip_rows)
{
    skip_rows = 0;
    const auto & pack_res = pack_filter.getPackResConst();
    const auto & pack_stats = dmfile->getPackStats();
    for (; next_pack_id < pack_res.size() && !pack_res[next_pack_id].isUse(); ++next_pack_id)
    {
        skip_rows += pack_stats[next_pack_id].rows;
        addSkippedRows(pack_stats[next_pack_id].rows);
    }
    next_row_offset += skip_rows;
    // return false if it is the end of stream.
    return next_pack_id < pack_res.size();
}

// Skip the block which should be returned by next read()
size_t DMFileReader::skipNextBlock()
{
    // find the first pack which is used
    if (size_t skip_rows; !getSkippedRows(skip_rows))
    {
        // no block left in the stream
        return 0;
    }

    // move forward next_pack_id and next_row_offset
    const auto [read_rows, rs_result] = getReadRows();
    if (read_rows == 0)
        return 0;

    addSkippedRows(read_rows);
    scan_context->late_materialization_skip_rows += read_rows;
    return read_rows;
}

// Get the number of rows to read in the next block
// Move forward next_pack_id and next_row_offset
std::pair<size_t, RSResult> DMFileReader::getReadRows()
{
    const auto & pack_res = pack_filter.getPackResConst();
    const size_t start_pack_id = next_pack_id;
    const size_t read_pack_limit = getReadPackLimit(start_pack_id);
    const auto & pack_stats = dmfile->getPackStats();
    size_t read_rows = 0;
    auto last_pack_res = RSResult::All;
    for (; next_pack_id < pack_res.size() && pack_res[next_pack_id].isUse() && read_rows < rows_threshold_per_read;
         ++next_pack_id)
    {
        if (next_pack_id - start_pack_id >= read_pack_limit)
            break;
        last_pack_res = last_pack_res && pack_res[next_pack_id];
        read_rows += pack_stats[next_pack_id].rows;
    }

    next_row_offset += read_rows;
    if (read_tag == ReadTag::Query && last_pack_res.allMatch())
        scan_context->rs_dmfile_read_with_all += next_pack_id - start_pack_id;
    return {read_rows, last_pack_res};
}

Block DMFileReader::readWithFilter(const IColumn::Filter & filter)
{
    /// 1. Skip filtered out packs.
    if (size_t skip_rows; !getSkippedRows(skip_rows))
    {
        // no block left in the stream
        return {};
    }

    /// 2. Mark pack_res[i] = None if all rows in the i-th pack are filtered out by filter.

    const auto & pack_stats = dmfile->getPackStats();
    auto & pack_res = pack_filter.getPackRes();

    size_t start_row_offset = next_row_offset;
    size_t start_pack_id = next_pack_id;
    const auto [read_rows, rs_result] = getReadRows();
    RUNTIME_CHECK(read_rows == filter.size(), read_rows, filter.size());
    size_t last_pack_id = next_pack_id;
    {
        size_t offset = 0;
        for (size_t i = start_pack_id; i < last_pack_id; ++i)
        {
            if (countBytesInFilter(filter, offset, pack_stats[i].rows) == 0)
                pack_res[i] = RSResult::None;
            offset += pack_stats[i].rows;
        }
    }

    /// 3. Mark the pack_res[last_pack_id] as None temporarily to avoid reading it and its following packs in this round

    auto next_pack_id_pack_res_cp = RSResult::None;
    if (last_pack_id < pack_res.size())
    {
        next_pack_id_pack_res_cp = pack_res[last_pack_id];
        pack_res[last_pack_id] = RSResult::None;
    }

    /// 4. Read and filter packs

    MutableColumns columns;
    columns.reserve(read_columns.size());
    size_t total_passed_count = countBytesInFilter(filter);
    for (const auto & cd : read_columns)
    {
        auto col = cd.type->createColumn();
        col->reserve(total_passed_count);
        columns.emplace_back(std::move(col));
    }

    size_t offset = 0;
    // reset next_pack_id to start_pack_id, next_row_offset to start_row_offset
    next_pack_id = start_pack_id;
    next_row_offset = start_row_offset;
    for (size_t pack_id = start_pack_id; pack_id < last_pack_id; ++pack_id)
    {
        // When the next pack is not used or the pack is the last pack, call read() to read theses packs and filter them
        // For example:
        //  When next_pack_id_cp = pack_res.size() and pack_res[next_pack_id:next_pack_id_cp] = [true, true, false, true, true, true]
        //  The algorithm runs as follows:
        //      When i = next_pack_id + 2, call read() to read {next_pack_id, next_pack_id + 1}th packs
        //      When i = next_pack_id + 5, call read() to read {next_pack_id + 3, next_pack_id + 4, next_pack_id + 5}th packs
        if (pack_res[pack_id].isUse() && (pack_id + 1 == pack_res.size() || !pack_res[pack_id + 1].isUse()))
        {
            Block block = read();
            size_t rows = block.rows();

            if (size_t passed_count = countBytesInFilter(filter, offset, rows); passed_count != rows)
            {
                IColumn::Offsets positions;
                positions.reserve(passed_count);
                for (size_t p = offset; p < offset + rows; ++p)
                {
                    if (filter[p])
                        positions.push_back(p - offset);
                }
                for (size_t i = 0; i < block.columns(); ++i)
                {
                    columns[i]->insertSelectiveFrom(*block.getByPosition(i).column, positions);
                }
            }
            else
            {
                for (size_t i = 0; i < block.columns(); ++i)
                {
                    columns[i]->insertRangeFrom(
                        *block.getByPosition(i).column,
                        0,
                        block.getByPosition(i).column->size());
                }
            }
            offset += rows;
        }
        else if (!pack_res[pack_id].isUse())
        {
            offset += pack_stats[pack_id].rows;
        }
    }

    /// 5. Restore the pack_res[last_pack_id]

    if (last_pack_id < pack_res.size())
        pack_res[last_pack_id] = next_pack_id_pack_res_cp;

    Block res = getHeader().cloneWithColumns(std::move(columns));
    res.setStartOffset(start_row_offset);
    res.setRSResult(rs_result);
    return res;
}

bool DMFileReader::isCacheableColumn(const ColumnDefine & cd)
{
    return cd.id == EXTRA_HANDLE_COLUMN_ID || cd.id == VERSION_COLUMN_ID;
}

Block DMFileReader::read()
{
    Stopwatch watch;
    SCOPE_EXIT(scan_context->total_dmfile_read_time_ns += watch.elapsed(););

    /// 1. Skip filtered out packs.
    if (size_t skip_rows; !getSkippedRows(skip_rows))
        return {};

    /// 2. Find the max continuous rows can be read.

    size_t start_pack_id = next_pack_id;
    size_t start_row_offset = next_row_offset;
    const auto [read_rows, rs_result] = getReadRows();
    if (read_rows == 0)
        return {};
    addScannedRows(read_rows);

    /// 3. Find packs can do clean read.

    const auto & pack_stats = dmfile->getPackStats();
    const auto & pack_properties = dmfile->getPackProperties();
    const auto & handle_res = pack_filter.getHandleRes(); // alias of handle_res in pack_filter
    const size_t read_packs = next_pack_id - start_pack_id;
    std::vector<size_t> handle_column_clean_read_packs;
    std::vector<size_t> del_column_clean_read_packs;
    std::vector<size_t> version_column_clean_read_packs;
    if (is_fast_scan)
    {
        if (enable_del_clean_read)
        {
            del_column_clean_read_packs.reserve(read_packs);
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
            handle_column_clean_read_packs.reserve(read_packs);
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
        handle_column_clean_read_packs.reserve(read_packs);
        version_column_clean_read_packs.reserve(read_packs);
        del_column_clean_read_packs.reserve(read_packs);
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

    /// 4. Read columns.

    ColumnsWithTypeAndName columns;
    columns.reserve(read_columns.size());
    for (auto & cd : read_columns)
    {
        try
        {
            ColumnPtr col;
            // For handle, tag and version column, we can try to do clean read.
            switch (cd.id)
            {
            case EXTRA_HANDLE_COLUMN_ID:
                col = readExtraColumn(cd, start_pack_id, read_packs, read_rows, handle_column_clean_read_packs);
                break;
            case TAG_COLUMN_ID:
                col = readExtraColumn(cd, start_pack_id, read_packs, read_rows, del_column_clean_read_packs);
                break;
            case VERSION_COLUMN_ID:
                col = readExtraColumn(cd, start_pack_id, read_packs, read_rows, version_column_clean_read_packs);
                break;
            default:
                col = readColumn(cd, start_pack_id, read_packs, read_rows);
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
    const std::vector<size_t> & clean_read_packs)
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
            src_col = readColumn(cd, range.first, range.second - range.first, rows_count);
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
    return column;
}

ColumnPtr DMFileReader::readColumn(const ColumnDefine & cd, size_t start_pack_id, size_t pack_count, size_t read_rows)
{
    // New column after ddl is not exist in this DMFile, fill with default value
    if (!column_streams.contains(DMFile::getFileNameBase(cd.id)))
        return createColumnWithDefaultValue(cd, read_rows);

    auto type_on_disk = dmfile->getColumnStat(cd.id).type;
    // try read PK column from ColumnCacheLongTerm
    if (column_cache_long_term && cd.id == pk_col_id && ColumnCacheLongTerm::isCacheableColumn(cd))
    {
        // ColumnCacheLongTerm only caches user assigned PrimaryKey column.
        auto column_all_data
            = column_cache_long_term->get(dmfile->parentPath(), dmfile->fileId(), cd.id, [&]() -> IColumn::Ptr {
                  // Always read all packs when filling cache
                  return readFromDiskOrSharingCache(cd, type_on_disk, 0, dmfile->getPacks(), dmfile->getRows());
              });

        auto column = type_on_disk->createColumn();
        column->insertRangeFrom(*column_all_data, next_row_offset - read_rows, read_rows);
        return convertColumnByColumnDefineIfNeed(type_on_disk, std::move(column), cd);
    }

    // Not cached
    if (!enable_column_cache || !isCacheableColumn(cd))
    {
        auto column = readFromDiskOrSharingCache(cd, type_on_disk, start_pack_id, pack_count, read_rows);
        return convertColumnByColumnDefineIfNeed(type_on_disk, std::move(column), cd);
    }

    // enable_column_cache && isCacheableColumn(cd)

    // try to get column from cache
    auto column = getColumnFromCache(
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
            return readFromDiskOrSharingCache(cd, type_on_disk, start_pack_id, pack_count, read_rows);
        });
    // add column to cache
    addColumnToCache(column_cache, cd.id, start_pack_id, pack_count, column);
    // Cast column's data from DataType in disk to what we need now
    return convertColumnByColumnDefineIfNeed(type_on_disk, std::move(column), cd);
}

ColumnPtr DMFileReader::readFromDisk(
    const ColumnDefine & cd,
    const DataTypePtr & type_on_disk,
    size_t start_pack_id,
    size_t read_rows)
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
        {});
    IDataType::updateAvgValueSizeHint(*mutable_col, top_stream->avg_size_hint);
    return mutable_col;
}

ColumnPtr DMFileReader::readFromDiskOrSharingCache(
    const ColumnDefine & cd,
    const DataTypePtr & type_on_disk,
    size_t start_pack_id,
    size_t pack_count,
    size_t read_rows)
{
    bool has_concurrent_reader = DMFileReaderPool::instance().hasConcurrentReader(*this);
    bool reach_sharing_column_memory_limit = shared_column_data_mem_tracker != nullptr
        && std::cmp_greater_equal(shared_column_data_mem_tracker->get(), max_sharing_column_bytes);
    if (reach_sharing_column_memory_limit)
    {
        GET_METRIC(tiflash_storage_read_thread_counter, type_add_cache_total_bytes_limit).Increment();
    }
    bool enable_sharing_column = has_concurrent_reader && !reach_sharing_column_memory_limit;
    ColumnPtr column;
    if (enable_sharing_column)
    {
        column = getColumnFromCache(
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
                return readFromDisk(cd, type_on_disk, start_pack_id, read_rows);
            });
    }
    else
    {
        column = readFromDisk(cd, type_on_disk, start_pack_id, read_rows);
    }

    // Set the column to DMFileReaderPool to share the column data.
    if (enable_sharing_column && data_sharing_col_data_cache != nullptr)
    {
        DMFileReaderPool::instance().set(*this, cd.id, start_pack_id, pack_count, column);
        // Delete column from local cache since it is not used anymore.
        data_sharing_col_data_cache->delColumn(cd.id, next_pack_id);
    }
    return column;
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

void DMFileReader::initAllMatchBlockInfo()
{
    const auto & pack_res = pack_filter.getPackResConst();
    const auto & pack_stats = dmfile->getPackStats();

    // Get continuous packs with RSResult::All
    auto get_all_match_block = [&](size_t start_pack) {
        size_t count = 0;
        size_t rows = 0;
        for (size_t i = start_pack; i < pack_res.size(); ++i)
        {
            if (!pack_res[i].allMatch() || rows >= rows_threshold_per_read)
                break;

            ++count;
            rows += pack_stats[i].rows;
        }
        return std::make_pair(count, rows);
    };

    for (size_t i = 0; i < pack_res.size();)
    {
        if (!pack_res[i].allMatch())
        {
            ++i;
            continue;
        }
        auto [pack_count, rows] = get_all_match_block(i);
        // Do not read block too small, it may hurts performance
        if (rows >= rows_threshold_per_read / 2)
            all_match_block_infos.emplace(i, pack_count);
        i += pack_count;
    }
}

size_t DMFileReader::getReadPackLimit(size_t start_pack_id)
{
    if (all_match_block_infos.empty() || read_one_pack_every_time)
        return read_one_pack_every_time ? 1 : std::numeric_limits<size_t>::max();

    const auto [next_all_match_block_start_pack_id, pack_count] = all_match_block_infos.front();
    // Read packs with RSResult::All
    if (next_all_match_block_start_pack_id == start_pack_id)
    {
        all_match_block_infos.pop();
        return pack_count;
    }
    // Read packs until next_all_match_block_start_pack_id
    RUNTIME_CHECK(
        next_all_match_block_start_pack_id > start_pack_id,
        next_all_match_block_start_pack_id,
        start_pack_id);
    return next_all_match_block_start_pack_id - start_pack_id;
}
} // namespace DB::DM
