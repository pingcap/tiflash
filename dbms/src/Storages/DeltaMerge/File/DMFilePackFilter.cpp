
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

#include <Common/Exception.h>
<<<<<<< HEAD
=======
#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <DataTypes/IDataType.h>
#include <IO/FileProvider/ChecksumReadBufferBuilder.h>
>>>>>>> b5beeee9fb (Storage: Fix TableScan performance regression under wide-sparse table (#10379))
#include <Storages/DeltaMerge/File/DMFilePackFilter.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/ScanContext.h>

#include <magic_enum.hpp>

namespace DB::DM
{

void DMFilePackFilter::init()
{
    size_t pack_count = dmfile->getPacks();
    auto read_all_packs = (rowkey_ranges.size() == 1 && rowkey_ranges[0].all()) || rowkey_ranges.empty();
    if (!read_all_packs)
    {
        tryLoadIndex(EXTRA_HANDLE_COLUMN_ID);
        std::vector<RSOperatorPtr> handle_filters;
        for (auto & rowkey_range : rowkey_ranges)
            handle_filters.emplace_back(toFilter(rowkey_range));
#ifndef NDEBUG
        // sanity check under debug mode to ensure the rowkey_range is correct common-handle or int64-handle
        if (!rowkey_ranges.empty())
        {
            bool is_common_handle = rowkey_ranges.begin()->is_common_handle;
            auto handle_col_type = dmfile->getColumnStat(EXTRA_HANDLE_COLUMN_ID).type;
            if (is_common_handle)
                RUNTIME_CHECK_MSG(
                    handle_col_type->getTypeId() == TypeIndex::String,
                    "handle_col_type_id={}",
                    magic_enum::enum_name(handle_col_type->getTypeId()));
            else
                RUNTIME_CHECK_MSG(
                    handle_col_type->getTypeId() == TypeIndex::Int64,
                    "handle_col_type_id={}",
                    magic_enum::enum_name(handle_col_type->getTypeId()));
            for (size_t i = 1; i < rowkey_ranges.size(); ++i)
            {
                RUNTIME_CHECK_MSG(
                    is_common_handle == rowkey_ranges[i].is_common_handle,
                    "i={} is_common_handle={} ith.is_common_handle={}",
                    i,
                    is_common_handle,
                    rowkey_ranges[i].is_common_handle);
            }
        }
#endif
        for (size_t i = 0; i < pack_count; ++i)
        {
            handle_res[i] = RSResult::None;
        }
        for (auto & handle_filter : handle_filters)
        {
            auto res = handle_filter->roughCheck(0, pack_count, param);
            std::transform(
                handle_res.begin(),
                handle_res.end(),
                res.begin(),
                handle_res.begin(),
                [](RSResult a, RSResult b) { return a || b; });
        }
    }

    ProfileEvents::increment(ProfileEvents::DMFileFilterNoFilter, pack_count);

    size_t after_pk = 0;
    size_t after_read_packs = 0;
    size_t after_filter = 0;

    /// Check packs by handle_res
    for (size_t i = 0; i < pack_count; ++i)
    {
        use_packs[i] = handle_res[i] != None;
    }

    for (auto u : use_packs)
        after_pk += u;

    /// Check packs by read_packs
    if (read_packs)
    {
        for (size_t i = 0; i < pack_count; ++i)
        {
            use_packs[i] = (static_cast<bool>(use_packs[i])) && read_packs->contains(i);
        }
    }

    for (auto u : use_packs)
        after_read_packs += u;
    ProfileEvents::increment(ProfileEvents::DMFileFilterAftPKAndPackSet, after_read_packs);


    /// Check packs by filter in where clause
    if (filter)
    {
        // Load index based on filter.
        ColIds ids = filter->getColumnIDs();
        for (const auto & id : ids)
        {
            tryLoadIndex(id);
        }

        Stopwatch watch;
        const auto check_results = filter->roughCheck(0, pack_count, param);
        std::transform(
            use_packs.begin(),
            use_packs.end(),
            check_results.begin(),
            use_packs.begin(),
            [](UInt8 a, RSResult b) { return (static_cast<bool>(a)) && (b != None); });
        scan_context->total_dmfile_rough_set_index_check_time_ns += watch.elapsed();
    }

    for (auto u : use_packs)
        after_filter += u;
    ProfileEvents::increment(ProfileEvents::DMFileFilterAftRoughSet, after_filter);

    Float64 filter_rate = 0.0;
    if (after_read_packs != 0)
    {
        filter_rate = (after_read_packs - after_filter) * 100.0 / after_read_packs;
        GET_METRIC(tiflash_storage_rough_set_filter_rate, type_dtfile_pack).Observe(filter_rate);
    }
    LOG_DEBUG(
        log,
        "RSFilter exclude rate: {:.2f}, after_pk: {}, after_read_packs: {}, after_filter: {}, handle_ranges: {}"
        ", read_packs: {}, pack_count: {}",
        ((after_read_packs == 0) ? std::numeric_limits<double>::quiet_NaN() : filter_rate),
        after_pk,
        after_read_packs,
        after_filter,
        toDebugString(rowkey_ranges),
        ((read_packs == nullptr) ? 0 : read_packs->size()),
        pack_count);
}

void DMFilePackFilter::loadIndex(
    ColumnIndexes & indexes,
    const DMFilePtr & dmfile,
    const FileProviderPtr & file_provider,
    const MinMaxIndexCachePtr & index_cache,
    bool set_cache_if_miss,
    ColId col_id,
    const ReadLimiterPtr & read_limiter,
    const ScanContextPtr & scan_context)
{
<<<<<<< HEAD
    const auto & type = dmfile->getColumnStat(col_id).type;
    const auto file_name_base = DMFile::getFileNameBase(col_id);

    auto load = [&]() {
        auto index_file_size = dmfile->colIndexSize(col_id);
        if (index_file_size == 0)
            return std::make_shared<MinMaxIndex>(*type);
        auto index_guard = S3::S3RandomAccessFile::setReadFileInfo({
            .size = dmfile->getReadFileSize(col_id, dmfile->colIndexFileName(file_name_base)),
            .scan_context = scan_context,
        });
        if (!dmfile->configuration) // v1
        {
            auto index_buf = ReadBufferFromRandomAccessFileBuilder::build(
                file_provider,
                dmfile->colIndexPath(file_name_base),
                dmfile->encryptionIndexPath(file_name_base),
                std::min(static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE), index_file_size),
                read_limiter);
            return MinMaxIndex::read(*type, index_buf, index_file_size);
        }
        else if (dmfile->useMetaV2()) // v3
        {
            auto info = dmfile->merged_sub_file_infos.find(dmfile->colIndexFileName(file_name_base));
            if (info == dmfile->merged_sub_file_infos.end())
            {
                throw Exception(
                    fmt::format("Unknown index file {}", dmfile->colIndexPath(file_name_base)),
                    ErrorCodes::LOGICAL_ERROR);
            }

            auto file_path = dmfile->mergedPath(info->second.number);
            auto encryp_path = dmfile->encryptionMergedPath(info->second.number);
            auto offset = info->second.offset;
            auto data_size = info->second.size;

            auto buffer = ReadBufferFromRandomAccessFileBuilder::build(
                file_provider,
                file_path,
                encryp_path,
                dmfile->getConfiguration()->getChecksumFrameLength(),
                read_limiter);
            buffer.seek(offset);

            String raw_data;
            raw_data.resize(data_size);

            buffer.read(reinterpret_cast<char *>(raw_data.data()), data_size);

            auto buf = ChecksumReadBufferBuilder::build(
                std::move(raw_data),
                dmfile->colDataPath(file_name_base),
                dmfile->getConfiguration()->getChecksumFrameLength(),
                dmfile->configuration->getChecksumAlgorithm(),
                dmfile->configuration->getChecksumFrameLength());

            auto header_size = dmfile->configuration->getChecksumHeaderLength();
            auto frame_total_size = dmfile->configuration->getChecksumFrameLength() + header_size;
            auto frame_count = index_file_size / frame_total_size + (index_file_size % frame_total_size != 0);

            return MinMaxIndex::read(*type, *buf, index_file_size - header_size * frame_count);
        }
        else
        { // v2
            auto index_buf = ChecksumReadBufferBuilder::build(
                file_provider,
                dmfile->colIndexPath(file_name_base),
                dmfile->encryptionIndexPath(file_name_base),
                index_file_size,
                read_limiter,
                dmfile->configuration->getChecksumAlgorithm(),
                dmfile->configuration->getChecksumFrameLength());
            auto header_size = dmfile->configuration->getChecksumHeaderLength();
            auto frame_total_size = dmfile->configuration->getChecksumFrameLength() + header_size;
            auto frame_count = index_file_size / frame_total_size + (index_file_size % frame_total_size != 0);
            return MinMaxIndex::read(*type, *index_buf, index_file_size - header_size * frame_count);
        }
    };
    MinMaxIndexPtr minmax_index;
    if (index_cache && set_cache_if_miss)
    {
        minmax_index = index_cache->getOrSet(dmfile->colIndexCacheKey(file_name_base), load);
=======
    auto [type, minmax_index]
        = loadIndex(*dmfile, file_provider, index_cache, set_cache_if_miss, col_id, read_limiter, scan_context);
    indexes.emplace(col_id, RSIndex(type, minmax_index));
}

class MinMaxIndexLoader
{
public:
    // Make the instance of `MinMaxIndexLoader` as a callable object that is used in
    // `index_cache->getOrSet(...)`.
    MinMaxIndexPtr operator()()
    {
        const auto & type = dmfile.getColumnStat(col_id).type;
        auto index_file_size = dmfile.colIndexSize(col_id);
        if (index_file_size == 0)
            return std::make_shared<MinMaxIndex>(*type);

        auto index_guard = S3::S3RandomAccessFile::setReadFileInfo({
            .size = dmfile.getReadFileSize(col_id, colIndexFileName(file_name_base)),
            .scan_context = scan_context,
        });

        if (likely(dmfile.useMetaV2()))
        {
            // the min-max index is merged into metav2
            return loadMinMaxIndexFromMetav2(type, index_file_size);
        }
        else if (unlikely(!dmfile.getConfiguration()))
        {
            // without checksum, simply load the raw bytes from file
            return loadRawMinMaxIndex(type, index_file_size);
        }
        else
        {
            // checksum is enabled but not merged into meta v2
            return loadMinMaxIndexWithChecksum(type, index_file_size);
        }
    }

public:
    MinMaxIndexLoader(
        const DMFile & dmfile_,
        const FileProviderPtr & file_provider_,
        ColId col_id_,
        const ReadLimiterPtr & read_limiter_,
        const ScanContextPtr & scan_context_)
        : dmfile(dmfile_)
        , file_name_base(DMFile::getFileNameBase(col_id_))
        , col_id(col_id_)
        , file_provider(file_provider_)
        , read_limiter(read_limiter_)
        , scan_context(scan_context_)
    {}

    const DMFile & dmfile;
    const FileNameBase file_name_base;
    ColId col_id;
    FileProviderPtr file_provider;
    ReadLimiterPtr read_limiter;
    ScanContextPtr scan_context;

private:
    MinMaxIndexPtr loadRawMinMaxIndex(const DataTypePtr & type, size_t index_file_size) const
    {
        auto index_buf = ReadBufferFromRandomAccessFileBuilder::build(
            file_provider,
            dmfile.colIndexPath(file_name_base),
            dmfile.encryptionIndexPath(file_name_base),
            std::min(static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE), index_file_size),
            read_limiter);
        return MinMaxIndex::read(*type, index_buf, index_file_size);
    }

    MinMaxIndexPtr loadMinMaxIndexWithChecksum(const DataTypePtr & type, size_t index_file_size) const
    {
        auto index_buf = ChecksumReadBufferBuilder::build(
            file_provider,
            dmfile.colIndexPath(file_name_base),
            dmfile.encryptionIndexPath(file_name_base),
            index_file_size,
            read_limiter,
            dmfile.getConfiguration()->getChecksumAlgorithm(),
            dmfile.getConfiguration()->getChecksumFrameLength());
        auto header_size = dmfile.getConfiguration()->getChecksumHeaderLength();
        auto frame_total_size = dmfile.getConfiguration()->getChecksumFrameLength() + header_size;
        auto frame_count = index_file_size / frame_total_size + (index_file_size % frame_total_size != 0);
        return MinMaxIndex::read(*type, *index_buf, index_file_size - header_size * frame_count);
    }

    MinMaxIndexPtr loadMinMaxIndexFromMetav2(const DataTypePtr & type, size_t index_file_size) const
    {
        const auto * dmfile_meta = typeid_cast<const DMFileMetaV2 *>(dmfile.meta.get());
        assert(dmfile_meta != nullptr);
        const auto col_index_fname = colIndexFileName(file_name_base);
        auto info_iter = dmfile_meta->merged_sub_file_infos.find(col_index_fname);
        RUNTIME_CHECK_MSG(
            info_iter != dmfile_meta->merged_sub_file_infos.end(),
            "Unknown index file, dmfile_path={} index_fname={}",
            dmfile.parentPath(),
            col_index_fname);

        const auto & merged_file_info = info_iter->second;
        const auto file_path = dmfile.meta->mergedPath(merged_file_info.number);
        const auto offset = merged_file_info.offset;
        const auto data_size = merged_file_info.size;

        // First, read from merged file to get the raw data(contains the header)
        // Note that we use min(`data_size`, checksum_frame_size) as the size of buffer size in order
        // to minimize read amplification in the merged file.
        auto buffer = ReadBufferFromRandomAccessFileBuilder::build(
            file_provider,
            file_path,
            dmfile_meta->encryptionMergedPath(merged_file_info.number),
            std::min(data_size, dmfile.getConfiguration()->getChecksumFrameLength()),
            read_limiter);
        buffer.seek(offset);

        String raw_data(data_size, '\0');
        buffer.read(reinterpret_cast<char *>(raw_data.data()), data_size);

        // Then read from the buffer based on the raw data. The buffer size is min(data.size(), checksum_frame_size)
        auto buf = ChecksumReadBufferBuilder::build(
            std::move(raw_data),
            file_path,
            dmfile.getConfiguration()->getChecksumAlgorithm(),
            dmfile.getConfiguration()->getChecksumFrameLength());

        auto header_size = dmfile.getConfiguration()->getChecksumHeaderLength();
        auto frame_total_size = dmfile.getConfiguration()->getChecksumFrameLength() + header_size;
        auto frame_count = index_file_size / frame_total_size + (index_file_size % frame_total_size != 0);

        return MinMaxIndex::read(*type, *buf, index_file_size - header_size * frame_count);
    }
};

std::pair<DataTypePtr, MinMaxIndexPtr> DMFilePackFilter::loadIndex(
    const DMFile & dmfile,
    const FileProviderPtr & file_provider,
    const MinMaxIndexCachePtr & index_cache,
    bool set_cache_if_miss,
    ColId col_id,
    const ReadLimiterPtr & read_limiter,
    const ScanContextPtr & scan_context)
{
    const auto & type = dmfile.getColumnStat(col_id).type;
    const auto file_name_base = DMFile::getFileNameBase(col_id);

    MinMaxIndexPtr minmax_index;
    if (index_cache && set_cache_if_miss)
    {
        auto loader = MinMaxIndexLoader(dmfile, file_provider, col_id, read_limiter, scan_context);
        minmax_index = index_cache->getOrSet(dmfile.colIndexCacheKey(file_name_base), loader);
>>>>>>> b5beeee9fb (Storage: Fix TableScan performance regression under wide-sparse table (#10379))
    }
    else
    {
        // try load from the cache first
        if (index_cache)
            minmax_index = index_cache->get(dmfile->colIndexCacheKey(file_name_base));
        if (minmax_index == nullptr)
            minmax_index = MinMaxIndexLoader(dmfile, file_provider, col_id, read_limiter, scan_context)();
    }
    indexes.emplace(col_id, RSIndex(type, minmax_index));
}

void DMFilePackFilter::tryLoadIndex(const ColId col_id)
{
    if (param.indexes.count(col_id))
        return;

    if (!dmfile->isColIndexExist(col_id))
        return;

    Stopwatch watch;
    loadIndex(param.indexes, dmfile, file_provider, index_cache, set_cache_if_miss, col_id, read_limiter, scan_context);

<<<<<<< HEAD
    scan_context->total_dmfile_rough_set_index_check_time_ns += watch.elapsed();
=======
std::pair<std::vector<DMFilePackFilter::Range>, DMFilePackFilterResults> DMFilePackFilter::getSkippedRangeAndFilter(
    const DMContext & dm_context,
    const DMFiles & dmfiles,
    const DMFilePackFilterResults & pack_filter_results,
    UInt64 start_ts)
{
    // Packs that all rows compliant with MVCC filter and RowKey filter requirements.
    // For building bitmap filter, we don't need to read these packs,
    // just set corresponding positions in the bitmap to true.
    // So we record the offset and rows of these packs and merge continuous ranges.
    std::vector<Range> skipped_ranges;
    // Packs that some rows compliant with MVCC filter and RowKey filter requirements.
    // We need to read these packs and do RowKey filter and MVCC filter for them.
    DMFilePackFilterResults new_pack_filter_results;
    new_pack_filter_results.reserve(dmfiles.size());
    RUNTIME_CHECK(pack_filter_results.size() == dmfiles.size());

    UInt64 current_offset = 0;

    auto file_provider = dm_context.global_context.getFileProvider();
    for (size_t i = 0; i < dmfiles.size(); ++i)
    {
        const auto & dmfile = dmfiles[i];
        const auto & pack_filter = pack_filter_results[i];
        const auto & pack_res = pack_filter->getPackRes();
        const auto & handle_res = pack_filter->getHandleRes();
        const auto & pack_stats = dmfile->getPackStats();
        DMFilePackFilterResultPtr new_pack_filter;
        for (size_t pack_id = 0; pack_id < pack_stats.size(); ++pack_id)
        {
            const auto & pack_stat = pack_stats[pack_id];
            auto prev_offset = current_offset;
            current_offset += pack_stat.rows;
            if (!pack_res[pack_id].isUse())
                continue;

            if (handle_res[pack_id] == RSResult::Some || pack_stat.not_clean > 0
                || pack_filter->getMaxVersion(dmfile, pack_id, file_provider, dm_context.scan_context) > start_ts)
            {
                // `not_clean > 0` means there are more than one version for some rowkeys in this pack
                // `pack.max_version > start_ts` means some rows will be filtered by MVCC reading
                // We need to read this pack to do RowKey or MVCC filter.
                continue;
            }

            if unlikely (!new_pack_filter)
                new_pack_filter = std::make_shared<DMFilePackFilterResult>(*pack_filter);

            // This pack is skipped by the skipped_range, do not need to read the rows from disk
            new_pack_filter->pack_res[pack_id] = RSResult::None;
            // When this pack is next to the previous skipped pack, we merge them.
            if (!skipped_ranges.empty() && skipped_ranges.back().offset + skipped_ranges.back().rows == prev_offset)
                skipped_ranges.back().rows += pack_stat.rows;
            else
                skipped_ranges.emplace_back(prev_offset, pack_stat.rows);
        }

        if (new_pack_filter)
            new_pack_filter_results.emplace_back(std::move(new_pack_filter));
        else
            new_pack_filter_results.emplace_back(pack_filter);
    }

    return {skipped_ranges, new_pack_filter_results};
}

std::pair<std::vector<DMFilePackFilter::Range>, DMFilePackFilterResults> DMFilePackFilter::
    getSkippedRangeAndFilterWithMultiVersion(
        const DMContext & dm_context,
        const DMFiles & dmfiles,
        const DMFilePackFilterResults & pack_filter_results,
        UInt64 start_ts,
        const DeltaIndexIterator & delta_index_begin,
        const DeltaIndexIterator & delta_index_end)
{
    // Packs that all rows compliant with MVCC filter and RowKey filter requirements.
    // For building bitmap filter, we don't need to read these packs,
    // just set corresponding positions in the bitmap to true.
    // So we record the offset and rows of these packs and merge continuous ranges.
    std::vector<Range> skipped_ranges;
    // Packs that some rows compliant with MVCC filter and RowKey filter requirements.
    // We need to read these packs and do RowKey filter and MVCC filter for them.
    DMFilePackFilterResults new_pack_filter_results;
    new_pack_filter_results.reserve(dmfiles.size());
    RUNTIME_CHECK(pack_filter_results.size() == dmfiles.size());

    UInt64 current_offset = 0;
    UInt64 prev_sid = 0;
    UInt64 sid = 0;
    UInt32 prev_delete_count = 0;

    auto delta_index_it = delta_index_begin;
    auto file_provider = dm_context.global_context.getFileProvider();
    for (size_t i = 0; i < dmfiles.size(); ++i)
    {
        const auto & dmfile = dmfiles[i];
        const auto & pack_filter = pack_filter_results[i];
        const auto & pack_res = pack_filter->getPackRes();
        const auto & handle_res = pack_filter->getHandleRes();
        const auto & pack_stats = dmfile->getPackStats();
        DMFilePackFilterResultPtr new_pack_filter;
        for (size_t pack_id = 0; pack_id < pack_stats.size(); ++pack_id)
        {
            const auto & pack_stat = pack_stats[pack_id];
            auto prev_offset = current_offset;
            current_offset += pack_stat.rows;
            if (!pack_res[pack_id].isUse())
                continue;

            // Find the first `delta_index_it` whose sid > prev_offset
            auto new_it = std::upper_bound(
                delta_index_it,
                delta_index_end,
                prev_offset,
                [](UInt64 val, const DeltaIndexCompacted::Entry & e) { return val < e.getSid(); });
            if (new_it != delta_index_it)
            {
                auto prev_it = std::prev(new_it);
                prev_sid = prev_it->getSid();
                prev_delete_count = prev_it->isDelete() ? prev_it->getCount() : 0;
                delta_index_it = new_it;
            }
            sid = delta_index_it != delta_index_end ? delta_index_it->getSid() : std::numeric_limits<UInt64>::max();

            // The sid range of the pack: (prev_offset, current_offset].
            // The continuously sorted sid range in delta index: (prev_sid, sid].

            // Since `delta_index_it` is the first element with sid > prev_offset,
            // the preceding element’s sid (prev_sid) must be <= prev_offset.
            RUNTIME_CHECK(prev_offset >= prev_sid);
            if (prev_offset == prev_sid)
            {
                // If `prev_offset == prev_sid`, the RowKey of the delta row preceding `prev_sid` should not
                // be the same as the RowKey of `prev_sid`. This is because for the same RowKey, the version
                // in the delta data should be greater than the version in the stable data.
                // However, this is not always the case and many situations need to be confirmed. For safety
                // reasons, the pack will not be skipped in this situation.
                // TODO: It might be possible to use a minmax index to compare the RowKey of the
                // `prev_sid` row with the RowKey of the preceding delta row.
                continue;
            }

            // Now check the right boundary of this pack(i.e. current_offset)
            if (current_offset >= sid)
            {
                // If `current_offset > sid`, it means some data in pack exceeds the right boundary of
                // (prev_sid, sid] so this pack can not be skipped.
                //
                // If `current_offset == sid`, the delta row following this sid row might have the same
                // RowKey. The pack also can not be skipped because delta merge and MVCC filter is necessary.
                // TODO: It might be possible to use a minmax index to compare the RowKey of the
                // current sid row with the RowKey of the following delta row.
                continue;
            }

            if (prev_delete_count > 0)
            {
                // The previous delta index iterator is a delete, we must check if the sid range of the
                // pack intersects with the delete range.
                // The sid range of the pack: (prev_offset, current_offset].
                // The delete sid range: (prev_sid, prev_sid + prev_delete_count].
                if (current_offset <= prev_sid + prev_delete_count)
                {
                    // The sid range of the pack is fully covered by the delete sid range, it means that
                    // every row in this pack has been deleted. In this case, the pack can be safely skipped.
                    if unlikely (!new_pack_filter)
                        new_pack_filter = std::make_shared<DMFilePackFilterResult>(*pack_filter);

                    new_pack_filter->pack_res[pack_id] = RSResult::None;
                    continue;
                }
                if (prev_offset < prev_sid + prev_delete_count)
                {
                    // Some rows in the pack are deleted while others are not, it means the pack cannot
                    // be skipped.
                    continue;
                }
                // None of the rows in the pack have been deleted
            }

            // Check other conditions that may allow the pack to be skipped
            if (handle_res[pack_id] == RSResult::Some || pack_stat.not_clean > 0
                || pack_filter->getMaxVersion(dmfile, pack_id, file_provider, dm_context.scan_context) > start_ts)
            {
                // `not_clean > 0` means there are more than one version for some rowkeys in this pack
                // `pack.max_version > start_ts` means some rows will be filtered by MVCC reading
                // We need to read this pack to do delta merge, RowKey or MVCC filter.
                continue;
            }

            if unlikely (!new_pack_filter)
                new_pack_filter = std::make_shared<DMFilePackFilterResult>(*pack_filter);

            // This pack is skipped by the skipped_range, do not need to read the rows from disk
            new_pack_filter->pack_res[pack_id] = RSResult::None;
            // When this pack is next to the previous skipped pack, we merge them.
            if (!skipped_ranges.empty() && skipped_ranges.back().offset + skipped_ranges.back().rows == prev_offset)
                skipped_ranges.back().rows += pack_stat.rows;
            else
                skipped_ranges.emplace_back(prev_offset, pack_stat.rows);
        }

        if (new_pack_filter)
            new_pack_filter_results.emplace_back(std::move(new_pack_filter));
        else
            new_pack_filter_results.emplace_back(pack_filter);
    }

    return {skipped_ranges, new_pack_filter_results};
>>>>>>> b5beeee9fb (Storage: Fix TableScan performance regression under wide-sparse table (#10379))
}

} // namespace DB::DM
