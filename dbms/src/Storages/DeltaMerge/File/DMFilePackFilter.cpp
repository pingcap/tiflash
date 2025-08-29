
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
#include <Storages/DeltaMerge/File/DMFilePackFilter.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/ScanContext.h>

#include <magic_enum.hpp>

namespace DB::DM
{

void DMFilePackFilter::init(ReadTag read_tag)
{
    Stopwatch watch;
    SCOPE_EXIT({ scan_context->total_rs_pack_filter_check_time_ns += watch.elapsed(); });
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

    /// Check packs by handle_res
    pack_res = handle_res;
    auto after_pk = countUsePack();

    /// Check packs by read_packs
    if (read_packs)
    {
        for (size_t i = 0; i < pack_count; ++i)
        {
            pack_res[i] = read_packs->contains(i) ? pack_res[i] : RSResult::None;
        }
    }
    auto after_read_packs = countUsePack();
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

        const auto check_results = filter->roughCheck(0, pack_count, param);
        std::transform(
            pack_res.cbegin(),
            pack_res.cend(),
            check_results.cbegin(),
            pack_res.begin(),
            [](RSResult a, RSResult b) { return a && b; });
    }
    else
    {
        // ColumnFileBig in DeltaValueSpace never pass a filter to DMFilePackFilter.
        // Assume its filter always return Some.
        std::transform(pack_res.cbegin(), pack_res.cend(), pack_res.begin(), [](RSResult a) {
            return a && RSResult::Some;
        });
    }

    auto [none_count, some_count, all_count, all_null_count] = countPackRes();
    auto after_filter = some_count + all_count + all_null_count;
    ProfileEvents::increment(ProfileEvents::DMFileFilterAftRoughSet, after_filter);
    // In table scanning, DMFilePackFilter of a DMFile may be created several times:
    // 1. When building MVCC bitmap (ReadTag::MVCC).
    // 2. When building LM filter stream (ReadTag::LM).
    // 3. When building stream of other columns (ReadTag::Query).
    // Only need to count the filter result once.
    // TODO: We can create DMFilePackFilter at the beginning and pass it to the stages described above.
    if (read_tag == ReadTag::Query)
    {
        scan_context->rs_pack_filter_none += none_count;
        scan_context->rs_pack_filter_some += some_count;
        scan_context->rs_pack_filter_all += all_count;
        scan_context->rs_pack_filter_all_null += all_null_count;
    }

    Float64 filter_rate = 0.0;
    if (after_read_packs != 0)
    {
        filter_rate = (after_read_packs - after_filter) * 100.0 / after_read_packs;
        GET_METRIC(tiflash_storage_rough_set_filter_rate, type_dtfile_pack).Observe(filter_rate);
    }
    LOG_DEBUG(
        log,
        "RSFilter exclude rate: {:.2f}, after_pk: {}, after_read_packs: {}, after_filter: {}, handle_ranges: {}"
        ", read_packs: {}, pack_count: {}, none_count: {}, some_count: {}, all_count: {}, all_null_count: {}, "
        "read_tag: {}",
        ((after_read_packs == 0) ? std::numeric_limits<double>::quiet_NaN() : filter_rate),
        after_pk,
        after_read_packs,
        after_filter,
        toDebugString(rowkey_ranges),
        ((read_packs == nullptr) ? 0 : read_packs->size()),
        pack_count,
        none_count,
        some_count,
        all_count,
        all_null_count,
        magic_enum::enum_name(read_tag));
}

std::tuple<UInt64, UInt64, UInt64, UInt64> DMFilePackFilter::countPackRes() const
{
    UInt64 none_count = 0;
    UInt64 some_count = 0;
    UInt64 all_count = 0;
    UInt64 all_null_count = 0;
    for (auto res : pack_res)
    {
        if (res == RSResult::None || res == RSResult::NoneNull)
            ++none_count;
        else if (res == RSResult::Some || res == RSResult::SomeNull)
            ++some_count;
        else if (res == RSResult::All)
            ++all_count;
        else if (res == RSResult::AllNull)
            ++all_null_count;
    }
    return {none_count, some_count, all_count, all_null_count};
}

UInt64 DMFilePackFilter::countUsePack() const
{
    return std::count_if(pack_res.cbegin(), pack_res.cend(), [](RSResult res) { return res.isUse(); });
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
    auto [type, minmax_index]
        = loadIndex(*dmfile, file_provider, index_cache, set_cache_if_miss, col_id, read_limiter, scan_context);
    indexes.emplace(col_id, RSIndex(type, minmax_index));
}

class MinMaxIndexLoader
{
public:
    // Make the instance of `MinMaxIndexLoader` as a callable object that is used in
    // `index_cache->getOrSet(...)`.
    MinMaxIndexPtr operator()() const
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
    }
    else
    {
        // try load from the cache first
        if (index_cache)
            minmax_index = index_cache->get(dmfile.colIndexCacheKey(file_name_base));
        if (minmax_index == nullptr)
            minmax_index = MinMaxIndexLoader(dmfile, file_provider, col_id, read_limiter, scan_context)();
    }
    return {type, minmax_index};
}

void DMFilePackFilter::tryLoadIndex(ColId col_id)
{
    if (param.indexes.count(col_id))
        return;

    if (!dmfile->isColIndexExist(col_id))
        return;

    Stopwatch watch;
    loadIndex(param.indexes, dmfile, file_provider, index_cache, set_cache_if_miss, col_id, read_limiter, scan_context);
}

} // namespace DB::DM
