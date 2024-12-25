// Copyright 2024 PingCAP, Inc.
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
#include <Common/TiFlashMetrics.h>
#include <IO/FileProvider/ChecksumReadBufferBuilder.h>
#include <Storages/DeltaMerge/File/DMFilePackFilter.h>
#include <Storages/DeltaMerge/Filter/FilterHelper.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/ScanContext.h>


namespace DB::DM
{

std::pair<size_t, size_t> DMFilePackFilter::loadValidRowsAndBytes(
    const DMContext & dm_context,
    const DMFilePtr & dmfile,
    bool set_cache_if_miss,
    const RowKeyRanges & rowkey_ranges)
{
    auto pack_filter = loadFrom(dm_context, dmfile, set_cache_if_miss, rowkey_ranges, EMPTY_RS_OPERATOR, {});

    size_t rows = 0;
    size_t bytes = 0;
    const auto & pack_stats = dmfile->getPackStats();
    for (size_t i = 0; i < pack_stats.size(); ++i)
    {
        if (pack_filter->pack_res[i].isUse())
        {
            rows += pack_stats[i].rows;
            bytes += pack_stats[i].bytes;
        }
    }
    return {rows, bytes};
}

DMFilePackFilterResultPtr DMFilePackFilter::load()
{
    Stopwatch watch;
    SCOPE_EXIT({ scan_context->total_rs_pack_filter_check_time_ns += watch.elapsed(); });
    size_t pack_count = dmfile->getPacks();
    DMFilePackFilterResult result(index_cache, read_limiter, pack_count);
    auto read_all_packs = (rowkey_ranges.size() == 1 && rowkey_ranges[0].all()) || rowkey_ranges.empty();
    if (!read_all_packs)
    {
        tryLoadIndex(result.param, EXTRA_HANDLE_COLUMN_ID);
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
            result.handle_res[i] = RSResult::None;
        }
        for (auto & handle_filter : handle_filters)
        {
            auto res = handle_filter->roughCheck(0, pack_count, result.param);
            std::transform(
                result.handle_res.begin(),
                result.handle_res.end(),
                res.begin(),
                result.handle_res.begin(),
                [](RSResult a, RSResult b) { return a || b; });
        }
    }

    ProfileEvents::increment(ProfileEvents::DMFileFilterNoFilter, pack_count);

    /// Check packs by handle_res
    result.pack_res = result.handle_res;
    auto after_pk = result.countUsePack();

    /// Check packs by read_packs
    if (read_packs)
    {
        for (size_t i = 0; i < pack_count; ++i)
        {
            result.pack_res[i] = read_packs->contains(i) ? result.pack_res[i] : RSResult::None;
        }
    }
    auto after_read_packs = result.countUsePack();
    ProfileEvents::increment(ProfileEvents::DMFileFilterAftPKAndPackSet, after_read_packs);

    /// Check packs by filter in where clause
    if (filter)
    {
        // Load index based on filter.
        ColIds ids = filter->getColumnIDs();
        for (const auto & id : ids)
        {
            tryLoadIndex(result.param, id);
        }

        const auto check_results = filter->roughCheck(0, pack_count, result.param);
        std::transform(
            result.pack_res.cbegin(),
            result.pack_res.cend(),
            check_results.cbegin(),
            result.pack_res.begin(),
            [](RSResult a, RSResult b) { return a && b; });
    }
    else
    {
        // ColumnFileBig in DeltaValueSpace never pass a filter to DMFilePackFilter.
        // Assume its filter always return Some.
        std::transform(result.pack_res.cbegin(), result.pack_res.cend(), result.pack_res.begin(), [](RSResult a) {
            return a && RSResult::Some;
        });
    }

    auto [none_count, some_count, all_count, all_null_count] = result.countPackRes();
    auto after_filter = some_count + all_count + all_null_count;
    ProfileEvents::increment(ProfileEvents::DMFileFilterAftRoughSet, after_filter);
    if (scan_context)
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
        ", read_packs: {}, pack_count: {}, none_count: {}, some_count: {}, all_count: {}, all_null_count: {}",
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
        all_null_count);
    return std::make_shared<DMFilePackFilterResult>(std::move(result));
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
    const auto & type = dmfile->getColumnStat(col_id).type;
    const auto file_name_base = DMFile::getFileNameBase(col_id);

    auto load = [&]() {
        auto index_file_size = dmfile->colIndexSize(col_id);
        if (index_file_size == 0)
            return std::make_shared<MinMaxIndex>(*type);
        auto index_guard = S3::S3RandomAccessFile::setReadFileInfo({
            .size = dmfile->getReadFileSize(col_id, colIndexFileName(file_name_base)),
            .scan_context = scan_context,
        });
        if (!dmfile->getConfiguration()) // v1
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
            const auto * dmfile_meta = typeid_cast<const DMFileMetaV2 *>(dmfile->meta.get());
            assert(dmfile_meta != nullptr);
            auto info = dmfile_meta->merged_sub_file_infos.find(colIndexFileName(file_name_base));
            if (info == dmfile_meta->merged_sub_file_infos.end())
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Unknown index file {}",
                    dmfile->colIndexPath(file_name_base));
            }

            auto file_path = dmfile->meta->mergedPath(info->second.number);
            auto encryp_path = dmfile_meta->encryptionMergedPath(info->second.number);
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
                dmfile->colIndexPath(file_name_base), // just for debug
                dmfile->getConfiguration()->getChecksumFrameLength(),
                dmfile->getConfiguration()->getChecksumAlgorithm(),
                dmfile->getConfiguration()->getChecksumFrameLength());

            auto header_size = dmfile->getConfiguration()->getChecksumHeaderLength();
            auto frame_total_size = dmfile->getConfiguration()->getChecksumFrameLength() + header_size;
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
                dmfile->getConfiguration()->getChecksumAlgorithm(),
                dmfile->getConfiguration()->getChecksumFrameLength());
            auto header_size = dmfile->getConfiguration()->getChecksumHeaderLength();
            auto frame_total_size = dmfile->getConfiguration()->getChecksumFrameLength() + header_size;
            auto frame_count = index_file_size / frame_total_size + (index_file_size % frame_total_size != 0);
            return MinMaxIndex::read(*type, *index_buf, index_file_size - header_size * frame_count);
        }
    };
    MinMaxIndexPtr minmax_index;
    if (index_cache && set_cache_if_miss)
    {
        minmax_index = index_cache->getOrSet(dmfile->colIndexCacheKey(file_name_base), load);
    }
    else
    {
        // try load from the cache first
        if (index_cache)
            minmax_index = index_cache->get(dmfile->colIndexCacheKey(file_name_base));
        if (minmax_index == nullptr)
            minmax_index = load();
    }
    indexes.emplace(col_id, RSIndex(type, minmax_index));
}

void DMFilePackFilter::tryLoadIndex(RSCheckParam & param, ColId col_id)
{
    if (param.indexes.count(col_id))
        return;

    if (!dmfile->isColIndexExist(col_id))
        return;

    Stopwatch watch;
    loadIndex(param.indexes, dmfile, file_provider, index_cache, set_cache_if_miss, col_id, read_limiter, scan_context);
}

} // namespace DB::DM
