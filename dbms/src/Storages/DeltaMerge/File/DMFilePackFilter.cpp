
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

#include <Storages/DeltaMerge/File/DMFilePackFilter.h>
#include <Storages/DeltaMerge/ScanContext.h>

namespace DB::DM
{

void DMFilePackFilter::init()
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
    auto [none_count, some_count, all_count] = countPackRes();
    auto after_filter = some_count + all_count;
    ProfileEvents::increment(ProfileEvents::DMFileFilterAftRoughSet, after_filter);
    scan_context->rs_pack_filter_none += none_count;
    scan_context->rs_pack_filter_some += some_count;
    scan_context->rs_pack_filter_all += all_count;

    Float64 filter_rate = 0.0;
    if (after_read_packs != 0)
    {
        filter_rate = (after_read_packs - after_filter) * 100.0 / after_read_packs;
        GET_METRIC(tiflash_storage_rough_set_filter_rate, type_dtfile_pack).Observe(filter_rate);
    }
    LOG_DEBUG(
        log,
        "RSFilter exclude rate: {:.2f}, after_pk: {}, after_read_packs: {}, after_filter: {}, handle_ranges: {}"
        ", read_packs: {}, pack_count: {}, none_count: {}, some_count: {}, all_count: {}",
        ((after_read_packs == 0) ? std::numeric_limits<double>::quiet_NaN() : filter_rate),
        after_pk,
        after_read_packs,
        after_filter,
        toDebugString(rowkey_ranges),
        ((read_packs == nullptr) ? 0 : read_packs->size()),
        pack_count,
        none_count,
        some_count,
        all_count);
}

std::tuple<UInt64, UInt64, UInt64> DMFilePackFilter::countPackRes() const
{
    UInt64 none_count = 0;
    UInt64 some_count = 0;
    UInt64 all_count = 0;
    for (auto res : pack_res)
    {
        switch (res)
        {
        case RSResult::None:
            ++none_count;
            break;
        case RSResult::Some:
            ++some_count;
            break;
        case RSResult::All:
            ++all_count;
            break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "{} is invalid", static_cast<Int32>(res));
        }
    }
    return {none_count, some_count, all_count};
}

UInt64 DMFilePackFilter::countUsePack() const
{
    return std::count_if(pack_res.cbegin(), pack_res.cend(), [](RSResult res) { return isUse(res); });
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
                    fmt::format("Unknown index file {}", dmfile->colIndexPath(file_name_base)),
                    ErrorCodes::LOGICAL_ERROR);
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
                dmfile->colDataPath(file_name_base),
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
