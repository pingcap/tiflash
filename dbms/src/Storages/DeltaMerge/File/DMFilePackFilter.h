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

#pragma once

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/TiFlashMetrics.h>
#include <Encryption/ReadBufferFromFileProvider.h>
#include <Encryption/createReadBufferFromFileBaseByFileProvider.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/Filter/FilterHelper.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/S3/S3Common.h>

namespace ProfileEvents
{
extern const Event DMFileFilterNoFilter;
extern const Event DMFileFilterAftPKAndPackSet;
extern const Event DMFileFilterAftRoughSet;
} // namespace ProfileEvents

namespace DB
{
namespace DM
{
using IdSet = std::set<UInt64>;
using IdSetPtr = std::shared_ptr<IdSet>;

class DMFilePackFilter
{
public:
    // Empty `rowkey_ranges` means do not filter by rowkey_ranges
    static DMFilePackFilter loadFrom(
        const DMFilePtr & dmfile,
        const MinMaxIndexCachePtr & index_cache,
        bool set_cache_if_miss,
        const RowKeyRanges & rowkey_ranges,
        const RSOperatorPtr & filter,
        const IdSetPtr & read_packs,
        const FileProviderPtr & file_provider,
        const ReadLimiterPtr & read_limiter,
        const ScanContextPtr & scan_context,
        const String & tracing_id)
    {
        auto pack_filter = DMFilePackFilter(
            dmfile,
            index_cache,
            set_cache_if_miss,
            rowkey_ranges,
            filter,
            read_packs,
            file_provider,
            read_limiter,
            scan_context,
            tracing_id);
        // clear indexes to save memory
        pack_filter.param.indexes.clear();
        return pack_filter;
    }

    inline const std::vector<RSResult> & getHandleRes() const { return handle_res; }
    inline const std::vector<UInt8> & getUsePacksConst() const { return use_packs; }
    inline std::vector<UInt8> & getUsePacks() { return use_packs; }

    Handle getMinHandle(size_t pack_id)
    {
        if (!handle_index.has_value())
            tryLoadIndex(EXTRA_HANDLE_COLUMN_ID, false);
        RUNTIME_CHECK(handle_index.has_value());
        return handle_index.value()->getIntMinMax(pack_id).first;
    }

    StringRef getMinStringHandle(size_t pack_id)
    {
        if (!handle_index.has_value())
            tryLoadIndex(EXTRA_HANDLE_COLUMN_ID, false);
        RUNTIME_CHECK(handle_index.has_value());
        return handle_index.value()->getStringMinMax(pack_id).first;
    }

    UInt64 getMaxVersion(size_t pack_id)
    {
        if (!version_index.has_value())
            tryLoadIndex(VERSION_COLUMN_ID, false);
        RUNTIME_CHECK(version_index.has_value());
        return version_index.value()->getUInt64MinMax(pack_id).second;
    }

    // Get valid rows and bytes after filter invalid packs by handle_range and filter
    std::pair<size_t, size_t> validRowsAndBytes()
    {
        size_t rows = 0;
        size_t bytes = 0;
        const auto & pack_stats = dmfile->getPackStats();
        for (size_t i = 0; i < pack_stats.size(); ++i)
        {
            // use_packs[i] can only be 0 or 1
            rows += (pack_stats[i].rows * use_packs[i]);
            bytes += (pack_stats[i].bytes * use_packs[i]);
        }
        return {rows, bytes};
    }

private:
    DMFilePackFilter(
        const DMFilePtr & dmfile_,
        const MinMaxIndexCachePtr & index_cache_,
        bool set_cache_if_miss_,
        const RowKeyRanges & rowkey_ranges_, // filter by handle range
        const RSOperatorPtr & filter_, // filter by push down where clause
        const IdSetPtr & read_packs_, // filter by pack index
        const FileProviderPtr & file_provider_,
        const ReadLimiterPtr & read_limiter_,
        const ScanContextPtr & scan_context_,
        const String & tracing_id)
        : dmfile(dmfile_)
        , index_cache(index_cache_)
        , set_cache_if_miss(set_cache_if_miss_)
        , file_provider(file_provider_)
        , handle_res(dmfile->getPacks(), RSResult::All)
        , use_packs(dmfile->getPacks())
        , scan_context(scan_context_)
        , log(Logger::get(tracing_id))
        , read_limiter(read_limiter_)
    {
        size_t pack_count = dmfile->getPacks();
        auto read_all_packs = (rowkey_ranges_.size() == 1 && rowkey_ranges_[0].all()) || rowkey_ranges_.empty();
        if (!read_all_packs)
        {
            tryLoadIndex(EXTRA_HANDLE_COLUMN_ID);
            std::vector<RSOperatorPtr> handle_filters;
            for (const auto & rowkey_range : rowkey_ranges_)
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
        if (read_packs_)
        {
            for (size_t i = 0; i < pack_count; ++i)
            {
                use_packs[i] = (static_cast<bool>(use_packs[i])) && read_packs_->contains(i);
            }
        }

        for (auto u : use_packs)
            after_read_packs += u;
        ProfileEvents::increment(ProfileEvents::DMFileFilterAftPKAndPackSet, after_read_packs);


        /// Check packs by filter in where clause
        if (filter_)
        {
            // Load index based on filter.
            ColIds ids = filter->getColumnIDs();
            for (const auto & id : ids)
            {
                tryLoadIndex(id);
            }

            Stopwatch watch;
            const auto check_results = filter_->roughCheck(0, pack_count, param);
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
            toDebugString(rowkey_ranges_),
            ((read_packs_ == nullptr) ? 0 : read_packs_->size()),
            pack_count);
    }

    static MinMaxIndexPtr loadIndex(
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
            auto index_guard = S3::S3RandomAccessFile::setReadFileInfo(
                {dmfile->getReadFileSize(col_id, dmfile->colIndexFileName(file_name_base)), scan_context});
            if (!dmfile->configuration) // v1
            {
                auto index_buf = ReadBufferFromFileProvider(
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

                auto buffer = ReadBufferFromFileProvider(
                    file_provider,
                    file_path,
                    encryp_path,
                    dmfile->getConfiguration()->getChecksumFrameLength(),
                    read_limiter);
                buffer.seek(offset);

                String raw_data;
                raw_data.resize(data_size);

                buffer.read(reinterpret_cast<char *>(raw_data.data()), data_size);

                auto buf = createReadBufferFromData(
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
                auto index_buf = createReadBufferFromFileBaseByFileProvider(
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
        }
        else
        {
            // try load from the cache first
            if (index_cache)
                minmax_index = index_cache->get(dmfile->colIndexCacheKey(file_name_base));
            if (minmax_index == nullptr)
                minmax_index = load();
        }
        return minmax_index;
    }

    void tryLoadIndex(const ColId col_id, bool add_into_param = true)
    {
        if (param.indexes.contains(col_id))
            return;

        if (!dmfile->isColIndexExist(col_id))
            return;

        Stopwatch watch;
        const auto minmax_index
            = loadIndex(dmfile, file_provider, index_cache, set_cache_if_miss, col_id, read_limiter, scan_context);

        if (add_into_param)
            param.indexes.emplace(col_id, RSIndex(dmfile->getColumnStat(col_id).type, minmax_index));

        if (col_id == EXTRA_HANDLE_COLUMN_ID)
            handle_index = minmax_index;
        else if (col_id == VERSION_COLUMN_ID)
            version_index = minmax_index;

        scan_context->total_dmfile_rough_set_index_check_time_ns += watch.elapsed();
    }

private:
    const DMFilePtr dmfile;
    const MinMaxIndexCachePtr index_cache;
    const bool set_cache_if_miss;
    const FileProviderPtr file_provider;

    RSCheckParam param;

    std::vector<RSResult> handle_res;
    std::vector<UInt8> use_packs;

    std::optional<MinMaxIndexPtr> handle_index = std::nullopt;
    std::optional<MinMaxIndexPtr> version_index = std::nullopt;

    const ScanContextPtr scan_context;
    LoggerPtr log;
    ReadLimiterPtr read_limiter;
};

} // namespace DM
} // namespace DB
