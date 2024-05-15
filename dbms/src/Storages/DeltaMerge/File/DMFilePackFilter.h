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
#include <IO/FileProvider/ChecksumReadBufferBuilder.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFilePackFilter_fwd.h>
#include <Storages/DeltaMerge/Filter/FilterHelper.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/ScanContext_fwd.h>
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
        pack_filter.init();
        return pack_filter;
    }

    const RSResults & getHandleRes() const { return handle_res; }
    const RSResults & getPackResConst() const { return pack_res; }
    RSResults & getPackRes() { return pack_res; }

    // None, Some, All
    std::tuple<UInt64, UInt64, UInt64> countPackRes() const;
    UInt64 countUsePack() const;

    Handle getMinHandle(size_t pack_id)
    {
        if (!param.indexes.count(EXTRA_HANDLE_COLUMN_ID))
            tryLoadIndex(EXTRA_HANDLE_COLUMN_ID);
        auto & minmax_index = param.indexes.find(EXTRA_HANDLE_COLUMN_ID)->second.minmax;
        return minmax_index->getIntMinMax(pack_id).first;
    }

    StringRef getMinStringHandle(size_t pack_id)
    {
        if (!param.indexes.count(EXTRA_HANDLE_COLUMN_ID))
            tryLoadIndex(EXTRA_HANDLE_COLUMN_ID);
        auto & minmax_index = param.indexes.find(EXTRA_HANDLE_COLUMN_ID)->second.minmax;
        return minmax_index->getStringMinMax(pack_id).first;
    }

    UInt64 getMaxVersion(size_t pack_id)
    {
        if (!param.indexes.count(VERSION_COLUMN_ID))
            tryLoadIndex(VERSION_COLUMN_ID);
        auto & minmax_index = param.indexes.find(VERSION_COLUMN_ID)->second.minmax;
        return minmax_index->getUInt64MinMax(pack_id).second;
    }

    // Get valid rows and bytes after filter invalid packs by handle_range and filter
    std::pair<size_t, size_t> validRowsAndBytes()
    {
        size_t rows = 0;
        size_t bytes = 0;
        const auto & pack_stats = dmfile->getPackStats();
        for (size_t i = 0; i < pack_stats.size(); ++i)
        {
            if (pack_res[i] != RSResult::None)
            {
                rows += pack_stats[i].rows;
                bytes += pack_stats[i].bytes;
            }
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
        , rowkey_ranges(rowkey_ranges_)
        , filter(filter_)
        , read_packs(read_packs_)
        , file_provider(file_provider_)
        , handle_res(dmfile->getPacks(), RSResult::All)
        , scan_context(scan_context_)
        , log(Logger::get(tracing_id))
        , read_limiter(read_limiter_)
    {}

    void init();

    static void loadIndex(
        ColumnIndexes & indexes,
        const DMFilePtr & dmfile,
        const FileProviderPtr & file_provider,
        const MinMaxIndexCachePtr & index_cache,
        bool set_cache_if_miss,
        ColId col_id,
        const ReadLimiterPtr & read_limiter,
        const ScanContextPtr & scan_context);

    void tryLoadIndex(ColId col_id);

private:
    DMFilePtr dmfile;
    MinMaxIndexCachePtr index_cache;
    bool set_cache_if_miss;
    RowKeyRanges rowkey_ranges;
    RSOperatorPtr filter;
    IdSetPtr read_packs;
    FileProviderPtr file_provider;

    RSCheckParam param;

    std::vector<RSResult> handle_res;
    std::vector<RSResult> pack_res;

    const ScanContextPtr scan_context;

    LoggerPtr log;
    ReadLimiterPtr read_limiter;
};

} // namespace DM
} // namespace DB
