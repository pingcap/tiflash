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

#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFilePackFilterResult.h>
#include <Storages/DeltaMerge/File/DMFilePackFilter_fwd.h>
#include <Storages/DeltaMerge/Filter/RSOperator_fwd.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/ScanContext_fwd.h>


namespace ProfileEvents
{
extern const Event DMFileFilterNoFilter;
extern const Event DMFileFilterAftPKAndPackSet;
extern const Event DMFileFilterAftRoughSet;
} // namespace ProfileEvents

namespace DB::DM
{

class DMFilePackFilter
{
    friend class DMFilePackFilterResult;

public:
    // Empty `rowkey_ranges` means do not filter by rowkey_ranges
    static DMFilePackFilterResultPtr loadFrom(
        const DMContext & dm_context,
        const DMFilePtr & dmfile,
        bool set_cache_if_miss,
        const RowKeyRanges & rowkey_ranges,
        const RSOperatorPtr & filter,
        const IdSetPtr & read_packs)
    {
        DMFilePackFilter pack_filter(
            dmfile,
            dm_context.global_context.getMinMaxIndexCache(),
            set_cache_if_miss,
            rowkey_ranges,
            filter,
            read_packs,
            dm_context.global_context.getFileProvider(),
            dm_context.global_context.getReadLimiter(),
            dm_context.scan_context,
            dm_context.tracing_id);
        return pack_filter.load();
    }

    static DMFilePackFilterResultPtr loadFrom(
        const MinMaxIndexCachePtr & index_cache_,
        const FileProviderPtr & file_provider_,
        const ReadLimiterPtr & read_limiter_,
        const ScanContextPtr & scan_context,
        const DMFilePtr & dmfile,
        bool set_cache_if_miss,
        const RowKeyRanges & rowkey_ranges,
        const RSOperatorPtr & filter,
        const IdSetPtr & read_packs,
        const String & tracing_id)
    {
        DMFilePackFilter pack_filter(
            dmfile,
            index_cache_,
            set_cache_if_miss,
            rowkey_ranges,
            filter,
            read_packs,
            file_provider_,
            read_limiter_,
            scan_context,
            tracing_id);
        return pack_filter.load();
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
        , scan_context(scan_context_)
        , log(Logger::get(tracing_id))
        , read_limiter(read_limiter_)
    {}

    DMFilePackFilterResultPtr load();

    static void loadIndex(
        ColumnIndexes & indexes,
        const DMFilePtr & dmfile,
        const FileProviderPtr & file_provider,
        const MinMaxIndexCachePtr & index_cache,
        bool set_cache_if_miss,
        ColId col_id,
        const ReadLimiterPtr & read_limiter,
        const ScanContextPtr & scan_context);

    void tryLoadIndex(RSCheckParam & param, ColId col_id);

private:
    DMFilePtr dmfile;

    MinMaxIndexCachePtr index_cache;
    bool set_cache_if_miss;
    RowKeyRanges rowkey_ranges;
    RSOperatorPtr filter;
    IdSetPtr read_packs;
    FileProviderPtr file_provider;

    const ScanContextPtr scan_context;

    LoggerPtr log;
    ReadLimiterPtr read_limiter;
};

} // namespace DB::DM
