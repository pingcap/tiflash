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
#include <Storages/DeltaMerge/Delta/DeltaValueSpace.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFilePackFilterResult.h>
#include <Storages/DeltaMerge/File/DMFilePackFilter_fwd.h>
#include <Storages/DeltaMerge/Filter/FilterHelper.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/ReadMode.h>
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
    friend class DMFilePackFilterResult;

public:
    // Empty `rowkey_ranges` means do not filter by rowkey_ranges
    static DMFilePackFilterResultPtr loadFrom(
        const DMFilePtr & dmfile,
        const MinMaxIndexCachePtr & index_cache,
        bool set_cache_if_miss,
        const RowKeyRanges & rowkey_ranges,
        const RSOperatorPtr & filter,
        const IdSetPtr & read_packs,
        const FileProviderPtr & file_provider,
        const ReadLimiterPtr & read_limiter,
        const ScanContextPtr & scan_context,
        const String & tracing_id,
        const ReadTag read_tag)
    {
        auto f = DMFilePackFilter(
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
        return f.load(read_tag);
    }

    struct Range
    {
        UInt64 offset;
        UInt64 rows;
        Range(UInt64 offset_, UInt64 rows_)
            : offset(offset_)
            , rows(rows_)
        {}

        bool operator==(const Range &) const = default;
    };
    /**
    * @brief For all the packs in `pack_filter_results`, if all the rows in the pack
    *        comply with RowKey filter and MVCC filter (by `start_ts`) requirements,
    *        and are continuously sorted in delta index, or are deleted, then we skip
    *        reading the packs from disk and return the skipped ranges(not deleted), 
    *        and new PackFilterResults for building bitmap.
    * @return <SkippedRanges, NewPackFilterResults>
    *        - SkippedRanges: All the rows in the ranges that comply with the requirements.
    *        - NewPackFilterResults: Those packs should be read from disk and go through
    *                                the delta merge, RowKey filter, and MVCC filter.
    */
    static std::pair<std::vector<Range>, DMFilePackFilterResults> getSkippedRangeAndFilterForBitmapNormal(
        const DMContext & dm_context,
        const DMFiles & dmfiles,
        const DMFilePackFilterResults & pack_filter_results,
        UInt64 start_ts,
        const DeltaIndexIterator & delta_index_begin,
        const DeltaIndexIterator & delta_index_end);


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

    DMFilePackFilterResultPtr load(ReadTag read_tag);

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

} // namespace DM
} // namespace DB
