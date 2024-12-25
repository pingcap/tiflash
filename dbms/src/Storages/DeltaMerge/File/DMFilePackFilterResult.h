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

#pragma once

#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Index/RSResult.h>

namespace DB::DM
{

class DMFilePackFilterResult
{
    friend class DMFilePackFilter;

    DMFilePackFilterResult(
        const MinMaxIndexCachePtr & index_cache_,
        const ReadLimiterPtr & read_limiter_,
        size_t pack_count)
        : index_cache(index_cache_)
        , read_limiter(read_limiter_)
        , handle_res(pack_count, RSResult::All)
        , pack_res(pack_count, RSResult::Some)
    {}

public:
    const RSResults & getHandleRes() const { return handle_res; }
    const RSResults & getPackRes() const { return pack_res; }
    UInt64 countUsePack() const;

    Handle getMinHandle(
        const DMFilePtr & dmfile,
        size_t pack_id,
        const FileProviderPtr & file_provider,
        const ScanContextPtr & scan_context) const
    {
        if (!param.indexes.count(EXTRA_HANDLE_COLUMN_ID))
            tryLoadIndex(dmfile, EXTRA_HANDLE_COLUMN_ID, file_provider, scan_context);
        auto & minmax_index = param.indexes.find(EXTRA_HANDLE_COLUMN_ID)->second.minmax;
        return minmax_index->getIntMinMax(pack_id).first;
    }

    StringRef getMinStringHandle(
        const DMFilePtr & dmfile,
        size_t pack_id,
        const FileProviderPtr & file_provider,
        const ScanContextPtr & scan_context) const
    {
        if (!param.indexes.count(EXTRA_HANDLE_COLUMN_ID))
            tryLoadIndex(dmfile, EXTRA_HANDLE_COLUMN_ID, file_provider, scan_context);
        auto & minmax_index = param.indexes.find(EXTRA_HANDLE_COLUMN_ID)->second.minmax;
        return minmax_index->getStringMinMax(pack_id).first;
    }

    UInt64 getMaxVersion(
        const DMFilePtr & dmfile,
        size_t pack_id,
        const FileProviderPtr & file_provider,
        const ScanContextPtr & scan_context) const
    {
        if (!param.indexes.count(VERSION_COLUMN_ID))
            tryLoadIndex(dmfile, VERSION_COLUMN_ID, file_provider, scan_context);
        auto & minmax_index = param.indexes.find(VERSION_COLUMN_ID)->second.minmax;
        return minmax_index->getUInt64MinMax(pack_id).second;
    }

    // None+NoneNull, Some+SomeNull, All, AllNull
    std::tuple<UInt64, UInt64, UInt64, UInt64> countPackRes() const;

private:
    void tryLoadIndex(
        const DMFilePtr & dmfile,
        ColId col_id,
        const FileProviderPtr & file_provider,
        const ScanContextPtr & scan_context) const;

private:
    MinMaxIndexCachePtr index_cache;
    ReadLimiterPtr read_limiter;

    mutable RSCheckParam param;

    // `handle_res` is the filter results of `rowkey_ranges`.
    std::vector<RSResult> handle_res;
    // `pack_res` is the filter results of `rowkey_ranges && filter && read_packs`.
    std::vector<RSResult> pack_res;
};

} // namespace DB::DM
