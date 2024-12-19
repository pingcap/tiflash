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

#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Index/RSResult.h>

namespace DB::DM
{

class DMFilePackFilterResult;
using DMFilePackFilterResultPtr = std::shared_ptr<DMFilePackFilterResult>;
using DMFilePackFilterResults = std::vector<DMFilePackFilterResultPtr>;

class DMFilePackFilterResult
{
    friend class DMFilePackFilter;

public:
    DMFilePackFilterResult(const DMContext & dm_context_, const DMFilePtr & dmfile_, size_t pack_count_)
        : dm_context(dm_context_)
        , dmfile(dmfile_)
        , handle_res(pack_count_, RSResult::All)
    {}

    const RSResults & getHandleRes() const { return handle_res; }
    const RSResults & getPackResConst() const { return pack_res; }
    RSResults & getPackRes() { return pack_res; }
    UInt64 countUsePack() const;

    Handle getMinHandle(size_t pack_id) const
    {
        if (!param.indexes.count(EXTRA_HANDLE_COLUMN_ID))
            tryLoadIndex(EXTRA_HANDLE_COLUMN_ID);
        auto & minmax_index = param.indexes.find(EXTRA_HANDLE_COLUMN_ID)->second.minmax;
        return minmax_index->getIntMinMax(pack_id).first;
    }

    StringRef getMinStringHandle(size_t pack_id) const
    {
        if (!param.indexes.count(EXTRA_HANDLE_COLUMN_ID))
            tryLoadIndex(EXTRA_HANDLE_COLUMN_ID);
        auto & minmax_index = param.indexes.find(EXTRA_HANDLE_COLUMN_ID)->second.minmax;
        return minmax_index->getStringMinMax(pack_id).first;
    }

    UInt64 getMaxVersion(size_t pack_id) const
    {
        if (!param.indexes.count(VERSION_COLUMN_ID))
            tryLoadIndex(VERSION_COLUMN_ID);
        auto & minmax_index = param.indexes.find(VERSION_COLUMN_ID)->second.minmax;
        return minmax_index->getUInt64MinMax(pack_id).second;
    }

    static DMFilePackFilterResultPtr emptyResult(const DMContext & dm_context, const DMFilePtr & dmfile)
    {
        return std::make_shared<DMFilePackFilterResult>(dm_context, dmfile, 0);
    }

    static DMFilePackFilterResults emptyResults(const DMContext & dm_context, const DMFiles & files)
    {
        DMFilePackFilterResults results;
        results.reserve(files.size());
        for (const auto & file : files)
        {
            results.push_back(emptyResult(dm_context, file));
        }
        return results;
    }

    // Get valid rows and bytes after filter invalid packs by handle_range and filter
    std::pair<size_t, size_t> validRowsAndBytes();

    // None+NoneNull, Some+SomeNull, All, AllNull
    std::tuple<UInt64, UInt64, UInt64, UInt64> countPackRes() const;

private:
    void tryLoadIndex(ColId col_id) const;

private:
    const DMContext & dm_context;

    DMFilePtr dmfile;
    mutable RSCheckParam param;

    // `handle_res` is the filter results of `rowkey_ranges`.
    std::vector<RSResult> handle_res;
    // `pack_res` is the filter results of `rowkey_ranges && filter && read_packs`.
    std::vector<RSResult> pack_res;
};

} // namespace DB::DM
