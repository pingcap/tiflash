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

#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/File/DMFilePackFilter.h>
#include <Storages/DeltaMerge/File/DMFilePackFilterResult.h>

namespace DB::DM
{

UInt64 DMFilePackFilterResult::countUsePack() const
{
    return std::count_if(pack_res.begin(), pack_res.end(), [](RSResult res) { return res.isUse(); });
}

std::pair<size_t, size_t> DMFilePackFilterResult::validRowsAndBytes()
{
    size_t rows = 0;
    size_t bytes = 0;
    const auto & pack_stats = dmfile->getPackStats();
    for (size_t i = 0; i < pack_stats.size(); ++i)
    {
        if (pack_res[i].isUse())
        {
            rows += pack_stats[i].rows;
            bytes += pack_stats[i].bytes;
        }
    }
    return {rows, bytes};
}

std::tuple<UInt64, UInt64, UInt64, UInt64> DMFilePackFilterResult::countPackRes() const
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

void DMFilePackFilterResult::tryLoadIndex(ColId col_id) const
{
    if (param.indexes.count(col_id))
        return;

    if (!dmfile->isColIndexExist(col_id))
        return;

    Stopwatch watch;
    DMFilePackFilter::loadIndex(
        param.indexes,
        dmfile,
        dm_context.global_context.getFileProvider(),
        dm_context.global_context.getMinMaxIndexCache(),
        true,
        col_id,
        dm_context.global_context.getReadLimiter(),
        dm_context.scan_context);
}

} // namespace DB::DM
