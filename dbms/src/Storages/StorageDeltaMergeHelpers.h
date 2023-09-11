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

#include <Common/Logger.h>
#include <Storages/DeltaMerge/Range.h>
#include <Storages/DeltaMerge/RowKeyRangeUtils.h>
#include <Storages/KVStore/Decode/TiKVHandle.h>
#include <Storages/RegionQueryInfo.h>

#include <algorithm>
#include <numeric>
#include <vector>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

inline DM::RowKeyRanges getQueryRanges(
    const DB::MvccQueryInfo::RegionsQueryInfo & regions,
    TableID table_id,
    bool is_common_handle,
    size_t rowkey_column_size,
    size_t expected_ranges_count = 1,
    const LoggerPtr & log = nullptr)
{
    // todo check table id in DecodedTiKVKey???
    DM::RowKeyRanges ranges;
    for (const auto & region_info : regions)
    {
        if (!region_info.required_handle_ranges.empty())
        {
            for (const auto & handle_range : region_info.required_handle_ranges)
                ranges.push_back(DM::RowKeyRange::fromRegionRange(
                    handle_range,
                    table_id,
                    table_id,
                    is_common_handle,
                    rowkey_column_size));
        }
        else
        {
            /// only used for test cases
            ranges.push_back(DM::RowKeyRange::fromRegionRange(
                region_info.range_in_table,
                table_id,
                table_id,
                is_common_handle,
                rowkey_column_size));
        }
    }
    if (ranges.empty())
    {
        // Just for test cases
        ranges.emplace_back(DB::DM::RowKeyRange::newAll(is_common_handle, rowkey_column_size));
    }

    if (ranges.size() == 1)
    {
        // Shortcut for only one region info
        return ranges;
    }

    DM::sortRangesByStartEdge(ranges);
    return tryMergeRanges(std::move(ranges), expected_ranges_count, log);
}

} // namespace DB
