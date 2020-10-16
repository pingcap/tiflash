#pragma once

#include <Storages/DeltaMerge/Range.h>
#include <Storages/DeltaMerge/RowKeyRangeUtils.h>
#include <Storages/RegionQueryInfo.h>
#include <Storages/Transaction/TiKVHandle.h>

#include <algorithm>
#include <numeric>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

inline DM::RowKeyRanges getQueryRanges(const DB::MvccQueryInfo::RegionsQueryInfo & regions, TableID table_id, bool is_common_handle,
    size_t rowkey_column_size, size_t expected_ranges_count = 1, Logger * log = nullptr)
{
    // todo check table id in DecodedTiKVKey???
    DM::RowKeyRanges ranges;
    for (const auto & region_info : regions)
    {
        if (!region_info.required_handle_ranges.empty())
        {
            for (const auto & handle_range : region_info.required_handle_ranges)
                ranges.push_back(
                    DM::RowKeyRange::fromRegionRange(handle_range, table_id, table_id, is_common_handle, rowkey_column_size));
        }
        else
        {
            /// only used for test cases
            ranges.push_back(
                DM::RowKeyRange::fromRegionRange(region_info.range_in_table, table_id, table_id, is_common_handle, rowkey_column_size));
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
