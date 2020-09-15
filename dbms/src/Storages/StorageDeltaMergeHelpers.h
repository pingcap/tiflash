#pragma once

#include <Storages/DeltaMerge/Range.h>
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

inline DM::RowKeyRanges getQueryRanges(
    const DB::MvccQueryInfo::RegionsQueryInfo & regions, TableID table_id, bool is_common_handle, size_t rowkey_column_size)
{
    // todo check table id in DecodedTiKVKey???
    DM::RowKeyRanges handle_ranges;
    for (const auto & region_info : regions)
    {
        if (!region_info.required_handle_ranges.empty())
        {
            for (const auto & handle_range : region_info.required_handle_ranges)
                handle_ranges.push_back(
                    DM::RowKeyRange::fromRegionRange(handle_range, table_id, table_id, is_common_handle, rowkey_column_size));
        }
        else
        {
            /// only used for test cases
            handle_ranges.push_back(
                DM::RowKeyRange::fromRegionRange(region_info.range_in_table, table_id, table_id, is_common_handle, rowkey_column_size));
        }
    }
    if (handle_ranges.empty())
    {
        // Just for test cases
        handle_ranges.emplace_back(DB::DM::RowKeyRange::newAll(is_common_handle, rowkey_column_size));
        return handle_ranges;
    }
    else if (handle_ranges.size() == 1)
    {
        // Shortcut for only one region info
        return handle_ranges;
    }

    DM::RowKeyRanges ranges;
    // Init index with [0, n)
    // http: //www.cplusplus.com/reference/numeric/iota/
    std::vector<size_t> sort_index(handle_ranges.size());
    std::iota(sort_index.begin(), sort_index.end(), 0);

    std::sort(sort_index.begin(), sort_index.end(), //
        [&handle_ranges](const size_t lhs, const size_t rhs) {
            int first_result = handle_ranges[lhs].start.value->compare(*handle_ranges[rhs].start.value);
            if (likely(first_result != 0))
                return first_result < 0;
            return handle_ranges[lhs].end.value->compare(*handle_ranges[rhs].end.value) < 0;
        });

    ranges.reserve(handle_ranges.size());

    DM::RowKeyRange current = handle_ranges[sort_index[0]];
    for (size_t i = 1; i < handle_ranges.size(); ++i)
    {
        const size_t region_idx = sort_index[i];
        const auto & handle_range = handle_ranges[region_idx];

        if (handle_range.none())
        {
            continue;
        }

        if (*current.end.value == *handle_range.start.value)
        {
            // concat this range_in_table to current
            current.end = handle_range.end;
        }
        else if (current.end.value->compare(*handle_range.start.value) < 0)
        {
            ranges.emplace_back(current);
            // start a new range
            current.start = handle_range.start;
            current.end = handle_range.end;
        }
        else
        {
            throw Exception("Overlap region range between " + current.start.toString() + "," + current.end.toString() + " and [" //
                + handle_range.start.toString() + "," + handle_range.end.toString() + ")");
        }
    }
    ranges.emplace_back(current);

    return ranges;
}

} // namespace DB
