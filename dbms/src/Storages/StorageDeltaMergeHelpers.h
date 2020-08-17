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

namespace
{
inline DB::HandleID getRangeEndID(const DB::TiKVHandle::Handle<HandleID> & end)
{
    switch (end.type)
    {
        case DB::TiKVHandle::HandleIDType::NORMAL:
            return end.handle_id;
        case DB::TiKVHandle::HandleIDType::MAX:
            return DB::DM::HandleRange::MAX;
        default:
            throw Exception("Unknown TiKVHandle type: " + end.toString(), ErrorCodes::LOGICAL_ERROR);
    }
}
} // namespace

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
            int first_result = handle_ranges[lhs].start->compare(*handle_ranges[rhs].start);
            if (likely(first_result != 0))
                return first_result < 0;
            return handle_ranges[lhs].end->compare(*handle_ranges[rhs].end) < 0;
        });

    ranges.reserve(handle_ranges.size());

    DM::RowKeyRange current;
    StringPtr start;
    StringPtr end;
    for (size_t i = 0; i < handle_ranges.size(); ++i)
    {
        const size_t region_idx = sort_index[i];
        const auto & handle_range = handle_ranges[region_idx];

        if (handle_range.none())
        {
            continue;
        }

        if (i == 0)
        {
            start = handle_range.start;
            end = handle_range.end;
        }
        else if (*end == *handle_range.start)
        {
            // concat this range_in_table to current
            end = handle_range.end;
        }
        else if (end->compare(*handle_range.start) < 0)
        {
            ranges.emplace_back(start, end, is_common_handle, rowkey_column_size);
            // start a new range
            start = handle_range.start;
            end = handle_range.end;
        }
        else
        {
            throw Exception("Overlap region range between " + *start + "," + *end + " and [" //
                + *handle_range.start + "," + *handle_range.end + ")");
        }
    }
    ranges.emplace_back(start, end, is_common_handle, rowkey_column_size);

    return ranges;
}

} // namespace DB
