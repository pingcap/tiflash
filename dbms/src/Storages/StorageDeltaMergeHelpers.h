#pragma once

#include <Storages/DeltaMerge/Range.h>
#include <Storages/RegionQueryInfo.h>
#include <Storages/Transaction/TiKVHandle.h>

#include <algorithm>
#include <numeric>
#include <vector>

namespace DB
{

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

inline DM::HandleRange toDMHandleRange(const HandleRange<HandleID> & range)
{
    return DM::HandleRange{range.first.handle_id, getRangeEndID(range.second)};
}

inline DM::HandleRanges getQueryRanges(const DB::MvccQueryInfo::RegionsQueryInfo & regions)
{
    DM::HandleRanges ranges;
    if (regions.empty())
    {
        // Just for test cases
        ranges.emplace_back(DB::DM::HandleRange::newAll());
        return ranges;
    }
    else if (regions.size() == 1)
    {
        // Shortcut for only one region info
        const auto & range_in_table = regions[0].range_in_table;
        ranges.emplace_back(toDMHandleRange(range_in_table));
        return ranges;
    }

    // Init index with [0, n)
    // http: //www.cplusplus.com/reference/numeric/iota/
    std::vector<size_t> sort_index(regions.size());
    std::iota(sort_index.begin(), sort_index.end(), 0);

    std::sort(sort_index.begin(), sort_index.end(), //
        [&regions](const size_t lhs, const size_t rhs) { return regions[lhs] < regions[rhs]; });

    ranges.reserve(regions.size());

    DM::HandleRange current;
    for (size_t i = 0; i < regions.size(); ++i)
    {
        const size_t region_idx = sort_index[i];
        const auto & region = regions[region_idx];
        const auto & range_in_table = region.range_in_table;

        if (i == 0)
        {
            current.start = range_in_table.first.handle_id;
            current.end = getRangeEndID(range_in_table.second);
        }
        else if (current.end == range_in_table.first.handle_id)
        {
            // concat this range_in_table to current
            current.end = getRangeEndID(range_in_table.second);
        }
        else if (current.end < range_in_table.first.handle_id)
        {
            ranges.emplace_back(current);

            // start a new range
            current.start = range_in_table.first.handle_id;
            current.end = getRangeEndID(range_in_table.second);
        }
        else
        {
            throw Exception("Overlap region range between " + current.toString() + " and [" //
                + range_in_table.first.toString() + "," + range_in_table.second.toString() + ")");
        }
    }
    ranges.emplace_back(current);

    return ranges;
}

} // namespace DB
