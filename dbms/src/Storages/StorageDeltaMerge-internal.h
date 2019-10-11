#pragma once

#include <algorithm>
#include <vector>

#include <Storages/DeltaMerge/Range.h>
#include <Storages/RegionQueryInfo.h>

namespace DB
{

namespace
{
inline ::DB::HandleID getRangeEndID(const ::DB::TiKVHandle::Handle<HandleID> & end)
{
    switch (end.type)
    {
        case ::DB::TiKVHandle::HandleIDType::NORMAL:
            return end.handle_id;
        case ::DB::TiKVHandle::HandleIDType::MAX:
            return ::DB::DM::HandleRange::MAX;
        default:
            throw Exception("Unknown TiKVHandle type: " + end.toString(), ErrorCodes::LOGICAL_ERROR);
    }
}
} // namespace

inline DM::HandleRanges getQueryRanges(const ::DB::MvccQueryInfo::RegionsQueryInfo & regions)
{
    DM::HandleRanges ranges;
    if (regions.empty())
    {
        // Just for test cases
        ranges.emplace_back(::DB::DM::HandleRange::newAll());
        return ranges;
    }
    else if (regions.size() == 1)
    {
        // Shortcut for only one region info
        DM::HandleRange range;
        const auto & range_in_table = regions[0].range_in_table;
        range.start = range_in_table.first.handle_id;
        range.end = getRangeEndID(range_in_table.second);
        ranges.emplace_back(range);
        return ranges;
    }

    std::vector<size_t> sort_index(regions.size());
    for (size_t i = 0; i < sort_index.size(); ++i)
        sort_index[i] = i;

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
