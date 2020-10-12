#pragma once

#include <Storages/DeltaMerge/Range.h>
#include <Storages/DeltaMerge/RangeUtils.h>
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

inline DM::HandleRange toDMHandleRange(const HandleRange<HandleID> & range)
{
    return DM::HandleRange{range.first.handle_id, getRangeEndID(range.second)};
}

inline DM::HandleRanges getDMRanges(const DB::MvccQueryInfo::RegionsQueryInfo & regions)
{
    DM::HandleRanges ranges;
    for (const auto & region_info : regions)
    {
        if (!region_info.required_handle_ranges.empty())
        {
            for (const auto & handle_range : region_info.required_handle_ranges)
                ranges.push_back(toDMHandleRange(handle_range));
        }
        else
        {
            /// only used for test cases
            ranges.push_back(toDMHandleRange(region_info.range_in_table));
        }
    }
    if (ranges.empty())
    {
        // Just for test cases
        ranges.emplace_back(DB::DM::HandleRange::newAll());
        return ranges;
    }

    return ranges;
}

inline DM::HandleRanges getQueryRanges(
    const DB::MvccQueryInfo::RegionsQueryInfo & regions, size_t expected_ranges_count = 1, Logger * log = nullptr)
{
    auto ranges = getDMRanges(regions);
    DM::sortRangesByStartEdge(ranges);
    return tryMergeRanges(std::move(ranges), expected_ranges_count, log);
}

} // namespace DB
