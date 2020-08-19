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

inline DM::HandleRange toDMHandleRange(const HandleRange<HandleID> & range)
{
    return DM::HandleRange{range.first.handle_id, getRangeEndID(range.second)};
}

inline DM::HandleRanges getQueryRanges(const DB::MvccQueryInfo::RegionsQueryInfo & regions)
{
    std::vector<HandleRange<HandleID>> handle_ranges;
    for (const auto & region_info : regions)
    {
        if (!region_info.required_handle_ranges.empty())
        {
            for (const auto & handle_range : region_info.required_handle_ranges)
                handle_ranges.push_back(handle_range);
        }
        else
        {
            /// only used for test cases
            handle_ranges.push_back(region_info.range_in_table);
        }
    }
    DM::HandleRanges ranges;
    if (handle_ranges.empty())
    {
        // Just for test cases
        ranges.emplace_back(DB::DM::HandleRange::newAll());
        return ranges;
    }
    else if (handle_ranges.size() == 1)
    {
        // Shortcut for only one region info
        const auto & range = handle_ranges[0];
        if (unlikely(range.first.type == DB::TiKVHandle::HandleIDType::MAX))
            throw TiFlashException("Income handle range [" + range.first.toString() + "," + range.second.toString()
                    + ") is illegal for region: " + DB::toString(regions[0].region_id),
                Errors::Coprocessor::BadRequest);
        else
            ranges.emplace_back(toDMHandleRange(range));
        return ranges;
    }

    // Init index with [0, n)
    // http: //www.cplusplus.com/reference/numeric/iota/
    std::vector<size_t> sort_index(handle_ranges.size());
    std::iota(sort_index.begin(), sort_index.end(), 0);

    std::sort(sort_index.begin(), sort_index.end(), //
        [&handle_ranges](const size_t lhs, const size_t rhs) { return handle_ranges[lhs] < handle_ranges[rhs]; });

    ranges.reserve(handle_ranges.size());

    DM::HandleRange current;
    for (size_t i = 0; i < handle_ranges.size(); ++i)
    {
        const size_t region_idx = sort_index[i];
        const auto & handle_range = handle_ranges[region_idx];

        if (handle_range.first.type == DB::TiKVHandle::HandleIDType::MAX)
        {
            // Ignore [Max, Max)
            if (handle_range.second.type == DB::TiKVHandle::HandleIDType::MAX)
                continue;
            else
                throw Exception(
                    "Can not merge invalid region range: [" + handle_range.first.toString() + "," + handle_range.second.toString() + ")",
                    ErrorCodes::LOGICAL_ERROR);
        }

        if (i == 0)
        {
            current.start = handle_range.first.handle_id;
            current.end = getRangeEndID(handle_range.second);
        }
        else if (current.end == handle_range.first.handle_id)
        {
            // concat this range_in_table to current
            current.end = getRangeEndID(handle_range.second);
        }
        else if (current.end < handle_range.first.handle_id)
        {
            ranges.emplace_back(current);

            // start a new range
            current.start = handle_range.first.handle_id;
            current.end = getRangeEndID(handle_range.second);
        }
        else
        {
            throw Exception("Overlap region range between " + current.toString() + " and [" //
                + handle_range.first.toString() + "," + handle_range.second.toString() + ")");
        }
    }
    ranges.emplace_back(current);

    return ranges;
}

} // namespace DB
