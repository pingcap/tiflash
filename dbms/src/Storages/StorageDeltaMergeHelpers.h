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

inline DM::HandleRanges getQueryRanges(
    const DB::MvccQueryInfo::RegionsQueryInfo & regions, size_t expected_ranges_count = 1, Logger * log = nullptr)
{
    DM::HandleRanges handle_ranges;
    for (const auto & region_info : regions)
    {
        if (!region_info.required_handle_ranges.empty())
        {
            for (const auto & handle_range : region_info.required_handle_ranges)
                handle_ranges.push_back(toDMHandleRange(handle_range));
        }
        else
        {
            /// only used for test cases
            handle_ranges.push_back(toDMHandleRange(region_info.range_in_table));
        }
    }
    if (handle_ranges.empty())
    {
        // Just for test cases
        handle_ranges.emplace_back(DB::DM::HandleRange::newAll());
        return handle_ranges;
    }
    else if (handle_ranges.size() == 1)
    {
        // Shortcut for only one region info
        return handle_ranges;
    }

    /// Sort the handle_ranges.
    std::sort(handle_ranges.begin(), handle_ranges.end(), [](const DM::HandleRange & lhs, const DM::HandleRange & rhs) {
        if (likely(lhs.start != rhs.start))
            return lhs.start < rhs.start;
        return lhs.end < rhs.end;
    });

    // The information about how the original ranges are merged.
    struct OffsetCount
    {
        size_t offset;
        size_t count;

    public:
        OffsetCount(size_t offset_, size_t count_) : offset(offset_), count(count_) {}
    };
    using OffsetCounts = std::vector<OffsetCount>;
    OffsetCounts merged_stats;
    merged_stats.reserve(expected_ranges_count);

    size_t offset = 0;
    size_t count = 1;
    for (size_t i = 1; i < handle_ranges.size(); ++i)
    {
        auto & prev = handle_ranges[i - 1];
        auto & current = handle_ranges[i];
        if (current.none() || prev.end == current.start)
        {
            // Merge current range into previous
            ++count;
        }
        else if (prev.end < current.start)
        {
            // Cannot merge, let's move on.
            merged_stats.emplace_back(offset, count);
            offset = i;
            count = 1;
        }
        else
        {
            throw Exception("Found overlap ranges: " + prev.toString() + ", " + current.toString());
        }
    }
    merged_stats.emplace_back(offset, count);

    size_t after_merge_count = merged_stats.size();

    /// Try to make the number of merged_ranges result larger or equal to expected_ranges_count. So that we can do parallelization.
    if (merged_stats.size() < expected_ranges_count)
    {
        // Use a heap to pick the range with largest count, then keep splitting it.
        // We don't use std::priority_queue here to avoid copy data around.
        // About heap operations: https://en.cppreference.com/w/cpp/algorithm/make_heap
        auto cmp = [](const OffsetCount & a, const OffsetCount & b) { return a.count < b.count; };
        std::make_heap(merged_stats.begin(), merged_stats.end(), cmp);

        // The range with largest count is retrieved by front().
        while (merged_stats.size() < expected_ranges_count && merged_stats.front().count > 1)
        {
            std::pop_heap(merged_stats.begin(), merged_stats.end(), cmp);

            auto top = merged_stats.back();
            merged_stats.pop_back();

            size_t split_count = top.count / 2;

            merged_stats.emplace_back(top.offset, split_count);
            std::push_heap(merged_stats.begin(), merged_stats.end(), cmp);

            merged_stats.emplace_back(top.offset + split_count, top.count - split_count);
            std::push_heap(merged_stats.begin(), merged_stats.end(), cmp);
        }

        // Sort by offset, so that we can have an increasing order by range's start.
        std::sort(
            merged_stats.begin(), merged_stats.end(), [](const OffsetCount & a, const OffsetCount & b) { return a.offset < b.offset; });
    }

    DM::HandleRanges merged_ranges;
    merged_ranges.reserve(merged_stats.size());
    for (auto stat : merged_stats)
    {
        DM::HandleRange range(handle_ranges[stat.offset].start, handle_ranges[stat.offset + stat.count - 1].end);
        merged_ranges.push_back(range);
    }

    if (log)
        LOG_TRACE(log,
            "Merge ranges: [original ranges: " << handle_ranges.size() << "] [expected ranges: " << expected_ranges_count
                                               << "] [after merged ranges: " << after_merge_count << "] [final ranges: " << merged_ranges.size()
                                               << "]");

    return merged_ranges;
}

} // namespace DB
