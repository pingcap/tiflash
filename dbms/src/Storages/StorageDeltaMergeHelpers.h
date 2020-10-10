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

inline DM::RowKeyRanges getQueryRanges(const DB::MvccQueryInfo::RegionsQueryInfo & regions,
    TableID table_id,
    bool is_common_handle,
    size_t rowkey_column_size,
    size_t expected_ranges_count = 1,
    Logger * log = nullptr)
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

    /// Sort the handle_ranges.
    std::sort(handle_ranges.begin(), handle_ranges.end(), [](const DM::RowKeyRange & lhs, const DM::RowKeyRange & rhs) {
        int first_result = lhs.start.value->compare(*rhs.start.value);
        if (likely(first_result != 0))
            return first_result < 0;
        return lhs.end.value->compare(*rhs.end.value) < 0;
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
        auto cmp_res = (*prev.end.value).compare(*current.start.value);
        if (current.none() || cmp_res == 0)
        {
            // Merge current range into previous
            ++count;
        }
        else if (cmp_res < 0)
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

    /// Try to make the number of merged_ranges result larger or equal to expected_ranges_count. So that we can do parallelize the read process.
    if (merged_stats.size() < expected_ranges_count)
    {
        // Use a heap to pick the range with largest count, then keep splitting it.
        // We don't use std::priority_queue here to avoid copying data around.
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

        // Sort by offset, so that we can have an increasing order by range's start edge.
        std::sort(
            merged_stats.begin(), merged_stats.end(), [](const OffsetCount & a, const OffsetCount & b) { return a.offset < b.offset; });
    }

    /// Generate merged result.
    DM::RowKeyRanges merged_ranges;
    merged_ranges.reserve(merged_stats.size());
    for (auto stat : merged_stats)
    {
        DM::RowKeyRange range(
            handle_ranges[stat.offset].start, handle_ranges[stat.offset + stat.count - 1].end, is_common_handle, rowkey_column_size);
        merged_ranges.push_back(range);
    }

    if (log)
        LOG_TRACE(log,
            "Merge ranges: [original ranges: " << handle_ranges.size() << "] [expected ranges: " << expected_ranges_count
                                               << "] [after merged ranges: " << after_merge_count
                                               << "] [final ranges: " << merged_ranges.size() << "]");

    return merged_ranges;
}

} // namespace DB
