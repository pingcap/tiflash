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

#include <Common/Logger.h>
#include <Common/TiFlashException.h>
#include <Storages/DeltaMerge/RowKeyRangeUtils.h>
#include <common/logger_useful.h>

namespace DB
{
namespace DM
{
struct OffsetCount;
using OffsetCounts = std::vector<OffsetCount>;


// The information about how the original ranges are merged.
struct OffsetCount
{
    size_t offset;
    size_t count;

public:
    OffsetCount(size_t offset_, size_t count_)
        : offset(offset_)
        , count(count_)
    {}
};

class MergeRangeHelper
{
private:
    RowKeyRanges sorted_ranges;
    OffsetCounts merged_stats;

public:
    explicit MergeRangeHelper(RowKeyRanges && sorted_ranges_)
        : sorted_ranges(std::move(sorted_ranges_))
    {
        genMergeStats();
    }

    void genMergeStats()
    {
        merged_stats.reserve(sorted_ranges.size());

        size_t offset = 0;
        size_t count = 1;
        for (size_t i = 1; i < sorted_ranges.size(); ++i)
        {
            auto & prev = sorted_ranges[i - 1];
            auto & current = sorted_ranges[i];
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
                /// since prev.end.value is excluded and current.start.value is included
                /// it is ok if prev.end.value == prefixNext of current.start.value
                /// refer to https://github.com/pingcap/tidb/blob/v5.0.0/kv/key.go#L38 to see the details of prefix next
                if ((*prev.end.value) == *current.start.toPrefixNext().value)
                    ++count;
                else
                    throw TiFlashException(
                        "Found overlap ranges: " + prev.toDebugString() + ", " + current.toDebugString(),
                        Errors::Coprocessor::BadRequest);
            }
        }
        merged_stats.emplace_back(offset, count);
    }

    void trySplit(size_t expected_ranges_count)
    {
        if (merged_stats.size() >= expected_ranges_count)
            return;

        // Use a heap to pick the range with largest count, then keep splitting it.
        // We don't use std::priority_queue here to avoid copy data around.
        // About heap operations: https://en.cppreference.com/w/cpp/algorithm/make_heap
        auto cmp = [](const OffsetCount & a, const OffsetCount & b) {
            return a.count < b.count;
        };
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
        std::sort(merged_stats.begin(), merged_stats.end(), [](const OffsetCount & a, const OffsetCount & b) {
            return a.offset < b.offset;
        });
    }

    RowKeyRanges getRanges()
    {
        bool is_common_handle = sorted_ranges.front().is_common_handle;
        size_t rowkey_column_size = sorted_ranges.front().rowkey_column_size;

        DM::RowKeyRanges merged_ranges;
        merged_ranges.reserve(merged_stats.size());
        for (auto & stat : merged_stats)
        {
            DM::RowKeyRange range(
                sorted_ranges[stat.offset].start,
                sorted_ranges[stat.offset + stat.count - 1].end,
                is_common_handle,
                rowkey_column_size);
            merged_ranges.push_back(range);
        }
        return merged_ranges;
    }

    size_t currentRangesCount() { return merged_stats.size(); }
};

void sortRangesByStartEdge(RowKeyRanges & ranges)
{
    std::sort(ranges.begin(), ranges.end(), [](const RowKeyRange & lhs, const RowKeyRange & rhs) {
        int start_cmp = lhs.start.value->compare(*(rhs.start.value));
        if (likely(start_cmp != 0))
            return start_cmp < 0;
        return lhs.end.value->compare(*(rhs.end.value)) < 0;
    });
}

RowKeyRanges tryMergeRanges(RowKeyRanges && sorted_ranges, size_t expected_ranges_count, const LoggerPtr & log)
{
    if (sorted_ranges.size() <= 1)
        return std::move(sorted_ranges);

    size_t ori_size = sorted_ranges.size();
    /// First merge continuously ranges together.
    MergeRangeHelper do_merge_ranges(std::move(sorted_ranges));

    size_t after_merge_count = do_merge_ranges.currentRangesCount();

    /// Try to make the number of merged_ranges result larger or equal to expected_ranges_count.
    do_merge_ranges.trySplit(expected_ranges_count);

    if (log)
        LOG_TRACE(
            log,
            "[original ranges: {}] [expected ranges: {}] [after merged ranges: {}] [final ranges: {}]",
            ori_size,
            expected_ranges_count,
            after_merge_count,
            do_merge_ranges.currentRangesCount());

    return do_merge_ranges.getRanges();
}
} // namespace DM
} // namespace DB
