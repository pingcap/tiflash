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

#include <Storages/DeltaMerge/tests/gtest_segment_test_basic.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <magic_enum.hpp>

namespace CurrentMetrics
{
extern const Metric DT_SnapshotOfSegmentIngest;
} // namespace CurrentMetrics

namespace DB
{
namespace DM
{
namespace tests
{

class SegmentRandomizedTest : public SegmentTestBasic
{
public:
    void run(size_t action_n, Int64 rand_min, Int64 rand_max)
    {
        // Hack: Before doing any operations, let's limit the segment to a smaller range, to make operations related with data more effective.
        {
            RUNTIME_CHECK(rand_min < rand_max, rand_min, rand_max);
            auto id_1 = splitSegmentAt(DELTA_MERGE_FIRST_SEGMENT_ID, rand_min, Segment::SplitMode::Physical);
            RUNTIME_CHECK(id_1.has_value());
            auto id_2 = splitSegmentAt(*id_1, rand_max, Segment::SplitMode::Physical);
            RUNTIME_CHECK(id_2.has_value());

            outbound_left_seg = DELTA_MERGE_FIRST_SEGMENT_ID;
            outbound_right_seg = *id_2;
            segments.erase(outbound_left_seg);
            segments.erase(outbound_right_seg);

            const auto [seg_min, seg_max] = getSegmentKeyRange(*id_1);
            RUNTIME_CHECK(seg_min == rand_min);
            RUNTIME_CHECK(seg_max == rand_max);
        }

        // Workload body: Iterate n times for all possible actions.
        {
            auto probabilities = std::vector<double>{};
            std::transform(actions.begin(), actions.end(), std::back_inserter(probabilities), [](auto v) {
                return v.first;
            });

            auto dist = std::discrete_distribution<size_t>{probabilities.begin(), probabilities.end()};
            for (size_t i = 0; i < action_n; i++)
            {
                auto op_idx = dist(random);
                actions[op_idx].second(this);

                // If there were check errors, there is no need to proceed. Let's just stop here.
                if (::testing::Test::HasFailure())
                    return;

                verifySegmentsIsEmpty();
            }
        }

        // TODO (wenxuan): Add some post checks, like, whether PageStorage has leaks.

        printFinishedOperations();
    }

protected:
    const std::vector<std::pair<double /* probability */, std::function<void(SegmentRandomizedTest *)>>> actions
        = {{1.0, &SegmentRandomizedTest::writeRandomSegment},
           {0.1, &SegmentRandomizedTest::deleteRangeRandomSegment},
           {0.5, &SegmentRandomizedTest::splitRandomSegment},
           {0.5, &SegmentRandomizedTest::splitAtRandomSegment},
           {0.25, &SegmentRandomizedTest::mergeRandomSegments},
           {1.0, &SegmentRandomizedTest::mergeDeltaRandomSegment},
           {1.0, &SegmentRandomizedTest::flushCacheRandomSegment},
           {0.5, &SegmentRandomizedTest::replaceRandomSegmentsData},
           {0.25, &SegmentRandomizedTest::writeRandomSegmentWithDeletedPack},

           // This keeps a for-update snapshot. The snapshot will be revoked before doing other updates.
           {0.25, &SegmentRandomizedTest::prepareReplaceDataSnapshot}};

    SegmentSnapshotPtr for_update_snapshot;
    std::optional<PageIdU64> for_update_snapshot_segment_id;
    std::optional<size_t> for_update_snapshot_rows;

    /**
     * (-∞, rand_min). Hack: This segment is intentionally removed from the "segments" map to avoid being picked up.
     */
    PageIdU64 outbound_left_seg{};

    /**
     * [rand_max, +∞). Hack: This segment is intentionally removed from the "segments" map to avoid being picked up.
     */
    PageIdU64 outbound_right_seg{};

    void clearReplaceDataSnapshot()
    {
        if (for_update_snapshot != nullptr)
        {
            LOG_DEBUG(logger, "cleared for_update snapshot to be used in replace data");
            for_update_snapshot = nullptr;
            for_update_snapshot_segment_id = std::nullopt;
            for_update_snapshot_rows = std::nullopt;
        }
    }

    void prepareReplaceDataSnapshot()
    {
        if (segments.empty())
            return;
        clearReplaceDataSnapshot();
        auto segment_id = getRandomSegmentId();
        for_update_snapshot_segment_id = segment_id;
        LOG_DEBUG(logger, "prepare a for_update snapshot, segment_id={}", segment_id);
        for_update_snapshot = segments[segment_id]->createSnapshot(
            *dm_context,
            /* for_update */ true,
            CurrentMetrics::DT_SnapshotOfSegmentIngest);
        for_update_snapshot_rows = getSegmentRowNumWithoutMVCC(segment_id);
        EXPECT_TRUE(for_update_snapshot != nullptr);
    }

    void verifySegmentsIsEmpty()
    {
        // For all segments, when isEmpty() == true, verify the result against getSegmentRowNum.
        for (const auto & seg_it : segments)
        {
            const auto seg_id = seg_it.first;

            if (for_update_snapshot_segment_id.has_value() && *for_update_snapshot_segment_id == seg_id)
                // Skip segments when there is a buffered for_update snapshot.
                continue;

            if (isSegmentDefinitelyEmpty(seg_id))
            {
                auto rows = getSegmentRowNum(seg_id);
                RUNTIME_CHECK(rows == 0);

                rows = getSegmentRowNumWithoutMVCC(seg_id);
                RUNTIME_CHECK(rows == 0);
            }
        }
    }

    void writeRandomSegment()
    {
        if (segments.empty())
            return;
        auto segment_id = getRandomSegmentId();
        auto write_rows = std::uniform_int_distribution<size_t>{20, 100}(random);
        LOG_DEBUG(
            logger,
            "start random write, segment_id={} write_rows={} all_segments={}",
            segment_id,
            write_rows,
            segments.size());
        writeSegment(segment_id, write_rows);
    }

    void writeRandomSegmentWithDeletedPack()
    {
        if (segments.empty())
            return;
        auto segment_id = getRandomSegmentId();
        auto write_rows = std::uniform_int_distribution<size_t>{20, 100}(random);
        LOG_DEBUG(
            logger,
            "start random write delete, segment_id={} write_rows={} all_segments={}",
            segment_id,
            write_rows,
            segments.size());
        writeSegmentWithDeletedPack(segment_id, write_rows);
    }

    void deleteRangeRandomSegment()
    {
        if (segments.empty())
            return;
        auto segment_id = getRandomSegmentId();
        LOG_DEBUG(logger, "start random delete range, segment_id={} all_segments={}", segment_id, segments.size());
        deleteRangeSegment(segment_id);
    }

    void splitRandomSegment()
    {
        if (segments.empty())
            return;
        // Just don't have too many segments, because it greatly reduces our efficiency of testing
        // correlated actions.
        if (segments.size() > 10)
            return;
        clearReplaceDataSnapshot();
        auto segment_id = getRandomSegmentId();
        auto split_mode = getRandomSplitMode();
        LOG_DEBUG(
            logger,
            "start random split, segment_id={} mode={} all_segments={}",
            segment_id,
            magic_enum::enum_name(split_mode),
            segments.size());
        splitSegment(segment_id, split_mode);
    }

    void splitAtRandomSegment()
    {
        if (segments.empty())
            return;
        if (segments.size() > 10)
            return;
        clearReplaceDataSnapshot();
        auto segment_id = getRandomSegmentId();
        auto split_mode = getRandomSplitMode();
        const auto [start, end] = getSegmentKeyRange(segment_id);
        if (end - start <= 1)
            return;
        auto split_at = std::uniform_int_distribution<Int64>{start, end - 1}(random);
        LOG_DEBUG(
            logger,
            "start random split at, segment_id={} split_at={} mode={} all_segments={}",
            segment_id,
            split_at,
            magic_enum::enum_name(split_mode),
            segments.size());
        splitSegmentAt(segment_id, split_at, split_mode);
    }

    void mergeRandomSegments()
    {
        if (segments.size() < 2)
            return;
        clearReplaceDataSnapshot();
        auto segments_id = getRandomMergeableSegments();
        LOG_DEBUG(
            logger,
            "start random merge, segments_id=[{}] all_segments={}",
            fmt::join(segments_id, ","),
            segments.size());
        mergeSegment(segments_id);
    }

    void mergeDeltaRandomSegment()
    {
        if (segments.empty())
            return;
        clearReplaceDataSnapshot();
        PageIdU64 random_segment_id = getRandomSegmentId();
        LOG_DEBUG(
            logger,
            "start random merge delta, segment_id={} all_segments={}",
            random_segment_id,
            segments.size());
        mergeSegmentDelta(random_segment_id);
    }

    void flushCacheRandomSegment()
    {
        if (segments.empty())
            return;
        PageIdU64 random_segment_id = getRandomSegmentId();
        LOG_DEBUG(
            logger,
            "start random flush cache, segment_id={} all_segments={}",
            random_segment_id,
            segments.size());
        flushSegmentCache(random_segment_id);
    }

    void replaceRandomSegmentsData()
    {
        if (segments.empty())
            return;

        auto segment_id = getRandomSegmentId();
        size_t newly_written_rows_since_snapshot = 0;

        if (for_update_snapshot != nullptr)
        {
            LOG_DEBUG(
                logger,
                "random replace segment data using an existing snapshot, snapshot_segment_id={}",
                *for_update_snapshot_segment_id);
            segment_id = *for_update_snapshot_segment_id;
            newly_written_rows_since_snapshot = getSegmentRowNumWithoutMVCC(segment_id) - *for_update_snapshot_rows;
        }

        auto [min_key, max_key] = getSegmentKeyRange(segment_id);

        std::vector<size_t> n_rows_collection{0, 10, 50, 1000};
        auto block_rows
            = n_rows_collection[std::uniform_int_distribution<size_t>{0, n_rows_collection.size() - 1}(random)];

        Int64 block_start_key = 0, block_end_key = 0;
        Block block{};

        if (block_rows > 0)
        {
            block_start_key = std::uniform_int_distribution<Int64>{min_key - 100, max_key + 100}(random);
            block_end_key = block_start_key + block_rows;
            block = prepareWriteBlock(block_start_key, block_end_key);
        }

        LOG_DEBUG(
            logger,
            "start random replace segment data, segment_id={} block=[{}, {}) all_segments={}",
            segment_id,
            block_start_key,
            block_end_key,
            segments.size());
        replaceSegmentData(segment_id, block, for_update_snapshot);

        size_t data_in_segments = 0;
        if (block_rows > 0)
        {
            auto data_start_key = std::max(min_key, block_start_key);
            auto data_end_key = std::min(max_key, block_end_key);
            data_in_segments = data_end_key >= data_start_key ? data_end_key - data_start_key : 0;
        }

        if (for_update_snapshot == nullptr)
            EXPECT_EQ(getSegmentRowNum(segment_id), data_in_segments);
        else
            EXPECT_EQ(getSegmentRowNumWithoutMVCC(segment_id), data_in_segments + newly_written_rows_since_snapshot);

        clearReplaceDataSnapshot();
    }

    Segment::SplitMode getRandomSplitMode()
    {
        int mode = std::uniform_int_distribution<Int64>{1, 2}(random);
        switch (mode)
        {
        case 1:
            return Segment::SplitMode::Physical;
        case 2:
            return Segment::SplitMode::Logical;
        default:
            throw DB::Exception("Unexpected mode");
        }
    }

    std::vector<PageIdU64> getRandomMergeableSegments()
    {
        RUNTIME_CHECK(segments.size() >= 2, segments.size());

        // Merge 2~6 segments (at most 1/2 of all segments).
        auto max_merge_segments
            = std::uniform_int_distribution<int>{2, std::clamp(static_cast<int>(segments.size()) / 2, 2, 6)}(random);

        std::vector<PageIdU64> segments_id;
        segments_id.reserve(max_merge_segments);

        while (true)
        {
            segments_id.clear();
            segments_id.push_back(getRandomSegmentId());

            for (int i = 1; i < max_merge_segments; i++)
            {
                auto last_segment_id = segments_id.back();
                RUNTIME_CHECK(segments.find(last_segment_id) != segments.end(), last_segment_id);
                auto last_segment = segments[last_segment_id];
                if (last_segment->getRowKeyRange().isEndInfinite())
                    break;
                if (last_segment->nextSegmentId() == outbound_right_seg)
                    break;

                auto next_segment_id = last_segment->nextSegmentId();
                RUNTIME_CHECK(segments.find(next_segment_id) != segments.end(), last_segment->info());
                auto next_segment = segments[next_segment_id];
                RUNTIME_CHECK(next_segment->segmentId() == next_segment_id, next_segment->info(), next_segment_id);
                RUNTIME_CHECK(
                    last_segment->getRowKeyRange().getEnd() == next_segment->getRowKeyRange().getStart(),
                    last_segment->info(),
                    next_segment->info());
                segments_id.push_back(next_segment_id);
            }

            if (segments_id.size() >= 2)
                break;
        }

        return segments_id;
    }
};


TEST_F(SegmentRandomizedTest, FastCommonHandle)
try
{
    reloadWithOptions({.is_common_handle = true});
    run(/* n */ 500, /* min key */ -50000, /* max key */ 50000);
}
CATCH


TEST_F(SegmentRandomizedTest, FastIntHandle)
try
{
    reloadWithOptions({.is_common_handle = false});
    run(/* n */ 500, /* min key */ -50000, /* max key */ 50000);
}
CATCH


// TODO: Run it in CI as a long-running test.
TEST_F(SegmentRandomizedTest, DISABLED_ForCI)
try
{
    reloadWithOptions({.is_common_handle = true});
    run(50000, /* min key */ -50000, /* max key */ 50000);
}
CATCH


} // namespace tests
} // namespace DM
} // namespace DB
