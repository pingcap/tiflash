// Copyright 2022 PingCAP, Ltd.
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
            std::transform(actions.begin(), actions.end(), std::back_inserter(probabilities), [](auto v) { return v.first; });

            auto dist = std::discrete_distribution<size_t>{probabilities.begin(), probabilities.end()};
            for (size_t i = 0; i < action_n; i++)
            {
                auto op_idx = dist(random);
                actions[op_idx].second(this);
            }
        }

        // TODO (wenxuan): Add some post checks, like, whether PageStorage has leaks.

        printFinishedOperations();
    }

protected:
    const std::vector<std::pair<double /* probability */, std::function<void(SegmentRandomizedTest *)>>> actions = {
        {1.0, &SegmentRandomizedTest::writeRandomSegment},
        {0.1, &SegmentRandomizedTest::deleteRangeRandomSegment},
        {1.0, &SegmentRandomizedTest::splitRandomSegment},
        {1.0, &SegmentRandomizedTest::splitAtRandomSegment},
        {0.25, &SegmentRandomizedTest::mergeRandomSegment},
        {1.0, &SegmentRandomizedTest::mergeDeltaRandomSegment},
        {1.0, &SegmentRandomizedTest::flushCacheRandomSegment},
        {0.25, &SegmentRandomizedTest::writeRandomSegmentWithDeletedPack}};

    /**
     * (-∞, rand_min). Hack: This segment is intentionally removed from the "segments" map to avoid being picked up.
     */
    PageId outbound_left_seg{};

    /**
     * [rand_max, +∞). Hack: This segment is intentionally removed from the "segments" map to avoid being picked up.
     */
    PageId outbound_right_seg{};

    void writeRandomSegment()
    {
        if (segments.empty())
            return;
        auto segment_id = getRandomSegmentId();
        auto write_rows = std::uniform_int_distribution<size_t>{20, 100}(random);
        LOG_FMT_DEBUG(logger, "start random write, segment_id={} write_rows={} all_segments={}", segment_id, write_rows, segments.size());
        writeSegment(segment_id, write_rows);
    }

    void writeRandomSegmentWithDeletedPack()
    {
        if (segments.empty())
            return;
        auto segment_id = getRandomSegmentId();
        auto write_rows = std::uniform_int_distribution<size_t>{20, 100}(random);
        LOG_FMT_DEBUG(logger, "start random write delete, segment_id={} write_rows={} all_segments={}", segment_id, write_rows, segments.size());
        writeSegmentWithDeletedPack(segment_id, write_rows);
    }

    void deleteRangeRandomSegment()
    {
        if (segments.empty())
            return;
        auto segment_id = getRandomSegmentId();
        LOG_FMT_DEBUG(logger, "start random delete range, segment_id={} all_segments={}", segment_id, segments.size());
        deleteRangeSegment(segment_id);
    }

    void splitRandomSegment()
    {
        if (segments.empty())
            return;
        auto segment_id = getRandomSegmentId();
        auto split_mode = getRandomSplitMode();
        LOG_FMT_DEBUG(logger, "start random split, segment_id={} mode={} all_segments={}", segment_id, magic_enum::enum_name(split_mode), segments.size());
        splitSegment(segment_id, split_mode);
    }

    void splitAtRandomSegment()
    {
        if (segments.empty())
            return;
        auto segment_id = getRandomSegmentId();
        auto split_mode = getRandomSplitMode();
        const auto [start, end] = getSegmentKeyRange(segment_id);
        if (end - start <= 1)
            return;
        auto split_at = std::uniform_int_distribution<Int64>{start, end - 1}(random);
        LOG_FMT_DEBUG(logger, "start random split at, segment_id={} split_at={} mode={} all_segments={}", segment_id, split_at, magic_enum::enum_name(split_mode), segments.size());
        splitSegmentAt(segment_id, split_at, split_mode);
    }

    void mergeRandomSegment()
    {
        if (segments.size() < 2)
            return;
        auto segments_id = getRandomMergeableSegments();
        LOG_FMT_DEBUG(logger, "start random merge, segments_id=[{}] all_segments={}", fmt::join(segments_id, ","), segments.size());
        mergeSegment(segments_id);
    }

    void mergeDeltaRandomSegment()
    {
        if (segments.empty())
            return;
        PageId random_segment_id = getRandomSegmentId();
        LOG_FMT_DEBUG(logger, "start random merge delta, segment_id={} all_segments={}", random_segment_id, segments.size());
        mergeSegmentDelta(random_segment_id);
    }

    void flushCacheRandomSegment()
    {
        if (segments.empty())
            return;
        PageId random_segment_id = getRandomSegmentId();
        LOG_FMT_DEBUG(logger, "start random flush cache, segment_id={} all_segments={}", random_segment_id, segments.size());
        flushSegmentCache(random_segment_id);
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

    std::vector<PageId> getRandomMergeableSegments()
    {
        RUNTIME_CHECK(segments.size() >= 2, segments.size());

        // Merge 2~6 segments (at most 1/2 of all segments).
        auto max_merge_segments = std::uniform_int_distribution<int>{2, std::clamp(static_cast<int>(segments.size()) / 2, 2, 6)}(random);

        std::vector<PageId> segments_id;
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
                RUNTIME_CHECK(compare(last_segment->getRowKeyRange().getEnd(), next_segment->getRowKeyRange().getStart()) == 0, last_segment->info(), next_segment->info());
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
    SegmentTestOptions options;
    options.is_common_handle = true;
    reloadWithOptions(options);
    run(/* n */ 500, /* min key */ -50000, /* max key */ 50000);
}
CATCH


TEST_F(SegmentRandomizedTest, FastIntHandle)
try
{
    SegmentTestOptions options;
    options.is_common_handle = false;
    reloadWithOptions(options);
    run(/* n */ 500, /* min key */ -50000, /* max key */ 50000);
}
CATCH


// TODO: Run it in CI as a long-running test.
TEST_F(SegmentRandomizedTest, DISABLED_ForCI)
try
{
    SegmentTestOptions options;
    options.is_common_handle = true;
    reloadWithOptions(options);
    run(50000, /* min key */ -5000000, /* max key */ 5000000);
}
CATCH


} // namespace tests
} // namespace DM
} // namespace DB
