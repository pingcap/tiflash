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
#include <Common/CurrentMetrics.h>
#include <Common/FailPoint.h>
#include <Common/Logger.h>
#include <Common/SyncPoint/Ctl.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/tests/gtest_segment_test_basic.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

#include <future>

namespace DB
{

namespace FailPoints
{
extern const char try_segment_logical_split[];
extern const char force_segment_logical_split[];
} // namespace FailPoints

namespace DM
{
namespace tests
{
class SegmentOperationTest : public SegmentTestBasic
{
protected:
    static void SetUpTestCase() {}
    void SetUp() override
    {
        log = DB::Logger::get("SegmentOperationTest");
    }

    DB::LoggerPtr log;
};

TEST_F(SegmentOperationTest, Issue4956)
try
{
    SegmentTestOptions options;
    reloadWithOptions(options);

    // flush data, make the segment can be split.
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    // write data to cache, reproduce the https://github.com/pingcap/tiflash/issues/4956
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID);
    deleteRangeSegment(DELTA_MERGE_FIRST_SEGMENT_ID);
    auto segment_id = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID);
    ASSERT_TRUE(segment_id.has_value());

    mergeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, *segment_id);
}
CATCH

TEST_F(SegmentOperationTest, TestSegment)
try
{
    SegmentTestOptions options;
    reloadWithOptions(options);
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);
    auto segment_id = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID);
    ASSERT_TRUE(segment_id.has_value());

    size_t origin_rows = getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID);

    writeSegment(*segment_id);
    flushSegmentCache(*segment_id);
    deleteRangeSegment(*segment_id);
    writeSegmentWithDeletedPack(*segment_id);
    mergeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, *segment_id);

    EXPECT_EQ(getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID), origin_rows);
}
CATCH

TEST_F(SegmentOperationTest, TestSegmentRandom)
try
{
    srand(time(nullptr));
    SegmentTestOptions options;
    options.is_common_handle = true;
    reloadWithOptions(options);
    randomSegmentTest(100);
}
CATCH

// run in CI weekly
TEST_F(SegmentOperationTest, DISABLED_TestSegmentRandomForCI)
try
{
    srand(time(nullptr));
    SegmentTestOptions options;
    options.is_common_handle = true;
    reloadWithOptions(options);
    randomSegmentTest(10000);
}
CATCH

TEST_F(SegmentOperationTest, SegmentLogicalSplit)
try
{
    {
        SegmentTestOptions options;
        options.db_settings.dt_segment_stable_pack_rows = 100;
        reloadWithOptions(options);
    }

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    // non flushed pack before split, should be ref in new splitted segments
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
    FailPointHelper::enableFailPoint(FailPoints::force_segment_logical_split);
    auto new_seg_id_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID);
    ASSERT_TRUE(new_seg_id_opt.has_value());


    for (size_t test_round = 0; test_round < 20; ++test_round)
    {
        // try further logical split
        auto rand_seg_id = getRandomSegmentId();
        auto seg_nrows = getSegmentRowNum(rand_seg_id);
        LOG_FMT_TRACE(&Poco::Logger::root(), "test_round={} seg={} nrows={}", test_round, rand_seg_id, seg_nrows);
        writeSegment(rand_seg_id, 150);
        flushSegmentCache(rand_seg_id);

        FailPointHelper::enableFailPoint(FailPoints::try_segment_logical_split);
        splitSegment(rand_seg_id);
    }
}
CATCH

TEST_F(SegmentOperationTest, Issue5570)
try
{
    {
        SegmentTestOptions options;
        // a smaller pack rows for logical split
        options.db_settings.dt_segment_stable_pack_rows = 100;
        reloadWithOptions(options);
    }

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    FailPointHelper::enableFailPoint(FailPoints::force_segment_logical_split);
    auto new_seg_id_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID);
    ASSERT_TRUE(new_seg_id_opt.has_value());
    auto new_seg_id = new_seg_id_opt.value();

    // Start a segment merge and suspend it before applyMerge
    auto sp_seg_merge_apply = SyncPointCtl::enableInScope("before_Segment::applyMerge");
    auto th_seg_merge = std::async([&]() {
        mergeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, new_seg_id, true);
    });
    sp_seg_merge_apply.waitAndPause();

    // flushed pack
    writeSegment(new_seg_id, 100);
    flushSegmentCache(new_seg_id);

    // Finish the segment merge
    sp_seg_merge_apply.next();
    th_seg_merge.wait();

    // logical split
    FailPointHelper::enableFailPoint(FailPoints::force_segment_logical_split);
    auto new_seg_id2_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID);
    ASSERT_TRUE(new_seg_id2_opt.has_value());
    auto new_seg_id2 = new_seg_id2_opt.value();

    {
        // further logical split on the left
        FailPointHelper::enableFailPoint(FailPoints::force_segment_logical_split);
        auto further_seg_id_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID);
        ASSERT_TRUE(further_seg_id_opt.has_value());
    }

    {
        // further logical split on the right(it fall back to physical split cause by current
        // implement of getSplitPointFast)
        FailPointHelper::enableFailPoint(FailPoints::try_segment_logical_split);
        auto further_seg_id_opt = splitSegment(new_seg_id2);
        ASSERT_TRUE(further_seg_id_opt.has_value());
    }
}
CATCH


TEST_F(SegmentOperationTest, Issue5570Case2)
try
{
    {
        SegmentTestOptions options;
        // a smaller pack rows for logical split
        options.db_settings.dt_segment_stable_pack_rows = 100;
        reloadWithOptions(options);
    }

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    FailPointHelper::enableFailPoint(FailPoints::force_segment_logical_split);
    auto new_seg_id_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID);
    ASSERT_TRUE(new_seg_id_opt.has_value());
    auto new_seg_id = new_seg_id_opt.value();

    LOG_DEBUG(log, "begin");

    // Start a segment merge and suspend it before applyMerge
    auto sp_seg_merge_apply = SyncPointCtl::enableInScope("before_Segment::applyMerge");
    auto th_seg_merge = std::async([&]() {
        mergeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, new_seg_id, true);
    });
    sp_seg_merge_apply.waitAndPause();
    LOG_DEBUG(log, "paused");

    // flushed pack
    writeSegment(new_seg_id, 100);
    // flushSegmentCache(new_seg_id); // do not flush

    // Finish the segment merge
    LOG_DEBUG(log, "next");
    sp_seg_merge_apply.next();
    th_seg_merge.wait();
    LOG_DEBUG(log, "finish");

    // logical split
    FailPointHelper::enableFailPoint(FailPoints::force_segment_logical_split);
    auto new_seg_id2_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID);
    ASSERT_TRUE(new_seg_id2_opt.has_value());
    auto new_seg_id2 = new_seg_id2_opt.value();

    {
        // further logical split on the left
        FailPointHelper::enableFailPoint(FailPoints::force_segment_logical_split);
        auto further_seg_id_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID);
        ASSERT_TRUE(further_seg_id_opt.has_value());
    }

    {
        // further logical split on the right(it fall back to physical split cause by current
        // implement of getSplitPointFast)
        FailPointHelper::enableFailPoint(FailPoints::try_segment_logical_split);
        auto further_seg_id_opt = splitSegment(new_seg_id2);
        ASSERT_TRUE(further_seg_id_opt.has_value());
    }
}
CATCH


} // namespace tests
} // namespace DM
} // namespace DB
