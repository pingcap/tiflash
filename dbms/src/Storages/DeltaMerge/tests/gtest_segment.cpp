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
#include <common/defines.h>
#include <gtest/gtest.h>

#include <future>

namespace CurrentMetrics
{
extern const Metric DT_SnapshotOfDeltaMerge;
} // namespace CurrentMetrics

namespace DB
{

namespace FailPoints
{
extern const char try_segment_logical_split[];
extern const char force_segment_logical_split[];
} // namespace FailPoints

namespace DM
{
namespace GC
{
bool shouldCompactStableWithTooMuchDataOutOfSegmentRange(const SegmentSnapshotPtr & snap, double invalid_data_ratio_threshold, const LoggerPtr & log);
}
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

    mergeSegment({DELTA_MERGE_FIRST_SEGMENT_ID, *segment_id});
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
    mergeSegment({DELTA_MERGE_FIRST_SEGMENT_ID, *segment_id});

    EXPECT_EQ(getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID), origin_rows);
}
CATCH

TEST_F(SegmentOperationTest, TestSegmentMemTableDataAfterSplit)
try
{
    SegmentTestOptions options;
    reloadWithOptions(options);
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 70); // Write data without flush
    auto segment_id_2nd = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID);
    ASSERT_TRUE(segment_id_2nd.has_value());
    ASSERT_EQ(segments.size(), 2);
    // The mem table data may be fallen in either segment (as we write randomly).
    ASSERT_EQ(getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID) + getSegmentRowNum(*segment_id_2nd), 170);
}
CATCH

TEST_F(SegmentOperationTest, TestSegmentMergeTwo)
try
{
    SegmentTestOptions options;
    reloadWithOptions(options);
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    auto segment_id_2nd = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID);
    // now we have segments = { DELTA_MERGE_FIRST_SEGMENT_ID, segment_id_2nd }
    ASSERT_TRUE(segment_id_2nd.has_value());
    ASSERT_EQ(getSegmentRowNumWithoutMVCC(DELTA_MERGE_FIRST_SEGMENT_ID), 50);
    ASSERT_EQ(getSegmentRowNumWithoutMVCC(*segment_id_2nd), 50);
    ASSERT_EQ(segments.size(), 2);

    auto segment_id_3rd = splitSegment(*segment_id_2nd);
    // now we have segments = { DELTA_MERGE_FIRST_SEGMENT_ID, segment_id_2nd, segment_id_3rd }
    ASSERT_TRUE(segment_id_3rd.has_value());
    ASSERT_EQ(getSegmentRowNumWithoutMVCC(*segment_id_2nd), 25);
    ASSERT_EQ(getSegmentRowNumWithoutMVCC(*segment_id_3rd), 25);
    ASSERT_EQ(segments.size(), 3);

    writeSegment(*segment_id_2nd, 7);
    ASSERT_EQ(getSegmentRowNumWithoutMVCC(*segment_id_2nd), 25 + 7);
    mergeSegment({*segment_id_2nd, *segment_id_3rd});
    // now we have segments = { DELTA_MERGE_FIRST_SEGMENT_ID, segment_id_2nd }
    ASSERT_EQ(getSegmentRowNumWithoutMVCC(*segment_id_2nd), 50 + 7);
    ASSERT_TRUE(segments.find(*segment_id_3rd) == segments.end());
    ASSERT_EQ(segments.size(), 2);
}
CATCH

TEST_F(SegmentOperationTest, TestSegmentMergeThree)
try
{
    SegmentTestOptions options;
    reloadWithOptions(options);
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    auto segment_id_2nd = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID);
    auto segment_id_3rd = splitSegment(*segment_id_2nd);
    // now we have segments = { DELTA_MERGE_FIRST_SEGMENT_ID, segment_id_2nd, segment_id_3rd }
    ASSERT_EQ(segments.size(), 3);
    ASSERT_EQ(getSegmentRowNumWithoutMVCC(DELTA_MERGE_FIRST_SEGMENT_ID), 50);
    ASSERT_EQ(getSegmentRowNumWithoutMVCC(*segment_id_2nd), 25);
    ASSERT_EQ(getSegmentRowNumWithoutMVCC(*segment_id_3rd), 25);

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 11);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    ASSERT_EQ(getSegmentRowNumWithoutMVCC(DELTA_MERGE_FIRST_SEGMENT_ID), 50 + 11);
    writeSegment(*segment_id_2nd, 7);
    ASSERT_EQ(getSegmentRowNumWithoutMVCC(*segment_id_2nd), 25 + 7);
    mergeSegment({DELTA_MERGE_FIRST_SEGMENT_ID, *segment_id_2nd, *segment_id_3rd});
    // now we have segments = { DELTA_MERGE_FIRST_SEGMENT_ID }
    ASSERT_EQ(getSegmentRowNumWithoutMVCC(DELTA_MERGE_FIRST_SEGMENT_ID), 100 + 11 + 7);
    ASSERT_TRUE(segments.find(*segment_id_2nd) == segments.end());
    ASSERT_TRUE(segments.find(*segment_id_3rd) == segments.end());
    ASSERT_EQ(segments.size(), 1);
}
CATCH

TEST_F(SegmentOperationTest, TestSegmentMergeInvalid)
try
{
    SegmentTestOptions options;
    reloadWithOptions(options);
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    auto segment_id_2nd = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID);
    auto segment_id_3rd = splitSegment(*segment_id_2nd);
    // now we have segments = { DELTA_MERGE_FIRST_SEGMENT_ID, segment_id_2nd, segment_id_3rd }

    ASSERT_THROW({ mergeSegment({DELTA_MERGE_FIRST_SEGMENT_ID, /* omit segment_id_2nd */ *segment_id_3rd}); }, DB::Exception);
}
CATCH

TEST_F(SegmentOperationTest, TestSegmentRandom)
try
{
    SegmentTestOptions options;
    options.is_common_handle = true;
    reloadWithOptions(options);
    randomSegmentTest(100);
}
CATCH

TEST_F(SegmentOperationTest, WriteDuringSegmentMergeDelta)
try
{
    SegmentTestOptions options;
    reloadWithOptions(options);
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    {
        LOG_DEBUG(log, "beginSegmentMergeDelta");

        // Start a segment merge and suspend it before applyMerge
        auto sp_seg_merge_delta_apply = SyncPointCtl::enableInScope("before_Segment::applyMergeDelta");
        auto th_seg_merge_delta = std::async([&]() {
            mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID, /* check_rows */ false);
        });
        sp_seg_merge_delta_apply.waitAndPause();

        LOG_DEBUG(log, "pausedBeforeApplyMergeDelta");

        // non-flushed column files
        writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
        ingestDTFileIntoSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
        sp_seg_merge_delta_apply.next();
        th_seg_merge_delta.wait();

        LOG_DEBUG(log, "finishApplyMergeDelta");
    }

    for (const auto & [seg_id, seg] : segments)
    {
        UNUSED(seg);
        deleteRangeSegment(seg_id);
        flushSegmentCache(seg_id);
        mergeSegmentDelta(seg_id);
    }
    ASSERT_EQ(segments.size(), 1);

    /// make sure all column file in delta value space is deleted
    ASSERT_TRUE(storage_pool->log_storage_v3 != nullptr || storage_pool->log_storage_v2 != nullptr);
    if (storage_pool->log_storage_v3)
    {
        storage_pool->log_storage_v3->gc(/* not_skip */ true);
        storage_pool->data_storage_v3->gc(/* not_skip */ true);
        ASSERT_EQ(storage_pool->log_storage_v3->getNumberOfPages(), 0);
        ASSERT_EQ(storage_pool->data_storage_v3->getNumberOfPages(), 1);
    }
    if (storage_pool->log_storage_v2)
    {
        storage_pool->log_storage_v2->gc(/* not_skip */ true);
        storage_pool->data_storage_v2->gc(/* not_skip */ true);
        ASSERT_EQ(storage_pool->log_storage_v2->getNumberOfPages(), 0);
        ASSERT_EQ(storage_pool->data_storage_v2->getNumberOfPages(), 1);
    }
}
CATCH

TEST_F(SegmentOperationTest, WriteDuringSegmentSplit)
try
{
    SegmentTestOptions options;
    reloadWithOptions(options);
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    {
        LOG_DEBUG(log, "beginSegmentSplit");

        // Start a segment merge and suspend it before applyMerge
        auto sp_seg_split_apply = SyncPointCtl::enableInScope("before_Segment::applySplit");
        PageId new_seg_id;
        auto th_seg_split = std::async([&]() {
            auto new_seg_id_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID, /* check_rows */ false);
            ASSERT_TRUE(new_seg_id_opt.has_value());
            new_seg_id = new_seg_id_opt.value();
        });
        sp_seg_split_apply.waitAndPause();

        LOG_DEBUG(log, "pausedBeforeApplySplit");

        // non-flushed column files
        writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
        ingestDTFileIntoSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
        sp_seg_split_apply.next();
        th_seg_split.wait();

        LOG_DEBUG(log, "finishApplySplit");
        mergeSegment({DELTA_MERGE_FIRST_SEGMENT_ID, new_seg_id});
    }

    for (const auto & [seg_id, seg] : segments)
    {
        UNUSED(seg);
        deleteRangeSegment(seg_id);
        flushSegmentCache(seg_id);
        mergeSegmentDelta(seg_id);
    }
    ASSERT_EQ(segments.size(), 1);

    /// make sure all column file in delta value space is deleted
    ASSERT_TRUE(storage_pool->log_storage_v3 != nullptr || storage_pool->log_storage_v2 != nullptr);
    if (storage_pool->log_storage_v3)
    {
        storage_pool->log_storage_v3->gc(/* not_skip */ true);
        storage_pool->data_storage_v3->gc(/* not_skip */ true);
        ASSERT_EQ(storage_pool->log_storage_v3->getNumberOfPages(), 0);
        ASSERT_EQ(storage_pool->data_storage_v3->getNumberOfPages(), 1);
    }
    if (storage_pool->log_storage_v2)
    {
        storage_pool->log_storage_v2->gc(/* not_skip */ true);
        storage_pool->data_storage_v2->gc(/* not_skip */ true);
        ASSERT_EQ(storage_pool->log_storage_v2->getNumberOfPages(), 0);
        ASSERT_EQ(storage_pool->data_storage_v2->getNumberOfPages(), 1);
    }
}
CATCH

TEST_F(SegmentOperationTest, WriteDuringSegmentMerge)
try
{
    SegmentTestOptions options;
    reloadWithOptions(options);
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    auto new_seg_id_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID);
    ASSERT_TRUE(new_seg_id_opt.has_value());
    auto new_seg_id = new_seg_id_opt.value();

    {
        LOG_DEBUG(log, "beginSegmentMerge");

        // Start a segment merge and suspend it before applyMerge
        auto sp_seg_merge_apply = SyncPointCtl::enableInScope("before_Segment::applyMerge");
        auto th_seg_merge = std::async([&]() {
            mergeSegment({DELTA_MERGE_FIRST_SEGMENT_ID, new_seg_id}, /* check_rows */ false);
        });
        sp_seg_merge_apply.waitAndPause();

        LOG_DEBUG(log, "pausedBeforeApplyMerge");

        // non-flushed column files
        writeSegment(new_seg_id, 100);
        ingestDTFileIntoSegment(new_seg_id, 100);
        sp_seg_merge_apply.next();
        th_seg_merge.wait();

        LOG_DEBUG(log, "finishApplyMerge");
    }

    for (const auto & [seg_id, seg] : segments)
    {
        UNUSED(seg);
        deleteRangeSegment(seg_id);
        flushSegmentCache(seg_id);
        mergeSegmentDelta(seg_id);
    }
    ASSERT_EQ(segments.size(), 1);

    /// make sure all column file in delta value space is deleted
    ASSERT_TRUE(storage_pool->log_storage_v3 != nullptr || storage_pool->log_storage_v2 != nullptr);
    if (storage_pool->log_storage_v3)
    {
        storage_pool->log_storage_v3->gc(/* not_skip */ true);
        storage_pool->data_storage_v3->gc(/* not_skip */ true);
        ASSERT_EQ(storage_pool->log_storage_v3->getNumberOfPages(), 0);
        ASSERT_EQ(storage_pool->data_storage_v3->getNumberOfPages(), 1);
    }
    if (storage_pool->log_storage_v2)
    {
        storage_pool->log_storage_v2->gc(/* not_skip */ true);
        storage_pool->data_storage_v2->gc(/* not_skip */ true);
        ASSERT_EQ(storage_pool->log_storage_v2->getNumberOfPages(), 0);
        ASSERT_EQ(storage_pool->data_storage_v2->getNumberOfPages(), 1);
    }
}
CATCH

// run in CI weekly
TEST_F(SegmentOperationTest, DISABLED_TestSegmentRandomForCI)
try
{
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

TEST_F(SegmentOperationTest, GCCheckAfterSegmentLogicalSplit)
try
{
    {
        SegmentTestOptions options;
        options.db_settings.dt_segment_stable_pack_rows = 100;
        reloadWithOptions(options);
    }

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 1000);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);
    auto invalid_data_ratio_threshold = dm_context->db_context.getSettingsRef().dt_bg_gc_delta_delete_ratio_to_trigger_gc;
    {
        auto segment = segments[DELTA_MERGE_FIRST_SEGMENT_ID];
        auto snap = segment->createSnapshot(*dm_context, /* for_update */ true, CurrentMetrics::DT_SnapshotOfDeltaMerge);
        ASSERT_FALSE(GC::shouldCompactStableWithTooMuchDataOutOfSegmentRange(snap, invalid_data_ratio_threshold, log));
    }

    FailPointHelper::enableFailPoint(FailPoints::force_segment_logical_split);
    auto new_seg_id_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID);
    ASSERT_TRUE(new_seg_id_opt.has_value());

    {
        auto segment = segments[DELTA_MERGE_FIRST_SEGMENT_ID];
        auto snap = segment->createSnapshot(*dm_context, /* for_update */ true, CurrentMetrics::DT_SnapshotOfDeltaMerge);
        ASSERT_TRUE(GC::shouldCompactStableWithTooMuchDataOutOfSegmentRange(snap, invalid_data_ratio_threshold, log));
    }
    {
        auto segment = segments[new_seg_id_opt.value()];
        auto snap = segment->createSnapshot(*dm_context, /* for_update */ true, CurrentMetrics::DT_SnapshotOfDeltaMerge);
        ASSERT_TRUE(GC::shouldCompactStableWithTooMuchDataOutOfSegmentRange(snap, invalid_data_ratio_threshold, log));
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

    LOG_DEBUG(log, "beginSegmentMerge");

    // Start a segment merge and suspend it before applyMerge
    auto sp_seg_merge_apply = SyncPointCtl::enableInScope("before_Segment::applyMerge");
    auto th_seg_merge = std::async([&]() {
        mergeSegment({DELTA_MERGE_FIRST_SEGMENT_ID, new_seg_id}, /*check_rows=*/false);
    });
    sp_seg_merge_apply.waitAndPause();
    LOG_DEBUG(log, "pausedBeforeApplyMerge");

    // flushed pack
    writeSegment(new_seg_id, 100);
    flushSegmentCache(new_seg_id);

    // Finish the segment merge
    LOG_DEBUG(log, "continueApplyMerge");
    sp_seg_merge_apply.next();
    th_seg_merge.wait();
    LOG_DEBUG(log, "finishApplyMerge");

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


TEST_F(SegmentOperationTest, DeltaPagesAfterDeltaMerge)
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

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(0, 5);

    for (size_t round = 0; round < 50; ++round)
    {
        LOG_DEBUG(log, "beginSegmentMerge");

        // Start a segment merge and suspend it before applyMerge
        auto sp_seg_merge_apply = SyncPointCtl::enableInScope("before_Segment::applyMerge");
        auto th_seg_merge = std::async([&]() {
            mergeSegment({DELTA_MERGE_FIRST_SEGMENT_ID, new_seg_id}, /*check_rows=*/false);
        });
        sp_seg_merge_apply.waitAndPause();
        LOG_DEBUG(log, "pausedBeforeApplyMerge");

        // randomly flushed or non flushed column file
        writeSegment(new_seg_id, 100);
        if (auto r = distrib(gen); r > 0)
        {
            flushSegmentCache(new_seg_id);
        }

        // Finish the segment merge
        LOG_DEBUG(log, "continueApplyMerge");
        sp_seg_merge_apply.next();
        th_seg_merge.wait();
        LOG_DEBUG(log, "finishApplyMerge");

        // logical split
        FailPointHelper::enableFailPoint(FailPoints::try_segment_logical_split);
        auto new_seg_id2_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID);
        ASSERT_TRUE(new_seg_id2_opt.has_value());
        new_seg_id = new_seg_id2_opt.value();

        const auto file_usage = storage_pool->log_storage_reader->getFileUsageStatistics();
        LOG_FMT_DEBUG(log, "log valid size on disk: {}", file_usage.total_valid_size);
    }
    // apply delete range && flush && delta-merge on all segments
    for (const auto & [seg_id, seg] : segments)
    {
        UNUSED(seg);
        deleteRangeSegment(seg_id);
        flushSegmentCache(seg_id);
        mergeSegmentDelta(seg_id);
    }

    {
        /// make sure all column file in delta value space is deleted
        ASSERT_TRUE(storage_pool->log_storage_v3 != nullptr || storage_pool->log_storage_v2 != nullptr);
        if (storage_pool->log_storage_v3)
        {
            storage_pool->log_storage_v3->gc(/* not_skip */ true);
            storage_pool->data_storage_v3->gc(/* not_skip */ true);
            EXPECT_EQ(storage_pool->log_storage_v3->getNumberOfPages(), 0);
        }
        if (storage_pool->log_storage_v2)
        {
            storage_pool->log_storage_v2->gc(/* not_skip */ true);
            storage_pool->data_storage_v2->gc(/* not_skip */ true);
            EXPECT_EQ(storage_pool->log_storage_v2->getNumberOfPages(), 0);
        }

        const auto file_usage = storage_pool->log_storage_reader->getFileUsageStatistics();
        LOG_FMT_DEBUG(log, "All delta-merged, log valid size on disk: {}", file_usage.total_valid_size);
        EXPECT_EQ(file_usage.total_valid_size, 0);
    }
}
CATCH


} // namespace tests
} // namespace DM
} // namespace DB
