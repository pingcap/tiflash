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
#include <Common/Logger.h>
#include <Common/SyncPoint/Ctl.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/tests/gtest_segment_test_basic.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <future>

namespace DB
{
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
        mergeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, new_seg_id);
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
            mergeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, new_seg_id, /* check_rows */ false);
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
    srand(time(nullptr));
    SegmentTestOptions options;
    options.is_common_handle = true;
    reloadWithOptions(options);
    randomSegmentTest(10000);
}
CATCH

} // namespace tests
} // namespace DM
} // namespace DB
