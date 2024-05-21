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

#include <Common/CurrentMetrics.h>
#include <Common/FailPoint.h>
#include <Common/Logger.h>
#include <Common/PODArray.h>
#include <Common/SyncPoint/Ctl.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Remote/RNReadTask.h>
#include <Storages/DeltaMerge/Remote/RNWorkerPrepareStreams.h>
#include <Storages/DeltaMerge/WriteBatchesImpl.h>
#include <Storages/DeltaMerge/tests/gtest_segment_test_basic.h>
#include <Storages/DeltaMerge/tests/gtest_segment_util.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/defines.h>
#include <gtest/gtest.h>

#include <future>

namespace ProfileEvents
{
extern const Event DMSegmentIsEmptyFastPath;
extern const Event DMSegmentIsEmptySlowPath;
} // namespace ProfileEvents

namespace CurrentMetrics
{
extern const Metric DT_SnapshotOfDeltaMerge;
} // namespace CurrentMetrics

namespace DB::ErrorCodes
{
extern const int DT_DELTA_INDEX_ERROR;
}

namespace DB
{
namespace DM
{
namespace GC
{
bool shouldCompactStableWithTooMuchDataOutOfSegmentRange(
    const DMContext & context, //
    const SegmentPtr & seg,
    const SegmentSnapshotPtr & snap,
    const SegmentPtr & prev_seg,
    const SegmentPtr & next_seg,
    double invalid_data_ratio_threshold,
    const LoggerPtr & log);
}
namespace tests
{

class SegmentOperationTest : public SegmentTestBasic
{
protected:
    DB::LoggerPtr log = DB::Logger::get("SegmentOperationTest");
};

TEST_F(SegmentOperationTest, Issue4956)
try
{
    // flush data, make the segment can be split.
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 1000, /* at */ 0);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    // write data to cache, reproduce the https://github.com/pingcap/tiflash/issues/4956
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID);
    deleteRangeSegment(DELTA_MERGE_FIRST_SEGMENT_ID);
    auto segment_id = splitSegmentAt(DELTA_MERGE_FIRST_SEGMENT_ID, 500, Segment::SplitMode::Physical);
    ASSERT_TRUE(segment_id.has_value());

    mergeSegment({DELTA_MERGE_FIRST_SEGMENT_ID, *segment_id});
    ASSERT_EQ(0, getSegmentRowNumWithoutMVCC(DELTA_MERGE_FIRST_SEGMENT_ID));
}
CATCH

TEST_F(SegmentOperationTest, TestSegment)
try
{
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

TEST_F(SegmentOperationTest, TestSegmentMergeTwo)
try
{
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
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    auto segment_id_2nd = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID);
    auto segment_id_3rd = splitSegment(*segment_id_2nd);
    // now we have segments = { DELTA_MERGE_FIRST_SEGMENT_ID, segment_id_2nd, segment_id_3rd }

    ASSERT_THROW(
        {
            mergeSegment({DELTA_MERGE_FIRST_SEGMENT_ID, /* omit segment_id_2nd */ *segment_id_3rd});
        },
        DB::Exception);
}
CATCH


TEST_F(SegmentOperationTest, WriteDuringSegmentMergeDelta)
try
{
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    {
        LOG_DEBUG(log, "beginSegmentMergeDelta");

        // Start a segment merge and suspend it before applyMerge
        auto sp_seg_merge_delta_apply = SyncPointCtl::enableInScope("before_Segment::applyMergeDelta");
        auto th_seg_merge_delta
            = std::async([&]() { mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID, /* check_rows */ false); });
        sp_seg_merge_delta_apply.waitAndPause();

        LOG_DEBUG(log, "pausedBeforeApplyMergeDelta");

        // non-flushed column files
        writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
        ingestDTFileIntoDelta(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
        sp_seg_merge_delta_apply.next();
        th_seg_merge_delta.get();

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
    ASSERT_EQ(getPageNumAfterGC(StorageType::Log, NAMESPACE_ID), 0);
    ASSERT_EQ(getPageNumAfterGC(StorageType::Data, NAMESPACE_ID), 1);
}
CATCH

TEST_F(SegmentOperationTest, CurrentV2RestoreFromStableV1)
try
{
    auto current = STORAGE_FORMAT_CURRENT;
    STORAGE_FORMAT_CURRENT = STORAGE_FORMAT_V5;
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    STORAGE_FORMAT_CURRENT = STORAGE_FORMAT_V6;
    auto segment = Segment::restoreSegment(log, *dm_context, DELTA_MERGE_FIRST_SEGMENT_ID);
    ASSERT_EQ(segment->stable->getRows(), 100);
    STORAGE_FORMAT_CURRENT = current;
}
CATCH

TEST_F(SegmentOperationTest, WriteDuringSegmentSplit)
try
{
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    {
        LOG_DEBUG(log, "beginSegmentSplit");

        // Start a segment merge and suspend it before applyMerge
        auto sp_seg_split_apply = SyncPointCtl::enableInScope("before_Segment::applySplit");
        PageIdU64 new_seg_id;
        auto th_seg_split = std::async([&]() {
            auto new_seg_id_opt
                = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID, Segment::SplitMode::Auto, /* check_rows */ false);
            ASSERT_TRUE(new_seg_id_opt.has_value());
            new_seg_id = new_seg_id_opt.value();
        });
        sp_seg_split_apply.waitAndPause();

        LOG_DEBUG(log, "pausedBeforeApplySplit");

        // non-flushed column files
        writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
        ingestDTFileIntoDelta(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
        sp_seg_split_apply.next();
        th_seg_split.get();

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
    ASSERT_EQ(getPageNumAfterGC(StorageType::Log, NAMESPACE_ID), 0);
    ASSERT_EQ(getPageNumAfterGC(StorageType::Data, NAMESPACE_ID), 1);
}
CATCH

TEST_F(SegmentOperationTest, WriteDuringSegmentMerge)
try
{
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
        ingestDTFileIntoDelta(new_seg_id, 100);
        sp_seg_merge_apply.next();
        th_seg_merge.get();

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
    ASSERT_EQ(getPageNumAfterGC(StorageType::Log, NAMESPACE_ID), 0);
    ASSERT_EQ(getPageNumAfterGC(StorageType::Data, NAMESPACE_ID), 1);
}
CATCH

TEST_F(SegmentOperationTest, CheckColumnFileSchema)
try
{
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    {
        LOG_DEBUG(log, "beginSegmentMergeDelta");

        // Start a segment merge and suspend it before applyMerge
        auto sp_seg_merge_delta_apply = SyncPointCtl::enableInScope("before_Segment::applyMergeDelta");
        auto th_seg_merge_delta
            = std::async([&]() { mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID, /* check_rows */ false); });
        sp_seg_merge_delta_apply.waitAndPause();

        LOG_DEBUG(log, "pausedBeforeApplyMergeDelta");

        // non-flushed column files
        writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
        sp_seg_merge_delta_apply.next();
        th_seg_merge_delta.get();

        LOG_DEBUG(log, "finishApplyMergeDelta");
    }

    {
        ingestDTFileIntoDelta(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
        writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
    }
    ASSERT_EQ(segments.size(), 1);
    {
        auto segment = segments[DELTA_MERGE_FIRST_SEGMENT_ID];
        auto delta = segment->getDelta();
        auto mem_table_set = delta->getMemTableSet();
        WriteBatches wbs(*dm_context->storage_pool);
        auto lock = segment->mustGetUpdateLock();
        auto [memory_cf, persisted_cf] = delta->cloneAllColumnFiles(lock, *dm_context, segment->getRowKeyRange(), wbs);
        ASSERT_FALSE(memory_cf.empty());
        ASSERT_TRUE(persisted_cf.empty());
        ColumnFileSchemaPtr last_schema;
        for (const auto & column_file : memory_cf)
        {
            if (auto * t_file = column_file->tryToTinyFile(); t_file)
            {
                auto current_schema = t_file->getSchema();
                ASSERT_TRUE(!last_schema || (last_schema == current_schema));
                last_schema = current_schema;
            }
        }
        // check last_schema is not nullptr after all
        ASSERT_NE(last_schema, nullptr);
    }
}
CATCH


TEST_F(SegmentOperationTest, SegmentLogicalSplit)
try
{
    buildFirstSegmentWithOptions(
        {.db_settings = {
             .dt_segment_stable_pack_rows = 100,
             .dt_enable_logical_split = true,
         }});

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 400, /* at */ 0);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    // non flushed pack before split, should be ref in new splitted segments
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100, /* at */ 10);
    auto new_seg_id_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID, Segment::SplitMode::Logical);
    ASSERT_TRUE(new_seg_id_opt.has_value());
    ASSERT_TRUE(areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *new_seg_id_opt}));
    ASSERT_EQ(300, getSegmentRowNumWithoutMVCC(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(200, getSegmentRowNumWithoutMVCC(*new_seg_id_opt));

    for (size_t test_round = 0; test_round < 20; ++test_round)
    {
        // try further logical split
        auto rand_seg_id = getRandomSegmentId();
        auto seg_nrows = getSegmentRowNum(rand_seg_id);
        LOG_TRACE(&Poco::Logger::root(), "test_round={} seg={} nrows={}", test_round, rand_seg_id, seg_nrows);
        writeSegment(rand_seg_id, 150);
        flushSegmentCache(rand_seg_id);
        splitSegment(rand_seg_id, Segment::SplitMode::Auto);
    }
}
CATCH


TEST_F(SegmentOperationTest, Issue5570)
try
{
    // a smaller pack rows for logical split
    buildFirstSegmentWithOptions({.db_settings = {.dt_segment_stable_pack_rows = 100}});

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 200);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    auto new_seg_id_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID, Segment::SplitMode::Logical);
    ASSERT_TRUE(new_seg_id_opt.has_value());
    ASSERT_TRUE(areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *new_seg_id_opt}));
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
    th_seg_merge.get();
    LOG_DEBUG(log, "finishApplyMerge");

    // logical split
    auto new_seg_id2_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID, Segment::SplitMode::Logical);
    ASSERT_TRUE(new_seg_id2_opt.has_value());
    ASSERT_TRUE(areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *new_seg_id2_opt}));
    auto new_seg_id2 = new_seg_id2_opt.value();

    {
        // further logical split on the left
        auto further_seg_id_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID, Segment::SplitMode::Logical);
        ASSERT_TRUE(further_seg_id_opt.has_value());
        ASSERT_TRUE(areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *further_seg_id_opt}));
    }

    {
        // further logical split on the right(it fall back to physical split cause by current
        // implement of getSplitPointFast)
        auto further_seg_id_opt = splitSegment(new_seg_id2, Segment::SplitMode::Logical);
        ASSERT_FALSE(further_seg_id_opt.has_value());
    }
}
CATCH


TEST_F(SegmentOperationTest, DeltaPagesAfterDeltaMerge)
try
{
    // a smaller pack rows for logical split
    buildFirstSegmentWithOptions(
        {.db_settings = {
             .dt_segment_stable_pack_rows = 100,
             .dt_enable_logical_split = true,
         }});

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    auto new_seg_id_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID, Segment::SplitMode::Logical);
    ASSERT_TRUE(new_seg_id_opt.has_value());
    ASSERT_TRUE(areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *new_seg_id_opt}));
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
        th_seg_merge.get();
        LOG_DEBUG(log, "finishApplyMerge");

        // logical split
        auto new_seg_id2_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID);
        ASSERT_TRUE(new_seg_id2_opt.has_value());
        new_seg_id = new_seg_id2_opt.value();

        const auto file_usage = storage_pool->log_storage_reader->getFileUsageStatistics();
        LOG_DEBUG(log, "log valid size on disk: {}", file_usage.total_valid_size);
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
        ASSERT_EQ(getPageNumAfterGC(StorageType::Log, NAMESPACE_ID), 0);
    }
}
CATCH


TEST_F(SegmentOperationTest, DeltaIndexError)
try
{
    // write stable
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 10000, 0);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);
    // split into 2 segment
    auto segment_id = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID);
    ASSERT_TRUE(segment_id.has_value());
    // write delta
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 1000, 0);
    writeSegment(*segment_id, 1000, 8000);

    // Init delta index
    {
        auto [first, first_snap] = getSegmentForRead(DELTA_MERGE_FIRST_SEGMENT_ID);
        first->placeDeltaIndex(*dm_context, first_snap);
    }
    auto [first, first_snap] = getSegmentForRead(DELTA_MERGE_FIRST_SEGMENT_ID);
    LOG_DEBUG(log, "First: {}", first_snap->delta->getSharedDeltaIndex()->toString());

    {
        auto [second, second_snap] = getSegmentForRead(*segment_id);
        second->placeDeltaIndex(*dm_context, second_snap);
    }
    auto [second, second_snap] = getSegmentForRead(*segment_id);
    LOG_DEBUG(log, "Second: {}", second_snap->delta->getSharedDeltaIndex()->toString());

    // Create a wrong delta index for first segment.
    auto [placed_rows, placed_deletes] = first_snap->delta->getSharedDeltaIndex()->getPlacedStatus();
    auto broken_delta_index = std::make_shared<DeltaIndex>(
        second_snap->delta->getSharedDeltaIndex()->getDeltaTree(),
        placed_rows,
        placed_deletes,
        first_snap->delta->getSharedDeltaIndex()->getRNCacheKey());
    first_snap->delta->shared_delta_index = broken_delta_index;

    auto meta = DB::DM::Remote::RNReadSegmentMeta{
        .keyspace_id = 0,
        .physical_table_id = 0,
        .segment_id = DELTA_MERGE_FIRST_SEGMENT_ID,
        .store_id = 0,
        .delta_tinycf_page_ids = {},
        .delta_tinycf_page_sizes = {},
        .segment = first,
        .segment_snap = first_snap,
        .store_address = {},
        .read_ranges = {first->getRowKeyRange()},
        .snapshot_id = {},
        .dm_context = createDMContext(),
    };
    auto task = std::make_shared<DB::DM::Remote::RNReadSegmentTask>(meta);

    auto worker = DB::DM::Remote::RNWorkerPrepareStreams::create({
        .source_queue = nullptr,
        .result_queue = nullptr,
        .log = Logger::get(),
        .concurrency = 1,
        .columns_to_read = tableColumns(),
        .read_tso = 0,
        .push_down_filter = nullptr,
        .read_mode = ReadMode::Bitmap,
    });

    ASSERT_FALSE(worker->initInputStream(task, true));

    try
    {
        [[maybe_unused]] auto succ = worker->initInputStream(task, false);
        FAIL() << "Should not come here.";
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::DT_DELTA_INDEX_ERROR);
    }

    try
    {
        db_context->getSettingsRef().set("dt_enable_delta_index_error_fallback", "false");
        task = worker->testDoWork(task);
        FAIL() << "Should not come here.";
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::DT_DELTA_INDEX_ERROR);
    }

    db_context->getSettingsRef().set("dt_enable_delta_index_error_fallback", "true");
    task = worker->testDoWork(task);
    auto stream = task->getInputStream();
    ASSERT_NE(stream, nullptr);
    std::vector<Block> blks;
    for (auto blk = stream->read(); blk; blk = stream->read())
    {
        blks.push_back(blk);
    }
    auto handle_col1 = vstackBlocks(std::move(blks)).getByName(EXTRA_HANDLE_COLUMN_NAME).column;
    auto handle_col2 = getSegmentHandle(task->meta.segment->segmentId(), {task->meta.segment->getRowKeyRange()});
    ASSERT_TRUE(sequenceEqual(
        toColumnVectorDataPtr<Int64>(handle_col2)->data(),
        toColumnVectorDataPtr<Int64>(handle_col1)->data(),
        handle_col1->size()));
}
CATCH

class SegmentEnableLogicalSplitTest : public SegmentOperationTest
{
protected:
    void SetUp() override
    {
        SegmentOperationTest::SetUp();
        buildFirstSegmentWithOptions(
            {.db_settings = {
                 .dt_segment_stable_pack_rows = 100,
                 .dt_enable_logical_split = true,
             }});
        ASSERT_TRUE(dm_context->enable_logical_split);
    }
};


TEST_F(SegmentEnableLogicalSplitTest, AutoModeLogicalSplit)
try
{
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 1000);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    auto new_seg_id_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID, Segment::SplitMode::Auto);
    ASSERT_TRUE(new_seg_id_opt.has_value());
    ASSERT_TRUE(areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *new_seg_id_opt}));
}
CATCH


TEST_F(SegmentEnableLogicalSplitTest, AutoModePhysicalSplitWhenStableIsEmpty)
try
{
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 1000);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);

    auto new_seg_id_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID, Segment::SplitMode::Auto);
    ASSERT_TRUE(new_seg_id_opt.has_value());
    ASSERT_FALSE(areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *new_seg_id_opt}));
}
CATCH


TEST_F(SegmentEnableLogicalSplitTest, AutoModePhysicalSplitWhenStablePacksAreFew)
try
{
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 200);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    auto new_seg_id_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID, Segment::SplitMode::Auto);
    ASSERT_TRUE(new_seg_id_opt.has_value());
    ASSERT_FALSE(areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *new_seg_id_opt}));
}
CATCH


TEST_F(SegmentEnableLogicalSplitTest, AutoModePhysicalSplitWhenDeltaIsLarger)
try
{
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 1000);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 2000);
    // Note: If we don't flush, then there will be logical split because mem table is not counted
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);

    auto new_seg_id_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID, Segment::SplitMode::Auto);
    ASSERT_TRUE(new_seg_id_opt.has_value());
    ASSERT_FALSE(areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *new_seg_id_opt}));
}
CATCH


class SegmentSplitTest : public SegmentTestBasic
{
};


TEST_F(SegmentSplitTest, AutoModePhycialSplitByDefault)
try
{
    buildFirstSegmentWithOptions({.db_settings = {.dt_segment_stable_pack_rows = 100}});
    ASSERT_FALSE(dm_context->enable_logical_split);

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 1000);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    auto new_seg_id_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID, Segment::SplitMode::Auto);
    ASSERT_TRUE(new_seg_id_opt.has_value());
    ASSERT_FALSE(areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *new_seg_id_opt}));
}
CATCH


TEST_F(SegmentSplitTest, PhysicalSplitMode)
try
{
    // Even if we explicitly set enable_logical_split, we will still do physical split in SplitMode::Physical.
    buildFirstSegmentWithOptions(
        {.db_settings = {
             .dt_segment_stable_pack_rows = 100,
             .dt_enable_logical_split = true,
         }});
    ASSERT_TRUE(dm_context->enable_logical_split);

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 1000);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    auto new_seg_id_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID, Segment::SplitMode::Physical);
    ASSERT_TRUE(new_seg_id_opt.has_value());
    ASSERT_FALSE(areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *new_seg_id_opt}));
}
CATCH


TEST_F(SegmentSplitTest, LogicalSplitWithMemTableData)
try
{
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 5000, /* at */ 0);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100, /* at */ 10); // Write data without flush
    auto new_seg_id_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID, Segment::SplitMode::Logical);
    ASSERT_TRUE(new_seg_id_opt.has_value());
    ASSERT_EQ(segments.size(), 2);
    ASSERT_TRUE(areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *new_seg_id_opt}));
    ASSERT_EQ(2600, getSegmentRowNumWithoutMVCC(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(2500, getSegmentRowNumWithoutMVCC(*new_seg_id_opt));
    ASSERT_EQ(2500, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(2500, getSegmentRowNum(*new_seg_id_opt));
}
CATCH


TEST_F(SegmentSplitTest, PhysicalSplitWithMemTableData)
try
{
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100, /* at */ 0);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 70, /* at */ 300); // Write data without flush
    auto new_seg_id_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID, Segment::SplitMode::Physical);
    ASSERT_TRUE(new_seg_id_opt.has_value());
    ASSERT_EQ(segments.size(), 2);
    ASSERT_FALSE(areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *new_seg_id_opt}));
    ASSERT_EQ(85, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(85, getSegmentRowNum(*new_seg_id_opt));
}
CATCH


TEST_F(SegmentSplitTest, LogicalSplitModeDoesLogicalSplit)
try
{
    buildFirstSegmentWithOptions({.db_settings = {.dt_segment_stable_pack_rows = 100}});
    // Logical split will be performed if we use logical split mode, even when enable_logical_split is false.
    ASSERT_FALSE(dm_context->enable_logical_split);

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 1000);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    auto new_seg_id_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID, Segment::SplitMode::Logical);
    ASSERT_TRUE(new_seg_id_opt.has_value());
    ASSERT_TRUE(areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *new_seg_id_opt}));
}
CATCH


TEST_F(SegmentSplitTest, LogicalSplitModeDoesNotFallbackWhenNoStable)
try
{
    auto new_seg_id_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID, Segment::SplitMode::Logical);
    ASSERT_FALSE(new_seg_id_opt.has_value());

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 50);
    new_seg_id_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID, Segment::SplitMode::Logical);
    ASSERT_FALSE(new_seg_id_opt.has_value());

    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    new_seg_id_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID, Segment::SplitMode::Logical);
    ASSERT_FALSE(new_seg_id_opt.has_value());

    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);
    new_seg_id_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID, Segment::SplitMode::Logical);
    ASSERT_TRUE(new_seg_id_opt.has_value());
    ASSERT_TRUE(areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *new_seg_id_opt}));
}
CATCH


TEST_F(SegmentSplitTest, LogicalSplitModeOnePackInStable)
try
{
    buildFirstSegmentWithOptions({.db_settings = {.dt_segment_stable_pack_rows = 100}});

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 50);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    auto new_seg_id_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID, Segment::SplitMode::Logical);
    ASSERT_TRUE(new_seg_id_opt.has_value());
    ASSERT_TRUE(areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *new_seg_id_opt}));
    ASSERT_EQ(25, getSegmentRowNumWithoutMVCC(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(25, getSegmentRowNumWithoutMVCC(*new_seg_id_opt));
}
CATCH


TEST_F(SegmentSplitTest, LogicalSplitModeOnePackWithHoleInStable)
try
{
    buildFirstSegmentWithOptions({.db_settings = {.dt_segment_stable_pack_rows = 100}});

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 10, /* at */ 0);
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 10, /* at */ 90);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    auto new_seg_id_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID, Segment::SplitMode::Logical);
    ASSERT_TRUE(new_seg_id_opt.has_value());
    ASSERT_TRUE(areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *new_seg_id_opt}));

    //                                 Calculated Split Point
    //                                 │
    //                                 │ new_seg
    //                                 │ ↓     ↓
    // Pack: [0~10  .... (Empty) ....  ↓ 90~100]
    //       ↑   ↑
    //       DELTA_MERGE_FIRST_SEGMENT
    ASSERT_EQ(10, getSegmentRowNumWithoutMVCC(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(10, getSegmentRowNumWithoutMVCC(*new_seg_id_opt));

    // Now, let's split them again! We will still get the same split point (which is invalid).
    {
        auto seg_2 = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID, Segment::SplitMode::Logical);
        ASSERT_FALSE(seg_2.has_value());
    }
    {
        auto seg_2 = splitSegment(*new_seg_id_opt, Segment::SplitMode::Logical);
        ASSERT_FALSE(seg_2.has_value());
    }
}
CATCH


TEST_F(SegmentSplitTest, LogicalSplitModeOneRowInStable)
try
{
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 1);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    auto new_seg_id_opt = splitSegment(DELTA_MERGE_FIRST_SEGMENT_ID, Segment::SplitMode::Logical);
    ASSERT_TRUE(new_seg_id_opt.has_value());
    ASSERT_TRUE(areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *new_seg_id_opt}));
    ASSERT_EQ(0, getSegmentRowNumWithoutMVCC(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(1, getSegmentRowNumWithoutMVCC(*new_seg_id_opt));
}
CATCH

class SegmentSplitAtTest : public SegmentTestBasic
{
};


TEST_F(SegmentSplitAtTest, AutoModeDisableLogicalSplit)
try
{
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100, /* at */ 0);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    ASSERT_FALSE(dm_context->enable_logical_split);
    auto new_seg_id = splitSegmentAt(DELTA_MERGE_FIRST_SEGMENT_ID, 25, Segment::SplitMode::Auto);
    ASSERT_TRUE(new_seg_id.has_value());
    ASSERT_FALSE(areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *new_seg_id}));
    ASSERT_EQ(25, getSegmentRowNumWithoutMVCC(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(75, getSegmentRowNumWithoutMVCC(*new_seg_id));
}
CATCH


TEST_F(SegmentSplitAtTest, AutoModeEnableLogicalSplit)
try
{
    buildFirstSegmentWithOptions({.db_settings = {.dt_enable_logical_split = true}});

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100, /* at */ 0);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    ASSERT_TRUE(dm_context->enable_logical_split);
    auto new_seg_id = splitSegmentAt(DELTA_MERGE_FIRST_SEGMENT_ID, 25, Segment::SplitMode::Auto);
    ASSERT_TRUE(new_seg_id.has_value());
    ASSERT_TRUE(areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *new_seg_id}));
    ASSERT_EQ(25, getSegmentRowNumWithoutMVCC(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(75, getSegmentRowNumWithoutMVCC(*new_seg_id));
}
CATCH


TEST_F(SegmentSplitAtTest, LogicalSplitMode)
try
{
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100, /* at */ 0);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    // We will do logical split even if enable_logical_split == false when SplitMode is specified as LogicalSplit.
    ASSERT_FALSE(dm_context->enable_logical_split);
    auto new_seg_id = splitSegmentAt(DELTA_MERGE_FIRST_SEGMENT_ID, 25, Segment::SplitMode::Logical);
    ASSERT_TRUE(new_seg_id.has_value());
    ASSERT_TRUE(areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *new_seg_id}));
    ASSERT_EQ(25, getSegmentRowNumWithoutMVCC(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(75, getSegmentRowNumWithoutMVCC(*new_seg_id));
}
CATCH


TEST_F(SegmentSplitAtTest, PhysicalSplitMode)
try
{
    buildFirstSegmentWithOptions({.db_settings = {.dt_enable_logical_split = true}});

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100, /* at */ 0);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    // We will do physical split even if enable_logical_split == true when SplitMode is specified as PhysicalSplit.
    ASSERT_TRUE(dm_context->enable_logical_split);
    auto new_seg_id = splitSegmentAt(DELTA_MERGE_FIRST_SEGMENT_ID, 25, Segment::SplitMode::Physical);
    ASSERT_TRUE(new_seg_id.has_value());
    ASSERT_FALSE(areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *new_seg_id}));
    ASSERT_EQ(25, getSegmentRowNumWithoutMVCC(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(75, getSegmentRowNumWithoutMVCC(*new_seg_id));
}
CATCH


class SegmentSplitAtModeTest
    : public SegmentTestBasic
    , public testing::WithParamInterface<bool>
{
public:
    SegmentSplitAtModeTest()
    {
        auto is_logical_split = GetParam();
        if (is_logical_split)
            split_mode = Segment::SplitMode::Logical;
        else
            split_mode = Segment::SplitMode::Physical;
    }

protected:
    Segment::SplitMode split_mode = Segment::SplitMode::Auto;
};

INSTANTIATE_TEST_CASE_P(IsLogicalSplit, SegmentSplitAtModeTest, testing::Bool());

TEST_P(SegmentSplitAtModeTest, EmptySegment)
try
{
    auto new_seg_id = splitSegmentAt(DELTA_MERGE_FIRST_SEGMENT_ID, 100, split_mode);
    ASSERT_TRUE(new_seg_id.has_value());
    ASSERT_EQ(
        split_mode == Segment::SplitMode::Logical,
        areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *new_seg_id}));
    ASSERT_EQ(0, getSegmentRowNumWithoutMVCC(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(0, getSegmentRowNumWithoutMVCC(*new_seg_id));
}
CATCH


TEST_P(SegmentSplitAtModeTest, SplitAtBoundary)
try
{
    auto new_seg_id = splitSegmentAt(DELTA_MERGE_FIRST_SEGMENT_ID, 100, split_mode);
    ASSERT_TRUE(new_seg_id.has_value());
    ASSERT_EQ(
        split_mode == Segment::SplitMode::Logical,
        areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *new_seg_id}));
    {
        auto r = splitSegmentAt(DELTA_MERGE_FIRST_SEGMENT_ID, 100, split_mode);
        ASSERT_FALSE(r.has_value());
    }
    {
        auto r = splitSegmentAt(*new_seg_id, 100, split_mode);
        ASSERT_FALSE(r.has_value());
    }
}
CATCH


TEST_P(SegmentSplitAtModeTest, SplitAtMemTableKey)
try
{
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100, /* at */ 0);

    auto new_seg_id = splitSegmentAt(DELTA_MERGE_FIRST_SEGMENT_ID, 30, split_mode);
    ASSERT_TRUE(new_seg_id.has_value());
    ASSERT_EQ(
        split_mode == Segment::SplitMode::Logical,
        areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *new_seg_id}));
    ASSERT_EQ(30, getSegmentRowNumWithoutMVCC(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(70, getSegmentRowNumWithoutMVCC(*new_seg_id));
}
CATCH


TEST_P(SegmentSplitAtModeTest, SplitAtDeltaKey)
try
{
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100, /* at */ 0);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);

    auto new_seg_id = splitSegmentAt(DELTA_MERGE_FIRST_SEGMENT_ID, 30, split_mode);
    ASSERT_TRUE(new_seg_id.has_value());
    ASSERT_EQ(
        split_mode == Segment::SplitMode::Logical,
        areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *new_seg_id}));
    ASSERT_EQ(30, getSegmentRowNumWithoutMVCC(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(70, getSegmentRowNumWithoutMVCC(*new_seg_id));
}
CATCH


TEST_P(SegmentSplitAtModeTest, SplitAtStableKey)
try
{
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100, /* at */ 0);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    auto new_seg_id = splitSegmentAt(DELTA_MERGE_FIRST_SEGMENT_ID, 30, split_mode);
    ASSERT_TRUE(new_seg_id.has_value());
    ASSERT_EQ(
        split_mode == Segment::SplitMode::Logical,
        areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *new_seg_id}));
    ASSERT_EQ(30, getSegmentRowNumWithoutMVCC(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(70, getSegmentRowNumWithoutMVCC(*new_seg_id));
}
CATCH


TEST_P(SegmentSplitAtModeTest, SplitAtEmptyKey)
try
{
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100, /* at */ 0);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    auto new_seg_id = splitSegmentAt(DELTA_MERGE_FIRST_SEGMENT_ID, 150, split_mode);
    ASSERT_TRUE(new_seg_id.has_value());
    ASSERT_EQ(
        split_mode == Segment::SplitMode::Logical,
        areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *new_seg_id}));
    ASSERT_EQ(100, getSegmentRowNumWithoutMVCC(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(0, getSegmentRowNumWithoutMVCC(*new_seg_id));
}
CATCH


TEST_P(SegmentSplitAtModeTest, StableWithMemTable)
try
{
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100, /* at */ 0);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 60, /* at */ -30);

    auto new_seg_id = splitSegmentAt(DELTA_MERGE_FIRST_SEGMENT_ID, 10, split_mode);
    ASSERT_TRUE(new_seg_id.has_value());
    ASSERT_EQ(
        split_mode == Segment::SplitMode::Logical,
        areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *new_seg_id}));
    ASSERT_EQ(50, getSegmentRowNumWithoutMVCC(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(110, getSegmentRowNumWithoutMVCC(*new_seg_id));
    ASSERT_EQ(40, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(90, getSegmentRowNum(*new_seg_id));
}
CATCH


TEST_P(SegmentSplitAtModeTest, FlushMemTableAfterSplit)
try
{
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100, /* at */ 0);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 60, /* at */ -30);

    auto new_seg_id = splitSegmentAt(DELTA_MERGE_FIRST_SEGMENT_ID, 10, split_mode);
    ASSERT_TRUE(new_seg_id.has_value());
    ASSERT_EQ(
        split_mode == Segment::SplitMode::Logical,
        areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *new_seg_id}));
    ASSERT_EQ(50, getSegmentRowNumWithoutMVCC(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(110, getSegmentRowNumWithoutMVCC(*new_seg_id));
    ASSERT_EQ(40, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(90, getSegmentRowNum(*new_seg_id));

    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    ASSERT_EQ(
        split_mode == Segment::SplitMode::Logical,
        areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *new_seg_id}));
    ASSERT_EQ(50, getSegmentRowNumWithoutMVCC(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(110, getSegmentRowNumWithoutMVCC(*new_seg_id));
    ASSERT_EQ(40, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(90, getSegmentRowNum(*new_seg_id));

    // Split again at 50.
    {
        auto right_id = splitSegmentAt(*new_seg_id, 50, split_mode);
        ASSERT_TRUE(right_id.has_value());
        ASSERT_EQ(
            split_mode == Segment::SplitMode::Logical,
            areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *right_id}));
        ASSERT_EQ(50, getSegmentRowNumWithoutMVCC(DELTA_MERGE_FIRST_SEGMENT_ID));
        ASSERT_EQ(40 + 20, getSegmentRowNumWithoutMVCC(*new_seg_id));
        ASSERT_EQ(50, getSegmentRowNumWithoutMVCC(*right_id));
    }
}
CATCH


TEST_P(SegmentSplitAtModeTest, EmptySegmentSplitMultipleTimes)
try
{
    auto new_seg_id = splitSegmentAt(DELTA_MERGE_FIRST_SEGMENT_ID, 100, split_mode);
    ASSERT_TRUE(new_seg_id.has_value());
    ASSERT_EQ(
        split_mode == Segment::SplitMode::Logical,
        areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *new_seg_id}));
    for (Int64 split_at = 99; split_at > -10; --split_at)
    {
        auto right_id = splitSegmentAt(DELTA_MERGE_FIRST_SEGMENT_ID, split_at, split_mode);
        ASSERT_TRUE(right_id.has_value());
        ASSERT_EQ(
            split_mode == Segment::SplitMode::Logical,
            areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *right_id}));
    }
}
CATCH


TEST_P(SegmentSplitAtModeTest, MemTableSplitMultipleTimes)
try
{
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 300, /* at */ 0);

    auto new_seg_id = splitSegmentAt(DELTA_MERGE_FIRST_SEGMENT_ID, 100, split_mode);
    ASSERT_TRUE(new_seg_id.has_value());
    ASSERT_EQ(
        split_mode == Segment::SplitMode::Logical,
        areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *new_seg_id}));
    ASSERT_EQ(100, getSegmentRowNumWithoutMVCC(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(200, getSegmentRowNumWithoutMVCC(*new_seg_id));
    ASSERT_EQ(100, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(200, getSegmentRowNum(*new_seg_id));

    for (Int64 split_at = 99; split_at >= 0; --split_at)
    {
        auto right_id = splitSegmentAt(DELTA_MERGE_FIRST_SEGMENT_ID, split_at, split_mode);
        ASSERT_TRUE(right_id.has_value());
        ASSERT_EQ(
            split_mode == Segment::SplitMode::Logical,
            areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *right_id}));
        ASSERT_EQ(1, getSegmentRowNumWithoutMVCC(*right_id));
        ASSERT_EQ(1, getSegmentRowNum(*right_id));
    }
}
CATCH


class IsEmptyTest : public SegmentTestBasic
{
};

TEST_F(IsEmptyTest, Basic)
try
{
    ASSERT_PROFILE_EVENT(ProfileEvents::DMSegmentIsEmptyFastPath, +1, {
        ASSERT_TRUE(isSegmentDefinitelyEmpty(DELTA_MERGE_FIRST_SEGMENT_ID));
    });
    ASSERT_EQ(0, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100, /* at */ 0);
    ASSERT_FALSE(isSegmentDefinitelyEmpty(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(100, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));

    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    ASSERT_FALSE(isSegmentDefinitelyEmpty(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(100, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));

    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);
    ASSERT_FALSE(isSegmentDefinitelyEmpty(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(100, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));

    // As long as there is a valid version, we consider the segment to be not empty.
    writeSegmentWithDeletedPack(DELTA_MERGE_FIRST_SEGMENT_ID, 100, /* at */ 0);
    ASSERT_FALSE(isSegmentDefinitelyEmpty(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(0, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));

    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);
    ASSERT_FALSE(isSegmentDefinitelyEmpty(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(0, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));
}
CATCH

TEST_F(IsEmptyTest, DeletedVersion)
try
{
    ASSERT_TRUE(isSegmentDefinitelyEmpty(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(0, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));

    // Even deleted version are considered to be not empty.
    writeSegmentWithDeletedPack(DELTA_MERGE_FIRST_SEGMENT_ID, 100, /* at */ 0);
    ASSERT_FALSE(isSegmentDefinitelyEmpty(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(0, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));
}
CATCH

TEST_F(IsEmptyTest, DeleteRange)
try
{
    ASSERT_TRUE(isSegmentDefinitelyEmpty(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(0, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100, /* at */ 0);
    ASSERT_FALSE(isSegmentDefinitelyEmpty(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(100, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));

    // We do not consider delete ranges currently.
    deleteRangeSegment(DELTA_MERGE_FIRST_SEGMENT_ID);
    ASSERT_FALSE(isSegmentDefinitelyEmpty(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(0, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));

    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    ASSERT_FALSE(isSegmentDefinitelyEmpty(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(0, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));

    // We will consider it to be empty after compaction.
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);
    ASSERT_PROFILE_EVENT(ProfileEvents::DMSegmentIsEmptyFastPath, +1, {
        ASSERT_TRUE(isSegmentDefinitelyEmpty(DELTA_MERGE_FIRST_SEGMENT_ID));
    });
    ASSERT_EQ(0, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));

    // For empty segment, delete range will not cause it to be "not empty".
    deleteRangeSegment(DELTA_MERGE_FIRST_SEGMENT_ID);
    ASSERT_PROFILE_EVENT(ProfileEvents::DMSegmentIsEmptyFastPath, +1, {
        ASSERT_TRUE(isSegmentDefinitelyEmpty(DELTA_MERGE_FIRST_SEGMENT_ID));
    });
    ASSERT_EQ(0, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));
}
CATCH

TEST_F(IsEmptyTest, LogicalSplitMemTableDelta)
try
{
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100, /* at */ 0);

    auto right_seg = splitSegmentAt(DELTA_MERGE_FIRST_SEGMENT_ID, 200, Segment::SplitMode::Logical);
    ASSERT_TRUE(right_seg.has_value());

    ASSERT_FALSE(isSegmentDefinitelyEmpty(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(100, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));

    // This is the slow path, because ColumnFileInMemory exists for both left and right segments after logical split.
    ASSERT_PROFILE_EVENT(ProfileEvents::DMSegmentIsEmptySlowPath, +1, {
        ASSERT_TRUE(isSegmentDefinitelyEmpty(*right_seg));
    });
    ASSERT_EQ(0, getSegmentRowNum(*right_seg));
}
CATCH

TEST_F(IsEmptyTest, LogicalSplitPersistedDelta)
try
{
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100, /* at */ 0);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);

    auto right_seg = splitSegmentAt(DELTA_MERGE_FIRST_SEGMENT_ID, 200, Segment::SplitMode::Logical);
    ASSERT_TRUE(right_seg.has_value());

    ASSERT_FALSE(isSegmentDefinitelyEmpty(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(100, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));

    // This is the slow path, because ColumnFileTiny exists for both left and right segments after logical split.
    ASSERT_PROFILE_EVENT(ProfileEvents::DMSegmentIsEmptySlowPath, +1, {
        ASSERT_TRUE(isSegmentDefinitelyEmpty(*right_seg));
    });
    ASSERT_EQ(0, getSegmentRowNum(*right_seg));
}
CATCH

TEST_F(IsEmptyTest, LogicalSplitStable)
try
{
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100, /* at */ 0);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    auto right_seg = splitSegmentAt(DELTA_MERGE_FIRST_SEGMENT_ID, 200, Segment::SplitMode::Logical);
    ASSERT_TRUE(right_seg.has_value());

    ASSERT_FALSE(isSegmentDefinitelyEmpty(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(100, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));

    // This goes into the fast path thanks to pack filter.
    ASSERT_PROFILE_EVENT(ProfileEvents::DMSegmentIsEmptyFastPath, +1, {
        ASSERT_TRUE(isSegmentDefinitelyEmpty(*right_seg));
    });
    ASSERT_EQ(0, getSegmentRowNum(*right_seg));
}
CATCH

TEST_F(IsEmptyTest, LogicalSplitStableHollow)
try
{
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100, /* at */ 0);
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 42, /* at */ 1000);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    auto seg_2 = splitSegmentAt(DELTA_MERGE_FIRST_SEGMENT_ID, 200, Segment::SplitMode::Logical);
    ASSERT_TRUE(seg_2.has_value());

    auto seg_3 = splitSegmentAt(*seg_2, 800, Segment::SplitMode::Logical);
    ASSERT_TRUE(seg_3.has_value());

    ASSERT_FALSE(isSegmentDefinitelyEmpty(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(100, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));

    // This is the slow path, because pack filter will not work.
    ASSERT_PROFILE_EVENT(ProfileEvents::DMSegmentIsEmptySlowPath, +1, {
        ASSERT_TRUE(isSegmentDefinitelyEmpty(*seg_2));
    });
    ASSERT_EQ(0, getSegmentRowNum(*seg_2));

    ASSERT_FALSE(isSegmentDefinitelyEmpty(*seg_3));
    ASSERT_EQ(42, getSegmentRowNum(*seg_3));
}
CATCH


} // namespace tests
} // namespace DM
} // namespace DB
