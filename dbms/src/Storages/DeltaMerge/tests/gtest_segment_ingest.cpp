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
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/tests/gtest_segment_test_basic.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

#include <future>

namespace ProfileEvents
{

extern const Event DMSegmentIngestDataByReplace;
extern const Event DMSegmentIngestDataIntoDelta;

} // namespace ProfileEvents

namespace DB
{

namespace DM
{

namespace tests
{

class SegmentIngestTestWithClear : public SegmentTestBasic
{
    // This test case is simple, because most cases should be covered by SegmentReplaceDataTest.
};

TEST_F(SegmentIngestTestWithClear, Basic)
try
{
    // Empty
    ASSERT_PROFILE_EVENT(ProfileEvents::DMSegmentIngestDataByReplace, +1, {
        ingestDTFileByReplace(DELTA_MERGE_FIRST_SEGMENT_ID, 100, /* at */ 0, /* clear */ true);
    });
    ASSERT_EQ(100, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));

    // Contains data
    ASSERT_PROFILE_EVENT(ProfileEvents::DMSegmentIngestDataByReplace, +1, {
        ingestDTFileByReplace(DELTA_MERGE_FIRST_SEGMENT_ID, 42, /* at */ 10, /* clear */ true);
    });
    ASSERT_EQ(42, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));

    auto right_seg = splitSegmentAt(DELTA_MERGE_FIRST_SEGMENT_ID, 30, Segment::SplitMode::Logical);
    ASSERT_TRUE(right_seg.has_value());
    ASSERT_EQ(20, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(22, getSegmentRowNum(*right_seg));

    auto stable_page_ids = getAliveExternalPageIdsAfterGC(NAMESPACE_ID);
    ASSERT_EQ(1, stable_page_ids.size());

    // Current segments: [-∞, 30), [30, +∞)
    ASSERT_PROFILE_EVENT(ProfileEvents::DMSegmentIngestDataByReplace, +1, {
        ingestDTFileByReplace(DELTA_MERGE_FIRST_SEGMENT_ID, 60, /* at */ 15, /* clear */ true);
    });
    ASSERT_EQ(15, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(22, getSegmentRowNum(*right_seg));

    stable_page_ids = getAliveExternalPageIdsAfterGC(NAMESPACE_ID);
    ASSERT_EQ(2, stable_page_ids.size());
}
CATCH

class SegmentIngestTestWithPreserve : public SegmentTestBasic
{
    // This test case is simple, because most cases should be covered by IsEmptyTest.
};

TEST_F(SegmentIngestTestWithPreserve, EmptySegment)
try
{
    ASSERT_PROFILE_EVENT(ProfileEvents::DMSegmentIngestDataByReplace, +1, {
        ingestDTFileByReplace(DELTA_MERGE_FIRST_SEGMENT_ID, 100, /* at */ 0, /* clear */ false);
    });
    ASSERT_EQ(100, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));
}
CATCH

TEST_F(SegmentIngestTestWithPreserve, NonEmptySegment)
try
{
    // As long as the segment is not empty, we will ingest into the delta,
    // instead of ingest by replacing the stable.

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100, /* at */ 0);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    ASSERT_PROFILE_EVENT(ProfileEvents::DMSegmentIngestDataIntoDelta, +1, {
        ingestDTFileByReplace(DELTA_MERGE_FIRST_SEGMENT_ID, 42, /* at */ 200, /* clear */ false);
    });
    ASSERT_EQ(142, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));
}
CATCH

TEST_F(SegmentIngestTestWithPreserve, DeleteRangeSegment)
try
{
    // Altough the segment is *actually* empty, we don't have ways to quickly
    // determine it and we treat this case as "likely to be not empty".
    // So finally we will ingest into the delta.

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100, /* at */ 0);
    deleteRangeSegment(DELTA_MERGE_FIRST_SEGMENT_ID);
    ASSERT_EQ(0, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));

    ASSERT_PROFILE_EVENT(ProfileEvents::DMSegmentIngestDataIntoDelta, +1, {
        ingestDTFileByReplace(DELTA_MERGE_FIRST_SEGMENT_ID, 42, /* at */ 200, /* clear */ false);
    });
    ASSERT_EQ(42, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));
}
CATCH

TEST_F(SegmentIngestTestWithPreserve, EmptySegmentButNonEmptyStable)
try
{
    // The segment is referencing a non-empty stable. But the stable's any pack
    // is not covered by the segment range.

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100, /* at */ 0);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    auto right_seg = splitSegmentAt(DELTA_MERGE_FIRST_SEGMENT_ID, 200, Segment::SplitMode::Logical);
    ASSERT_EQ(0, getSegmentRowNum(*right_seg));

    auto stable_page_ids = getAliveExternalPageIdsAfterGC(NAMESPACE_ID);
    ASSERT_EQ(1, stable_page_ids.size());

    ASSERT_PROFILE_EVENT(ProfileEvents::DMSegmentIngestDataByReplace, +1, {
        ingestDTFileByReplace(*right_seg, 42, /* at */ 200, /* clear */ false);
    });
    ASSERT_EQ(42, getSegmentRowNum(*right_seg));
    ASSERT_EQ(100, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));

    // After ingestion, we should have 2 stables.
    stable_page_ids = getAliveExternalPageIdsAfterGC(NAMESPACE_ID);
    ASSERT_EQ(2, stable_page_ids.size());
}
CATCH


} // namespace tests
} // namespace DM
} // namespace DB
