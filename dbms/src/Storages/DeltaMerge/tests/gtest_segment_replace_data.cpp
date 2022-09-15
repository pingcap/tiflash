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
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/tests/gtest_segment_test_basic.h>
#include <TestUtils/InputStreamTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

#include <future>

namespace DB
{

namespace DM
{

extern DMFilePtr writeIntoNewDMFile(DMContext & dm_context,
                                    const ColumnDefinesPtr & schema_snap,
                                    const BlockInputStreamPtr & input_stream,
                                    UInt64 file_id,
                                    const String & parent_path,
                                    DMFileBlockOutputStream::Flags flags);

namespace tests
{

class SegmentReplaceDataTest : public SegmentTestBasic
    , public testing::WithParamInterface<UInt64>
{
public:
    SegmentReplaceDataTest()
    {
        replace_to_rows = GetParam();
    }

protected:
    UInt64 replace_to_rows{};
};

INSTANTIATE_TEST_CASE_P(
    ReplaceToNRows,
    SegmentReplaceDataTest,
    testing::Values(0, 37)); // Note: some tests rely on the exact value of 37. Adding arbitrary values may break test.

class SegmentReplaceDataBasicTest : public SegmentTestBasic
{
};

TEST_F(SegmentReplaceDataBasicTest, ThrowWhenDMFileNotInDelegator)
try
{
    auto delegator = storage_path_pool->getStableDiskDelegator();
    auto file_id = storage_pool->newDataPageIdForDTFile(delegator, __PRETTY_FUNCTION__);
    auto input_stream = std::make_shared<OneBlockInputStream>(Block{});
    auto dm_file = writeIntoNewDMFile(
        *dm_context,
        table_columns,
        input_stream,
        file_id,
        delegator.choosePath(),
        DMFileBlockOutputStream::Flags{});

    ASSERT_THROW({
        replaceSegmentData({DELTA_MERGE_FIRST_SEGMENT_ID}, dm_file);
    },
                 DB::Exception);
}
CATCH


TEST_F(SegmentReplaceDataBasicTest, ThrowWhenDMFileNotInPS)
try
{
    auto delegator = storage_path_pool->getStableDiskDelegator();
    auto file_id = storage_pool->newDataPageIdForDTFile(delegator, __PRETTY_FUNCTION__);
    auto input_stream = std::make_shared<OneBlockInputStream>(Block{});
    auto dm_file = writeIntoNewDMFile(
        *dm_context,
        table_columns,
        input_stream,
        file_id,
        delegator.choosePath(),
        DMFileBlockOutputStream::Flags{});

    delegator.addDTFile(file_id, dm_file->getBytesOnDisk(), dm_file->parentPath());

    ASSERT_THROW({
        replaceSegmentData({DELTA_MERGE_FIRST_SEGMENT_ID}, dm_file);
    },
                 DB::Exception);
}
CATCH


TEST_P(SegmentReplaceDataTest, Basic)
try
{
    // Data in memtable should be discarded after replaceData
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
    ASSERT_EQ(100, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));
    {
        auto replace_block = prepareWriteBlock(/* from */ 0, /* to */ replace_to_rows);
        replaceSegmentData({DELTA_MERGE_FIRST_SEGMENT_ID}, replace_block);
    }
    ASSERT_EQ(replace_to_rows, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));

    // Even flush will not "rescue" these memtable data.
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    ASSERT_EQ(replace_to_rows, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));

    ASSERT_TRUE(storage_pool->log_storage_v3 != nullptr);
    storage_pool->log_storage_v3->gc(/* not_skip */ true);
    storage_pool->data_storage_v3->gc(/* not_skip */ true);
    ASSERT_EQ(storage_pool->log_storage_v3->getNumberOfPages(), 0);
    ASSERT_EQ(storage_pool->data_storage_v3->getNumberOfPages(), 2); // 1 DMFile, 1 Ref
    PageId replaced_stable_id{};
    {
        auto stable_page_ids = storage_pool->data_storage_v3->getAliveExternalPageIds(NAMESPACE_ID);
        ASSERT_EQ(1, stable_page_ids.size());
        replaced_stable_id = *stable_page_ids.begin();
    }

    // Write some data and create a new stable.
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 47, /* at */ replace_to_rows + 100);
    ASSERT_EQ(47 + replace_to_rows, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    ASSERT_EQ(47 + replace_to_rows, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);
    ASSERT_EQ(47 + replace_to_rows, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));

    storage_pool->log_storage_v3->gc(/* not_skip */ true);
    storage_pool->data_storage_v3->gc(/* not_skip */ true);
    ASSERT_EQ(storage_pool->log_storage_v3->getNumberOfPages(), 0);
    ASSERT_EQ(storage_pool->data_storage_v3->getNumberOfPages(), 1);

    auto const stable_files = segments[DELTA_MERGE_FIRST_SEGMENT_ID]->getStable()->getDMFiles();
    {
        // Only the new stable DMFile is alive (and we should have a different DMFile).
        auto stable_page_ids = storage_pool->data_storage_v3->getAliveExternalPageIds(NAMESPACE_ID);
        ASSERT_EQ(1, stable_page_ids.size());
        ASSERT_TRUE(stable_page_ids.count(stable_files[0]->fileId()));
        ASSERT_FALSE(stable_page_ids.count(replaced_stable_id));
    }

    // Now let's replace data again. Everything in the current stable will be discarded.
    {
        auto replace_block = prepareWriteBlock(/* from */ 0, /* to */ replace_to_rows);
        replaceSegmentData({DELTA_MERGE_FIRST_SEGMENT_ID}, replace_block);
    }
    ASSERT_EQ(replace_to_rows, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));
    {
        storage_pool->data_storage_v3->gc(/* not_skip */ true);
        auto stable_page_ids = storage_pool->data_storage_v3->getAliveExternalPageIds(NAMESPACE_ID);
        ASSERT_EQ(1, stable_page_ids.size());
        // The stable before replaceData should be not alive anymore.
        ASSERT_FALSE(stable_page_ids.count(stable_files[0]->fileId()));
    }
}
CATCH

TEST_P(SegmentReplaceDataTest, WriteAfterReplace)
try
{
    if (replace_to_rows == 0)
    {
        return;
    }

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
    ASSERT_EQ(100, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));
    {
        auto replace_block = prepareWriteBlock(/* from */ 0, /* to */ replace_to_rows);
        replaceSegmentData({DELTA_MERGE_FIRST_SEGMENT_ID}, replace_block);
    }
    ASSERT_EQ(replace_to_rows, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 47, /* at */ replace_to_rows - 10); // 10 rows will be overlapped
    ASSERT_EQ(37 + replace_to_rows, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);
    ASSERT_EQ(37 + replace_to_rows, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(47 + replace_to_rows, getSegmentRowNumWithoutMVCC(DELTA_MERGE_FIRST_SEGMENT_ID));
}
CATCH


/**
 * This test verify that, the DMFile will never be marked as GCable, during different segment operations.
 * Otherwise, the DMFile will be unsafe to be used in another replaceData.
 */
TEST_F(SegmentReplaceDataBasicTest, DMFileGCIsUnchanged)
try
{
    WriteBatches ingest_wbs(dm_context->storage_pool, dm_context->getWriteLimiter());

    auto delegator = storage_path_pool->getStableDiskDelegator();
    auto file_id = storage_pool->newDataPageIdForDTFile(delegator, __PRETTY_FUNCTION__);
    auto input_stream = std::make_shared<OneBlockInputStream>(Block{});
    auto dm_file = writeIntoNewDMFile(
        *dm_context,
        table_columns,
        input_stream,
        file_id,
        delegator.choosePath(),
        DMFileBlockOutputStream::Flags{});

    ingest_wbs.data.putExternal(file_id, /* tag */ 0);
    ingest_wbs.writeLogAndData();
    delegator.addDTFile(file_id, dm_file->getBytesOnDisk(), dm_file->parentPath());

    replaceSegmentData({DELTA_MERGE_FIRST_SEGMENT_ID}, dm_file);
    ASSERT_EQ(0, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));

    ingest_wbs.rollbackWrittenLogAndData();

    // Note: we have not yet enabled GC for the dmfile here.
    ASSERT_FALSE(dm_file->canGC());
    {
        auto stable_page_ids = storage_pool->data_storage_v3->getAliveExternalPageIds(NAMESPACE_ID);
        ASSERT_TRUE(stable_page_ids.count(dm_file->fileId()));
    }

    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 47);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    // Even when the stable is replaced, the DMFile should not be marked as GCable.
    ASSERT_FALSE(dm_file->canGC());
    {
        storage_pool->data_storage_v3->gc(/* not_skip */ true);
        auto stable_page_ids = storage_pool->data_storage_v3->getAliveExternalPageIds(NAMESPACE_ID);
        ASSERT_EQ(1, stable_page_ids.size());
        ASSERT_FALSE(stable_page_ids.count(dm_file->fileId()));
    }

    // TODO: May be check split and merge as well.

    dm_file->enableGC();
}
CATCH


TEST_P(SegmentReplaceDataTest, MultipleSegmentsSharingDMFile)
try
{
    std::optional<PageId> seg_right_id;
    Block block{};

    if (replace_to_rows == 0)
    {
        seg_right_id = splitSegmentAt(DELTA_MERGE_FIRST_SEGMENT_ID, 0);
        // block is empty, split point doesn't matter.
    }
    else
    {
        seg_right_id = splitSegmentAt(DELTA_MERGE_FIRST_SEGMENT_ID, replace_to_rows - 10); /* right seg should contain 10 rows after replacing data */
        block = prepareWriteBlock(0, replace_to_rows);
    }

    ASSERT_TRUE(seg_right_id.has_value());
    replaceSegmentData({*seg_right_id, DELTA_MERGE_FIRST_SEGMENT_ID}, block);
    ASSERT_TRUE(areSegmentsSharingStable({*seg_right_id, DELTA_MERGE_FIRST_SEGMENT_ID}));

    UInt64 expected_left_rows, expected_right_rows;
    if (replace_to_rows == 0)
    {
        expected_left_rows = 0;
        expected_right_rows = 0;
    }
    else
    {
        expected_left_rows = replace_to_rows - 10;
        expected_right_rows = 10;
    }
    ASSERT_EQ(expected_left_rows, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(expected_right_rows, getSegmentRowNum(*seg_right_id));

    // Now let's write something and perform merge delta for the right seg
    writeSegment(*seg_right_id, 151);
    expected_right_rows += 151;
    ASSERT_EQ(expected_right_rows, getSegmentRowNumWithoutMVCC(*seg_right_id));
    flushSegmentCache(*seg_right_id);
    mergeSegmentDelta(*seg_right_id);
    ASSERT_EQ(expected_right_rows, getSegmentRowNumWithoutMVCC(*seg_right_id));
    // Left is not affected
    ASSERT_EQ(expected_left_rows, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_FALSE(areSegmentsSharingStable({*seg_right_id, DELTA_MERGE_FIRST_SEGMENT_ID}));

    ASSERT_TRUE(storage_pool->log_storage_v3 != nullptr);
    storage_pool->data_storage_v3->gc(/* not_skip */ true);
    auto stable_page_ids = storage_pool->data_storage_v3->getAliveExternalPageIds(NAMESPACE_ID);
    ASSERT_EQ(2, stable_page_ids.size());

    mergeSegment({DELTA_MERGE_FIRST_SEGMENT_ID, *seg_right_id});
    storage_pool->data_storage_v3->gc(/* not_skip */ true);
    stable_page_ids = storage_pool->data_storage_v3->getAliveExternalPageIds(NAMESPACE_ID);
    ASSERT_EQ(1, stable_page_ids.size());
}
CATCH


TEST_F(SegmentReplaceDataBasicTest, ReplaceMultipleTimes)
try
{
    for (size_t i = 0; i < 20; ++i)
    {
        auto rows = std::uniform_int_distribution<>(1, 100)(random);
        auto block = prepareWriteBlock(0, rows);
        replaceSegmentData({DELTA_MERGE_FIRST_SEGMENT_ID}, block);
        ASSERT_EQ(rows, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));

        // Write some rows doesn't affect our next replaceData
        writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID);
    }

    ASSERT_TRUE(storage_pool->log_storage_v3 != nullptr);
    storage_pool->data_storage_v3->gc(/* not_skip */ true);
    auto stable_page_ids = storage_pool->data_storage_v3->getAliveExternalPageIds(NAMESPACE_ID);
    ASSERT_EQ(1, stable_page_ids.size());
}
CATCH


TEST_P(SegmentReplaceDataTest, ReplaceSameDMFileMultipleTimes)
try
{
    auto block = prepareWriteBlock(0, replace_to_rows);

    WriteBatches ingest_wbs(dm_context->storage_pool, dm_context->getWriteLimiter());

    auto delegator = storage_path_pool->getStableDiskDelegator();
    auto file_id = storage_pool->newDataPageIdForDTFile(delegator, __PRETTY_FUNCTION__);
    auto input_stream = std::make_shared<OneBlockInputStream>(block);
    auto dm_file = writeIntoNewDMFile(
        *dm_context,
        table_columns,
        input_stream,
        file_id,
        delegator.choosePath(),
        DMFileBlockOutputStream::Flags{});

    ingest_wbs.data.putExternal(file_id, /* tag */ 0);
    ingest_wbs.writeLogAndData();
    delegator.addDTFile(file_id, dm_file->getBytesOnDisk(), dm_file->parentPath());

    for (size_t i = 0; i < 20; ++i)
    {
        replaceSegmentData({DELTA_MERGE_FIRST_SEGMENT_ID}, block);
        ASSERT_EQ(replace_to_rows, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));
        // Write some rows doesn't affect our next replaceData
        writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID);
    }

    dm_file->enableGC();
    ingest_wbs.rollbackWrittenLogAndData();

    ASSERT_TRUE(storage_pool->log_storage_v3 != nullptr);
    storage_pool->data_storage_v3->gc(/* not_skip */ true);
    auto stable_page_ids = storage_pool->data_storage_v3->getAliveExternalPageIds(NAMESPACE_ID);
    ASSERT_EQ(1, stable_page_ids.size());
}
CATCH


/**
 * The out of bound data introduced by replaceData should not be seen after the merge.
 */
TEST_F(SegmentReplaceDataBasicTest, ReplaceOutOfBoundAndMerge)
try
{
    auto seg_right_id = splitSegmentAt(DELTA_MERGE_FIRST_SEGMENT_ID, 100, Segment::SplitMode::Physical);
    ASSERT_TRUE(seg_right_id.has_value());

    writeSegment(*seg_right_id, 10);
    ASSERT_EQ(0, getSegmentRowNumWithoutMVCC(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(10, getSegmentRowNumWithoutMVCC(*seg_right_id));

    auto block = prepareWriteBlock(0, 300);
    // Only replace this block to the left seg, whose range is [-∞, 100).
    replaceSegmentData({DELTA_MERGE_FIRST_SEGMENT_ID}, block);
    ASSERT_EQ(100, getSegmentRowNumWithoutMVCC(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(10, getSegmentRowNumWithoutMVCC(*seg_right_id));

    mergeSegment({DELTA_MERGE_FIRST_SEGMENT_ID, *seg_right_id});
    ASSERT_EQ(110, getSegmentRowNumWithoutMVCC(DELTA_MERGE_FIRST_SEGMENT_ID));

    ASSERT_TRUE(storage_pool->log_storage_v3 != nullptr);
    storage_pool->log_storage_v3->gc(/* not_skip */ true);
    storage_pool->data_storage_v3->gc(/* not_skip */ true);
    ASSERT_EQ(storage_pool->log_storage_v3->getNumberOfPages(), 0);
    ASSERT_EQ(storage_pool->data_storage_v3->getNumberOfPages(), 1);
    auto stable_page_ids = storage_pool->data_storage_v3->getAliveExternalPageIds(NAMESPACE_ID);
    ASSERT_EQ(1, stable_page_ids.size());
}
CATCH


TEST_F(SegmentReplaceDataBasicTest, ReleaseExistingSharedDMFile)
try
{
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 500, /* at */ 0);
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);

    // Use logical split to create two segments sharing the same stable.
    auto seg_right_id = splitSegmentAt(DELTA_MERGE_FIRST_SEGMENT_ID, 100, Segment::SplitMode::Logical);
    ASSERT_TRUE(seg_right_id.has_value());
    ASSERT_TRUE(areSegmentsSharingStable({DELTA_MERGE_FIRST_SEGMENT_ID, *seg_right_id}));

    ASSERT_EQ(100, getSegmentRowNumWithoutMVCC(DELTA_MERGE_FIRST_SEGMENT_ID));
    ASSERT_EQ(400, getSegmentRowNumWithoutMVCC(*seg_right_id));

    auto shared_dm_files = segments[*seg_right_id]->getStable()->getDMFiles();

    // As stable is shared in logical split, we should only have 1 alive external file.
    ASSERT_TRUE(storage_pool->log_storage_v3 != nullptr);
    storage_pool->data_storage_v3->gc(/* not_skip */ true);
    auto stable_page_ids = storage_pool->data_storage_v3->getAliveExternalPageIds(NAMESPACE_ID);

    // Now let's replace one segment.
    auto block = prepareWriteBlock(0, 300);
    replaceSegmentData({DELTA_MERGE_FIRST_SEGMENT_ID}, block);

    ASSERT_EQ(100, getSegmentRowNumWithoutMVCC(DELTA_MERGE_FIRST_SEGMENT_ID)); // We should only see [0, 100)
    ASSERT_EQ(400, getSegmentRowNumWithoutMVCC(*seg_right_id));

    // The previously-shared stable should be still valid.
    storage_pool->data_storage_v3->gc(/* not_skip */ true);
    stable_page_ids = storage_pool->data_storage_v3->getAliveExternalPageIds(NAMESPACE_ID);
    ASSERT_EQ(2, stable_page_ids.size());
    ASSERT_TRUE(stable_page_ids.count(shared_dm_files[0]->fileId()));
}
CATCH


TEST_F(SegmentReplaceDataBasicTest, ReadSnapshotBeforeReplace)
try
{
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 400); // 400 in stable
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 41); // 41 in memtable

    auto segment = segments[DELTA_MERGE_FIRST_SEGMENT_ID];
    auto in_stream = segment->getInputStreamRaw(*dm_context, *tableColumns());

    // Now let's replace data.
    auto block = prepareWriteBlock(0, 233);
    replaceSegmentData({DELTA_MERGE_FIRST_SEGMENT_ID}, block);

    // There is a snapshot alive, so we should have 2 stables.
    storage_pool->data_storage_v3->gc(/* not_skip */ true);
    auto stable_page_ids = storage_pool->data_storage_v3->getAliveExternalPageIds(NAMESPACE_ID);
    ASSERT_EQ(2, stable_page_ids.size());

    // Continue the read
    auto n_rows = DB::tests::getInputStreamNRows(in_stream);
    ASSERT_EQ(441, n_rows);

    ASSERT_EQ(233, getSegmentRowNumWithoutMVCC(DELTA_MERGE_FIRST_SEGMENT_ID));

    // Snapshot is dropped.
    in_stream = {};
    storage_pool->data_storage_v3->gc(/* not_skip */ true);
    stable_page_ids = storage_pool->data_storage_v3->getAliveExternalPageIds(NAMESPACE_ID);
    ASSERT_EQ(1, stable_page_ids.size());
}
CATCH


} // namespace tests
} // namespace DM
} // namespace DB
