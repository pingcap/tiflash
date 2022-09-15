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
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/tests/gtest_segment_test_basic.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/defines.h>
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
{
};

TEST_F(SegmentReplaceDataTest, ThrowWhenDMFileNotInDelegator)
try
{
    reloadWithOptions({});

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
        replaceDataSegment(DELTA_MERGE_FIRST_SEGMENT_ID, dm_file);
    }, DB::Exception);
}
CATCH


TEST_F(SegmentReplaceDataTest, ThrowWhenDMFileNotInPS)
try
{
    reloadWithOptions({});

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
        replaceDataSegment(DELTA_MERGE_FIRST_SEGMENT_ID, dm_file);
    }, DB::Exception);
}
CATCH


TEST_F(SegmentReplaceDataTest, EmptyDMFile)
try
{
    reloadWithOptions({});

    // Data in memtable should be discarded after replaceData
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
    ASSERT_EQ(100, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));
    replaceDataSegment(DELTA_MERGE_FIRST_SEGMENT_ID, Block{});
    ASSERT_EQ(0, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));

    // Even flush will not "rescue" these memtable data.
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    ASSERT_EQ(0, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));

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
    writeSegment(DELTA_MERGE_FIRST_SEGMENT_ID, 47);
    ASSERT_EQ(47, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));
    flushSegmentCache(DELTA_MERGE_FIRST_SEGMENT_ID);
    ASSERT_EQ(47, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));
    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);
    ASSERT_EQ(47, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));

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
    replaceDataSegment(DELTA_MERGE_FIRST_SEGMENT_ID, Block{});
    ASSERT_EQ(0, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));
    {
        storage_pool->data_storage_v3->gc(/* not_skip */ true);
        auto stable_page_ids = storage_pool->data_storage_v3->getAliveExternalPageIds(NAMESPACE_ID);
        ASSERT_EQ(1, stable_page_ids.size());
        // The stable before replaceData should be not alive anymore.
        ASSERT_FALSE(stable_page_ids.count(stable_files[0]->fileId()));
    }
}
CATCH


/**
 * This test verify that, the DMFile will never be marked as GCable, during different segment operations.
 * Otherwise, the DMFile will be unsafe to be used in another replaceData.
 */
TEST_F(SegmentReplaceDataTest, GCIsUnchanged)
try
{
    reloadWithOptions({});

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

    replaceDataSegment(DELTA_MERGE_FIRST_SEGMENT_ID, dm_file);

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


TEST_F(SegmentReplaceDataTest, MultipleSegmentsSharingDMFile)
try
{
    auto left_id = DELTA_MERGE_FIRST_SEGMENT_ID;
//    auto right_id =
}
CATCH


// replaceData same segment for multiple times

// replaceData different segment refereicing same file

// TODO: Change rows to 0 or N


//
//TEST_F(SegmentReplaceDataTest, EmptySegment)
//try
//{
//    reloadWithOptions({});
//
//    auto delegator = storage_path_pool->getStableDiskDelegator();
//    auto parent_path = delegator.choosePath();
//    auto file_provider = db_context->getFileProvider();
//    auto scanIds = DMFile::listAllInPath(file_provider, parent_path, DMFile::ListOptions{ .only_list_can_gc = true });
//
//    LOG_FMT_INFO(Logger::get("test"), "---- GCable ---- scanIds = {}", fmt::join(scanIds, ","));
//    LOG_FMT_INFO(Logger::get("test"), "---- this_seg={}", segments[DELTA_MERGE_FIRST_SEGMENT_ID]->info());
//
//    ASSERT_EQ(0, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));
//
//    auto blocks = prepareWriteBlocksInSegmentRange(DELTA_MERGE_FIRST_SEGMENT_ID, 100);
//    ASSERT_EQ(blocks.size(), 1);
//
//    auto current_seg = segments[DELTA_MERGE_FIRST_SEGMENT_ID];
//
//    WriteBatches ingest_wbs(dm_context->storage_pool, dm_context->getWriteLimiter());
//
//    auto file_id = storage_pool->newDataPageIdForDTFile(delegator, __PRETTY_FUNCTION__);
//    auto input_stream = std::make_shared<OneBlockInputStream>(blocks[0]);
//    auto dm_file = writeIntoNewDMFile(
//        *dm_context,
//        table_columns,
//        input_stream,
//        file_id,
//        parent_path,
//        {});
//
//    ASSERT_THROW({
//        auto res = segments[DELTA_MERGE_FIRST_SEGMENT_ID]->dangerouslyReplaceDataForTest(*dm_context, dm_file);
//        UNUSED(res);
//    }, DB::Exception);
//
//    ingest_wbs.data.putExternal(file_id, /* tag */ 0);
//    ingest_wbs.writeLogAndData();
//    delegator.addDTFile(file_id, dm_file->getBytesOnDisk(), parent_path);
//
//    auto new_seg = segments[DELTA_MERGE_FIRST_SEGMENT_ID]->dangerouslyReplaceDataForTest(*dm_context, dm_file);
//    segments[DELTA_MERGE_FIRST_SEGMENT_ID] = new_seg;
//
//    dm_file->enableGC();
//    ingest_wbs.rollbackWrittenLogAndData();
//
//    ASSERT_EQ(100, getSegmentRowNum(DELTA_MERGE_FIRST_SEGMENT_ID));
//
//    current_seg->abandon(*dm_context);
//
//    ASSERT_TRUE(storage_pool->log_storage_v3 != nullptr);
//    storage_pool->log_storage_v3->gc(/* not_skip */ true);
//    storage_pool->data_storage_v3->gc(/* not_skip */ true);
//    ASSERT_EQ(storage_pool->log_storage_v3->getNumberOfPages(), 0);
//    ASSERT_EQ(storage_pool->data_storage_v3->getNumberOfPages(), 2);
//    ASSERT_EQ(storage_pool->data_storage_v3->getAliveExternalPageIds(100).size(), 1);
//    ASSERT_EQ(*storage_pool->data_storage_v3->getAliveExternalPageIds(100).begin(), file_id);
//
//    scanIds = DMFile::listAllInPath(file_provider, parent_path, DMFile::ListOptions{ .only_list_can_gc = true });
//
//    LOG_FMT_INFO(Logger::get("test"), "---- GCable ---- scanIds = {},  my_file_id = {}", fmt::join(scanIds, ","), file_id);
//    LOG_FMT_INFO(Logger::get("test"), "---- current_seg={}, new_seg={}", current_seg->info(), new_seg->info());
//
//    mergeSegmentDelta(DELTA_MERGE_FIRST_SEGMENT_ID);
//
//    storage_pool->log_storage_v3->gc(/* not_skip */ true);
//    storage_pool->data_storage_v3->gc(/* not_skip */ true);
//    ASSERT_EQ(storage_pool->log_storage_v3->getNumberOfPages(), 0);
//    ASSERT_EQ(storage_pool->data_storage_v3->getNumberOfPages(), 1);
//    ASSERT_EQ(storage_pool->data_storage_v3->getAliveExternalPageIds(100).size(), 1);
//    ASSERT_NE(*storage_pool->data_storage_v3->getAliveExternalPageIds(100).begin(), file_id);
//
//    scanIds = DMFile::listAllInPath(file_provider, parent_path, DMFile::ListOptions{ .only_list_can_gc = true });
//    LOG_FMT_INFO(Logger::get("test"), "---- GCable after merge delta ---- scanIds = {}", fmt::join(scanIds, ","));
//}
//CATCH
//
//
//
//TEST_F(SegmentReplaceDataTest, TriggerDMFileGC)
//try
//{
//
//}
//CATCH
//
//
//TEST_F(SegmentReplaceDataTest, WriteDuringReplaceData)
//try
//{
//
//}
//CATCH
//
//

// Test Share DMFile

// Test GC (new_me->setLastCheckGCSafePoint(context.min_version);)


}
}
}
