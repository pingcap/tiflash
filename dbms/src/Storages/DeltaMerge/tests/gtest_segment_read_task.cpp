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
#include <Core/BlockUtils.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStoreMock.h>
#include <Storages/DeltaMerge/Remote/DisaggSnapshot.h>
#include <Storages/DeltaMerge/Remote/Serializer.h>
#include <Storages/DeltaMerge/SegmentReadTask.h>
#include <Storages/DeltaMerge/tests/gtest_dm_delta_merge_store_test_basic.h>
#include <Storages/DeltaMerge/tests/gtest_segment_test_basic.h>
#include <Storages/DeltaMerge/tests/gtest_segment_util.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/InputStreamTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
using namespace DB::tests;

namespace DB::ErrorCodes
{
extern const int DT_DELTA_INDEX_ERROR;
extern const int FETCH_PAGES_ERROR;
} // namespace DB::ErrorCodes

namespace DB::DM::tests
{

class SegmentReadTaskTest : public SegmentTestBasic
{
protected:
    DB::LoggerPtr log = DB::Logger::get("SegmentReadTaskTest");
};

TEST_F(SegmentReadTaskTest, InitInputStream)
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

    auto task = std::make_shared<DM::SegmentReadTask>(
        first,
        first_snap,
        createDMContext(),
        RowKeyRanges{first->getRowKeyRange()});

    const auto & column_defines = *tableColumns();
    ASSERT_FALSE(task->doInitInputStreamWithErrorFallback(
        column_defines,
        0,
        nullptr,
        ReadMode::Bitmap,
        DEFAULT_BLOCK_SIZE,
        true));

    try
    {
        [[maybe_unused]] auto succ = task->doInitInputStreamWithErrorFallback(
            column_defines,
            0,
            nullptr,
            ReadMode::Bitmap,
            DEFAULT_BLOCK_SIZE,
            false);
        FAIL() << "Should not come here.";
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::DT_DELTA_INDEX_ERROR);
    }

    task->initInputStream(column_defines, 0, nullptr, ReadMode::Bitmap, DEFAULT_BLOCK_SIZE, true);
    auto stream = task->getInputStream();
    ASSERT_NE(stream, nullptr);
    std::vector<Block> blks;
    for (auto blk = stream->read(); blk; blk = stream->read())
    {
        blks.push_back(blk);
    }
    auto handle_col1 = vstackBlocks(std::move(blks)).getByName(EXTRA_HANDLE_COLUMN_NAME).column;
    auto handle_col2 = getSegmentHandle(task->segment->segmentId(), {task->segment->getRowKeyRange()});
    ASSERT_TRUE(sequenceEqual(
        toColumnVectorDataPtr<Int64>(handle_col2)->data(),
        toColumnVectorDataPtr<Int64>(handle_col1)->data(),
        handle_col1->size()));
}
CATCH


TEST_F(DeltaMergeStoreTest, DisaggReadSnapshot)
try
{
    auto table_column_defines = DMTestEnv::getDefaultColumns();
    store = reload(table_column_defines);

    // stable
    {
        auto block = DMTestEnv::prepareSimpleWriteBlock(0, 4096, false);
        store->write(*db_context, db_context->getSettingsRef(), block);
        store->mergeDeltaAll(*db_context);
    }

    // cf delete range
    {
        HandleRange range(0, 128);
        store->deleteRange(*db_context, db_context->getSettingsRef(), RowKeyRange::fromHandleRange(range));
    }

    // cf big
    {
        auto block = DMTestEnv::prepareSimpleWriteBlock(0, 128, false);
        auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
        auto [range, file_ids] = genDMFile(*dm_context, block);
        store->ingestFiles(dm_context, range, file_ids, false);
    }

    // cf tiny
    {
        auto block = DMTestEnv::prepareSimpleWriteBlock(0, 128, false);
        store->write(*db_context, db_context->getSettingsRef(), block);
        store->flushCache(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));
    }
    {
        auto block = DMTestEnv::prepareSimpleWriteBlock(0, 128, false);
        store->write(*db_context, db_context->getSettingsRef(), block);
        store->flushCache(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));
    }

    // cf mem
    {
        auto block = DMTestEnv::prepareSimpleWriteBlock(0, 128, false);
        store->write(*db_context, db_context->getSettingsRef(), block);
    }

    auto scan_context = std::make_shared<ScanContext>();
    auto snap = store->writeNodeBuildRemoteReadSnapshot(
        *db_context,
        db_context->getSettingsRef(),
        {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
        1,
        "req_id",
        {},
        scan_context);

    snap->column_defines = std::make_shared<ColumnDefines>(store->getTableColumns());

    MemTrackerWrapper mem_tracker_wrapper(nullptr);
    auto remote_table_pb = Remote::Serializer::serializeTo(snap, /*task_id*/ {}, mem_tracker_wrapper);

    ASSERT_GT(remote_table_pb.segments_size(), 0);

    db_context->getSharedContextDisagg()->remote_data_store
        = std::make_shared<DM::Remote::DataStoreMock>(db_context->getFileProvider());

    for (const auto & remote_seg : remote_table_pb.segments())
    {
        auto seg_task = std::make_shared<SegmentReadTask>(
            Logger::get(),
            *db_context,
            scan_context,
            remote_seg,
            DisaggTaskId{},
            /*store_id*/ 1,
            /*store_address*/ "127.0.0.1",
            store->keyspace_id,
            store->physical_table_id);

        auto seg_id = seg_task->segment->segmentId();

        auto itr = store->id_to_segment.find(seg_id);
        ASSERT_NE(itr, store->id_to_segment.end()) << seg_id;

        auto seg = itr->second;
        ASSERT_NE(seg, nullptr) << seg_id;
        auto delta_wn = seg->getDelta();
        ASSERT_NE(delta_wn, nullptr) << seg_id;
        auto stable_wn = seg->getStable();
        ASSERT_NE(stable_wn, nullptr) << seg_id;

        ASSERT_NE(seg_task->segment, nullptr) << seg_id;
        auto delta_cn = seg->getDelta();
        ASSERT_NE(delta_cn, nullptr) << seg_id;
        auto stable_cn = seg->getStable();
        ASSERT_NE(stable_cn, nullptr) << seg_id;

        // Check Delta
        ASSERT_EQ(delta_wn->getDeltaIndexEpoch(), delta_cn->getDeltaIndexEpoch());
        ASSERT_EQ(delta_wn->simpleInfo(), delta_cn->simpleInfo());
        ASSERT_EQ(delta_wn->info(), delta_cn->info());
        ASSERT_EQ(delta_wn->getId(), delta_cn->getId());

        // cf mem set
        auto mem_set_wn = delta_wn->getMemTableSet();
        auto mem_set_cn = delta_cn->getMemTableSet();
        ASSERT_EQ(mem_set_wn->getColumnFileCount(), 1);
        ASSERT_EQ(mem_set_cn->getColumnFileCount(), 1);
        for (size_t i = 0; i < mem_set_wn->getColumnFileCount(); i++)
        {
            auto * cf_wn = mem_set_wn->column_files[i]->tryToInMemoryFile();
            ASSERT_NE(cf_wn, nullptr);
            auto * cf_cn = mem_set_cn->column_files[i]->tryToInMemoryFile();
            ASSERT_NE(cf_cn, nullptr);
            ASSERT_EQ(cf_wn->toString(), cf_cn->toString());
            String msg;
            ASSERT_TRUE(blockEqual(cf_wn->getCache()->block, cf_cn->getCache()->block, msg));
        }

        auto check_dmfile = [](const DMFilePtr & dmfile_wn, const DMFilePtr & dmfile_cn) {
            ASSERT_EQ(dmfile_wn->file_id, dmfile_cn->file_id);
            ASSERT_EQ(dmfile_wn->page_id, dmfile_cn->page_id);
            ASSERT_EQ(dmfile_wn->parent_path, dmfile_cn->parent_path);
            ASSERT_EQ(dmfile_wn->status, dmfile_cn->status);
            ASSERT_EQ(dmfile_wn->version, dmfile_cn->version);

            ASSERT_TRUE(dmfile_wn->configuration.has_value());
            ASSERT_TRUE(dmfile_cn->configuration.has_value());
            ASSERT_EQ(
                dmfile_wn->configuration->getChecksumFrameLength(),
                dmfile_cn->configuration->getChecksumFrameLength());
            ASSERT_EQ(
                dmfile_wn->configuration->getChecksumHeaderLength(),
                dmfile_cn->configuration->getChecksumHeaderLength());
            ASSERT_EQ(
                dmfile_wn->configuration->getChecksumAlgorithm(),
                dmfile_cn->configuration->getChecksumAlgorithm());
            ASSERT_EQ(dmfile_wn->configuration->getEmbeddedChecksum(), dmfile_cn->configuration->getEmbeddedChecksum());
            ASSERT_EQ(dmfile_wn->configuration->getDebugInfo(), dmfile_cn->configuration->getDebugInfo());

            ASSERT_EQ(dmfile_wn->pack_stats.size(), dmfile_cn->pack_stats.size());
            for (size_t j = 0; j < dmfile_wn->pack_stats.size(); j++)
            {
                ASSERT_EQ(dmfile_wn->pack_stats[j].toDebugString(), dmfile_cn->pack_stats[j].toDebugString());
            }

            ASSERT_EQ(dmfile_wn->pack_properties.property_size(), dmfile_cn->pack_properties.property_size());
            for (int j = 0; j < dmfile_wn->pack_properties.property_size(); j++)
            {
                ASSERT_EQ(
                    dmfile_wn->pack_properties.property(j).ShortDebugString(),
                    dmfile_cn->pack_properties.property(j).ShortDebugString());
            }

            ASSERT_EQ(dmfile_wn->column_stats.size(), dmfile_cn->column_stats.size());
            for (const auto & [col_id, col_stat_wn] : dmfile_wn->column_stats)
            {
                auto itr = dmfile_cn->column_stats.find(col_id);
                ASSERT_NE(itr, dmfile_cn->column_stats.end());
                const auto & col_stat_cn = itr->second;
                WriteBufferFromOwnString wb_wn;
                col_stat_wn.serializeToBuffer(wb_wn);
                WriteBufferFromOwnString wb_cn;
                col_stat_cn.serializeToBuffer(wb_cn);
                ASSERT_EQ(wb_wn.str(), wb_cn.str());
            }

            ASSERT_EQ(dmfile_wn->column_indices, dmfile_cn->column_indices);

            ASSERT_EQ(dmfile_wn->merged_files.size(), dmfile_cn->merged_files.size());
            for (size_t j = 0; j < dmfile_wn->merged_files.size(); j++)
            {
                const auto & merged_file_wn = dmfile_wn->merged_files[j];
                const auto & merged_file_cn = dmfile_cn->merged_files[j];
                ASSERT_EQ(merged_file_wn.number, merged_file_cn.number);
                ASSERT_EQ(merged_file_wn.size, merged_file_cn.size);
            }

            ASSERT_EQ(dmfile_wn->merged_sub_file_infos.size(), dmfile_cn->merged_sub_file_infos.size());
            for (const auto & [fname, sub_files_wn] : dmfile_wn->merged_sub_file_infos)
            {
                auto itr = dmfile_cn->merged_sub_file_infos.find(fname);
                ASSERT_NE(itr, dmfile_cn->merged_sub_file_infos.end());
                const auto & sub_files_cn = itr->second;
                WriteBufferFromOwnString wb_wn;
                sub_files_wn.serializeToBuffer(wb_wn);
                WriteBufferFromOwnString wb_cn;
                sub_files_cn.serializeToBuffer(wb_cn);
                ASSERT_EQ(wb_wn.str(), wb_cn.str());
            }
        };

        // cf persist set
        auto persist_set_wn = delta_wn->getPersistedFileSet();
        auto persist_set_cn = delta_cn->getPersistedFileSet();
        ASSERT_EQ(persist_set_wn->getColumnFileCount(), 4);
        ASSERT_EQ(persist_set_cn->getColumnFileCount(), 4);
        ASSERT_EQ(persist_set_wn->detailInfo(), persist_set_cn->detailInfo());
        for (size_t i = 0; i < persist_set_wn->getColumnFileCount(); i++)
        {
            auto cf_wn = persist_set_wn->getFiles()[i];
            auto cf_cn = persist_set_cn->getFiles()[i];

            if (i == 0)
            {
                auto * cf_del_wn = cf_wn->tryToDeleteRange();
                ASSERT_NE(cf_del_wn, nullptr);
                auto * cf_del_cn = cf_cn->tryToDeleteRange();
                ASSERT_NE(cf_del_cn, nullptr);
                ASSERT_EQ(cf_del_wn->getDeleteRange(), cf_del_cn->getDeleteRange());
            }
            else if (i == 1)
            {
                auto * cf_big_wn = cf_wn->tryToBigFile();
                ASSERT_NE(cf_big_wn, nullptr);
                auto * cf_big_cn = cf_cn->tryToBigFile();
                ASSERT_NE(cf_big_cn, nullptr);
                check_dmfile(cf_big_wn->getFile(), cf_big_cn->getFile());
            }
            else
            {
                auto * cf_tiny_wn = cf_wn->tryToTinyFile();
                ASSERT_NE(cf_tiny_wn, nullptr);
                auto * cf_tiny_cn = cf_cn->tryToTinyFile();
                ASSERT_NE(cf_tiny_cn, nullptr);
                ASSERT_EQ(cf_tiny_wn->getDataPageId(), cf_tiny_cn->getDataPageId());
                ASSERT_EQ(cf_tiny_wn->getDataPageSize(), cf_tiny_cn->getDataPageSize());
            }
        }

        // Check Stable
        ASSERT_EQ(stable_wn->getId(), stable_cn->getId());
        ASSERT_EQ(stable_wn->getRows(), stable_cn->getRows());
        ASSERT_EQ(stable_wn->getBytes(), stable_cn->getBytes());
        ASSERT_EQ(stable_wn->getDMFiles().size(), 1);
        ASSERT_EQ(stable_cn->getDMFiles().size(), 1);
        for (size_t i = 0; i < stable_wn->getDMFiles().size(); i++)
        {
            const auto & dmfile_wn = stable_wn->getDMFiles()[i];
            const auto & dmfile_cn = stable_cn->getDMFiles()[i];
            check_dmfile(dmfile_wn, dmfile_cn);
        }
    }
}
CATCH

} // namespace DB::DM::tests
