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
#include <Common/SyncPoint/SyncPoint.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/WriteBatchesImpl.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/PathPool.h>
#include <Storages/Transaction/TMTContext.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

#include <future>
#include <limits>
#include <memory>

namespace CurrentMetrics
{
extern const Metric DT_SnapshotOfRead;
} // namespace CurrentMetrics
namespace DB
{
namespace DM
{
extern DMFilePtr writeIntoNewDMFile(DMContext & dm_context, //
                                    const ColumnDefinesPtr & schema_snap,
                                    const BlockInputStreamPtr & input_stream,
                                    UInt64 file_id,
                                    const String & parent_path);
namespace tests
{
void assertBlocksEqual(const Blocks & blocks1, const Blocks & blocks2)
{
    // use rows and hash to check the read results
    size_t rows1 = 0;
    SipHash hash1;
    for (const auto & block : blocks1)
    {
        rows1 += block.rows();
        block.updateHash(hash1);
    }

    size_t rows2 = 0;
    SipHash hash2;
    for (const auto & block : blocks2)
    {
        rows2 += block.rows();
        block.updateHash(hash2);
    }

    ASSERT_EQ(rows1, rows2);
    ASSERT_EQ(hash1.get64(), hash2.get64());
}

class DeltaValueSpaceTest : public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        table_columns = std::make_shared<ColumnDefines>();

        delta = reload();
        ASSERT_EQ(delta->getId(), delta_id);
    }

protected:
    DeltaValueSpacePtr reload(const ColumnDefinesPtr & pre_define_columns = {}, DB::Settings && db_settings = DB::Settings())
    {
        TiFlashStorageTestBasic::reload(std::move(db_settings));
        storage_path_pool = std::make_shared<StoragePathPool>(db_context->getPathPool().withTable("test", "t1", false));
        storage_pool = std::make_shared<StoragePool>(*db_context, NullspaceID, table_id, *storage_path_pool, "test.t1");
        storage_pool->restore();
        ColumnDefinesPtr cols = (!pre_define_columns) ? DMTestEnv::getDefaultColumns() : pre_define_columns;
        setColumns(cols);

        return std::make_unique<DeltaValueSpace>(delta_id);
    }

    // setColumns should update dm_context at the same time
    void setColumns(const ColumnDefinesPtr & columns)
    {
        *table_columns = *columns;

        dm_context = std::make_unique<DMContext>(*db_context,
                                                 storage_path_pool,
                                                 storage_pool,
                                                 /*min_version_*/ 0,
                                                 NullspaceID,
                                                 /*physical_table_id*/ 100,
                                                 false,
                                                 1,
                                                 db_context->getSettingsRef());
    }

    const ColumnDefinesPtr & tableColumns() const { return table_columns; }

    DMContext & dmContext() { return *dm_context; }

protected:
    /// all these var lives as ref in dm_context
    std::shared_ptr<StoragePathPool> storage_path_pool;
    std::shared_ptr<StoragePool> storage_pool;
    ColumnDefinesPtr table_columns;
    DM::DeltaMergeStore::Settings settings;
    /// dm_context
    std::unique_ptr<DMContext> dm_context;

    // the delta we are going to test
    DeltaValueSpacePtr delta;

    static constexpr TableID table_id = 100;
    static constexpr PageIdU64 delta_id = 1;
    static constexpr size_t num_rows_write_per_batch = 100;
};

Block appendBlockToDeltaValueSpace(DMContext & context, DeltaValueSpacePtr delta, size_t rows_start, size_t rows_num, UInt64 tso = 2)
{
    Block block = DMTestEnv::prepareSimpleWriteBlock(rows_start, rows_start + rows_num, false, tso);
    delta->appendToCache(context, block, 0, block.rows());
    return block;
}

Block appendColumnFileTinyToDeltaValueSpace(DMContext & context, DeltaValueSpacePtr delta, size_t rows_start, size_t rows_num, WriteBatches & wbs, UInt64 tso = 2)
{
    Block block = DMTestEnv::prepareSimpleWriteBlock(rows_start, rows_start + rows_num, false, tso);
    auto tiny_file = ColumnFileTiny::writeColumnFile(context, block, 0, block.rows(), wbs);
    wbs.writeLogAndData();
    delta->appendColumnFile(context, tiny_file);
    return block;
}

Block appendColumnFileBigToDeltaValueSpace(DMContext & context, ColumnDefinesPtr column_defines, DeltaValueSpacePtr delta, size_t rows_start, size_t rows_num, WriteBatches & wbs, UInt64 tso = 2)
{
    Block block = DMTestEnv::prepareSimpleWriteBlock(rows_start, rows_start + rows_num, false, tso);
    auto delegator = context.path_pool->getStableDiskDelegator();
    auto file_id = context.storage_pool->newDataPageIdForDTFile(delegator, __PRETTY_FUNCTION__);
    auto input_stream = std::make_shared<OneBlockInputStream>(block);
    auto store_path = delegator.choosePath();
    auto dmfile
        = writeIntoNewDMFile(context, std::make_shared<ColumnDefines>(*column_defines), input_stream, file_id, store_path);
    delegator.addDTFile(file_id, dmfile->getBytesOnDisk(), store_path);

    auto & pk_column = block.getByPosition(0).column;
    auto min_pk = pk_column->getInt(0);
    auto max_pk = pk_column->getInt(block.rows() - 1);
    HandleRange range(min_pk, max_pk + 1);

    auto column_file = std::make_shared<ColumnFileBig>(context, dmfile, RowKeyRange::fromHandleRange(range));
    wbs.data.putExternal(file_id, 0);
    wbs.writeLogAndData();
    delta->ingestColumnFiles(context, RowKeyRange::fromHandleRange(range), {column_file}, false);
    return block;
}

// This function do the following check
// 1. read all rows using `DeltaValueReader` and verify its correctness
// 2. read rows in the `handle_range` and verify the rows matches
static void checkDeltaValueSpaceData(
    const DeltaValueSpacePtr & delta,
    DMContext & dm_context,
    const ColumnDefinesPtr & table_columns,
    const Blocks & expected_all_blocks,
    size_t expected_all_rows,
    const HandleRange & handle_range,
    size_t expected_range_rows)
{
    ASSERT(!expected_all_blocks.empty());
    auto snapshot = delta->createSnapshot(dm_context, false, CurrentMetrics::DT_SnapshotOfRead);
    auto rows = snapshot->getRows();
    ASSERT_EQ(rows, expected_all_rows);

    {
        auto reader = std::make_shared<DeltaValueReader>(
            dm_context,
            snapshot,
            table_columns,
            RowKeyRange::newAll(false, 1));
        auto columns = expected_all_blocks[0].cloneEmptyColumns();
        ASSERT_EQ(reader->readRows(columns, 0, expected_all_rows, nullptr), expected_all_rows);
        Blocks result_blocks;
        result_blocks.push_back(expected_all_blocks[0].cloneWithColumns(std::move(columns)));
        assertBlocksEqual(expected_all_blocks, result_blocks);
    }

    // read with a specific range
    {
        // For `ColumnFileBig`, the same column file reader cannot be used twice, wo we create a new `DeltaValueReader` here.
        auto reader = std::make_shared<DeltaValueReader>(
            dm_context,
            snapshot,
            table_columns,
            RowKeyRange::newAll(false, 1));
        auto columns = expected_all_blocks[0].cloneEmptyColumns();
        RowKeyRange read_range = RowKeyRange::fromHandleRange(handle_range);
        ASSERT_EQ(reader->readRows(columns, 0, expected_all_rows, &read_range), expected_range_rows);
    }
}

TEST_F(DeltaValueSpaceTest, WriteRead)
{
    Blocks write_blocks;
    size_t total_rows_write = 0;
    WriteBatches wbs(*dmContext().storage_pool, dmContext().getWriteLimiter());
    // `ColumnFileInMemory`, `ColumnFileTiny`, `ColumnFileDeleteRange` and `ColumnFileBig` in `MemTableSet`
    {
        // `ColumnFileInMemory`
        write_blocks.push_back(appendBlockToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch));
        total_rows_write += num_rows_write_per_batch;

        // `ColumnFileDeleteRange`
        // the actual delete range value doesn't matter
        delta->appendDeleteRange(dmContext(), RowKeyRange::fromHandleRange(HandleRange(0, num_rows_write_per_batch)));

        // `ColumnFileTiny`
        write_blocks.push_back(appendColumnFileTinyToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch, wbs));
        total_rows_write += num_rows_write_per_batch;

        // `ColumnFileBig`
        write_blocks.push_back(appendColumnFileBigToDeltaValueSpace(dmContext(), table_columns, delta, total_rows_write, num_rows_write_per_batch, wbs));
        total_rows_write += num_rows_write_per_batch;

        checkDeltaValueSpaceData(delta, dmContext(), table_columns, write_blocks, total_rows_write, HandleRange(total_rows_write - num_rows_write_per_batch, total_rows_write - num_rows_write_per_batch / 2), num_rows_write_per_batch / 2);
    }

    // `ColumnFileInMemory`, `ColumnFileTiny`, `ColumnFileDeleteRange` and `ColumnFileBig` in `ColumnFilePersistedSet`
    {
        ASSERT_EQ(delta->getUnsavedRows(), total_rows_write);
        delta->flush(dmContext());
        ASSERT_EQ(delta->getUnsavedRows(), 0);
        checkDeltaValueSpaceData(delta, dmContext(), table_columns, write_blocks, total_rows_write, HandleRange(total_rows_write - num_rows_write_per_batch, total_rows_write - num_rows_write_per_batch / 2), num_rows_write_per_batch / 2);
    }

    // `ColumnFileInMemory`, `ColumnFileTiny`, `ColumnFileDeleteRange` and `ColumnFileBig` in `MemTableSet`
    // `ColumnFileInMemory`, `ColumnFileTiny`, `ColumnFileDeleteRange` and `ColumnFileBig` in `ColumnFilePersistedSet`
    {
        // `ColumnFileInMemory`
        write_blocks.push_back(appendBlockToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch));
        total_rows_write += num_rows_write_per_batch;

        // `ColumnFileDeleteRange`
        // the actual delete range value doesn't matter
        delta->appendDeleteRange(dmContext(), RowKeyRange::fromHandleRange(HandleRange(0, num_rows_write_per_batch)));

        // `ColumnFileTiny`
        write_blocks.push_back(appendColumnFileTinyToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch, wbs));
        total_rows_write += num_rows_write_per_batch;

        // `ColumnFileBig`
        write_blocks.push_back(appendColumnFileBigToDeltaValueSpace(dmContext(), table_columns, delta, total_rows_write, num_rows_write_per_batch, wbs));
        total_rows_write += num_rows_write_per_batch;

        checkDeltaValueSpaceData(delta, dmContext(), table_columns, write_blocks, total_rows_write, HandleRange(total_rows_write - num_rows_write_per_batch, total_rows_write - num_rows_write_per_batch / 2), num_rows_write_per_batch / 2);
    }
}

// Write data to MemTableSet when do flush at the same time
TEST_F(DeltaValueSpaceTest, Flush)
{
    auto mem_table_set = delta->getMemTableSet();
    auto persisted_file_set = delta->getPersistedFileSet();
    WriteBatches wbs(*dmContext().storage_pool, dmContext().getWriteLimiter());
    size_t total_rows_write = 0;
    // write some column_file
    {
        {
            appendBlockToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch);
            total_rows_write += num_rows_write_per_batch;
        }
        {
            delta->appendDeleteRange(dmContext(), RowKeyRange::fromHandleRange(HandleRange(0, num_rows_write_per_batch)));
        }
        {
            appendColumnFileTinyToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch, wbs);
            total_rows_write += num_rows_write_per_batch;
        }
    }
    // build flush task and finish prepare stage
    ColumnFileFlushTaskPtr flush_task;
    {
        flush_task = mem_table_set->buildFlushTask(dmContext(), persisted_file_set->getRows(), persisted_file_set->getDeletes(), persisted_file_set->getCurrentFlushVersion());
        ASSERT_EQ(flush_task->getTaskNum(), 3);
        ASSERT_EQ(flush_task->getFlushRows(), 2 * num_rows_write_per_batch);
        ASSERT_EQ(flush_task->getFlushDeletes(), 1);
        flush_task->prepare(wbs);
    }
    // another thread write more data to the delta value space
    {
        appendBlockToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch);
        total_rows_write += num_rows_write_per_batch;
    }
    // commit the flush task and check the status after flush
    {
        ASSERT_TRUE(flush_task->commit(persisted_file_set, wbs));
        ASSERT_EQ(persisted_file_set->getRows(), 2 * num_rows_write_per_batch);
        ASSERT_EQ(persisted_file_set->getDeletes(), 1);
        ASSERT_EQ(mem_table_set->getRows(), total_rows_write - persisted_file_set->getRows());
    }
}

TEST_F(DeltaValueSpaceTest, MinorCompaction)
{
    auto persisted_file_set = delta->getPersistedFileSet();
    WriteBatches wbs(*dmContext().storage_pool, dmContext().getWriteLimiter());
    size_t total_rows_write = 0;
    // write some column_file and flush
    {
        {
            appendBlockToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch);
            total_rows_write += num_rows_write_per_batch;
        }
        {
            appendColumnFileTinyToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch, wbs);
            total_rows_write += num_rows_write_per_batch;
        }
        {
            appendBlockToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch);
            total_rows_write += num_rows_write_per_batch;
        }
        {
            delta->appendDeleteRange(dmContext(), RowKeyRange::fromHandleRange(HandleRange(0, num_rows_write_per_batch)));
        }
        {
            appendBlockToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch);
            total_rows_write += num_rows_write_per_batch;
        }
        delta->flush(dmContext());
    }
    // build compaction task and finish prepare stage
    MinorCompactionPtr compaction_task;
    {
        PageReaderPtr reader = dmContext().storage_pool->newLogReader(dmContext().getReadLimiter(), true, "");
        compaction_task = persisted_file_set->pickUpMinorCompaction(dmContext());
        // There should be three compaction sub_tasks.
        // The first task try to compact the first three column files to a larger one.
        // The second task is a trivial move for a ColumnFileDeleteRange.
        // The third task is a trivial move for and a ColumnFileTiny.
        const auto & tasks = compaction_task->getTasks();
        ASSERT_EQ(compaction_task->getFirsCompactIndex(), 0);
        ASSERT_EQ(tasks.size(), 3);
        ASSERT_EQ(tasks[0].to_compact.size(), 3);
        ASSERT_EQ(tasks[0].is_trivial_move, false);
        ASSERT_EQ(tasks[1].to_compact.size(), 1);
        ASSERT_EQ(tasks[1].is_trivial_move, true);
        ASSERT_EQ(tasks[2].to_compact.size(), 1);
        ASSERT_EQ(tasks[2].is_trivial_move, true);
        compaction_task->prepare(dmContext(), wbs, *reader);
    }

    // another thread write more data to the delta value space and flush it
    {
        appendBlockToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch);
        total_rows_write += num_rows_write_per_batch;
        delta->flush(dmContext());
        ASSERT_EQ(delta->getUnsavedRows(), 0);
        ASSERT_EQ(persisted_file_set->getRows(), total_rows_write);
        ASSERT_EQ(persisted_file_set->getDeletes(), 1);
        ASSERT_EQ(persisted_file_set->getColumnFileCount(), 6);
    }
    // commit the compaction task and check the status
    {
        ASSERT_TRUE(compaction_task->commit(persisted_file_set, wbs));
        ASSERT_EQ(persisted_file_set->getRows(), total_rows_write);
        ASSERT_EQ(persisted_file_set->getDeletes(), 1);
        ASSERT_EQ(persisted_file_set->getColumnFileCount(), 4);
    }
    // now the column files in persisted_file_set should be: T_300, D_0_100, T_100, T_100
    {
        // generate but not commit
        compaction_task = persisted_file_set->pickUpMinorCompaction(dmContext());
        EXPECT_EQ(compaction_task->getFirsCompactIndex(), 2);
        // generate and commit
        PageReaderPtr reader = dmContext().storage_pool->newLogReader(dmContext().getReadLimiter(), true, "");
        compaction_task = persisted_file_set->pickUpMinorCompaction(dmContext());
        EXPECT_EQ(compaction_task->getFirsCompactIndex(), 2);
        compaction_task->prepare(dmContext(), wbs, *reader);
        ASSERT_TRUE(compaction_task->commit(persisted_file_set, wbs));
        ASSERT_EQ(persisted_file_set->getRows(), total_rows_write);
        ASSERT_EQ(persisted_file_set->getDeletes(), 1);
        ASSERT_EQ(persisted_file_set->getColumnFileCount(), 3);
    }
    // now the column files in persisted_file_set should be: T_300, D_0_100, T_200
    // so there is no compaction task to do
    {
        compaction_task = persisted_file_set->pickUpMinorCompaction(dmContext());
        ASSERT_TRUE(!compaction_task);
    }
    // do a lot of minor compaction and check the status
    {
        for (size_t i = 0; i < 20; i++)
        {
            appendBlockToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch);
            total_rows_write += num_rows_write_per_batch;
            delta->flush(dmContext());
            while (true)
            {
                PageReaderPtr reader = dmContext().storage_pool->newLogReader(dmContext().getReadLimiter(), true, "");
                auto minor_compaction_task = persisted_file_set->pickUpMinorCompaction(dmContext());
                if (!minor_compaction_task)
                    break;
                ASSERT_NE(minor_compaction_task->getFirsCompactIndex(), std::numeric_limits<size_t>::max());
                minor_compaction_task->prepare(dmContext(), wbs, *reader);
                minor_compaction_task->commit(persisted_file_set, wbs);
            }
            wbs.writeRemoves();
            ASSERT_EQ(persisted_file_set->getRows(), total_rows_write);
            ASSERT_EQ(persisted_file_set->getDeletes(), 1);
        }
    }
}

TEST_F(DeltaValueSpaceTest, Restore)
{
    auto persisted_file_set = delta->getPersistedFileSet();
    size_t total_rows_write = 0;
    // write some column_file, flush and compact it
    {
        WriteBatches wbs(*dmContext().storage_pool, dmContext().getWriteLimiter());
        {
            appendBlockToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch);
            total_rows_write += num_rows_write_per_batch;
        }
        {
            appendColumnFileTinyToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch, wbs);
            total_rows_write += num_rows_write_per_batch;
        }
        {
            delta->appendDeleteRange(dmContext(), RowKeyRange::fromHandleRange(HandleRange(0, num_rows_write_per_batch)));
        }
        delta->flush(dmContext());
        delta->compact(dmContext());
        // after compaction, the two ColumnFileTiny must be compacted to a large column file, so there are just two column files left.
        ASSERT_EQ(delta->getColumnFileCount(), 2);
    }
    // write more data and flush it, and then there are two levels in the persisted_file_set
    {
        {
            appendBlockToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch);
            total_rows_write += num_rows_write_per_batch;
        }
        delta->flush(dmContext());
        ASSERT_EQ(delta->getColumnFileCount(), 3);
        ASSERT_EQ(delta->getRows(), total_rows_write);
    }
    // check the column file order remain the same after restore
    {
        Blocks old_delta_blocks;
        {
            auto old_delta_snapshot = delta->createSnapshot(dmContext(), false, CurrentMetrics::DT_SnapshotOfRead);
            DeltaValueInputStream old_delta_stream(dmContext(), old_delta_snapshot, table_columns, RowKeyRange::newAll(false, 1));
            old_delta_stream.readPrefix();
            while (true)
            {
                auto block = old_delta_stream.read();
                if (!block)
                    break;
                old_delta_blocks.push_back(std::move(block));
            }
            old_delta_stream.readSuffix();
        }
        Blocks new_delta_blocks;
        {
            auto new_delta = delta->restore(dmContext(), RowKeyRange::newAll(false, 1), delta_id);
            auto new_delta_snapshot = new_delta->createSnapshot(dmContext(), false, CurrentMetrics::DT_SnapshotOfRead);
            DeltaValueInputStream new_delta_stream(dmContext(), new_delta_snapshot, table_columns, RowKeyRange::newAll(false, 1));
            new_delta_stream.readPrefix();
            while (true)
            {
                auto block = new_delta_stream.read();
                if (!block)
                    break;
                new_delta_blocks.push_back(std::move(block));
            }
            new_delta_stream.readSuffix();
        }
        assertBlocksEqual(old_delta_blocks, new_delta_blocks);
    }
}

TEST_F(DeltaValueSpaceTest, CloneNewlyAppendedColumnFiles)
{
    auto persisted_file_set = delta->getPersistedFileSet();
    size_t total_rows_write = 0;
    WriteBatches wbs(*dmContext().storage_pool, dmContext().getWriteLimiter());
    {
        {
            appendBlockToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch);
            total_rows_write += num_rows_write_per_batch;
        }
        {
            appendColumnFileTinyToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch, wbs);
            total_rows_write += num_rows_write_per_batch;
        }
        delta->flush(dmContext());
        delta->compact(dmContext());
        ASSERT_EQ(delta->getColumnFileCount(), 1);
        {
            appendBlockToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch);
            total_rows_write += num_rows_write_per_batch;
        }
        {
            appendColumnFileTinyToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch, wbs);
            total_rows_write += num_rows_write_per_batch;
        }
        delta->flush(dmContext());
        delta->compact(dmContext());
        ASSERT_EQ(delta->getColumnFileCount(), 1);
        {
            appendBlockToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch);
            total_rows_write += num_rows_write_per_batch;
        }
        {
            appendColumnFileTinyToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch, wbs);
            total_rows_write += num_rows_write_per_batch;
        }
        delta->flush(dmContext());
        delta->compact(dmContext());
        ASSERT_EQ(delta->getColumnFileCount(), 1);
    }
    {
        auto snapshot = delta->createSnapshot(dmContext(), true, CurrentMetrics::DT_SnapshotOfRead);
        auto snapshot_rows = snapshot->getRows();
        ASSERT_EQ(snapshot_rows, total_rows_write);
        // write some more column file to persisted_file_set and memory_table_set
        for (size_t i = 0; i < 2; i++)
        {
            // ColumnFileInMemory
            appendBlockToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch);
            total_rows_write += num_rows_write_per_batch;
            // ColumnFileDeleteRange
            delta->appendDeleteRange(dmContext(), RowKeyRange::newAll(false, 1));
            // ColumnFileTiny
            appendColumnFileTinyToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch, wbs);
            total_rows_write += num_rows_write_per_batch;
            // ColumnFileBig
            appendColumnFileBigToDeltaValueSpace(dmContext(), table_columns, delta, total_rows_write, num_rows_write_per_batch, wbs);
            total_rows_write += num_rows_write_per_batch;
            if (i == 0)
                delta->flush(dmContext());
        }
        auto lock = delta->getLock();
        auto [persisted_column_files, in_memory_files] = delta->cloneNewlyAppendedColumnFiles(
            *lock,
            dmContext(),
            RowKeyRange::newAll(false, 1),
            *snapshot,
            wbs);
        wbs.writeLogAndData();
        ASSERT_EQ(persisted_column_files.size(), 4);
        ASSERT_EQ(in_memory_files.size(), 4);
        size_t tail_rows = 0;
        for (const auto & file : persisted_column_files)
            tail_rows += file->getRows();
        for (const auto & file : in_memory_files)
            tail_rows += file->getRows();
        ASSERT_EQ(snapshot_rows + tail_rows, total_rows_write);
    }
}

TEST_F(DeltaValueSpaceTest, GetPlaceItems)
{
    size_t total_rows_write = 0;
    WriteBatches wbs(*dmContext().storage_pool, dmContext().getWriteLimiter());
    // write some data to persisted_file_set and mem_table_set
    {
        appendColumnFileTinyToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch, wbs);
        total_rows_write += num_rows_write_per_batch;
        appendColumnFileTinyToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch, wbs);
        total_rows_write += num_rows_write_per_batch;
        appendColumnFileTinyToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch, wbs);
        total_rows_write += num_rows_write_per_batch;
        delta->flush(dmContext());
        appendBlockToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch);
        total_rows_write += num_rows_write_per_batch;
    }
    // read
    {
        auto snapshot = delta->createSnapshot(dmContext(), false, CurrentMetrics::DT_SnapshotOfRead);
        auto rows = snapshot->getRows();
        ASSERT_EQ(rows, total_rows_write);
        // write some more data after create snapshot
        appendBlockToDeltaValueSpace(dmContext(), delta, total_rows_write, num_rows_write_per_batch);
        ASSERT_EQ(delta->getRows(true), total_rows_write + num_rows_write_per_batch);
        auto reader = std::make_shared<DeltaValueReader>(
            dmContext(),
            snapshot,
            table_columns,
            RowKeyRange::newAll(false, 1));
        auto place_items = reader->getPlaceItems(0, 0, snapshot->getRows(), snapshot->getDeletes());
        ASSERT_EQ(place_items.size(), 2);
        size_t total_place_rows = 0;
        for (auto & item : place_items)
        {
            ASSERT_EQ(item.isBlock(), true);
            auto block = item.getBlock();
            total_place_rows += block.rows();
        }
        ASSERT_EQ(total_place_rows, total_rows_write);
    }
}
TEST_F(DeltaValueSpaceTest, ShouldPlace)
{
    size_t tso = 100;
    WriteBatches wbs(*dmContext().storage_pool, dmContext().getWriteLimiter());
    appendBlockToDeltaValueSpace(dmContext(), delta, 0, num_rows_write_per_batch, tso);
    {
        auto snapshot = delta->createSnapshot(dmContext(), false, CurrentMetrics::DT_SnapshotOfRead);
        auto reader = std::make_shared<DeltaValueReader>(
            dmContext(),
            snapshot,
            table_columns,
<<<<<<< HEAD
            RowKeyRange::newAll(false, 1));
        ASSERT_TRUE(reader->shouldPlace(dmContext(), snapshot->getSharedDeltaIndex(), RowKeyRange::newAll(false, 1), RowKeyRange::fromHandleRange(HandleRange(0, 100)), tso + 1));
        ASSERT_FALSE(reader->shouldPlace(dmContext(), snapshot->getSharedDeltaIndex(), RowKeyRange::newAll(false, 1), RowKeyRange::fromHandleRange(HandleRange(0, 100)), tso - 1));
=======
            RowKeyRange::newAll(false, 1),
            ReadTag::Internal);
        auto [placed_rows, placed_deletes] = snapshot->getSharedDeltaIndex()->getPlacedStatus();
        ASSERT_TRUE(reader->shouldPlace(
            dmContext(),
            placed_rows,
            placed_deletes,
            RowKeyRange::newAll(false, 1),
            RowKeyRange::fromHandleRange(HandleRange(0, 100)),
            tso + 1));
        ASSERT_FALSE(reader->shouldPlace(
            dmContext(),
            placed_rows,
            placed_deletes,
            RowKeyRange::newAll(false, 1),
            RowKeyRange::fromHandleRange(HandleRange(0, 100)),
            tso - 1));
>>>>>>> 8e170090fa (Storages: Fix cloning delta index when there are duplicated tuples (#9000))
    }
    {
        delta->flush(dmContext());
        auto snapshot = delta->createSnapshot(dmContext(), false, CurrentMetrics::DT_SnapshotOfRead);
        auto reader = std::make_shared<DeltaValueReader>(
            dmContext(),
            snapshot,
            table_columns,
<<<<<<< HEAD
            RowKeyRange::newAll(false, 1));
        ASSERT_TRUE(reader->shouldPlace(dmContext(), snapshot->getSharedDeltaIndex(), RowKeyRange::newAll(false, 1), RowKeyRange::fromHandleRange(HandleRange(0, 100)), tso + 1));
        ASSERT_FALSE(reader->shouldPlace(dmContext(), snapshot->getSharedDeltaIndex(), RowKeyRange::newAll(false, 1), RowKeyRange::fromHandleRange(HandleRange(0, 100)), tso - 1));
=======
            RowKeyRange::newAll(false, 1),
            ReadTag::Internal);
        auto [placed_rows, placed_deletes] = snapshot->getSharedDeltaIndex()->getPlacedStatus();
        ASSERT_TRUE(reader->shouldPlace(
            dmContext(),
            placed_rows,
            placed_deletes,
            RowKeyRange::newAll(false, 1),
            RowKeyRange::fromHandleRange(HandleRange(0, 100)),
            tso + 1));
        ASSERT_FALSE(reader->shouldPlace(
            dmContext(),
            placed_rows,
            placed_deletes,
            RowKeyRange::newAll(false, 1),
            RowKeyRange::fromHandleRange(HandleRange(0, 100)),
            tso - 1));
>>>>>>> 8e170090fa (Storages: Fix cloning delta index when there are duplicated tuples (#9000))
    }
}


TEST_F(DeltaValueSpaceTest, CreateSnapshotForUpdate)
try
{
    appendBlockToDeltaValueSpace(dmContext(), delta, 0, 1000);
    auto snapshot_1 = delta->createSnapshot(dmContext(), true, CurrentMetrics::DT_SnapshotOfRead);
    ASSERT_TRUE(snapshot_1);
    // Snapshot includes data in memtable
    ASSERT_EQ(1000, snapshot_1->getRows());
    ASSERT_EQ(1000, snapshot_1->getMemTableSetSnapshot()->getRows());
    ASSERT_EQ(0, snapshot_1->getPersistedFileSetSnapshot()->getRows());
    ASSERT_EQ(0, snapshot_1->getDeletes());

    // When for_update snapshot is alive, flush is allowed;
    ASSERT_TRUE(delta->flush(dmContext()));

    auto snapshot_2 = delta->createSnapshot(dmContext(), true, CurrentMetrics::DT_SnapshotOfRead);
    // Only one for update snapshot is allowed
    ASSERT_FALSE(snapshot_2);

    // Snapshot_1 is unchanged
    ASSERT_EQ(1000, snapshot_1->getRows());
    ASSERT_EQ(1000, snapshot_1->getMemTableSetSnapshot()->getRows());
    ASSERT_EQ(1, snapshot_1->getMemTableSetSnapshot()->getColumnFileCount());
    ASSERT_EQ(1000, snapshot_1->getMemTableSetSnapshot()->getColumnFiles()[0]->tryToInMemoryFile()->getCache()->block.rows());
    ASSERT_EQ(0, snapshot_1->getPersistedFileSetSnapshot()->getRows());
    ASSERT_EQ(0, snapshot_1->getDeletes());

    snapshot_1.reset();

    snapshot_2 = delta->createSnapshot(dmContext(), true, CurrentMetrics::DT_SnapshotOfRead);
    ASSERT_TRUE(snapshot_2);
    ASSERT_EQ(1000, snapshot_2->getRows());
    ASSERT_EQ(0, snapshot_2->getMemTableSetSnapshot()->getRows());
    ASSERT_EQ(1000, snapshot_2->getPersistedFileSetSnapshot()->getRows());
    ASSERT_EQ(0, snapshot_2->getDeletes());

    appendBlockToDeltaValueSpace(dmContext(), delta, 100, 200);
    // Snapshot_2 is unchanged
    ASSERT_EQ(1000, snapshot_2->getRows());
    ASSERT_EQ(0, snapshot_2->getMemTableSetSnapshot()->getRows());
    ASSERT_EQ(1000, snapshot_2->getPersistedFileSetSnapshot()->getRows());
    ASSERT_EQ(0, snapshot_2->getDeletes());

    snapshot_2.reset();
    auto snapshot_3 = delta->createSnapshot(dmContext(), true, CurrentMetrics::DT_SnapshotOfRead);
    ASSERT_TRUE(snapshot_3);
    ASSERT_EQ(1200, snapshot_3->getRows());
    ASSERT_EQ(200, snapshot_3->getMemTableSetSnapshot()->getRows());
    ASSERT_EQ(1, snapshot_3->getMemTableSetSnapshot()->getColumnFileCount());
    ASSERT_EQ(200, snapshot_3->getMemTableSetSnapshot()->getColumnFiles()[0]->tryToInMemoryFile()->getCache()->block.rows());
    ASSERT_EQ(1000, snapshot_3->getPersistedFileSetSnapshot()->getRows());
    ASSERT_EQ(1, snapshot_3->getPersistedFileSetSnapshot()->getColumnFileCount());
    ASSERT_EQ(1000, snapshot_3->getPersistedFileSetSnapshot()->getColumnFiles()[0]->getRows());
    ASSERT_EQ(0, snapshot_3->getDeletes());
    ASSERT_EQ(2, snapshot_3->getColumnFileCount());

    // Append again. Snapshot 3 is unchanged. The data is unchanged and the statistics is unchanged.
    appendBlockToDeltaValueSpace(dmContext(), delta, 400, 200);
    ASSERT_EQ(1200, snapshot_3->getRows());
    ASSERT_EQ(200, snapshot_3->getMemTableSetSnapshot()->getRows());
    ASSERT_EQ(1, snapshot_3->getMemTableSetSnapshot()->getColumnFileCount());
    ASSERT_EQ(200, snapshot_3->getMemTableSetSnapshot()->getColumnFiles()[0]->tryToInMemoryFile()->getCache()->block.rows());
    ASSERT_EQ(1000, snapshot_3->getPersistedFileSetSnapshot()->getRows());
    ASSERT_EQ(1, snapshot_3->getPersistedFileSetSnapshot()->getColumnFileCount());
    ASSERT_EQ(1000, snapshot_3->getPersistedFileSetSnapshot()->getColumnFiles()[0]->getRows());
    ASSERT_EQ(0, snapshot_3->getDeletes());
    ASSERT_EQ(2, snapshot_3->getColumnFileCount());

    // The new snapshot will see our just-appended value.
    snapshot_3.reset();
    auto snapshot_4 = delta->createSnapshot(dmContext(), true, CurrentMetrics::DT_SnapshotOfRead);
    ASSERT_TRUE(snapshot_4);
    ASSERT_EQ(1400, snapshot_4->getRows());
    ASSERT_EQ(400, snapshot_4->getMemTableSetSnapshot()->getRows());
    ASSERT_EQ(2, snapshot_4->getMemTableSetSnapshot()->getColumnFileCount());
    ASSERT_EQ(1000, snapshot_4->getPersistedFileSetSnapshot()->getRows());
    ASSERT_EQ(1, snapshot_4->getPersistedFileSetSnapshot()->getColumnFileCount());
    ASSERT_EQ(0, snapshot_4->getDeletes());
    ASSERT_EQ(3, snapshot_4->getColumnFileCount());
}
CATCH

TEST_F(DeltaValueSpaceTest, CreateSnapshotNotForUpdate)
try
{
    appendBlockToDeltaValueSpace(dmContext(), delta, 0, 1000);
    auto snapshot_1 = delta->createSnapshot(dmContext(), true, CurrentMetrics::DT_SnapshotOfRead);
    ASSERT_TRUE(snapshot_1);
    ASSERT_EQ(1000, snapshot_1->getRows());
    ASSERT_EQ(1000, snapshot_1->getMemTableSetSnapshot()->getRows());
    ASSERT_EQ(0, snapshot_1->getPersistedFileSetSnapshot()->getRows());
    ASSERT_EQ(0, snapshot_1->getDeletes());

    delta->flush(dmContext());
    auto snapshot_2 = delta->createSnapshot(dmContext(), false, CurrentMetrics::DT_SnapshotOfRead);
    // For for_update = false, the creation will success even when there is a snapshot with for_update = true.
    ASSERT_TRUE(snapshot_2);
    ASSERT_EQ(1000, snapshot_2->getRows());
    ASSERT_EQ(0, snapshot_2->getMemTableSetSnapshot()->getRows());
    ASSERT_EQ(1000, snapshot_2->getPersistedFileSetSnapshot()->getRows());
    ASSERT_EQ(0, snapshot_2->getDeletes());
    // Snapshot_1 is unchanged
    ASSERT_EQ(1000, snapshot_1->getRows());
    ASSERT_EQ(1000, snapshot_1->getMemTableSetSnapshot()->getRows());
    ASSERT_EQ(0, snapshot_1->getPersistedFileSetSnapshot()->getRows());
    ASSERT_EQ(0, snapshot_1->getDeletes());

    // Append something. Snapshot_1 and Snapshot_2 are unchanged.
    // For snapshot_1, it is a for_update snapshot, will never change.
    // For snapshot_2, it does not contain memtable cf, so it will not change as well.
    appendBlockToDeltaValueSpace(dmContext(), delta, 100, 200);
    ASSERT_EQ(1000, snapshot_1->getRows());
    ASSERT_EQ(1000, snapshot_1->getMemTableSetSnapshot()->getRows());
    ASSERT_EQ(1, snapshot_1->getMemTableSetSnapshot()->getColumnFileCount());
    ASSERT_EQ(1000, snapshot_1->getMemTableSetSnapshot()->getColumnFiles()[0]->tryToInMemoryFile()->getCache()->block.rows());
    ASSERT_EQ(0, snapshot_1->getPersistedFileSetSnapshot()->getRows());
    ASSERT_EQ(0, snapshot_1->getDeletes());
    ASSERT_EQ(1000, snapshot_2->getRows());
    ASSERT_EQ(0, snapshot_2->getMemTableSetSnapshot()->getRows());
    ASSERT_EQ(1000, snapshot_2->getPersistedFileSetSnapshot()->getRows());
    ASSERT_EQ(0, snapshot_2->getDeletes());

    auto snapshot_3 = delta->createSnapshot(dmContext(), false, CurrentMetrics::DT_SnapshotOfRead);
    ASSERT_TRUE(snapshot_3);
    ASSERT_EQ(1200, snapshot_3->getRows());
    ASSERT_EQ(200, snapshot_3->getMemTableSetSnapshot()->getRows());
    ASSERT_EQ(1, snapshot_3->getMemTableSetSnapshot()->getColumnFileCount());
    ASSERT_EQ(200, snapshot_3->getMemTableSetSnapshot()->getColumnFiles()[0]->getRows());
    ASSERT_EQ(200, snapshot_3->getMemTableSetSnapshot()->getColumnFiles()[0]->tryToInMemoryFile()->getCache()->block.rows());
    ASSERT_EQ(1000, snapshot_3->getPersistedFileSetSnapshot()->getRows());
    ASSERT_EQ(1, snapshot_3->getPersistedFileSetSnapshot()->getColumnFileCount());
    ASSERT_EQ(1000, snapshot_3->getPersistedFileSetSnapshot()->getColumnFiles()[0]->getRows());
    ASSERT_EQ(0, snapshot_3->getDeletes());
    ASSERT_EQ(2, snapshot_3->getColumnFileCount());

    // Append again. This time, the data of existing memtable is changed... No new column file is appended.
    appendBlockToDeltaValueSpace(dmContext(), delta, 400, 200);
    // snapshot_1 - always unchanged
    ASSERT_EQ(1000, snapshot_1->getRows());
    ASSERT_EQ(1000, snapshot_1->getMemTableSetSnapshot()->getRows());
    ASSERT_EQ(1, snapshot_1->getMemTableSetSnapshot()->getColumnFileCount());
    ASSERT_EQ(1000, snapshot_1->getMemTableSetSnapshot()->getColumnFiles()[0]->tryToInMemoryFile()->getCache()->block.rows());
    ASSERT_EQ(0, snapshot_1->getPersistedFileSetSnapshot()->getRows());
    ASSERT_EQ(0, snapshot_1->getDeletes());
    // snapshot_2 - unchanged
    ASSERT_EQ(1000, snapshot_2->getRows());
    ASSERT_EQ(0, snapshot_2->getMemTableSetSnapshot()->getRows());
    ASSERT_EQ(1000, snapshot_2->getPersistedFileSetSnapshot()->getRows());
    ASSERT_EQ(0, snapshot_2->getDeletes());
    // snapshot_3 - statistics are unchanged but the underlying data is changed!
    ASSERT_EQ(1200, snapshot_3->getRows());
    ASSERT_EQ(200, snapshot_3->getMemTableSetSnapshot()->getRows());
    ASSERT_EQ(1, snapshot_3->getMemTableSetSnapshot()->getColumnFileCount());
    ASSERT_EQ(200, snapshot_3->getMemTableSetSnapshot()->getColumnFiles()[0]->getRows());
    //        ↑↑↑
    // The tricky thing is, the block data is changed.
    // We treat this as a "feature" of for_update=false snapshot, and let's verify it anyway.
    //        ↓↓↓
    ASSERT_EQ(400, snapshot_3->getMemTableSetSnapshot()->getColumnFiles()[0]->tryToInMemoryFile()->getCache()->block.rows());
    ASSERT_EQ(1000, snapshot_3->getPersistedFileSetSnapshot()->getRows());
    ASSERT_EQ(1, snapshot_3->getPersistedFileSetSnapshot()->getColumnFileCount());
    ASSERT_EQ(1000, snapshot_3->getPersistedFileSetSnapshot()->getColumnFiles()[0]->getRows());
    ASSERT_EQ(0, snapshot_3->getDeletes());
    ASSERT_EQ(2, snapshot_3->getColumnFileCount());

    // The new snapshot will have correct statistics and see all data as well
    auto snapshot_4 = delta->createSnapshot(dmContext(), false, CurrentMetrics::DT_SnapshotOfRead);
    ASSERT_TRUE(snapshot_4);
    ASSERT_EQ(1400, snapshot_4->getRows());
    ASSERT_EQ(400, snapshot_4->getMemTableSetSnapshot()->getRows());
    ASSERT_EQ(1, snapshot_4->getMemTableSetSnapshot()->getColumnFileCount());
    ASSERT_EQ(1000, snapshot_4->getPersistedFileSetSnapshot()->getRows());
    ASSERT_EQ(1, snapshot_4->getPersistedFileSetSnapshot()->getColumnFileCount());
    ASSERT_EQ(0, snapshot_4->getDeletes());
    ASSERT_EQ(2, snapshot_4->getColumnFileCount());
}
CATCH

class DeltaValueSpaceCloneNewlyAppendedTest : public DeltaValueSpaceTest
{
};

TEST_F(DeltaValueSpaceCloneNewlyAppendedTest, SnapshotIsNotForUpdate)
try
{
    WriteBatches wbs(*dmContext().storage_pool, dmContext().getWriteLimiter());
    wbs.setRollback();

    appendBlockToDeltaValueSpace(dmContext(), delta, 0, 1000);
    auto snapshot = delta->createSnapshot(dmContext(), false, CurrentMetrics::DT_SnapshotOfRead);

    auto lock = delta->getLock();
    ASSERT_THROW(
        {
            delta->cloneNewlyAppendedColumnFiles(
                *lock,
                dmContext(),
                RowKeyRange::newAll(false, 1),
                *snapshot,
                wbs);
        },
        DB::Exception);
}
CATCH

TEST_F(DeltaValueSpaceCloneNewlyAppendedTest, NoChangeAfterSnapshot)
try
{
    WriteBatches wbs(*dmContext().storage_pool, dmContext().getWriteLimiter());
    wbs.setRollback();

    appendBlockToDeltaValueSpace(dmContext(), delta, 0, 1000);
    auto snapshot = delta->createSnapshot(dmContext(), true, CurrentMetrics::DT_SnapshotOfRead);

    auto lock = delta->getLock();
    auto [new_mem, new_persisted] = delta->cloneNewlyAppendedColumnFiles(
        *lock,
        dmContext(),
        RowKeyRange::newAll(false, 1),
        *snapshot,
        wbs);
    ASSERT_EQ(0, new_mem.size());
    ASSERT_EQ(0, new_persisted.size());
}
CATCH

TEST_F(DeltaValueSpaceCloneNewlyAppendedTest, WriteAfterSnapshot)
try
{
    WriteBatches wbs(*dmContext().storage_pool, dmContext().getWriteLimiter());
    wbs.setRollback();

    appendBlockToDeltaValueSpace(dmContext(), delta, 0, 1000);
    auto snapshot = delta->createSnapshot(dmContext(), true, CurrentMetrics::DT_SnapshotOfRead);

    appendBlockToDeltaValueSpace(dmContext(), delta, 0, 42);

    auto lock = delta->getLock();
    auto [new_mem, new_persisted] = delta->cloneNewlyAppendedColumnFiles(
        *lock,
        dmContext(),
        RowKeyRange::newAll(false, 1),
        *snapshot,
        wbs);
    ASSERT_EQ(1, new_mem.size());
    ASSERT_EQ(42, new_mem[0]->getRows());
    ASSERT_EQ(0, new_persisted.size());
}
CATCH

TEST_F(DeltaValueSpaceCloneNewlyAppendedTest, FlushAfterSnapshot)
try
{
    WriteBatches wbs(*dmContext().storage_pool, dmContext().getWriteLimiter());
    wbs.setRollback();

    appendBlockToDeltaValueSpace(dmContext(), delta, 0, 1000);
    auto snapshot = delta->createSnapshot(dmContext(), true, CurrentMetrics::DT_SnapshotOfRead);

    ASSERT_TRUE(delta->flush(dmContext()));

    auto lock = delta->getLock();
    auto [new_mem, new_persisted] = delta->cloneNewlyAppendedColumnFiles(
        *lock,
        dmContext(),
        RowKeyRange::newAll(false, 1),
        *snapshot,
        wbs);
    ASSERT_EQ(0, new_mem.size());
    ASSERT_EQ(0, new_persisted.size());
}
CATCH

TEST_F(DeltaValueSpaceCloneNewlyAppendedTest, MultipleFlushWriteAfterSnapshot)
try
{
    WriteBatches wbs(*dmContext().storage_pool, dmContext().getWriteLimiter());
    wbs.setRollback();

    appendBlockToDeltaValueSpace(dmContext(), delta, 0, 200);
    appendBlockToDeltaValueSpace(dmContext(), delta, 0, 300);
    auto snapshot = delta->createSnapshot(dmContext(), true, CurrentMetrics::DT_SnapshotOfRead);

    ASSERT_TRUE(delta->flush(dmContext()));
    appendBlockToDeltaValueSpace(dmContext(), delta, 0, 100);
    appendBlockToDeltaValueSpace(dmContext(), delta, 0, 37);
    ASSERT_TRUE(delta->flush(dmContext()));
    appendBlockToDeltaValueSpace(dmContext(), delta, 200, 42);
    appendBlockToDeltaValueSpace(dmContext(), delta, 200, 5);

    auto lock = delta->getLock();
    auto [new_mem, new_persisted] = delta->cloneNewlyAppendedColumnFiles(
        *lock,
        dmContext(),
        RowKeyRange::newAll(false, 1),
        *snapshot,
        wbs);
    ASSERT_EQ(1, new_mem.size());
    ASSERT_EQ(47, new_mem[0]->getRows());
    ASSERT_EQ(1, new_persisted.size());
    ASSERT_EQ(137, new_persisted[0]->getRows());
}
CATCH

TEST_F(DeltaValueSpaceCloneNewlyAppendedTest, PersistedIsNotEmptyWhenSnapshot)
try
{
    WriteBatches wbs(*dmContext().storage_pool, dmContext().getWriteLimiter());
    wbs.setRollback();

    appendBlockToDeltaValueSpace(dmContext(), delta, 0, 42);
    delta->appendDeleteRange(dmContext(), RowKeyRange::fromHandleRange(HandleRange(10, 50)));
    ASSERT_TRUE(delta->flush(dmContext()));
    appendBlockToDeltaValueSpace(dmContext(), delta, 0, 100);
    delta->appendDeleteRange(dmContext(), RowKeyRange::fromHandleRange(HandleRange(-5, 20)));

    auto snapshot = delta->createSnapshot(dmContext(), true, CurrentMetrics::DT_SnapshotOfRead);
    ASSERT_EQ(100, snapshot->getMemTableSetSnapshot()->getRows());
    ASSERT_EQ(1, snapshot->getMemTableSetSnapshot()->getDeletes());
    ASSERT_EQ(42, snapshot->getPersistedFileSetSnapshot()->getRows());
    ASSERT_EQ(1, snapshot->getPersistedFileSetSnapshot()->getDeletes());

    ASSERT_TRUE(delta->flush(dmContext()));
    appendBlockToDeltaValueSpace(dmContext(), delta, 0, 100);
    appendBlockToDeltaValueSpace(dmContext(), delta, 0, 37);
    delta->appendDeleteRange(dmContext(), RowKeyRange::fromHandleRange(HandleRange(3, 7)));
    ASSERT_TRUE(delta->flush(dmContext()));
    appendBlockToDeltaValueSpace(dmContext(), delta, 200, 42);
    delta->appendDeleteRange(dmContext(), RowKeyRange::fromHandleRange(HandleRange(42, 101)));
    appendBlockToDeltaValueSpace(dmContext(), delta, 200, 5);

    auto lock = delta->getLock();
    auto [new_mem, new_persisted] = delta->cloneNewlyAppendedColumnFiles(
        *lock,
        dmContext(),
        RowKeyRange::newAll(false, 1),
        *snapshot,
        wbs);
    ASSERT_EQ(3, new_mem.size());
    ASSERT_EQ(42, new_mem[0]->getRows());
    ASSERT_TRUE(new_mem[1]->isDeleteRange());
    ASSERT_EQ(5, new_mem[2]->getRows());
    ASSERT_EQ(2, new_persisted.size());
    ASSERT_EQ(137, new_persisted[0]->getRows());
    ASSERT_TRUE(new_persisted[1]->isDeleteRange());
}
CATCH

TEST_F(DeltaValueSpaceCloneNewlyAppendedTest, FlushPartially)
try
{
    WriteBatches wbs(*dmContext().storage_pool, dmContext().getWriteLimiter());
    wbs.setRollback();

    // 2 CF in mem
    appendBlockToDeltaValueSpace(dmContext(), delta, 0, 42);
    delta->appendDeleteRange(dmContext(), RowKeyRange::fromHandleRange(HandleRange(10, 50)));

    // Prepare flushing the 2 CF in mem
    auto sp_flush_prepared = SyncPointCtl::enableInScope("after_DeltaValueSpace::flush|prepare_flush");

    auto th_flush = std::async([&]() {
        ASSERT_TRUE(delta->flush(dmContext()));
    });

    sp_flush_prepared.waitAndPause();

    // Append another 2 CF in mem
    appendBlockToDeltaValueSpace(dmContext(), delta, 0, 10);
    delta->appendDeleteRange(dmContext(), RowKeyRange::fromHandleRange(HandleRange(1, 2)));

    auto snapshot = delta->createSnapshot(dmContext(), true, CurrentMetrics::DT_SnapshotOfRead);

    // Append 1 more CF after the snapshot. Now there are 2+2+1 CF in mem.
    appendBlockToDeltaValueSpace(dmContext(), delta, 0, 7);

    // Continue the flush
    sp_flush_prepared.next();
    th_flush.get();

    // Now let's checkout which CFs are newly appended..
    // We only have 1 CF newly appended since the snapshot.
    auto lock = delta->getLock();
    auto [new_mem, new_persisted] = delta->cloneNewlyAppendedColumnFiles(
        *lock,
        dmContext(),
        RowKeyRange::newAll(false, 1),
        *snapshot,
        wbs);
    ASSERT_EQ(1, new_mem.size());
    ASSERT_EQ(7, new_mem[0]->getRows());
    ASSERT_EQ(0, new_persisted.size());
}
CATCH

} // namespace tests
} // namespace DM
} // namespace DB
