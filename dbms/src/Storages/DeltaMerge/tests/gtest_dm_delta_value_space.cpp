#include "dm_basic_include.h"

#include <Poco/File.h>

#include <Core/Block.h>
#include <DataStreams/BlocksListBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>

#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/DiskValueSpace.h>

namespace DB
{
namespace DM
{
namespace tests
{

class DeltaDiskValueSpace_test : public ::testing::Test
{
public:
    DeltaDiskValueSpace_test()
        : name("tmp"), //
          path(DB::tests::TiFlashTestEnv::getTemporaryPath() + name),
          delta_path(path + "/delta"),
          storage_pool()
    {
    }

private:
    void dropDataInDisk()
    {
        Poco::File file(path);
        if (file.exists())
            file.remove(true);
    }

protected:
    void SetUp() override
    {
        dropDataInDisk();

        storage_pool        = std::make_unique<StoragePool>("test.t1", path);
        Context & context   = DMTestEnv::getContext();
        table_columns       = DMTestEnv::getDefaultColumns();
        table_handle_define = table_columns.at(0);

        dm_context = std::make_unique<DMContext>(context,
                                                 path,
                                                 context.getExtraPaths(),
                                                 *storage_pool,
                                                 0,
                                                 table_columns,
                                                 table_handle_define,
                                                 0,
                                                 settings.not_compress_columns,
                                                 context.getSettingsRef().dm_segment_limit_rows,
                                                 context.getSettingsRef().dm_segment_delta_limit_rows,
                                                 context.getSettingsRef().dm_segment_delta_cache_limit_rows,
                                                 context.getSettingsRef().dm_segment_stable_chunk_rows,
                                                 context.getSettingsRef().dm_enable_logical_split);
    }

    static void initOutputColumns(MutableColumns & columns, const ColumnDefines & defines)
    {
        columns.resize(defines.size());
        for (size_t i = 0; i < defines.size(); ++i)
            columns[i] = defines[i].type->createColumn();
    }

    void writeToDelta(const DeltaSpacePtr & delta, BlockOrDelete && update)
    {
        WriteBatches wbs;
        // Append data to disk
        DeltaSpace::AppendTaskPtr task;
        if (update.isDelete())
            task = delta->appendToDisk(update.delete_range, wbs, *dm_context);
        else
            task = delta->appendToDisk(std::move(update.block), wbs, *dm_context);
        wbs.writeLogAndData(dm_context->storage_pool);
        // Commit new delta in disk
        wbs.writeMeta(dm_context->storage_pool);
        // Apply changes to delta
        delta->applyAppend(task);
    }

protected:
    // the table name
    String name;
    // the path to the dir of table
    String path;
    String delta_path;
    /// all these var lives as ref in dm_context
    std::unique_ptr<StoragePool>  storage_pool;
    ColumnDefine                  table_handle_define;
    ColumnDefines                 table_columns;
    DM::DeltaMergeStore::Settings settings;
    /// dm_context
    std::unique_ptr<DMContext> dm_context;
};

TEST_F(DeltaDiskValueSpace_test, SimpleGetValues)
try
{
    auto delta = std::make_shared<DeltaSpace>(1, delta_path);

    auto write_to_delta = [&](size_t first_pk, size_t num_rows) {
        // write to DiskValueSpace
        Block block1 = DMTestEnv::prepareSimpleWriteBlock(first_pk, first_pk + num_rows, false);
        writeToDelta(delta, std::move(block1));
    };

    auto read_check = [&](size_t first_pk, size_t num_rows_write, size_t num_rows_to_read) -> void {
        const size_t num_rows_actual_read = std::min(num_rows_write, num_rows_to_read);

        MutableColumns columns;
        initOutputColumns(columns, table_columns);

        // read using `getInputStream`
        auto snapshot = delta->getSnapshot();
        auto values   = snapshot->getValues(table_columns, *dm_context);
        values->write(columns, 0, num_rows_to_read);

        Block header = toEmptyBlock(table_columns);
        Block block  = header.cloneWithColumns(std::move(columns));
        ASSERT_EQ(block.rows(), num_rows_actual_read);
        for (const auto & iter : block)
        {
            auto c = iter.column;
            for (size_t i = 0; i < c->size(); ++i)
            {
                if (iter.name == EXTRA_HANDLE_COLUMN_NAME)
                {
                    EXPECT_EQ(c->getInt(i), static_cast<Int64>(first_pk + i));
                }
            }
        }
    };

    // Write one chunk: [20, 100)
    const size_t value_beg       = 20;
    const size_t num_rows_write1 = 80;
    size_t       num_rows_write  = num_rows_write1;
    write_to_delta(value_beg, num_rows_write1);
    EXPECT_EQ(delta->numChunks(), 1UL);
    EXPECT_EQ(delta->numRows(), num_rows_write);
    read_check(value_beg, num_rows_write, 8192);
    read_check(value_beg, num_rows_write, 17);

    // Write another continuous chunk: [100, 155)
    const size_t num_rows_write2 = 55;
    num_rows_write += num_rows_write2;
    write_to_delta(value_beg + num_rows_write1, num_rows_write2);
    EXPECT_EQ(delta->numChunks(), 2UL);
    EXPECT_EQ(delta->numRows(), num_rows_write);
    read_check(value_beg, num_rows_write, 8192);
    read_check(value_beg, num_rows_write, 17);
    read_check(value_beg, num_rows_write, num_rows_write1 + 17);
}
CATCH


TEST_F(DeltaDiskValueSpace_test, SimpleGetMergeBlocks)
try
{
    auto delta = std::make_shared<DeltaSpace>(1, delta_path);

    auto write_to_delta = [&](size_t first_pk, size_t num_rows) {
        // write to DiskValueSpace
        Block block1 = DMTestEnv::prepareSimpleWriteBlock(first_pk, first_pk + num_rows, false);
        writeToDelta(delta, std::move(block1));
    };

    // Write [10, 30)
    write_to_delta(10, 20);
    EXPECT_EQ(delta->numChunks(), 1UL);
    EXPECT_EQ(delta->numRows(), 20UL);

    {
        auto           snap   = delta->getSnapshot();
        BlockOrDeletes blocks = snap->getMergeBlocks(table_handle_define, 0, 0, *dm_context);
        ASSERT_EQ(blocks.size(), 1UL);
        auto & b = *blocks.begin();
        ASSERT_FALSE(b.isDelete());
        EXPECT_EQ(b.block.rows(), 20UL);
    }

    // Write [10, 35) again, `getMergeBlocks` just simply concate all blocks.
    write_to_delta(10, 25);
    EXPECT_EQ(delta->numChunks(), 2UL);
    EXPECT_EQ(delta->numRows(), 20 + 25UL);
    {
        BlockOrDeletes blocks;
        BlockOrDelete  b;
        auto           snap = delta->getSnapshot();
        {
            // Read all
            blocks = snap->getMergeBlocks(table_handle_define, 0, 0, *dm_context);
            ASSERT_EQ(blocks.size(), 1UL);
            b = *blocks.begin();
            ASSERT_FALSE(b.isDelete());
            EXPECT_EQ(b.block.rows(), 45UL);
        }
        {
            // Read the second chunk
            blocks = snap->getMergeBlocks(table_handle_define, 20, 0, *dm_context);
            ASSERT_EQ(blocks.size(), 1UL);
            b = *blocks.begin();
            ASSERT_FALSE(b.isDelete());
            EXPECT_EQ(b.block.rows(), 25UL);
        }
    }
}
CATCH


// TODO: serialize && deserialize

TEST_F(DeltaDiskValueSpace_test, CreateNextGenerationOfDelta)
try
{
    auto delta = std::make_shared<DeltaSpace>(1, delta_path);

    auto write_to_delta = [&](size_t first_pk, size_t num_rows) {
        // write to DiskValueSpace
        Block block1 = DMTestEnv::prepareSimpleWriteBlock(first_pk, first_pk + num_rows, false);
        writeToDelta(delta, std::move(block1));
    };

    // Write [10, 30)
    write_to_delta(10, 20);
    EXPECT_EQ(delta->numChunks(), 1UL);
    EXPECT_EQ(delta->numRows(), 20UL);
    auto snap1 = delta->getSnapshot();
    EXPECT_EQ(snap1->numChunks(), 1UL);
    EXPECT_EQ(snap1->numRows(), 20UL);

    // Write [10, 35)
    write_to_delta(10, 25);
    EXPECT_EQ(delta->numChunks(), 2UL);
    EXPECT_EQ(delta->numRows(), 20 + 25UL);
    auto snap2 = delta->getSnapshot();
    EXPECT_EQ(snap2->numChunks(), 2UL);
    EXPECT_EQ(snap2->numRows(), 20 + 25UL);
    // check that old snapshot don't change
    EXPECT_EQ(snap1->numChunks(), 1UL);
    EXPECT_EQ(snap1->numRows(), 20UL);

    WriteBatches wbs;

    /// Mock that we apply a delta merge after one chunk wrote.
    // Remove snap1( Chunk[10,30) ) and generate a new delta
    auto new_delta = delta->nextGeneration(snap1, wbs);
    auto snap3     = new_delta->getSnapshot();
    EXPECT_EQ(snap3->numChunks(), 1UL);
    EXPECT_EQ(snap3->numRows(), 25UL);
    // check that old snapshot don't change
    EXPECT_EQ(snap1->numChunks(), 1UL);
    EXPECT_EQ(snap1->numRows(), 20UL);
    EXPECT_EQ(snap2->numChunks(), 2UL);
    EXPECT_EQ(snap2->numRows(), 20 + 25UL);

    {
        // check read from old snapshot
        auto blocks = snap1->getMergeBlocks(table_handle_define, 0, 0, *dm_context);
        ASSERT_EQ(blocks.size(), 1UL);
        auto block = *blocks.begin();
        ASSERT_FALSE(block.isDelete());
        EXPECT_EQ(block.block.rows(), 20UL);
    }
    {
        // check read from old snapshot
        auto blocks = snap2->getMergeBlocks(table_handle_define, 0, 0, *dm_context);
        ASSERT_EQ(blocks.size(), 1UL);
        auto block = *blocks.begin();
        ASSERT_FALSE(block.isDelete());
        EXPECT_EQ(block.block.rows(), 20 + 25UL);
    }
    {
        // check read from new snapshot
        auto blocks = snap3->getMergeBlocks(table_handle_define, 0, 0, *dm_context);
        ASSERT_EQ(blocks.size(), 1UL);
        auto block = *blocks.begin();
        ASSERT_FALSE(block.isDelete());
        EXPECT_EQ(block.block.rows(), 25UL);
    }

    // check wbs, we need to mark old chunks are removed.
    {
        auto & writes = wbs.removed_log.getWrites();
        EXPECT_EQ(writes.size(), 1UL);
        EXPECT_EQ(writes[0].type, WriteBatch::WriteType::DEL);
        auto & chunks_in_snap1 = snap1->getChunks();
        EXPECT_EQ(writes[0].page_id, chunks_in_snap1[0].id);
    }

    // Release old delta and read from snapshot again.
    delta.reset();
    {
        auto blocks = snap1->getMergeBlocks(table_handle_define, 0, 0, *dm_context);
        ASSERT_EQ(blocks.size(), 1UL);
        auto block = *blocks.begin();
        ASSERT_FALSE(block.isDelete());
        EXPECT_EQ(block.block.rows(), 20UL);
    }
    {
        auto blocks = snap2->getMergeBlocks(table_handle_define, 0, 0, *dm_context);
        ASSERT_EQ(blocks.size(), 1UL);
        auto block = *blocks.begin();
        ASSERT_FALSE(block.isDelete());
        EXPECT_EQ(block.block.rows(), 20 + 25UL);
    }
    {
        auto blocks = snap3->getMergeBlocks(table_handle_define, 0, 0, *dm_context);
        ASSERT_EQ(blocks.size(), 1UL);
        auto block = *blocks.begin();
        ASSERT_FALSE(block.isDelete());
        EXPECT_EQ(block.block.rows(), 25UL);
    }

    /// Write to new_delta
    {
        // write [20, 50)
        Block block1 = DMTestEnv::prepareSimpleWriteBlock(20, 50, false);
        writeToDelta(new_delta, std::move(block1));
    }
    // Read from new snapshot
    auto snap4 = new_delta->getSnapshot();
    {
        ASSERT_EQ(snap4->numChunks(), 2UL);
        EXPECT_EQ(snap4->numRows(), 25 + 30UL);
        // new chunk is written to another DMFile
        auto chunk1 = snap4->getChunks()[0];
        auto chunk2 = snap4->getChunks()[1];
        EXPECT_NE(chunk1.file_id, chunk2.file_id);
    }
    {
        auto blocks = snap4->getMergeBlocks(table_handle_define, 0, 0, *dm_context);
        ASSERT_EQ(blocks.size(), 1UL);
        auto block = *blocks.begin();
        ASSERT_FALSE(block.isDelete());
        EXPECT_EQ(block.block.rows(), 25 + 30UL);
    }

    // Read from old snapshots
    {
        auto blocks = snap1->getMergeBlocks(table_handle_define, 0, 0, *dm_context);
        ASSERT_EQ(blocks.size(), 1UL);
        auto block = *blocks.begin();
        ASSERT_FALSE(block.isDelete());
        EXPECT_EQ(block.block.rows(), 20UL);
    }
    {
        auto blocks = snap2->getMergeBlocks(table_handle_define, 0, 0, *dm_context);
        ASSERT_EQ(blocks.size(), 1UL);
        auto block = *blocks.begin();
        ASSERT_FALSE(block.isDelete());
        EXPECT_EQ(block.block.rows(), 20 + 25UL);
    }
    {
        auto blocks = snap3->getMergeBlocks(table_handle_define, 0, 0, *dm_context);
        ASSERT_EQ(blocks.size(), 1UL);
        auto block = *blocks.begin();
        ASSERT_FALSE(block.isDelete());
        EXPECT_EQ(block.block.rows(), 25UL);
    }
}
CATCH

TEST_F(DeltaDiskValueSpace_test, CreateRefDelta)
try
{
    GenPageId log_gen_page_id = std::bind(&StoragePool::newLogPageId, &(*storage_pool));

    auto delta = std::make_shared<DeltaSpace>(1, delta_path);

    auto write_to_delta = [this](const DeltaSpacePtr & d, size_t first_pk, size_t num_rows) {
        // write to DiskValueSpace
        Block block1 = DMTestEnv::prepareSimpleWriteBlock(first_pk, first_pk + num_rows, false);
        writeToDelta(d, std::move(block1));
    };

    // Write [10, 30), [10, 35)
    write_to_delta(delta, 10, 20);
    write_to_delta(delta, 10, 25);
    auto snap1 = delta->getSnapshot();
    ASSERT_EQ(snap1->numChunks(), 2UL);
    ASSERT_EQ(snap1->numRows(), 20 + 25UL);

    WriteBatches wbs;

    auto lhs    = DeltaSpace::newRef(snap1, 1, delta_path, log_gen_page_id, wbs);
    auto rhs_id = log_gen_page_id();
    auto rhs    = DeltaSpace::newRef(snap1, rhs_id, delta_path, log_gen_page_id, wbs);

    // Write [15, 25), [20, 25) to left
    write_to_delta(lhs, 15, 10);
    write_to_delta(lhs, 20, 5);
    // Write [25, 100) to right
    write_to_delta(rhs, 25, 75);
    auto snap_lhs = lhs->getSnapshot();
    {
        ASSERT_EQ(snap_lhs->numChunks(), 4UL);
        ASSERT_EQ(snap_lhs->numRows(), 20 + 25 + 10 + 5UL);
        auto & chunk0 = snap_lhs->getChunks()[0];
        auto & chunk1 = snap_lhs->getChunks()[1];
        auto & chunk2 = snap_lhs->getChunks()[2];
        auto & chunk3 = snap_lhs->getChunks()[3];
        ASSERT_EQ(chunk0.file_id, chunk1.file_id);
        ASSERT_NE(chunk1.file_id, chunk2.file_id);
        ASSERT_EQ(chunk2.file_id, chunk3.file_id);
    }
    auto snap_rhs = rhs->getSnapshot();
    {
        ASSERT_EQ(snap_rhs->numChunks(), 3UL);
        ASSERT_EQ(snap_rhs->numRows(), 20 + 25 + 75UL);
        auto & chunk0 = snap_lhs->getChunks()[0];
        auto & chunk1 = snap_lhs->getChunks()[1];
        auto & chunk2 = snap_lhs->getChunks()[2];
        ASSERT_EQ(chunk0.file_id, chunk1.file_id);
        ASSERT_NE(chunk1.file_id, chunk2.file_id);
    }
    // read tests
    {
        auto blocks = snap_lhs->getMergeBlocks(table_handle_define, 0, 0, *dm_context);
        ASSERT_EQ(blocks.size(), 1UL);
        auto block = *blocks.begin();
        ASSERT_FALSE(block.isDelete());
        EXPECT_EQ(block.block.rows(), 20 + 25 + 10 + 5UL);
    }
    {
        auto blocks = snap_rhs->getMergeBlocks(table_handle_define, 0, 0, *dm_context);
        ASSERT_EQ(blocks.size(), 1UL);
        auto block = *blocks.begin();
        ASSERT_FALSE(block.isDelete());
        EXPECT_EQ(block.block.rows(), 20 + 25 + 75UL);
    }

    // TODO: check wbs
}
CATCH


} // namespace tests
} // namespace DM
} // namespace DB
