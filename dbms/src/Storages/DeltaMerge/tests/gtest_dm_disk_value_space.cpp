#include "dm_basic_include.h"

#include <Poco/File.h>

#include <DataStreams/BlocksListBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>

#include <DataStreams/BlocksListBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/DiskValueSpace.h>

namespace DB
{
namespace DM
{
namespace tests
{

class DiskValueSpace_test : public ::testing::Test
{
public:
    DiskValueSpace_test() : name("tmp"), path(DB::tests::TiFlashTestEnv::getTemporaryPath() + name), storage_pool() {}

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
        table_handle_define = ColumnDefine(EXTRA_HANDLE_COLUMN_ID, EXTRA_HANDLE_COLUMN_NAME, EXTRA_HANDLE_COLUMN_TYPE);
        table_columns.clear();
        table_columns.emplace_back(table_handle_define);
        table_columns.emplace_back(getVersionColumnDefine());
        table_columns.emplace_back(getTagColumnDefine());

        // TODO fill columns
        // table_info.columns.emplace_back();

        dm_context = std::make_unique<DMContext>(context,
                                                 ".",
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
                                                 context.getSettingsRef().dm_enable_logical_split,
                                                 false,
                                                 false);
    }

protected:
    // the table name
    String name;
    // the path to the dir of table
    String path;
    /// all these var lives as ref in dm_context
    std::unique_ptr<StoragePool>  storage_pool;
    TiDB::TableInfo               table_info;
    ColumnDefine                  table_handle_define;
    ColumnDefines                 table_columns;
    DM::DeltaMergeStore::Settings settings;
    /// dm_context
    std::unique_ptr<DMContext> dm_context;
};

TEST_F(DiskValueSpace_test, LogStorageWriteRead)
try
{
    const size_t value_beg      = 20;
    const size_t num_rows_write = 80;
    auto         delta          = std::make_shared<DiskValueSpace>(true, 0);
    {
        // write to DiskValueSpace
        Block      block1 = DMTestEnv::prepareSimpleWriteBlock(value_beg, value_beg + num_rows_write / 2, false);
        Block      block2 = DMTestEnv::prepareSimpleWriteBlock(value_beg + num_rows_write / 2, value_beg + num_rows_write, false);
        auto       opc    = DiskValueSpace::OpContext::createForLogStorage(*dm_context);
        WriteBatch wb;
        Chunks     chunks1 = DiskValueSpace::writeChunks(opc, std::make_shared<OneBlockInputStream>(block1), wb);
        Chunks     chunks2 = DiskValueSpace::writeChunks(opc, std::make_shared<OneBlockInputStream>(block2), wb);
        dm_context->storage_pool.log().write(wb);

        for (auto & chunk : chunks1)
        {
            delta->appendChunkWithCache(opc, std::move(chunk), block1);
        }

        for (auto & chunk : chunks2)

        {
            delta->appendChunkWithCache(opc, std::move(chunk), block2);
        }

        EXPECT_EQ(num_rows_write, delta->num_rows(0, 2));
        WriteBatch remove_wb;
        delta = delta->tryFlushCache(opc, remove_wb, true);
        opc.data_storage.write(remove_wb);
        EXPECT_FALSE(!delta);
        EXPECT_EQ(num_rows_write, delta->num_rows(0, 1));
    }

    {
        // read using `getInputStream`
        PageReader          page_reader(dm_context->storage_pool.log());
        BlockInputStreamPtr in            = delta->getInputStream(table_columns, page_reader);
        size_t              num_rows_read = 0;
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            for (const auto & iter : block)
            {
                auto c = iter.column;
            }
        }
        EXPECT_EQ(num_rows_read, num_rows_write);
    }

    {
        // read using `read` of offset && limit
        const size_t read_offset     = 15;
        const size_t num_rows_expect = 20;
        PageReader   page_reader(dm_context->storage_pool.log());
        Block        block = delta->read(table_columns, page_reader, read_offset, num_rows_expect);

        // check the order of cols is the same as read_columns
        const Names colnames = block.getNames();
        ASSERT_EQ(colnames.size(), table_columns.size());
        for (size_t i = 0; i < colnames.size(); ++i)
        {
            EXPECT_EQ(colnames[i], table_columns[i].name);
        }

        // check the value
        ASSERT_EQ(block.rows(), num_rows_expect);
        for (const auto & iter : block)
        {
            auto c = iter.column;
            for (size_t i = 0; i < c->size(); ++i)
            {
                if (iter.name == EXTRA_HANDLE_COLUMN_NAME)
                {
                    EXPECT_EQ(c->getInt(i), static_cast<int64_t>(value_beg + read_offset + i));
                    //printf("%lld\n", c->getInt(i));
                }
            }
        }
    }

    {
        // read using `read` of chunk_index
        const size_t chunk_index = 0;
        PageReader   page_reader(dm_context->storage_pool.log());
        Block        block = delta->read(table_columns, page_reader, chunk_index);

        // check the order of cols is the same as read_columns
        const Names col_names = block.getNames();
        ASSERT_EQ(col_names.size(), table_columns.size());
        for (size_t i = 0; i < col_names.size(); ++i)
        {
            EXPECT_EQ(col_names[i], table_columns[i].name);
        }

        // check the value
        ASSERT_EQ(block.rows(), num_rows_write);
        for (const auto & iter : block)
        {
            auto c = iter.column;
            for (size_t i = 0; i < c->size(); ++i)
            {
                if (iter.name == EXTRA_HANDLE_COLUMN_NAME)
                {
                    EXPECT_EQ(c->getInt(i), static_cast<int64_t>(value_beg + i));
                }
            }
        }
    }
}
CATCH

TEST_F(DiskValueSpace_test, writeChunks_OneBlock)
{
    const Int64 pk_min = 20, pk_max = 40;
    Block       block = DMTestEnv::prepareSimpleWriteBlock(pk_min, pk_max, false);
    auto        in    = std::make_shared<OneBlockInputStream>(block);

    WriteBatch wb;
    auto       opc    = DiskValueSpace::OpContext::createForDataStorage(*dm_context);
    auto       chunks = DiskValueSpace::writeChunks(opc, in, wb);
    ASSERT_EQ(chunks.size(), 1UL);

    const Chunk & chunk = chunks[0];
    ASSERT_EQ(chunk.getRows(), size_t(pk_max - pk_min));
    ASSERT_EQ(chunk.getHandleFirstLast(), HandlePair(pk_min, pk_max - 1));
}

TEST_F(DiskValueSpace_test, writeChunks_NonOverlapBlocks)
{
    const Int64         pk_min = 20, pk_max = 40;
    const Int64         pk_span = pk_max - pk_min;
    BlockInputStreamPtr in      = {};
    {
        BlocksList blocks;
        // First block [20, 30)
        Block block = DMTestEnv::prepareSimpleWriteBlock(pk_min, pk_min + pk_span / 2, false);
        {
            auto col = block.getByName(DMTestEnv::pk_name);
            EXPECT_EQ(col.column->getInt(0), pk_min);
            EXPECT_EQ(col.column->getInt(col.column->size() - 1), pk_min + pk_span / 2 - 1);
            EXPECT_EQ(block.rows(), size_t(10));
        }
        blocks.emplace_back(block);
        // First block [30, 40)
        block = DMTestEnv::prepareSimpleWriteBlock(pk_min + pk_span / 2, pk_max, false);
        {
            auto col = block.getByName(DMTestEnv::pk_name);
            EXPECT_EQ(col.column->getInt(0), pk_min + pk_span / 2);
            EXPECT_EQ(col.column->getInt(col.column->size() - 1), pk_max - 1);
            EXPECT_EQ(block.rows(), size_t(10));
        }
        blocks.emplace_back(block);
        in = std::make_shared<BlocksListBlockInputStream>(std::move(blocks));
    }

    WriteBatch wb;
    auto       opc    = DiskValueSpace::OpContext::createForDataStorage(*dm_context);
    auto       chunks = DiskValueSpace::writeChunks(opc, in, wb);
    ASSERT_EQ(chunks.size(), 2UL);

    {
        const Chunk & chunk = chunks[0];
        ASSERT_EQ(chunk.getRows(), size_t(pk_span / 2));
        EXPECT_EQ(chunk.getHandleFirstLast(), HandlePair(pk_min, pk_min + pk_span / 2 - 1));
    }
    {
        const Chunk & chunk = chunks[1];
        ASSERT_EQ(chunk.getRows(), size_t(pk_span / 2));
        EXPECT_EQ(chunk.getHandleFirstLast(), HandlePair(pk_min + pk_span / 2, pk_max - 1));
    }
}

TEST_F(DiskValueSpace_test, writeChunks_OverlapBlocks)
{
    BlockInputStreamPtr in = {};
    {
        BlocksList blocks;
        // First block [20, 30]
        Block block = DMTestEnv::prepareSimpleWriteBlock(20, 31, false);
        {
            auto col = block.getByName(DMTestEnv::pk_name);
            EXPECT_EQ(col.column->getInt(0), 20);
            EXPECT_EQ(col.column->getInt(col.column->size() - 1), 30);
            EXPECT_EQ(block.rows(), size_t(11));
        }
        blocks.emplace_back(block);

        // Second block [30, 40), and pk=30 have multiple version
        {
            // version [100, 110) for pk=30
            Block block_of_multi_versions = DMTestEnv::prepareBlockWithIncreasingTso(30, 100, 110);
            // pk [31,40]
            Block block_2 = DMTestEnv::prepareSimpleWriteBlock(31, 41, false);
            DM::concat(block_of_multi_versions, block_2);
            block = block_of_multi_versions;
            {
                auto col = block.getByName(DMTestEnv::pk_name);
                EXPECT_EQ(col.column->getInt(0), 30);
                EXPECT_EQ(col.column->getInt(col.column->size() - 1), 40);
                EXPECT_EQ(block.rows(), size_t(10 + 10));
            }
        }
        blocks.emplace_back(block);

        // Third block [40, 50), and pk=40 have multiple version
        {
            // version [300, 305) for pk=40
            Block block_of_multi_versions = DMTestEnv::prepareBlockWithIncreasingTso(40, 300, 305);
            // pk [41,50)
            Block block_2 = DMTestEnv::prepareSimpleWriteBlock(41, 50, false);
            DM::concat(block_of_multi_versions, block_2);
            block = block_of_multi_versions;
            {
                auto col = block.getByName(DMTestEnv::pk_name);
                EXPECT_EQ(col.column->getInt(0), 40);
                EXPECT_EQ(col.column->getInt(col.column->size() - 1), 49);
                EXPECT_EQ(block.rows(), size_t(5 + 9));
            }
        }
        blocks.emplace_back(block);

        in = std::make_shared<BlocksListBlockInputStream>(std::move(blocks));
    }

    WriteBatch wb;
    auto       opc    = DiskValueSpace::OpContext::createForDataStorage(*dm_context);
    auto       chunks = DiskValueSpace::writeChunks(opc, in, wb);
    ASSERT_EQ(chunks.size(), 3UL);

    {
        const Chunk & chunk = chunks[0];
        // should be [20, 30], and pk=30 with 11 versions
        ASSERT_EQ(chunk.getRows(), size_t(11 + 10));
        EXPECT_EQ(chunk.getHandleFirstLast(), HandlePair(20, 30));
    }
    {
        const Chunk & chunk = chunks[1];
        // should be [31, 40], and pk=40 with 6 versions
        ASSERT_EQ(chunk.getRows(), size_t(9 + 6));
        EXPECT_EQ(chunk.getHandleFirstLast(), HandlePair(31, 40));
    }
    {
        const Chunk & chunk = chunks[2];
        // should be [41, 50)
        ASSERT_EQ(chunk.getRows(), size_t(9));
        EXPECT_EQ(chunk.getHandleFirstLast(), HandlePair(41, 49));
    }
}

TEST_F(DiskValueSpace_test, writeChunks_OverlapBlocksMerged)
{
    BlockInputStreamPtr in = {};
    {
        BlocksList blocks;
        // First block [20, 30]
        Block block = DMTestEnv::prepareSimpleWriteBlock(20, 31, false);
        {
            auto col = block.getByName(DMTestEnv::pk_name);
            EXPECT_EQ(col.column->getInt(0), 20);
            EXPECT_EQ(col.column->getInt(col.column->size() - 1), 30);
            EXPECT_EQ(block.rows(), size_t(11));
        }
        blocks.emplace_back(block);

        // Second block [30, 31), and pk=30 have multiple version
        {
            // version [100, 110) for pk=30
            Block block_of_multi_versions = DMTestEnv::prepareBlockWithIncreasingTso(30, 100, 110);
            block                         = block_of_multi_versions;
            {
                auto col = block.getByName(DMTestEnv::pk_name);
                EXPECT_EQ(col.column->getInt(0), 30);
                EXPECT_EQ(col.column->getInt(col.column->size() - 1), 30);
                EXPECT_EQ(block.rows(), size_t(10));
            }
        }
        blocks.emplace_back(block);

        // Third block [30, 50), and pk=30 have multiple version
        {
            // version [300, 305) for pk=30
            Block block_of_multi_versions = DMTestEnv::prepareBlockWithIncreasingTso(30, 300, 305);
            // pk [41,50)
            Block block_2 = DMTestEnv::prepareSimpleWriteBlock(31, 50, false);
            DM::concat(block_of_multi_versions, block_2);
            block = block_of_multi_versions;
            {
                auto col = block.getByName(DMTestEnv::pk_name);
                EXPECT_EQ(col.column->getInt(0), 30);
                EXPECT_EQ(col.column->getInt(col.column->size() - 1), 49);
                EXPECT_EQ(block.rows(), size_t(5 + 19));
            }
        }
        blocks.emplace_back(block);

        in = std::make_shared<BlocksListBlockInputStream>(std::move(blocks));
    }

    WriteBatch wb;
    auto       opc    = DiskValueSpace::OpContext::createForDataStorage(*dm_context);
    auto       chunks = DiskValueSpace::writeChunks(opc, in, wb);
    // Second block is merge to the first
    ASSERT_EQ(chunks.size(), 2UL);

    {
        const Chunk & chunk = chunks[0];
        // should be [20, 30], and pk=30 with 1 + 10 + 5 versions
        ASSERT_EQ(chunk.getRows(), size_t(11 + 10 + 5));
        EXPECT_EQ(chunk.getHandleFirstLast(), HandlePair(20, 30));
    }
    {
        const Chunk & chunk = chunks[1];
        // should be [31, 50)
        ASSERT_EQ(chunk.getRows(), size_t(19));
        EXPECT_EQ(chunk.getHandleFirstLast(), HandlePair(31, 49));
    }
}


} // namespace tests
} // namespace DM
} // namespace DB
