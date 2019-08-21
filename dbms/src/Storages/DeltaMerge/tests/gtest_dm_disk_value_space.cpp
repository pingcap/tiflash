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
    DiskValueSpace_test() : name("t"), path("./" + name), storage_pool() {}

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

        storage_pool        = std::make_unique<StoragePool>(path);
        Context context     = DMTestEnv::getContext();
        table_handle_define = ColumnDefine(1, "pk", std::make_shared<DataTypeInt64>());
        table_columns.clear();
        table_columns.emplace_back(table_handle_define);
        table_columns.emplace_back(VERSION_COLUMN_DEFINE);
        table_columns.emplace_back(TAG_COLUMN_DEFINE);

        // TODO fill columns
        // table_info.columns.emplace_back();

        dm_context = std::make_unique<DMContext>(
            DMContext{.db_context          = context,
                      .storage_pool        = *storage_pool,
                      .store_columns       = table_columns,
                      .sort_column = table_handle_define,
                      .min_version         = 0,

                      .not_compress            = settings.not_compress_columns,
                      .delta_limit_rows        = context.getSettingsRef().dm_segment_delta_limit_rows,
                      .delta_limit_bytes       = context.getSettingsRef().dm_segment_delta_limit_bytes,
                      .delta_cache_limit_rows  = context.getSettingsRef().dm_segment_delta_cache_limit_rows,
                      .delta_cache_limit_bytes = context.getSettingsRef().dm_segment_delta_cache_limit_bytes});
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
{
    const size_t   value_beg      = 20;
    const size_t   num_rows_write = 80;
    DiskValueSpace delta(true, 0);
    {
        // write to DiskValueSpace
        Block block1 = DMTestEnv::prepareSimpleWriteBlock(value_beg, value_beg + num_rows_write / 2, false);
        Block block2 = DMTestEnv::prepareSimpleWriteBlock(value_beg + num_rows_write / 2, value_beg + num_rows_write, false);
        DiskValueSpace::OpContext opc     = DiskValueSpace::OpContext::createForLogStorage(*dm_context);
        Chunks                    chunks1 = DiskValueSpace::writeChunks(opc, std::make_shared<OneBlockInputStream>(block1));
        Chunks                    chunks2 = DiskValueSpace::writeChunks(opc, std::make_shared<OneBlockInputStream>(block2));
        for (auto & chunk : chunks1)
        {
            delta.appendChunkWithCache(opc, std::move(chunk), block1);
        }

        for (auto & chunk : chunks2)
        {
            delta.appendChunkWithCache(opc, std::move(chunk), block2);
        }

        EXPECT_EQ(num_rows_write, delta.num_rows(0, 2));
        delta.tryFlushCache(opc, true);
        EXPECT_EQ(num_rows_write, delta.num_rows(0, 1));
    }

    {
        // read using `getInputStream`
        PageReader          page_reader(dm_context->storage_pool.log());
        BlockInputStreamPtr in            = delta.getInputStream(table_columns, page_reader);
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
        Block        block = delta.read(table_columns, page_reader, read_offset, num_rows_expect);

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
                if (iter.name == "pk")
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
        Block        block = delta.read(table_columns, page_reader, chunk_index);

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
                if (iter.name == "pk")
                {
                    EXPECT_EQ(c->getInt(i), static_cast<int64_t>(value_beg + i));
                }
            }
        }
    }
}

} // namespace tests
} // namespace DM
} // namespace DB
