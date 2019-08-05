#include "dm_basic_include.h"

#include <Poco/File.h>
#include <DataStreams/OneBlockInputStream.h>

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
        table_handle_define = ColumnDefine{
            .name = "pk",
            .type = std::make_shared<DataTypeInt64>(),
            .id   = 1,
        };
        table_columns.clear();
        table_columns.emplace_back(table_handle_define);
        table_columns.emplace_back(VERSION_COLUMN_DEFINE);
        table_columns.emplace_back(TAG_COLUMN_DEFINE);

        dm_context = std::make_unique<DMContext>(
            DMContext{.db_context          = context,
                      .storage_pool        = *storage_pool,
                      .table_name          = name,
                      .table_columns       = table_columns,
                      .table_handle_define = table_handle_define,
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
        Block                     block  = DMTestEnv::prepareSimpleWriteBlock(value_beg, value_beg + num_rows_write, false);
        DiskValueSpace::OpContext opc    = DiskValueSpace::OpContext::createForLogStorage(*dm_context);
        Chunks                    chunks = DiskValueSpace::writeChunks(opc, std::make_shared<OneBlockInputStream>(block));
        for (auto & chunk : chunks)
        {
            delta.appendChunkWithCache(opc, std::move(chunk), block);
        }
    }

    {
        // read using `getInputStream`
        BlockInputStreamPtr in            = delta.getInputStream(table_columns, dm_context->storage_pool.log());
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
        Block        block           = delta.read(table_columns, dm_context->storage_pool.log(), read_offset, num_rows_expect);

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
}

} // namespace tests
} // namespace DM
} // namespace DB
