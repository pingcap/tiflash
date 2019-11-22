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

        storage_pool        = std::make_unique<StoragePool>("test.t1", path);
        Context & context   = DMTestEnv::getContext();
        table_handle_define = ColumnDefine(1, "pk", std::make_shared<DataTypeInt64>());
        table_columns.clear();
        table_columns.emplace_back(table_handle_define);
        table_columns.emplace_back(getVersionColumnDefine());
        table_columns.emplace_back(getTagColumnDefine());

        // TODO fill columns
        // table_info.columns.emplace_back();

        dm_context = std::make_unique<DMContext>(
            DMContext{.db_context    = context,
                      .storage_pool  = *storage_pool,
                      .store_columns = table_columns,
                      .handle_column = table_handle_define,
                      .min_version   = 0,

                      .not_compress = settings.not_compress_columns,

                      .segment_limit_rows = context.getSettingsRef().dm_segment_limit_rows,

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

TEST_F(DiskValueSpace_test, SimpleReadWrite)
try
{
    const size_t num_rows_write = 100;
    auto         delta          = std::make_shared<DiskValueSpace>(true, 0);
    auto         op_context     = DiskValueSpace::OpContext::createForLogStorage(*dm_context);

    // Simple write
    {
        Block         block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        WriteBatches  wbs;
        BlockOrDelete update    = BlockOrDelete(std::move(block));
        auto          task      = delta->createAppendTask(op_context, wbs, update);
        auto          new_delta = delta->applyAppendTask(op_context, task, update);
        if (new_delta)
        {
            delta = new_delta;
        }
        wbs.writeAll(*storage_pool);
        ASSERT_EQ(delta->num_rows(), num_rows_write);
        ASSERT_GT(delta->cacheRows(), 0UL);
    }

    // Simple read
    {
        PageReader reader(op_context.data_storage);
        Block      block = delta->read(table_columns, reader, 0, num_rows_write);

        for (auto & itr : block)
        {
            auto col = itr.column;
            if (col->getName() == "pk")
            {
                for (size_t i = 0; i < col->size(); i++)
                {
                    EXPECT_EQ(col->getInt(i), Int64(i));
                }
            }
        }
    }

    // Append delete so we should flush
    {
        HandleRange   range = HandleRange::newAll();
        WriteBatches  wbs;
        BlockOrDelete update    = BlockOrDelete(range);
        auto          task      = delta->createAppendTask(op_context, wbs, update);
        auto          new_delta = delta->applyAppendTask(op_context, task, update);
        if (new_delta)
        {
            delta = new_delta;
        }
        wbs.writeAll(*storage_pool);
        ASSERT_EQ(delta->num_rows(), num_rows_write);
        ASSERT_EQ(delta->cacheRows(), 0UL);
        ASSERT_EQ(delta->num_deletes(), 1UL);
    }
}
catch (const Exception & e)
{
    std::string text = e.displayText();

    auto embedded_stack_trace_pos = text.find("Stack trace");
    std::cerr << "Code: " << e.code() << ". " << text << std::endl << std::endl;
    if (std::string::npos == embedded_stack_trace_pos)
        std::cerr << "Stack trace:" << std::endl << e.getStackTrace().toString() << std::endl;

    throw;
}

TEST_F(DiskValueSpace_test, MassiveWrite)
try
{
    // Modify delta cache rows limit with black magic
    size_t & rows = const_cast<size_t &>(dm_context->delta_cache_limit_rows);
    rows          = 11;

    auto delta      = std::make_shared<DiskValueSpace>(true, 0);
    auto op_context = DiskValueSpace::OpContext::createForLogStorage(*dm_context);

    size_t num_rows_write = 0;
    while (num_rows_write < 1000)
    {
        Block         block = DMTestEnv::prepareSimpleWriteBlock(num_rows_write, num_rows_write + 10, false);
        WriteBatches  wbs;
        BlockOrDelete update    = BlockOrDelete(std::move(block));
        auto          task      = delta->createAppendTask(op_context, wbs, update);
        auto          new_delta = delta->applyAppendTask(op_context, task, update);
        if (new_delta)
        {
            delta = new_delta;
        }
        wbs.writeAll(*storage_pool);
        num_rows_write += 10;
    }

    ASSERT_EQ(delta->num_chunks(), 50UL);
    ASSERT_EQ(delta->cacheRows(), 0UL);
    ASSERT_EQ(delta->num_rows(), 1000UL);
}
catch (const Exception & e)
{
    std::string text = e.displayText();

    auto embedded_stack_trace_pos = text.find("Stack trace");
    std::cerr << "Code: " << e.code() << ". " << text << std::endl << std::endl;
    if (std::string::npos == embedded_stack_trace_pos)
        std::cerr << "Stack trace:" << std::endl << e.getStackTrace().toString() << std::endl;

    throw;
}

TEST_F(DiskValueSpace_test, MVCC)
try
{
    const size_t num_rows_write = 100;
    auto         delta          = std::make_shared<DiskValueSpace>(true, 0);
    auto         op_context     = DiskValueSpace::OpContext::createForLogStorage(*dm_context);

    // Simple write
    {
        Block         block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        WriteBatches  wbs;
        BlockOrDelete update    = BlockOrDelete(std::move(block));
        auto          task      = delta->createAppendTask(op_context, wbs, update);
        auto          new_delta = delta->applyAppendTask(op_context, task, update);
        if (new_delta)
        {
            delta = new_delta;
        }
        wbs.writeAll(*storage_pool);
    }

    size_t snap_delta_rows = delta->num_rows();
    // size_t snap_cache_rows = delta->cacheRows();
    auto   snapshot        = delta;

    // Keep appending blocks
    for (size_t i = 0; i < 10; i++)
    {
        Block         block = DMTestEnv::prepareSimpleWriteBlock(i * num_rows_write, (i + 1) * num_rows_write, false);
        WriteBatches  wbs;
        BlockOrDelete update    = BlockOrDelete(std::move(block));
        auto          task      = delta->createAppendTask(op_context, wbs, update);
        auto          new_delta = delta->applyAppendTask(op_context, task, update);
        if (new_delta)
        {
            delta = new_delta;
        }
        wbs.writeAll(*storage_pool);
    }

    ASSERT_GT(delta->num_rows(), snap_delta_rows);

    // Try read snapshot
    {
        PageReader reader(op_context.data_storage);
        Block      block = snapshot->read(table_columns, reader, 0, snap_delta_rows);

        for (auto & itr : block)
        {
            auto c = itr.column;
            if (c->getName() == "pk")
            {
                for (size_t i = 0; i < c->size(); i++)
                {
                    EXPECT_EQ(c->getInt(i), Int64(i));
                }
            }
        }
        ASSERT_EQ(snap_delta_rows, block.rows());
    }
}
catch (const Exception & e)
{
    std::string text = e.displayText();

    auto embedded_stack_trace_pos = text.find("Stack trace");
    std::cerr << "Code: " << e.code() << ". " << text << std::endl << std::endl;
    if (std::string::npos == embedded_stack_trace_pos)
        std::cerr << "Stack trace:" << std::endl << e.getStackTrace().toString() << std::endl;

    throw;
}

} // namespace tests
} // namespace DM
} // namespace DB
