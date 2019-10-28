#include <gtest/gtest.h>
#include <memory>
#include "dm_basic_include.h"

#include <Poco/ConsoleChannel.h>
#include <Poco/File.h>
#include <Poco/FormattingChannel.h>
#include <Poco/PatternFormatter.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/DeltaMerge/DeltaMergeStore-internal.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>

namespace DB
{
namespace DM
{
namespace tests
{

class DeltaMergeStore_test : public ::testing::Test
{
public:
    DeltaMergeStore_test() : name("t"), path("./" + name) {}

protected:
    static void SetUpTestCase()
    {
        Poco::AutoPtr<Poco::ConsoleChannel>   channel = new Poco::ConsoleChannel(std::cerr);
        Poco::AutoPtr<Poco::PatternFormatter> formatter(new Poco::PatternFormatter);
        formatter->setProperty("pattern", "%L%Y-%m-%d %H:%M:%S.%i <%p> %s: %t");
        Poco::AutoPtr<Poco::FormattingChannel> formatting_channel(new Poco::FormattingChannel(formatter, channel));
        Logger::root().setChannel(formatting_channel);
        Logger::root().setLevel("trace");
    }

    void SetUp() override
    {
        // drop former-gen table's data in disk
        Poco::File file(path);
        if (file.exists())
            file.remove(true);

        context = std::make_unique<Context>(DMTestEnv::getContext());
        store   = reload();

        Logger::get("DeltaMergeStore").setLevel("trace");
    }

    DeltaMergeStorePtr reload(const ColumnDefines & pre_define_columns = {})
    {
        ColumnDefines cols                 = pre_define_columns.empty() ? DMTestEnv::getDefaultColumns() : pre_define_columns;
        ColumnDefine  handle_column_define = cols[0];

        DeltaMergeStorePtr s
            = std::make_shared<DeltaMergeStore>(*context, path, "test", name, cols, handle_column_define, DeltaMergeStore::Settings());
        return s;
    }

private:
    // the table name
    String name;
    // the path to the dir of table
    String path;

protected:
    // a ptr to context, we can reload context with different settings if need.
    std::unique_ptr<Context> context;
    DeltaMergeStorePtr       store;
};

TEST_F(DeltaMergeStore_test, Create)
{
    // create table
    ASSERT_NE(store, nullptr);

    {
        // check handle column of store
        auto & h = store->getHandle();
        ASSERT_EQ(h.name, "pk");
        ASSERT_EQ(h.id, 1);
        ASSERT_TRUE(h.type->equals(*DataTypeFactory::instance().get("Int64")));
    }
    {
        // check column structure of store
        auto & cols = store->getTableColumns();
        // version & tag column added
        ASSERT_EQ(cols.size(), 3UL);
    }
}

TEST_F(DeltaMergeStore_test, OpenWithExtraColumns)
{
    const ColumnDefine col_str_define(2, "col2", std::make_shared<DataTypeString>());
    const ColumnDefine col_i8_define(3, "i8", std::make_shared<DataTypeInt8>());
    {
        ColumnDefines table_column_defines = DMTestEnv::getDefaultColumns();
        table_column_defines.emplace_back(col_str_define);
        table_column_defines.emplace_back(col_i8_define);
        store = reload(table_column_defines);
    }

    {
        // check column structure
        const auto & cols = store->getTableColumns();
        ASSERT_EQ(cols.size(), 5UL);
        const auto & str_col = cols[3];
        ASSERT_EQ(str_col.name, col_str_define.name);
        ASSERT_EQ(str_col.id, col_str_define.id);
        ASSERT_TRUE(str_col.type->equals(*col_str_define.type));
        const auto & i8_col = cols[4];
        ASSERT_EQ(i8_col.name, col_i8_define.name);
        ASSERT_EQ(i8_col.id, col_i8_define.id);
        ASSERT_TRUE(i8_col.type->equals(*col_i8_define.type));
    }
}

TEST_F(DeltaMergeStore_test, SimpleWriteRead)
{
    const ColumnDefine col_str_define(2, "col2", std::make_shared<DataTypeString>());
    const ColumnDefine col_i8_define(3, "i8", std::make_shared<DataTypeInt8>());
    {
        ColumnDefines table_column_defines = DMTestEnv::getDefaultColumns();
        table_column_defines.emplace_back(col_str_define);
        table_column_defines.emplace_back(col_i8_define);
        store = reload(table_column_defines);
    }

    {
        // check column structure
        const auto & cols = store->getTableColumns();
        ASSERT_EQ(cols.size(), 5UL);
        const auto & str_col = cols[3];
        ASSERT_EQ(str_col.name, col_str_define.name);
        ASSERT_EQ(str_col.id, col_str_define.id);
        ASSERT_TRUE(str_col.type->equals(*col_str_define.type));
        const auto & i8_col = cols[4];
        ASSERT_EQ(i8_col.name, col_i8_define.name);
        ASSERT_EQ(i8_col.id, col_i8_define.id);
        ASSERT_TRUE(i8_col.type->equals(*col_i8_define.type));
    }

    const size_t num_rows_write = 128;
    {
        // write to store
        Block block;
        {
            block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
            // Add a column of col2:String for test
            ColumnWithTypeAndName col2(col_str_define.type, col_str_define.name);
            {
                IColumn::MutablePtr m_col2 = col2.type->createColumn();
                for (size_t i = 0; i < num_rows_write; i++)
                {
                    String s = DB::toString(i);
                    Field  field(s.c_str(), s.size());
                    m_col2->insert(field);
                }
                col2.column = std::move(m_col2);
            }
            block.insert(std::move(col2));

            // Add a column of i8:Int8 for test
            ColumnWithTypeAndName i8(col_i8_define.type, col_i8_define.name);
            {
                IColumn::MutablePtr m_i8 = i8.type->createColumn();
                for (size_t i = 0; i < num_rows_write; i++)
                {
                    Int64 num = i * (i % 2 == 0 ? -1 : 1);
                    m_i8->insert(Field(num));
                }
                i8.column = std::move(m_i8);
            }
            block.insert(std::move(i8));
        }
        store->write(*context, context->getSettingsRef(), block);
    }

    {
        // TODO read data from more than one block
        // TODO read data from mutli streams
        // TODO read partial columns from store
        // TODO read data of max_version

        // read all columns from store
        const auto &        columns = store->getTableColumns();
        BlockInputStreamPtr in      = store->read(*context,
                                             context->getSettingsRef(),
                                             columns,
                                             {HandleRange::newAll()},
                                             /* num_streams= */ 1,
                                             /* max_version= */ std::numeric_limits<UInt64>::max(),
                                             /* expected_block_size= */ 1024)[0];

        size_t num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            for (auto && iter : block)
            {
                auto c = iter.column;
                for (Int64 i = 0; i < Int64(c->size()); ++i)
                {
                    if (iter.name == "pk")
                    {
                        //printf("pk:%lld\n", c->getInt(i));
                        EXPECT_EQ(c->getInt(i), i);
                    }
                    else if (iter.name == col_str_define.name)
                    {
                        //printf("%s:%s\n", col_str_define.name.c_str(), c->getDataAt(i).data);
                        EXPECT_EQ(c->getDataAt(i), DB::toString(i));
                    }
                    else if (iter.name == col_i8_define.name)
                    {
                        //printf("%s:%lld\n", col_i8_define.name.c_str(), c->getInt(i));
                        Int64 num = i * (i % 2 == 0 ? -1 : 1);
                        EXPECT_EQ(c->getInt(i), num);
                    }
                }
            }
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }

    {
        // test readRaw
        const auto &        columns = store->getTableColumns();
        BlockInputStreamPtr in      = store->readRaw(*context, context->getSettingsRef(), columns, 1)[0];

        size_t num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            for (auto && iter : block)
            {
                auto c = iter.column;
                for (Int64 i = 0; i < Int64(c->size()); ++i)
                {
                    if (iter.name == "pk")
                    {
                        EXPECT_EQ(c->getInt(i), i);
                    }
                    else if (iter.name == col_str_define.name)
                    {
                        EXPECT_EQ(c->getDataAt(i), DB::toString(i));
                    }
                    else if (iter.name == col_i8_define.name)
                    {
                        Int64 num = i * (i % 2 == 0 ? -1 : 1);
                        EXPECT_EQ(c->getInt(i), num);
                    }
                }
            }
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}

TEST_F(DeltaMergeStore_test, DeleteRead)
{
    const size_t num_rows_write = 128;
    {
        // Create a block with sequential Int64 handle in range [0, 128)
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, 128, false);
        store->write(*context, context->getSettingsRef(), block);
    }
    // Test Reading first
    {
        const auto &        columns       = store->getTableColumns();
        BlockInputStreamPtr in            = store->read(*context,
                                             context->getSettingsRef(),
                                             columns,
                                             {HandleRange::newAll()},
                                             /* num_streams= */ 1,
                                             /* max_version= */ std::numeric_limits<UInt64>::max(),
                                             /* expected_block_size= */ 1024)[0];
        size_t              num_rows_read = 0;
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            for (auto && iter : block)
            {
                auto c = iter.column;
                for (Int64 i = 0; i < Int64(c->size()); ++i)
                {
                    if (iter.name == "pk")
                    {
                        ASSERT_EQ(c->getInt(i), i);
                    }
                }
            }
        }

        ASSERT_EQ(num_rows_read, num_rows_write);
    }
    // Delete range [0, 64)
    const size_t num_deleted_rows = 64;
    {
        HandleRange range(0, num_deleted_rows);
        store->deleteRange(*context, context->getSettingsRef(), range);
    }
    // Read after deletion
    {
        const auto &        columns       = store->getTableColumns();
        BlockInputStreamPtr in            = store->read(*context,
                                             context->getSettingsRef(),
                                             columns,
                                             {HandleRange::newAll()},
                                             /* num_streams= */ 1,
                                             /* max_version= */ std::numeric_limits<UInt64>::max(),
                                             /* expected_block_size= */ 1024)[0];
        size_t              num_rows_read = 0;
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            for (auto && iter : block)
            {
                auto c = iter.column;
                for (Int64 i = 0; i < Int64(c->size()); ++i)
                {
                    if (iter.name == "pk")
                    {
                        // Range after deletion is [64, 128)
                        ASSERT_EQ(c->getInt(i), i + Int64(num_deleted_rows));
                    }
                }
            }
        }

        ASSERT_EQ(num_rows_read, num_rows_write - num_deleted_rows);
    }
}

TEST_F(DeltaMergeStore_test, WriteMultipleBlock)
{
    const size_t num_write_rows = 32;

    // Test write multi blocks without overlap
    {
        Block block1 = DMTestEnv::prepareSimpleWriteBlock(0, 1 * num_write_rows, false);
        Block block2 = DMTestEnv::prepareSimpleWriteBlock(1 * num_write_rows, 2 * num_write_rows, false);
        Block block3 = DMTestEnv::prepareSimpleWriteBlock(2 * num_write_rows, 3 * num_write_rows, false);
        store->write(*context, context->getSettingsRef(), block1);
        store->write(*context, context->getSettingsRef(), block2);
        store->write(*context, context->getSettingsRef(), block3);
    }

    {
        const auto &        columns       = store->getTableColumns();
        BlockInputStreamPtr in            = store->read(*context,
                                             context->getSettingsRef(),
                                             columns,
                                             {HandleRange::newAll()},
                                             /* num_streams= */ 1,
                                             /* max_version= */ std::numeric_limits<UInt64>::max(),
                                             /* expected_block_size= */ 1024)[0];
        size_t              num_rows_read = 0;
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            for (auto && iter : block)
            {
                auto c = iter.column;
                for (Int64 i = 0; i < Int64(c->size()); ++i)
                {
                    if (iter.name == "pk")
                    {
                        ASSERT_EQ(c->getInt(i), i);
                    }
                }
            }
        }

        ASSERT_EQ(num_rows_read, 3 * num_write_rows);
    }

    store = reload();

    // Test write multi blocks with overlap
    {
        UInt64 tso1   = 1;
        UInt64 tso2   = 100;
        Block  block1 = DMTestEnv::prepareSimpleWriteBlock(0, 1 * num_write_rows, false, tso1);
        Block  block2 = DMTestEnv::prepareSimpleWriteBlock(1 * num_write_rows, 2 * num_write_rows, false, tso1);
        Block  block3 = DMTestEnv::prepareSimpleWriteBlock(num_write_rows / 2, num_write_rows / 2 + num_write_rows, false, tso2);
        store->write(*context, context->getSettingsRef(), block1);
        store->write(*context, context->getSettingsRef(), block2);
        store->write(*context, context->getSettingsRef(), block3);
    }
    // Read without version
    {
        const auto &        columns       = store->getTableColumns();
        BlockInputStreamPtr in            = store->read(*context,
                                             context->getSettingsRef(),
                                             columns,
                                             {HandleRange::newAll()},
                                             /* num_streams= */ 1,
                                             /* max_version= */ std::numeric_limits<UInt64>::max(),
                                             /* expected_block_size= */ 1024)[0];
        size_t              num_rows_read = 0;
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            for (auto && iter : block)
            {
                auto c = iter.column;
                for (Int64 i = 0; i < Int64(c->size()); ++i)
                {
                    if (iter.name == "pk")
                    {
                        ASSERT_EQ(c->getInt(i), i);
                    }
                }
            }
        }

        ASSERT_EQ(num_rows_read, 3 * num_write_rows);
    }
    // Read with version
    {
        const auto &        columns       = store->getTableColumns();
        BlockInputStreamPtr in            = store->read(*context,
                                             context->getSettingsRef(),
                                             columns,
                                             {HandleRange::newAll()},
                                             /* num_streams= */ 1,
                                             /* max_version= */ UInt64(1),
                                             /* expected_block_size= */ 1024)[0];
        size_t              num_rows_read = 0;
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            for (auto && iter : block)
            {
                auto c = iter.column;
                for (Int64 i = 0; i < Int64(c->size()); ++i)
                {
                    if (iter.name == "pk")
                    {
                        ASSERT_EQ(c->getInt(i), i);
                    }
                }
            }
        }

        ASSERT_EQ(num_rows_read, 2 * num_write_rows);
    }
}

// DEPRECATED:
//   This test case strongly depends on implementation of `shouldSplit()` and `shouldMerge()`.
//   The machanism of them may be changed one day. So uncomment the test if need.
TEST_F(DeltaMergeStore_test, DISABLED_WriteLargeBlock)
{
    DB::Settings settings = context->getSettings();
    // Mock dm_segment_rows for test
    // if rows > 8 will split
    // if left->rows < 2 && right->rows + left->rows < 4 will merge
    settings.dm_segment_limit_rows = 4;

    {
        store->check(*context);
    }

    {
        // Write 7 rows that would not trigger a split
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, 8, false);
        store->write(*context, settings, block);
    }

    {
        const auto &        columns       = store->getTableColumns();
        BlockInputStreamPtr in            = store->read(*context,
                                             settings,
                                             columns,
                                             {HandleRange::newAll()},
                                             /* num_streams= */ 1,
                                             /* max_version= */ std::numeric_limits<UInt64>::max(),
                                             /* expected_block_size= */ 1024)[0];
        size_t              num_rows_read = 0;
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            for (auto & iter : block)
            {
                auto c = iter.column;
                for (Int64 i = 0; i < Int64(c->size()); i++)
                {
                    if (iter.name == "pk")
                    {
                        EXPECT_EQ(c->getInt(i), i);
                    }
                }
            }
        }
        ASSERT_EQ(num_rows_read, 8UL);
    }

    {
        // Write rows that would trigger a split
        Block block = DMTestEnv::prepareSimpleWriteBlock(8, 9, false);
        store->write(*context, settings, block);
    }

    // Now there is 2 segments
    // segment1: 0, 1, 2, 3
    // segment2: 4, 5, 6, 7, 8
    {
        const auto &        columns       = store->getTableColumns();
        BlockInputStreamPtr in            = store->read(*context,
                                             settings,
                                             columns,
                                             {HandleRange::newAll()},
                                             /* num_streams= */ 1,
                                             /* max_version= */ std::numeric_limits<UInt64>::max(),
                                             /* expected_block_size= */ 1024)[0];
        size_t              num_rows_read = 0;
        // block_num represents index of current segment
        int block_num = 0;
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            for (auto & iter : block)
            {
                auto c = iter.column;
                for (Int64 i = 0; i < Int64(c->size()); i++)
                {
                    if (iter.name == "pk" && block_num == 0)
                    {
                        EXPECT_EQ(c->getInt(i), i);
                    }
                    else if (iter.name == "pk" && block_num == 1)
                    {
                        EXPECT_EQ(c->getInt(i), i + 4);
                    }
                }
            }
            block_num++;
        }
        ASSERT_EQ(num_rows_read, 9UL);
    }
}

TEST_F(DeltaMergeStore_test, ReadWithSpecifyTso)
{
    const UInt64 tso1          = 4;
    const size_t num_rows_tso1 = 128;
    {
        // write to store
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_tso1, false, tso1);
        store->write(*context, context->getSettingsRef(), block);
    }

    const UInt64 tso2          = 890;
    const size_t num_rows_tso2 = 256;
    {
        // write to store
        Block block = DMTestEnv::prepareSimpleWriteBlock(num_rows_tso1, num_rows_tso1 + num_rows_tso2, false, tso2);
        store->write(*context, context->getSettingsRef(), block);
    }

    {
        // read all data of max_version
        const auto &      columns = store->getTableColumns();
        BlockInputStreams ins     = store->read(*context,
                                            context->getSettingsRef(),
                                            columns,
                                            {HandleRange::newAll()},
                                            /* num_streams= */ 1,
                                            /* max_version= */ std::numeric_limits<UInt64>::max(),
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1UL);
        BlockInputStreamPtr in = ins[0];

        size_t num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
            num_rows_read += block.rows();
        in->readSuffix();
        EXPECT_EQ(num_rows_read, num_rows_tso1 + num_rows_tso2);
    }

    {
        // read all data <= tso2
        const auto &      columns = store->getTableColumns();
        BlockInputStreams ins     = store->read(*context,
                                            context->getSettingsRef(),
                                            columns,
                                            {HandleRange::newAll()},
                                            /* num_streams= */ 1,
                                            /* max_version= */ tso2,
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1UL);
        BlockInputStreamPtr in = ins[0];

        size_t num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
            num_rows_read += block.rows();
        in->readSuffix();
        EXPECT_EQ(num_rows_read, num_rows_tso1 + num_rows_tso2);
    }

    {
        // read all data <= tso1
        const auto &      columns = store->getTableColumns();
        BlockInputStreams ins     = store->read(*context,
                                            context->getSettingsRef(),
                                            columns,
                                            {HandleRange::newAll()},
                                            /* num_streams= */ 1,
                                            /* max_version= */ tso1,
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1UL);
        BlockInputStreamPtr in = ins[0];

        size_t num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
            num_rows_read += block.rows();
        in->readSuffix();
        EXPECT_EQ(num_rows_read, num_rows_tso1);
    }

    {
        // read all data < tso1
        const auto &      columns = store->getTableColumns();
        BlockInputStreams ins     = store->read(*context,
                                            context->getSettingsRef(),
                                            columns,
                                            {HandleRange::newAll()},
                                            /* num_streams= */ 1,
                                            /* max_version= */ tso1 - 1,
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1UL);
        BlockInputStreamPtr in = ins[0];

        size_t num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
            num_rows_read += block.rows();
        in->readSuffix();
        EXPECT_EQ(num_rows_read, 0UL);
    }
}

TEST_F(DeltaMergeStore_test, Split)
try
{
    // set some params to smaller threshold so that we can trigger split faster
    auto settings                        = context->getSettings();
    settings.dm_segment_limit_rows       = 11;
    settings.dm_segment_delta_limit_rows = 7;

    size_t num_rows_write_in_total = 0;

    const size_t num_rows_per_write = 5;
    while (true)
    {
        {
            // write to store
            Block block = DMTestEnv::prepareSimpleWriteBlock( //
                num_rows_write_in_total + 1,                  //
                num_rows_write_in_total + 1 + num_rows_per_write,
                false);

            store->write(*context, settings, block);
            num_rows_write_in_total += num_rows_per_write;
        }

        {
            // read all columns from store
            const auto &      columns = store->getTableColumns();
            BlockInputStreams ins     = store->read(*context,
                                                context->getSettingsRef(),
                                                columns,
                                                {HandleRange::newAll()},
                                                /* num_streams= */ 1,
                                                /* max_version= */ std::numeric_limits<UInt64>::max(),
                                                /* expected_block_size= */ 1024);
            ASSERT_EQ(ins.size(), 1UL);
            BlockInputStreamPtr in = ins[0];

            LOG_TRACE(&Poco::Logger::get(GET_GTEST_FULL_NAME), "start to check data of [1," << num_rows_write_in_total << "]");

            size_t num_rows_read = 0;
            in->readPrefix();
            Int64 expected_row_pk = 1;
            while (Block block = in->read())
            {
                num_rows_read += block.rows();
                for (auto && iter : block)
                {
                    auto c = iter.column;
                    for (size_t i = 0; i < c->size(); ++i)
                    {
                        if (iter.name == "pk")
                        {
                            auto expected = expected_row_pk++;
                            auto value    = c->getInt(i);
                            EXPECT_EQ(expected, value);
                            //std::cerr << "pk:" << c->getInt(i) << std::endl;
                        }
                    }
                }
            }
            in->readSuffix();
            ASSERT_EQ(num_rows_read, num_rows_write_in_total);

            LOG_TRACE(&Poco::Logger::get(GET_GTEST_FULL_NAME), "done checking data of [1," << num_rows_write_in_total << "]");
        }

        if (num_rows_write_in_total >= 1000)
            break;
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

TEST_F(DeltaMergeStore_test, DDLChanegInt8ToInt32)
try
{
    const String      col_name_ddl        = "i8";
    const ColId       col_id_ddl          = 2;
    const DataTypePtr col_type_before_ddl = DataTypeFactory::instance().get("Int8");
    const DataTypePtr col_type_after_ddl  = DataTypeFactory::instance().get("Int32");
    {
        ColumnDefines table_column_defines = DMTestEnv::getDefaultColumns();
        ColumnDefine  cd(col_id_ddl, col_name_ddl, col_type_before_ddl);
        table_column_defines.emplace_back(cd);
        store = reload(table_column_defines);
    }

    {
        // check column structure
        const auto & cols = store->getTableColumns();
        ASSERT_EQ(cols.size(), 4UL);
        const auto & str_col = cols[3];
        ASSERT_EQ(str_col.name, col_name_ddl);
        ASSERT_EQ(str_col.id, col_id_ddl);
        ASSERT_TRUE(str_col.type->equals(*col_type_before_ddl));
    }

    const size_t num_rows_write = 128;
    {
        // write to store
        Block block;
        {
            block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
            // Add a column of col2:String for test
            ColumnWithTypeAndName col2(std::make_shared<DataTypeInt8>(), col_name_ddl);
            {
                IColumn::MutablePtr m_col2 = col2.type->createColumn();
                for (size_t i = 0; i < num_rows_write; i++)
                {
                    Int64 num = i * (i % 2 == 0 ? -1 : 1);
                    m_col2->insert(Field(num));
                }
                col2.column = std::move(m_col2);
            }
            block.insert(col2);
        }
        store->write(*context, context->getSettingsRef(), block);
    }

    {
        // DDL change col from i8 -> i32
        AlterCommands commands;
        {
            AlterCommand com;
            com.type        = AlterCommand::MODIFY_COLUMN;
            com.data_type   = col_type_after_ddl;
            com.column_name = col_name_ddl;
            com.column_id   = col_id_ddl;
            commands.emplace_back(std::move(com));
        }
        ColumnID _ignored = 0;
        store->applyAlters(commands, std::nullopt, _ignored, *context);
    }

    {
        // read all columns from store
        const auto &      columns = store->getTableColumns();
        BlockInputStreams ins     = store->read(*context,
                                            context->getSettingsRef(),
                                            columns,
                                            {HandleRange::newAll()},
                                            /* num_streams= */ 1,
                                            /* max_version= */ std::numeric_limits<UInt64>::max(),
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1UL);
        BlockInputStreamPtr & in = ins[0];
        {
            // check col type
            const Block  head = in->getHeader();
            const auto & col  = head.getByName(col_name_ddl);
            ASSERT_EQ(col.name, col_name_ddl);
            ASSERT_EQ(col.column_id, col_id_ddl);
            ASSERT_TRUE(col.type->equals(*col_type_after_ddl));
        }

        size_t num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            for (auto && iter : block)
            {
                auto c = iter.column;
                for (Int64 i = 0; i < Int64(c->size()); ++i)
                {
                    if (iter.name == "pk")
                    {
                        //printf("pk:%lld\n", c->getInt(i));
                        EXPECT_EQ(c->getInt(i), i);
                    }
                    else if (iter.name == col_name_ddl)
                    {
                        //printf("col2:%s\n", c->getDataAt(i).data);
                        Int64 num = i * (i % 2 == 0 ? -1 : 1);
                        EXPECT_EQ(c->getInt(i), num);
                    }
                }
            }
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
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


TEST_F(DeltaMergeStore_test, DDLDropColumn)
try
{
    const String      col_name_to_drop = "i8";
    const ColId       col_id_to_drop   = 2;
    const DataTypePtr col_type_to_drop = DataTypeFactory::instance().get("Int8");
    {
        ColumnDefines table_column_defines = DMTestEnv::getDefaultColumns();
        ColumnDefine  cd(col_id_to_drop, col_name_to_drop, col_type_to_drop);
        table_column_defines.emplace_back(cd);
        store = reload(table_column_defines);
    }

    {
        // check column structure
        const auto & cols = store->getTableColumns();
        ASSERT_EQ(cols.size(), 4UL);
        const auto & str_col = cols[3];
        ASSERT_EQ(str_col.name, col_name_to_drop);
        ASSERT_EQ(str_col.id, col_id_to_drop);
        ASSERT_TRUE(str_col.type->equals(*col_type_to_drop));
    }

    const size_t num_rows_write = 128;
    {
        // write to store
        Block block;
        {
            block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
            // Add a column of col2:String for test
            ColumnWithTypeAndName col2(std::make_shared<DataTypeInt8>(), col_name_to_drop);
            {
                IColumn::MutablePtr m_col2 = col2.type->createColumn();
                for (size_t i = 0; i < num_rows_write; i++)
                {
                    Int64 num = i * (i % 2 == 0 ? -1 : 1);
                    m_col2->insert(Field(num));
                }
                col2.column = std::move(m_col2);
            }
            block.insert(col2);
        }
        store->write(*context, context->getSettingsRef(), block);
    }

    {
        // DDL change delete col i8
        AlterCommands commands;
        {
            AlterCommand com;
            com.type        = AlterCommand::DROP_COLUMN;
            com.data_type   = col_type_to_drop;
            com.column_name = col_name_to_drop;
            com.column_id   = col_id_to_drop;
            commands.emplace_back(std::move(com));
        }
        ColumnID ignored = 0;
        store->applyAlters(commands, std::nullopt, ignored, *context);
    }

    {
        // read all columns from store
        const auto &      columns = store->getTableColumns();
        BlockInputStreams ins     = store->read(*context,
                                            context->getSettingsRef(),
                                            columns,
                                            {HandleRange::newAll()},
                                            /* num_streams= */ 1,
                                            /* max_version= */ std::numeric_limits<UInt64>::max(),
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1UL);
        BlockInputStreamPtr & in = ins[0];
        {
            const Block head = in->getHeader();
            ASSERT_FALSE(head.has(col_name_to_drop));
        }

        size_t num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            for (auto && iter : block)
            {
                auto c = iter.column;
                for (Int64 i = 0; i < Int64(c->size()); ++i)
                {
                    if (iter.name == "pk")
                    {
                        EXPECT_EQ(c->getInt(i), i);
                    }
                }
            }
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
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

TEST_F(DeltaMergeStore_test, DDLAddColumn)
try
{
    const String      col_name_c1 = "i8";
    const ColId       col_id_c1   = 2;
    const DataTypePtr col_type_c1 = DataTypeFactory::instance().get("Int8");

    const String      col_name_to_add = "i32";
    const ColId       col_id_to_add   = 3;
    const DataTypePtr col_type_to_add = DataTypeFactory::instance().get("Int32");
    {
        ColumnDefines table_column_defines = DMTestEnv::getDefaultColumns();
        ColumnDefine  cd(col_id_c1, col_name_c1, col_type_c1);
        table_column_defines.emplace_back(cd);
        store = reload(table_column_defines);
    }

    const size_t num_rows_write = 128;
    {
        // write to store
        Block block;
        {
            block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
            // Add a column of col1:String for test
            ColumnWithTypeAndName col1(std::make_shared<DataTypeInt8>(), col_name_c1);
            {
                IColumn::MutablePtr m_col2 = col1.type->createColumn();
                for (size_t i = 0; i < num_rows_write; i++)
                {
                    Int64 num = i * (i % 2 == 0 ? -1 : 1);
                    m_col2->insert(Field(num));
                }
                col1.column = std::move(m_col2);
            }
            block.insert(col1);
        }
        store->write(*context, context->getSettingsRef(), block);
    }

    {
        // DDL change add col i32
        AlterCommands commands;
        {
            AlterCommand com;
            com.type        = AlterCommand::ADD_COLUMN;
            com.data_type   = col_type_to_add;
            com.column_name = col_name_to_add;
            commands.emplace_back(std::move(com));
        }
        ColumnID _col_to_add = col_id_to_add;
        store->applyAlters(commands, std::nullopt, _col_to_add, *context);
    }

    {
        // read all columns from store
        const auto &      columns = store->getTableColumns();
        BlockInputStreams ins     = store->read(*context,
                                            context->getSettingsRef(),
                                            columns,
                                            {HandleRange::newAll()},
                                            /* num_streams= */ 1,
                                            /* max_version= */ std::numeric_limits<UInt64>::max(),
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1UL);
        BlockInputStreamPtr & in = ins[0];
        {
            const Block head = in->getHeader();
            {
                const auto & col = head.getByName(col_name_c1);
                ASSERT_EQ(col.name, col_name_c1);
                ASSERT_EQ(col.column_id, col_id_c1);
                ASSERT_TRUE(col.type->equals(*col_type_c1));
            }

            {
                const auto & col = head.getByName(col_name_to_add);
                ASSERT_EQ(col.name, col_name_to_add);
                ASSERT_EQ(col.column_id, col_id_to_add);
                ASSERT_TRUE(col.type->equals(*col_type_to_add));
            }
        }

        size_t num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            for (auto && iter : block)
            {
                auto c = iter.column;
                for (Int64 i = 0; i < Int64(c->size()); ++i)
                {
                    if (iter.name == "pk")
                    {
                        EXPECT_EQ(c->getInt(i), i);
                    }
                    else if (iter.name == col_name_c1)
                    {
                        Int64 num = i * (i % 2 == 0 ? -1 : 1);
                        EXPECT_EQ(c->getInt(i), num);
                    }
                    else if (iter.name == col_name_to_add)
                    {
                        EXPECT_EQ(c->getInt(i), 0);
                    }
                }
            }
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
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

TEST_F(DeltaMergeStore_test, DDLAddColumnFloat32)
try
{
    const String      col_name_to_add = "f32";
    const ColId       col_id_to_add   = 2;
    const DataTypePtr col_type_to_add = DataTypeFactory::instance().get("Float32");

    // write some rows before DDL
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, 1, false);
        store->write(*context, context->getSettingsRef(), block);
    }

    // DDL add column f32 with default value
    {
        AlterCommands commands;
        {
            AlterCommand com;
            com.type        = AlterCommand::ADD_COLUMN;
            com.data_type   = col_type_to_add;
            com.column_name = col_name_to_add;

            // mock default value
            // actual ddl is like: ADD COLUMN `f32` Float32 DEFAULT 1.23
            auto cast = std::make_shared<ASTFunction>();
            {
                cast->name      = "CAST";
                ASTPtr arg      = std::make_shared<ASTLiteral>(toField(DecimalField(Decimal32(123), 2)));
                cast->arguments = std::make_shared<ASTExpressionList>();
                cast->children.push_back(cast->arguments);
                cast->arguments->children.push_back(arg);
                cast->arguments->children.push_back(ASTPtr()); // dummy alias
            }
            com.default_expression = cast;
            commands.emplace_back(std::move(com));
        }
        ColumnID _col_to_add = col_id_to_add;
        store->applyAlters(commands, std::nullopt, _col_to_add, *context);
    }

    // try read
    {
        auto in = store->read(*context,
                              context->getSettingsRef(),
                              store->getTableColumns(),
                              {HandleRange::newAll()},
                              /* num_streams= */ 1,
                              /* max_version= */ std::numeric_limits<UInt64>::max(),
                              /* expected_block_size= */ 1024)[0];

        in->readPrefix();
        while (Block block = in->read())
        {
            for (auto & itr : block)
            {
                auto c = itr.column;
                for (size_t i = 0; i < c->size(); i++)
                {
                    if (itr.name == "f32")
                    {
                        Field tmp;
                        c->get(i, tmp);
                        // There is some loss of precision during the convertion, so we just do a rough comparison
                        Float64 epsilon = 0.00001;
                        EXPECT_TRUE(std::abs(tmp.get<Float64>() - 1.23) < epsilon);
                    }
                }
            }
        }
        in->readSuffix();
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

TEST_F(DeltaMergeStore_test, DDLAddColumnDateTime)
try
{
    const String      col_name_to_add = "dt";
    const ColId       col_id_to_add   = 2;
    const DataTypePtr col_type_to_add = DataTypeFactory::instance().get("DateTime");

    // write some rows before DDL
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, 1, false);
        store->write(*context, context->getSettingsRef(), block);
    }

    // DDL add column date with default value
    {
        AlterCommands commands;
        {
            AlterCommand com;
            com.type        = AlterCommand::ADD_COLUMN;
            com.data_type   = col_type_to_add;
            com.column_name = col_name_to_add;

            // mock default value
            // actual ddl is like: ADD COLUMN `date` DateTime DEFAULT '1999-09-09 12:34:56'
            auto cast = std::make_shared<ASTFunction>();
            {
                cast->name      = "CAST";
                ASTPtr arg      = std::make_shared<ASTLiteral>(toField(String("1999-09-09 12:34:56")));
                cast->arguments = std::make_shared<ASTExpressionList>();
                cast->children.push_back(cast->arguments);
                cast->arguments->children.push_back(arg);
                cast->arguments->children.push_back(ASTPtr()); // dummy alias
            }
            com.default_expression = cast;
            commands.emplace_back(std::move(com));
        }
        ColumnID _col_to_add = col_id_to_add;
        store->applyAlters(commands, std::nullopt, _col_to_add, *context);
    }

    // try read
    {
        auto in = store->read(*context,
                              context->getSettingsRef(),
                              store->getTableColumns(),
                              {HandleRange::newAll()},
                              /* num_streams= */ 1,
                              /* max_version= */ std::numeric_limits<UInt64>::max(),
                              /* expected_block_size= */ 1024)[0];

        in->readPrefix();
        while (Block block = in->read())
        {
            for (auto & itr : block)
            {
                auto c = itr.column;
                for (size_t i = 0; i < c->size(); i++)
                {
                    if (itr.name == "dt")
                    {
                        // Timestamp for '1999-09-09 12:34:56'
                        EXPECT_EQ(c->getUInt(i), 936851696UL);
                    }
                }
            }
        }
        in->readSuffix();
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

TEST_F(DeltaMergeStore_test, DDLRenameColumn)
try
{
    const String      col_name_before_ddl = "i8";
    const String      col_name_after_ddl  = "i8_tmp";
    const ColId       col_id_ddl          = 2;
    const DataTypePtr col_type            = DataTypeFactory::instance().get("Int32");
    {
        ColumnDefines table_column_defines = DMTestEnv::getDefaultColumns();
        ColumnDefine  cd(col_id_ddl, col_name_before_ddl, col_type);
        table_column_defines.emplace_back(cd);
        store = reload(table_column_defines);
    }

    {
        // check column structure
        const auto & cols = store->getTableColumns();
        ASSERT_EQ(cols.size(), 4UL);
        const auto & str_col = cols[3];
        ASSERT_EQ(str_col.name, col_name_before_ddl);
        ASSERT_EQ(str_col.id, col_id_ddl);
        ASSERT_TRUE(str_col.type->equals(*col_type));
    }

    const size_t num_rows_write = 128;
    {
        // write to store
        Block block;
        {
            block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
            // Add a column for test
            ColumnWithTypeAndName col2(col_type, col_name_before_ddl);
            {
                IColumn::MutablePtr m_col2 = col2.type->createColumn();
                for (size_t i = 0; i < num_rows_write; i++)
                {
                    Int64 num = i * (i % 2 == 0 ? -1 : 1);
                    m_col2->insert(Field(num));
                }
                col2.column = std::move(m_col2);
            }
            block.insert(col2);
        }
        store->write(*context, context->getSettingsRef(), block);
    }

    {
        // DDL change col from i8 -> i32
        AlterCommands commands;
        {
            AlterCommand com;
            com.type            = AlterCommand::RENAME_COLUMN;
            com.data_type       = col_type;
            com.column_name     = col_name_before_ddl;
            com.new_column_name = col_name_after_ddl;
            com.column_id       = col_id_ddl;
            commands.emplace_back(std::move(com));
        }
        ColumnID _ignored = 0;
        store->applyAlters(commands, std::nullopt, _ignored, *context);
    }

    {
        // read all columns from store
        const auto &      columns = store->getTableColumns();
        BlockInputStreams ins     = store->read(*context,
                                            context->getSettingsRef(),
                                            columns,
                                            {HandleRange::newAll()},
                                            /* num_streams= */ 1,
                                            /* max_version= */ std::numeric_limits<UInt64>::max(),
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1UL);
        BlockInputStreamPtr & in = ins[0];
        {
            // check col rename is success
            const Block  head = in->getHeader();
            const auto & col  = head.getByName(col_name_after_ddl);
            ASSERT_EQ(col.name, col_name_after_ddl);
            ASSERT_EQ(col.column_id, col_id_ddl);
            ASSERT_TRUE(col.type->equals(*col_type));
            // check old col name is not exist
            ASSERT_THROW(head.getByName(col_name_before_ddl), ::DB::Exception);
        }

        size_t num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            for (auto && iter : block)
            {
                auto c = iter.column;
                for (Int64 i = 0; i < Int64(c->size()); ++i)
                {
                    if (iter.name == "pk")
                    {
                        //printf("pk:%lld\n", c->getInt(i));
                        EXPECT_EQ(c->getInt(i), i);
                    }
                    else if (iter.name == col_name_after_ddl)
                    {
                        //printf("col2:%s\n", c->getDataAt(i).data);
                        Int64 num = i * (i % 2 == 0 ? -1 : 1);
                        EXPECT_EQ(c->getInt(i), num);
                    }
                }
            }
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
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


/// tests for prepare write actions

namespace
{

DeltaMergeStore::SegmentSortedMap prepareSegments(const HandleRanges & ranges)
{
    DeltaMergeStore::SegmentSortedMap segments;

    const UInt64 epoch      = 0;
    PageId       segment_id = 0;
    PageId       delta_id   = 1024;
    PageId       stable_id  = 2048;

    auto segment_generator = [&](HandleRange range) -> SegmentPtr {
        SegmentPtr s = std::make_shared<Segment>(
            epoch, /* range= */ range, /* segment_id= */ segment_id, /*next_segment_id=*/segment_id + 1, delta_id, stable_id);
        segment_id++;
        delta_id++;
        stable_id++;
        return s;
    };

    for (const auto & range : ranges)
    {
        auto seg = segment_generator(range);
        segments.insert({range.end, seg});
    }

    return segments;
}

} // namespace

TEST(DeltaMergeStoreInternal_test, PrepareWriteForBlock)
{
    std::shared_mutex m;

    const HandleRanges ranges = {
        {-100, -23},
        {-23, 25},
        {25, 49},
        {49, 103},
    };

    const size_t block_pk_beg = -4;
    const size_t block_pk_end = 49;

    DeltaMergeStore::SegmentSortedMap segments = prepareSegments(ranges);
    Block                             block    = DMTestEnv::prepareSimpleWriteBlock(block_pk_beg, block_pk_end, false);
    const String                      pk_name  = "pk";

    auto actions = prepareWriteActions(block, segments, pk_name, std::shared_lock(m));
    ASSERT_EQ(actions.size(), 2UL);

    auto & act0 = actions[0];
    ASSERT_NE(act0.segment, nullptr);
    ASSERT_RANGE_EQ(act0.segment->getRange(), ranges[1]);
    EXPECT_EQ(act0.offset, 0UL);
    const size_t end_off_for_act0 = ranges[1].end - block_pk_beg;
    EXPECT_EQ(act0.limit, end_off_for_act0);

    auto & act1 = actions[1];
    ASSERT_NE(act1.segment, nullptr);
    ASSERT_RANGE_EQ(act1.segment->getRange(), ranges[2]);
    EXPECT_EQ(act1.offset, end_off_for_act0);
    EXPECT_EQ(act1.limit, block.rows() - end_off_for_act0);
}

TEST(DeltaMergeStoreInternal_test, PrepareWriteForDeleteRange)
{
    std::shared_mutex m;

    const HandleRanges ranges = {
        {-100, -23},
        {-23, 25},
        {25, 49},
        {49, 103},
    };

    DeltaMergeStore::SegmentSortedMap segments = prepareSegments(ranges);
    HandleRange                       delete_range(-4, 49);

    auto actions = prepareWriteActions(delete_range, segments, std::shared_lock(m));
    ASSERT_EQ(actions.size(), 2UL);

    auto & act0 = actions[0];
    ASSERT_NE(act0.segment, nullptr);
    EXPECT_RANGE_EQ(act0.segment->getRange(), ranges[1]);
    ASSERT_FALSE(act0.update.block);                         // no rows in block
    EXPECT_RANGE_EQ(act0.update.delete_range, delete_range); // TODO maybe more precise

    auto & act1 = actions[1];
    ASSERT_NE(act1.segment, nullptr);
    EXPECT_RANGE_EQ(act1.segment->getRange(), ranges[2]);
    ASSERT_FALSE(act1.update.block); // no rows in block
    EXPECT_RANGE_EQ(act1.update.delete_range, delete_range);
}

} // namespace tests
} // namespace DM
} // namespace DB
