#include <gtest/gtest.h>
#include "dm_basic_include.h"

#include <Poco/File.h>

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
    void SetUp() override
    {
        // drop former-gen table's data in disk
        Poco::File file(path);
        if (file.exists())
            file.remove(true);

        context = std::make_unique<Context>(DMTestEnv::getContext());
        store   = reload();
    }

    DeltaMergeStorePtr reload(const ColumnDefines & pre_define_columns = {})
    {
        ColumnDefines cols                 = pre_define_columns.empty() ? DMTestEnv::getDefaultColumns() : pre_define_columns;
        ColumnDefine  handle_column_define = cols[0];

        DeltaMergeStorePtr s
            = std::make_shared<DeltaMergeStore>(*context, path, name, cols, handle_column_define, DeltaMergeStore::Settings());
        return s;
    }

private:
    // the table name
    String name;
    // the path to the dir of table
    String path;

protected:
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

} // namespace tests
} // namespace DM
} // namespace DB
