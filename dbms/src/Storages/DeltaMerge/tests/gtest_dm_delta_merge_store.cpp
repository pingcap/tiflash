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
    {
        ColumnDefines table_column_defines = DMTestEnv::getDefaultColumns();
        ColumnDefine  cd(2, "col2", std::make_shared<DataTypeString>());
        table_column_defines.emplace_back(cd);
        reload(table_column_defines);
    }

    {
        // check column structure
        const auto & cols = store->getTableColumns();
        ASSERT_EQ(cols.size(), 4UL);
        auto & str_col = cols[3];
        ASSERT_EQ(str_col.name, "col2");
        ASSERT_EQ(str_col.id, 2);
        ASSERT_TRUE(str_col.type->equals(*DataTypeFactory::instance().get("String")));
    }

    const size_t num_rows_write = 500;
    {
        // write to store
        Block block;
        {
            block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, true);
            // Add a column of col2:String for test
            ColumnWithTypeAndName col2(std::make_shared<DataTypeString>(), "col2");
            {
                IColumn::MutablePtr m_col2 = col2.type->createColumn();
                for (size_t i = 0; i < num_rows_write; i++)
                {
                    Field field("a", 1);
                    m_col2->insert(field);
                }
                col2.column = std::move(m_col2);
            }
            block.insert(col2);
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
                    else if (iter.name == "col2")
                    {
                        //printf("col2:%s\n", c->getDataAt(i).data);
                        EXPECT_EQ(c->getDataAt(i), "a");
                    }
                }
            }
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}

TEST_F(DeltaMergeStore_test, DDLChanegInt8ToInt32)
{
    const String col_name_ddl_change_type = "i8";
    const ColId  col_id_ddl_change_type   = 2;
    {
        ColumnDefines table_column_defines = DMTestEnv::getDefaultColumns();
        ColumnDefine  cd(col_id_ddl_change_type, col_name_ddl_change_type, std::make_shared<DataTypeInt8>());
        table_column_defines.emplace_back(cd);
        reload(table_column_defines);
    }

    {
        // check column structure
        const auto & cols = store->getTableColumns();
        ASSERT_EQ(cols.size(), 4UL);
        auto & str_col = cols[3];
        ASSERT_EQ(str_col.name, col_name_ddl_change_type);
        ASSERT_EQ(str_col.id, 2);
        ASSERT_TRUE(str_col.type->equals(*DataTypeFactory::instance().get("String")));
    }

    const size_t num_rows_write = 500;
    {
        // write to store
        Block block;
        {
            block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, true);
            // Add a column of col2:String for test
            ColumnWithTypeAndName col2(std::make_shared<DataTypeInt8>(), col_name_ddl_change_type);
            {
                IColumn::MutablePtr m_col2 = col2.type->createColumn();
                for (size_t i = 0; i < num_rows_write; i++)
                {
                    Field field = static_cast<NearestFieldType<Int8>::Type>(i);
                    m_col2->insert(field);
                }
                col2.column = std::move(m_col2);
            }
            block.insert(col2);
        }
        store->write(*context, context->getSettingsRef(), block);
    }

    {
        // DDL change col from i8 -> i32
        // store->alterFromTiDB();
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
            const auto & col  = head.getByName(col_name_ddl_change_type);
            ASSERT_EQ(col.name, col_name_ddl_change_type);
            ASSERT_EQ(col.column_id, col_id_ddl_change_type);
            ASSERT_TRUE(col.type->equals(*DataTypeFactory::instance().get("Int32")));
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
                    else if (iter.name == "i8")
                    {
                        //printf("col2:%s\n", c->getDataAt(i).data);
                        EXPECT_EQ(c->getInt(i), i);
                    }
                }
            }
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}
} // namespace tests
} // namespace DM
} // namespace DB
