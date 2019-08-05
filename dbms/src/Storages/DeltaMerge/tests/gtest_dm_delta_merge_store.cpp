#include <gtest/gtest.h>
#include "dm_basic_include.h"

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
    }

protected:
    // the table name
    String name;
    // the path to the dir of table
    String path;
};

TEST_F(DeltaMergeStore_test, Case1)
{
    // create table
    Context      context = DMTestEnv::getContext();
    ColumnDefine handle_column_define{
        .name = "pk",
        .type = std::make_shared<DataTypeInt64>(),
        .id   = 1,
    };
    ColumnDefines table_column_defines;
    {
        table_column_defines.emplace_back(handle_column_define);
        table_column_defines.emplace_back(ColumnDefine{.name = "col2", .type = std::make_shared<DataTypeString>(), .id = 2});
    }

    DeltaMergeStorePtr store
        = std::make_shared<DeltaMergeStore>(context, path, name, table_column_defines, handle_column_define, DeltaMergeStore::Settings());
    {
        // check handle column of store
        auto & h = store->getHandle();
        ASSERT_EQ(h.name, handle_column_define.name);
        ASSERT_EQ(h.id, handle_column_define.id);
        ASSERT_TRUE(h.type->equals(*handle_column_define.type));
    }
    {
        // check column structure of store
        auto & cols = store->getTableColumns();
        // version & tag column added
        ASSERT_EQ(cols.size(), table_column_defines.size() + 2);
        // TODO check other cols name/type
    }

    const size_t num_rows_write = 500;
    {
        // write to store
        Block block;
        {
            block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, true);

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
        store->write(context, context.getSettingsRef(), block);
    }

    {
        // TODO read data from more than one block
        // TODO read data from mutli streams
        // TODO read partial columns from store
        // TODO read data of max_version
        // read all columns from store
        BlockInputStreamPtr in = store->read(context,
                                             context.getSettingsRef(),
                                             table_column_defines,
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

} // namespace tests
} // namespace DM
} // namespace DB
