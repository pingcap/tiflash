#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/File/DMFileWriter.h>

#include "dm_basic_include.h"

namespace DB
{
namespace DM
{
namespace tests
{

using DMFileBlockOutputStreamPtr = std::shared_ptr<DMFileBlockOutputStream>;
using DMFileBlockInputStreamPtr  = std::shared_ptr<DMFileBlockInputStream>;

class DMFile_Test : public ::testing::Test
{
public:
    DMFile_Test() : path(DB::tests::TiFlashTestEnv::getTemporaryPath() + "/dm_file_tests"), dm_file(nullptr) {}

    void SetUp() override
    {
        dropFiles();
        storage_pool = std::make_unique<StoragePool>("test.t1", path);
        dm_file      = DMFile::create(0, path);
        db_context   = std::make_unique<Context>(DMTestEnv::getContext(DB::Settings()));

        reload();
    }

    void dropFiles()
    {
        Poco::File file(path);
        if (file.exists())
        {
            file.remove(true);
        }
    }

    void reload(const ColumnDefines & cols = DMTestEnv::getDefaultColumns())
    {
        dm_context = std::make_unique<DMContext>(*db_context,
                                                 path,
                                                 db_context->getExtraPaths(),
                                                 *storage_pool,
                                                 0,
                                                 cols,
                                                 cols.at(0),
                                                 0,
                                                 settings.not_compress_columns,
                                                 db_context->getSettingsRef().dm_segment_limit_rows,
                                                 db_context->getSettingsRef().dm_segment_delta_limit_rows,
                                                 db_context->getSettingsRef().dm_segment_delta_cache_limit_rows,
                                                 db_context->getSettingsRef().dm_segment_stable_chunk_rows,
                                                 db_context->getSettingsRef().dm_enable_logical_split,
                                                 false,
                                                 false);
    }


    DMContext & dmContext() { return *dm_context; }

    Context & dbContext() { return *db_context; }

private:
    String                     path;
    std::unique_ptr<Context>   db_context;
    std::unique_ptr<DMContext> dm_context;

    std::unique_ptr<StoragePool> storage_pool;

    DeltaMergeStore::Settings settings;

protected:
    DMFilePtr dm_file;
};


TEST_F(DMFile_Test, WriteRead)
try
{
    auto         cols           = DMTestEnv::getDefaultColumns();
    const size_t num_rows_write = 128;

    {
        // Prepare for write
        Block block1 = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write / 2, false);
        Block block2 = DMTestEnv::prepareSimpleWriteBlock(num_rows_write / 2, num_rows_write, false);
        auto  stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, cols);
        stream->writePrefix();
        stream->write(block1, 0);
        stream->write(block2, 0);
        stream->writeSuffix();
    }

    {
        // Test read
        auto stream = std::make_shared<DMFileBlockInputStream>(dbContext(), //
                                                               std::numeric_limits<UInt64>::max(),
                                                               false,
                                                               dmContext().hash_salt,
                                                               dm_file,
                                                               cols,
                                                               HandleRange::newAll(),
                                                               RSOperatorPtr{},
                                                               IdSetPtr{});

        size_t num_rows_read = 0;
        stream->readPrefix();
        while (Block in = stream->read())
        {
            for (auto itr : in)
            {
                auto c = itr.column;
                if (itr.name == DMTestEnv::pk_name)
                {
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        EXPECT_EQ(c->getInt(i), Int64(i));
                        num_rows_read++;
                    }
                }
            }
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }
} // namespace tests
CATCH

TEST_F(DMFile_Test, NumberTypes)
try
{
    auto cols = DMTestEnv::getDefaultColumns();
    // Prepare columns
    ColumnDefine i64_col(2, "i64", DataTypeFactory::instance().get("Int64"));
    ColumnDefine f64_col(3, "f64", DataTypeFactory::instance().get("Float64"));
    cols.push_back(i64_col);
    cols.push_back(f64_col);

    reload(cols);

    {
        // Prepare write
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, 128, false);

        auto col = i64_col.type->createColumn();
        for (int i = 0; i < 128; i++)
        {
            col->insert(toField(Int64(i)));
        }
        ColumnWithTypeAndName i64(std::move(col), i64_col.type, i64_col.name, i64_col.id);

        col = f64_col.type->createColumn();
        for (int i = 0; i < 128; i++)
        {
            col->insert(toField(Float64(0.125)));
        }
        ColumnWithTypeAndName f64(std::move(col), f64_col.type, f64_col.name, f64_col.id);

        block.insert(i64);
        block.insert(f64);

        auto stream = std::make_unique<DMFileBlockOutputStream>(dbContext(), dm_file, cols);
        stream->writePrefix();
        stream->write(block, 0);
        stream->writeSuffix();
    }

    {
        // Test Read
        auto stream = std::make_unique<DMFileBlockInputStream>(dbContext(), //
                                                               std::numeric_limits<UInt64>::max(),
                                                               false,
                                                               dmContext().hash_salt,
                                                               dm_file,
                                                               cols,
                                                               HandleRange::newAll(),
                                                               RSOperatorPtr{},
                                                               IdSetPtr{});

        size_t num_rows_read = 0;
        stream->readPrefix();
        while (Block in = stream->read())
        {
            for (auto itr : in)
            {
                auto c = itr.column;
                if (itr.name == "i64")
                {
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        EXPECT_EQ(c->getInt(i), Int64(i));
                    }
                }
                else if (itr.name == "f64")
                {
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        Field value;
                        c->get(i, value);
                        Float64 v = value.get<Float64>();
                        EXPECT_EQ(v, 0.125);
                    }
                }
            }
            num_rows_read += in.rows();
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, 128UL);
    }
}
CATCH

TEST_F(DMFile_Test, StringType)
{
    auto cols = DMTestEnv::getDefaultColumns();
    // Prepare columns
    ColumnDefine fixed_str_col(2, "str", DataTypeFactory::instance().get("FixedString(5)"));
    cols.push_back(fixed_str_col);

    reload(cols);

    {
        // Prepare write
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, 128, false);

        auto col = fixed_str_col.type->createColumn();
        for (int i = 0; i < 128; i++)
        {
            col->insert(toField(String("hello")));
        }
        ColumnWithTypeAndName str(std::move(col), fixed_str_col.type, fixed_str_col.name, fixed_str_col.id);

        block.insert(str);

        auto stream = std::make_unique<DMFileBlockOutputStream>(dbContext(), dm_file, cols);
        stream->writePrefix();
        stream->write(block, 0);
        stream->writeSuffix();
    }

    {
        // Test Read
        auto stream = std::make_unique<DMFileBlockInputStream>(dbContext(), //
                                                               std::numeric_limits<UInt64>::max(),
                                                               false,
                                                               dmContext().hash_salt,
                                                               dm_file,
                                                               cols,
                                                               HandleRange::newAll(),
                                                               RSOperatorPtr{},
                                                               IdSetPtr{});

        size_t num_rows_read = 0;
        stream->readPrefix();
        while (Block in = stream->read())
        {
            for (auto itr : in)
            {
                auto c = itr.column;
                if (itr.name == "str")
                {
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        Field value;
                        c->get(i, value);
                        EXPECT_EQ(value.get<String>(), "hello");
                    }
                }
            }
            num_rows_read += in.rows();
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, 128UL);
    }
}

TEST_F(DMFile_Test, NullableType)
try
{
    auto cols = DMTestEnv::getDefaultColumns();
    {
        // Prepare columns
        ColumnDefine nullable_col(2, "i32_null", DataTypeFactory::instance().get("Nullable(Int32)"));
        cols.emplace_back(nullable_col);
    }

    reload(cols);

    {
        // Prepare write
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, 128, false);

        ColumnWithTypeAndName nullable_col({}, DataTypeFactory::instance().get("Nullable(Int32)"), "i32_null", 2);
        auto                  col = nullable_col.type->createColumn();
        for (int i = 0; i < 64; i++)
        {
            col->insert(toField(i));
        }
        for (int i = 64; i < 128; i++)
        {
            col->insertDefault();
        }
        nullable_col.column = std::move(col);

        block.insert(nullable_col);
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, cols);
        stream->writePrefix();
        stream->write(block, 0);
        stream->writeSuffix();
    }

    {
        // Test read
        auto   stream        = std::make_shared<DMFileBlockInputStream>(dbContext(), //
                                                               std::numeric_limits<UInt64>::max(),
                                                               false,
                                                               dmContext().hash_salt,
                                                               dm_file,
                                                               cols,
                                                               HandleRange::newAll(),
                                                               RSOperatorPtr{},
                                                               IdSetPtr{});
        size_t num_rows_read = 0;
        stream->readPrefix();
        while (Block in = stream->read())
        {
            for (auto itr : in)
            {
                auto c = itr.column;
                if (itr.name == DMTestEnv::pk_name)
                {
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        EXPECT_EQ(c->getInt(i), Int64(i));
                    }
                }
                else if (itr.column_id == 2)
                {
                    const auto col    = typeid_cast<const ColumnNullable *>(c.get());
                    auto       nested = col->getNestedColumnPtr();
                    for (size_t i = 0; i < col->size(); i++)
                    {
                        if (i < 64)
                        {
                            EXPECT_FALSE(col->isNullAt(i));
                            EXPECT_EQ(nested->getInt(i), Int64(i));
                        }
                        else
                        {
                            EXPECT_TRUE(col->isNullAt(i));
                        }
                    }
                }
            }
            num_rows_read += in.rows();
        }
        ASSERT_EQ(num_rows_read, 128UL);
        stream->readSuffix();
    }
}
CATCH

} // namespace tests
} // namespace DM
} // namespace DB
