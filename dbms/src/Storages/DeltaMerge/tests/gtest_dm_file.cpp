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

        auto settings  = DB::Settings();
        storage_pool   = std::make_unique<StoragePool>("test.t1", path, settings);
        dm_file        = DMFile::create(0, path);
        db_context     = std::make_unique<Context>(DMTestEnv::getContext(settings));
        table_columns_ = std::make_shared<ColumnDefines>();

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

    // Update dm_context.
    void reload(const ColumnDefinesPtr & cols = DMTestEnv::getDefaultColumns())
    {
        *table_columns_ = *cols;

        dm_context = std::make_unique<DMContext>( //
            *db_context,
            path,
            db_context->getExtraPaths(),
            *storage_pool,
            /*hash_salt*/ 0,
            table_columns_,
            0,
            settings.not_compress_columns,
            db_context->getSettingsRef());
    }


    DMContext & dmContext() { return *dm_context; }

    Context & dbContext() { return *db_context; }

private:
    String                     path;
    std::unique_ptr<Context>   db_context;
    std::unique_ptr<DMContext> dm_context;
    /// all these var live as ref in dm_context
    std::unique_ptr<StoragePool> storage_pool;
    ColumnDefinesPtr             table_columns_;
    DeltaMergeStore::Settings    settings;

protected:
    DMFilePtr dm_file;
};


TEST_F(DMFile_Test, WriteRead)
try
{
    auto cols = DMTestEnv::getDefaultColumns();

    const size_t num_rows_write = 128;

    {
        // Prepare for write
        Block block1 = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write / 2, false);
        Block block2 = DMTestEnv::prepareSimpleWriteBlock(num_rows_write / 2, num_rows_write, false);
        auto  stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);
        stream->writePrefix();
        stream->write(block1, 0);
        stream->write(block2, 0);
        stream->writeSuffix();
    }

    {
        // Test read
        auto stream = std::make_shared<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dmContext().hash_salt,
            dm_file,
            *cols,
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
}
CATCH

TEST_F(DMFile_Test, NumberTypes)
try
{
    auto cols = DMTestEnv::getDefaultColumns();
    // Prepare columns
    ColumnDefine i64_col(2, "i64", typeFromString("Int64"));
    ColumnDefine f64_col(3, "f64", typeFromString("Float64"));
    cols->push_back(i64_col);
    cols->push_back(f64_col);

    reload(cols);

    const size_t num_rows_write = 128;
    {
        // Prepare write
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);

        auto col = i64_col.type->createColumn();
        for (size_t i = 0; i < num_rows_write; i++)
        {
            col->insert(toField(Int64(i)));
        }
        ColumnWithTypeAndName i64(std::move(col), i64_col.type, i64_col.name, i64_col.id);

        col = f64_col.type->createColumn();
        for (size_t i = 0; i < num_rows_write; i++)
        {
            col->insert(toField(Float64(0.125)));
        }
        ColumnWithTypeAndName f64(std::move(col), f64_col.type, f64_col.name, f64_col.id);

        block.insert(i64);
        block.insert(f64);

        auto stream = std::make_unique<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);
        stream->writePrefix();
        stream->write(block, 0);
        stream->writeSuffix();
    }

    {
        // Test Read
        auto stream = std::make_unique<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dmContext().hash_salt,
            dm_file,
            *cols,
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
        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}
CATCH

TEST_F(DMFile_Test, StringType)
{
    auto cols = DMTestEnv::getDefaultColumns();
    // Prepare columns
    ColumnDefine fixed_str_col(2, "str", typeFromString("FixedString(5)"));
    cols->push_back(fixed_str_col);

    reload(cols);

    const size_t num_rows_write = 128;
    {
        // Prepare write
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);

        auto col = fixed_str_col.type->createColumn();
        for (size_t i = 0; i < num_rows_write; i++)
        {
            col->insert(toField(String("hello")));
        }
        ColumnWithTypeAndName str(std::move(col), fixed_str_col.type, fixed_str_col.name, fixed_str_col.id);

        block.insert(str);

        auto stream = std::make_unique<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);
        stream->writePrefix();
        stream->write(block, 0);
        stream->writeSuffix();
    }

    {
        // Test Read
        auto stream = std::make_unique<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dmContext().hash_salt,
            dm_file,
            *cols,
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
        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}

TEST_F(DMFile_Test, NullableType)
try
{
    auto cols = DMTestEnv::getDefaultColumns();
    {
        // Prepare columns
        ColumnDefine nullable_col(2, "i32_null", typeFromString("Nullable(Int32)"));
        cols->emplace_back(nullable_col);
    }

    reload(cols);

    const size_t num_rows_write = 128;
    {
        // Prepare write
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);

        ColumnWithTypeAndName nullable_col({}, typeFromString("Nullable(Int32)"), "i32_null", 2);
        auto                  col = nullable_col.type->createColumn();
        for (size_t i = 0; i < 64; i++)
        {
            col->insert(toField(Int64(i)));
        }
        for (size_t i = 64; i < num_rows_write; i++)
        {
            col->insertDefault();
        }
        nullable_col.column = std::move(col);

        block.insert(nullable_col);
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);
        stream->writePrefix();
        stream->write(block, 0);
        stream->writeSuffix();
    }

    {
        // Test read
        auto stream = std::make_shared<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dmContext().hash_salt,
            dm_file,
            *cols,
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
        ASSERT_EQ(num_rows_read, num_rows_write);
        stream->readSuffix();
    }
}
CATCH

/// DDL test cases
class DMFile_DDL_Test : public DMFile_Test
{
public:
    /// Write some data into DMFile.
    /// return rows write, schema
    std::pair<size_t, ColumnDefines> prepareSomeDataToDMFile()
    {
        size_t num_rows_write  = 128;
        auto   cols_before_ddl = DMTestEnv::getDefaultColumns();

        ColumnDefine i8_col(2, "i8", typeFromString("Int8"));
        ColumnDefine f64_col(3, "f64", typeFromString("Float64"));
        cols_before_ddl->push_back(i8_col);
        cols_before_ddl->push_back(f64_col);

        reload(cols_before_ddl);

        {
            // Prepare write
            Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);

            auto col = i8_col.type->createColumn();
            for (size_t i = 0; i < num_rows_write; i++)
            {
                col->insert(toField(Int64(i) * (-1 * (i % 2))));
            }
            ColumnWithTypeAndName i64(std::move(col), i8_col.type, i8_col.name, i8_col.id);

            col = f64_col.type->createColumn();
            for (size_t i = 0; i < num_rows_write; i++)
            {
                col->insert(toField(Float64(0.125)));
            }
            ColumnWithTypeAndName f64(std::move(col), f64_col.type, f64_col.name, f64_col.id);

            block.insert(i64);
            block.insert(f64);

            auto stream = std::make_unique<DMFileBlockOutputStream>(dbContext(), dm_file, *cols_before_ddl);
            stream->writePrefix();
            stream->write(block, 0);
            stream->writeSuffix();

            return {num_rows_write, *cols_before_ddl};
        }
    }
};

TEST_F(DMFile_DDL_Test, AddColumn)
try
{
    // Prepare some data before ddl
    const auto [num_rows_write, cols_before_ddl] = prepareSomeDataToDMFile();

    // Mock that we add new column after ddl
    auto cols_after_ddl = std::make_shared<ColumnDefines>();
    *cols_after_ddl     = cols_before_ddl;
    ColumnDefine new_s_col(100, "s", typeFromString("String"));
    cols_after_ddl->emplace_back(new_s_col);
    ColumnDefine new_i_col_with_default(101, "i", typeFromString("Int64"));
    new_i_col_with_default.default_value = Field(Int64(5));
    cols_after_ddl->emplace_back(new_i_col_with_default);

    {
        // Test read with new columns after ddl
        auto stream = std::make_unique<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dmContext().hash_salt,
            dm_file,
            *cols_after_ddl,
            HandleRange::newAll(),
            RSOperatorPtr{},
            IdSetPtr{});

        size_t num_rows_read = 0;
        stream->readPrefix();
        while (Block in = stream->read())
        {
            ASSERT_TRUE(in.has("i8"));
            ASSERT_TRUE(in.has("f64"));
            ASSERT_TRUE(in.has(new_s_col.name));
            ASSERT_TRUE(in.has(new_i_col_with_default.name));
            for (auto itr : in)
            {
                auto c = itr.column;
                if (itr.name == new_s_col.name)
                {
                    EXPECT_EQ(itr.column_id, new_s_col.id);
                    EXPECT_TRUE(itr.type->equals(*new_s_col.type));
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        Field value = (*c)[i];
                        ASSERT_EQ(value.getType(), Field::Types::String);
                        // Empty default value
                        ASSERT_EQ(value, new_s_col.type->getDefault());
                    }
                }
                else if (itr.name == new_i_col_with_default.name)
                {
                    EXPECT_EQ(itr.column_id, new_i_col_with_default.id);
                    EXPECT_TRUE(itr.type->equals(*new_i_col_with_default.type));
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        auto value = c->getInt(i);
                        ASSERT_EQ(value, 5); // Should fill with default value
                    }
                }
                // Check old columns before ddl
                else if (itr.name == "i8")
                {
                    EXPECT_EQ(itr.column_id, 2L);
                    EXPECT_TRUE(itr.type->equals(*typeFromString("Int8")));
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        EXPECT_EQ(c->getInt(i), Int64(i * (-1 * (i % 2))));
                    }
                }
                else if (itr.name == "f64")
                {
                    EXPECT_EQ(itr.column_id, 3L);
                    EXPECT_TRUE(itr.type->equals(*typeFromString("Float64")));
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        Field   value = (*c)[i];
                        Float64 v     = value.get<Float64>();
                        EXPECT_EQ(v, 0.125);
                    }
                }
            }
            num_rows_read += in.rows();
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}
CATCH

TEST_F(DMFile_DDL_Test, UpcastColumnType)
try
{
    // Prepare some data before ddl
    const auto [num_rows_write, cols_before_ddl] = prepareSomeDataToDMFile();

    // Mock that we achange a column type from int8 -> int32, and its name to "i8_new" after ddl
    auto cols_after_ddl        = std::make_shared<ColumnDefines>();
    *cols_after_ddl            = cols_before_ddl;
    const ColumnDefine old_col = cols_before_ddl[3];
    ASSERT_TRUE(old_col.type->equals(*typeFromString("Int8")));
    ColumnDefine new_col = old_col;
    new_col.type         = typeFromString("Int32");
    new_col.name         = "i8_new";
    (*cols_after_ddl)[3] = new_col;

    {
        // Test read with new columns after ddl
        auto stream = std::make_unique<DMFileBlockInputStream>( //
            dbContext(),
            std::numeric_limits<UInt64>::max(),
            false,
            dmContext().hash_salt,
            dm_file,
            *cols_after_ddl,
            HandleRange::newAll(),
            RSOperatorPtr{},
            IdSetPtr{});

        size_t num_rows_read = 0;
        stream->readPrefix();
        while (Block in = stream->read())
        {
            ASSERT_TRUE(in.has(new_col.name));
            ASSERT_TRUE(!in.has("i8"));
            ASSERT_TRUE(in.has("f64"));
            for (auto itr : in)
            {
                auto c = itr.column;
                if (itr.name == new_col.name)
                {
                    EXPECT_EQ(itr.column_id, new_col.id);
                    EXPECT_TRUE(itr.type->equals(*new_col.type));
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        auto value = c->getInt(Int64(i));
                        ASSERT_EQ(value, (Int64)(i * (-1 * (i % 2))));
                    }
                }
                // Check old columns before ddl
                else if (itr.name == "f64")
                {
                    EXPECT_EQ(itr.column_id, 3L);
                    EXPECT_TRUE(itr.type->equals(*typeFromString("Float64")));
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        Field   value = (*c)[i];
                        Float64 v     = value.get<Float64>();
                        EXPECT_EQ(v, 0.125);
                    }
                }
            }
            num_rows_read += in.rows();
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}
CATCH

} // namespace tests
} // namespace DM
} // namespace DB
