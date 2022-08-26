// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileBig.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/File/DMFileWriter.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/FunctionTestUtils.h>

#include <vector>

namespace DB
{
namespace DM
{
namespace tests
{
class ColumnFileTest
    : public DB::base::TiFlashStorageTestBasic
{
public:
    ColumnFileTest() = default;

    static void SetUpTestCase() {}

    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();

        parent_path = TiFlashStorageTestBasic::getTemporaryPath();
        path_pool = std::make_unique<StoragePathPool>(db_context->getPathPool().withTable("test", "DMFile_Test", false));
        storage_pool = std::make_unique<StoragePool>(*db_context, /*ns_id*/ 100, *path_pool, "test.t1");
        column_cache = std::make_shared<ColumnCache>();
        dm_context = std::make_unique<DMContext>( //
            *db_context,
            *path_pool,
            *storage_pool,
            /*min_version_*/ 0,
            settings.not_compress_columns,
            false,
            1,
            db_context->getSettingsRef());
    }

    DMContext & dmContext() { return *dm_context; }

    Context & dbContext() { return *db_context; }

private:
    std::unique_ptr<DMContext> dm_context;
    /// all these var live as ref in dm_context
    std::unique_ptr<StoragePathPool> path_pool;
    std::unique_ptr<StoragePool> storage_pool;
    DeltaMergeStore::Settings settings;

protected:
    String parent_path;
    ColumnCachePtr column_cache;
};

TEST_F(ColumnFileTest, ColumnFileBigRead)
try
{
    auto table_columns = DMTestEnv::getDefaultColumns();
    auto dm_file = DMFile::create(1, parent_path, false, std::make_optional<DMChecksumConfig>());
    const size_t num_rows_write_per_batch = 8192;
    const size_t batch_num = 3;
    const UInt64 tso_value = 100;
    {
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *table_columns);
        stream->writePrefix();
        for (size_t i = 0; i < batch_num; i += 1)
        {
            Block block = DMTestEnv::prepareSimpleWriteBlock(num_rows_write_per_batch * i, num_rows_write_per_batch * (i + 1), false, 100);
            stream->write(block, {});
        }
        stream->writeSuffix();
    }

    // test read
    ColumnFileBig column_file_big(dmContext(), dm_file, RowKeyRange::newAll(false, 1));
    ColumnDefinesPtr column_defines = std::make_shared<ColumnDefines>();
    column_defines->emplace_back(getExtraHandleColumnDefine(/*is_common_handle=*/false));
    column_defines->emplace_back(getVersionColumnDefine());
    auto column_file_big_reader = column_file_big.getReader(dmContext(), /*storage_snap*/ nullptr, column_defines);
    {
        size_t num_rows_read = 0;
        while (Block in = column_file_big_reader->readNextBlock())
        {
            ASSERT_EQ(in.columns(), column_defines->size());
            ASSERT_TRUE(in.has(EXTRA_HANDLE_COLUMN_NAME));
            ASSERT_TRUE(in.has(VERSION_COLUMN_NAME));
            auto & pk_column = in.getByName(EXTRA_HANDLE_COLUMN_NAME).column;
            for (size_t i = 0; i < pk_column->size(); i++)
            {
                ASSERT_EQ(pk_column->getInt(i), num_rows_read + i);
            }
            auto & version_column = in.getByName(VERSION_COLUMN_NAME).column;
            for (size_t i = 0; i < version_column->size(); i++)
            {
                ASSERT_EQ(version_column->getInt(i), tso_value);
            }
            num_rows_read += in.rows();
        }
        ASSERT_EQ(num_rows_read, num_rows_write_per_batch * batch_num);
    }

    {
        ColumnDefinesPtr column_defines_pk_and_del = std::make_shared<ColumnDefines>();
        column_defines_pk_and_del->emplace_back(getExtraHandleColumnDefine(/*is_common_handle=*/false));
        column_defines_pk_and_del->emplace_back(getTagColumnDefine());
        auto column_file_big_reader2 = column_file_big_reader->createNewReader(column_defines_pk_and_del);
        size_t num_rows_read = 0;
        while (Block in = column_file_big_reader2->readNextBlock())
        {
            ASSERT_EQ(in.columns(), column_defines_pk_and_del->size());
            ASSERT_TRUE(in.has(EXTRA_HANDLE_COLUMN_NAME));
            ASSERT_TRUE(in.has(TAG_COLUMN_NAME));
            auto & pk_column = in.getByName(EXTRA_HANDLE_COLUMN_NAME).column;
            for (size_t i = 0; i < pk_column->size(); i++)
            {
                ASSERT_EQ(pk_column->getInt(i), num_rows_read + i);
            }
            auto & del_column = in.getByName(TAG_COLUMN_NAME).column;
            for (size_t i = 0; i < del_column->size(); i++)
            {
                ASSERT_EQ(del_column->getInt(i), 0);
            }
            num_rows_read += in.rows();
        }
        ASSERT_EQ(num_rows_read, num_rows_write_per_batch * batch_num);
    }

    {
        auto column_file_big_reader3 = column_file_big_reader->createNewReader(table_columns);
        size_t num_rows_read = 0;
        while (Block in = column_file_big_reader3->readNextBlock())
        {
            ASSERT_EQ(in.columns(), table_columns->size());
            num_rows_read += in.rows();
        }
        ASSERT_EQ(num_rows_read, num_rows_write_per_batch * batch_num);
    }
}
CATCH

} // namespace tests
} // namespace DM
} // namespace DB
