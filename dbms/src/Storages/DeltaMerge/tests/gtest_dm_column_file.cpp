// Copyright 2023 PingCAP, Inc.
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

#include <Common/Exception.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <IO/Encryption/MockKeyManager.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileBig.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDeleteRange.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/File/DMFileWriter.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/DeltaMerge/WriteBatchesImpl.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/KVStore/Types.h>
#include <Storages/PathPool.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <ext/scope_guard.h>
#include <vector>

namespace DB::DM::tests
{

class ColumnFileTest
    : public DB::base::TiFlashStorageTestBasic
    , public testing::WithParamInterface<std::tuple<bool, bool>>
{
public:
    ColumnFileTest() = default;

    static void SetUpTestCase() {}

    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        bool is_encrypted = std::get<0>(GetParam());
        bool is_keyspace_encrypted = std::get<1>(GetParam());
        KeyManagerPtr key_manager = std::make_shared<MockKeyManager>(is_encrypted);
        auto file_provider = std::make_shared<FileProvider>(key_manager, is_encrypted, is_keyspace_encrypted);
        db_context->setFileProvider(file_provider);
        KeyspaceID keyspace_id = 0x12345678;

        parent_path = TiFlashStorageTestBasic::getTemporaryPath();
        path_pool
            = std::make_shared<StoragePathPool>(db_context->getPathPool().withTable("test", "DMFile_Test", false));
        storage_pool = std::make_shared<StoragePool>(*db_context, keyspace_id, /*ns_id*/ 100, *path_pool, "test.t1");
        column_cache = std::make_shared<ColumnCache>();
        dm_context = DMContext::createUnique(
            *db_context,
            path_pool,
            storage_pool,
            /*min_version_*/ 0,
            keyspace_id,
            /*physical_table_id*/ 100,
            /*pk_col_id*/ 0,
            false,
            1,
            db_context->getSettingsRef());
    }

    DMContext & dmContext() { return *dm_context; }

    Context & dbContext() { return *db_context; }

private:
    std::unique_ptr<DMContext> dm_context;
    /// all these var live as ref in dm_context
    std::shared_ptr<StoragePathPool> path_pool;
    std::shared_ptr<StoragePool> storage_pool;
    DeltaMergeStore::Settings settings;

protected:
    String parent_path;
    ColumnCachePtr column_cache;
};

TEST_P(ColumnFileTest, ColumnFileBigRead)
try
{
    auto table_columns = DMTestEnv::getDefaultColumns();
    auto dm_file = DMFile::create(1, parent_path, std::make_optional<DMChecksumConfig>());
    const size_t num_rows_write_per_batch = 8192;
    const size_t batch_num = 3;
    const UInt64 tso_value = 100;
    {
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *table_columns);
        stream->writePrefix();
        for (size_t i = 0; i < batch_num; i += 1)
        {
            Block block = DMTestEnv::prepareSimpleWriteBlock(
                num_rows_write_per_batch * i,
                num_rows_write_per_batch * (i + 1),
                false,
                100);
            stream->write(block, {});
        }
        stream->writeSuffix();
    }

    // test read
    ColumnFileBig column_file_big(dmContext(), dm_file, RowKeyRange::newAll(false, 1));
    ColumnDefinesPtr column_defines = std::make_shared<ColumnDefines>();
    column_defines->emplace_back(getExtraHandleColumnDefine(/*is_common_handle=*/false));
    column_defines->emplace_back(getVersionColumnDefine());
    auto column_file_big_reader
        = column_file_big.getReader(dmContext(), /*storage_snap*/ nullptr, column_defines, ReadTag::Internal);
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
        auto column_file_big_reader2 = column_file_big_reader->createNewReader(column_defines_pk_and_del, ReadTag::Internal);
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
        auto column_file_big_reader3 = column_file_big_reader->createNewReader(table_columns, ReadTag::Internal);
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

TEST_P(ColumnFileTest, SerializeColumnFilePersisted)
try
{
    WriteBatches wbs(*dmContext().storage_pool, dmContext().getWriteLimiter());
    MemoryWriteBuffer buff;
    {
        ColumnFilePersisteds column_file_persisteds;
        size_t rows = 100; // arbitrary value
        auto block = DMTestEnv::prepareSimpleWriteBlock(0, rows, false);
        column_file_persisteds.push_back(ColumnFileTiny::writeColumnFile(dmContext(), block, 0, rows, wbs));
        column_file_persisteds.emplace_back(std::make_shared<ColumnFileDeleteRange>(RowKeyRange::newAll(false, 1)));
        column_file_persisteds.push_back(ColumnFileTiny::writeColumnFile(dmContext(), block, 0, rows, wbs));
        column_file_persisteds.emplace_back(std::make_shared<ColumnFileDeleteRange>(RowKeyRange::newAll(false, 1)));
        column_file_persisteds.push_back(ColumnFileTiny::writeColumnFile(dmContext(), block, 0, rows, wbs));
        serializeSavedColumnFilesInV3Format(buff, column_file_persisteds);
    }

    {
        auto read_buff = buff.tryGetReadBuffer();
        auto column_file_persisteds
            = deserializeSavedColumnFilesInV3Format(dmContext(), RowKeyRange::newAll(false, 1), *read_buff);
        ASSERT_EQ(column_file_persisteds.size(), 5);
        ASSERT_EQ(
            column_file_persisteds[0]->tryToTinyFile()->getSchema(),
            column_file_persisteds[2]->tryToTinyFile()->getSchema());
        ASSERT_EQ(
            column_file_persisteds[2]->tryToTinyFile()->getSchema(),
            column_file_persisteds[4]->tryToTinyFile()->getSchema());
    }
}
CATCH

TEST_P(ColumnFileTest, SerializeEmptyBlock)
try
{
    size_t num_rows_write = 0;
    Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
    WriteBatches wbs(*dmContext().storage_pool);
    EXPECT_THROW(ColumnFileTiny::writeColumnFile(dmContext(), block, 0, num_rows_write, wbs), DB::Exception);
}
CATCH

TEST_P(ColumnFileTest, ReadColumns)
try
{
    size_t num_rows_write = 10;
    Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
    ColumnFileTinyPtr cf;
    {
        WriteBatches wbs(*dmContext().storage_pool);
        cf = ColumnFileTiny::writeColumnFile(dmContext(), block, 0, num_rows_write, wbs);
        wbs.writeAll();
    }
    auto storage_snap = std::make_shared<StorageSnapshot>(*dmContext().storage_pool, nullptr, "", true);
    auto data_from_storage_snap = ColumnFileDataProviderLocalStoragePool::create(storage_snap);

    {
        // Read columns exactly the same as we have written
        auto columns_to_read = std::make_shared<ColumnDefines>(getColumnDefinesFromBlock(block));
        auto reader = cf->getReader(dmContext(), data_from_storage_snap, columns_to_read, ReadTag::Internal);
        auto block_read = reader->readNextBlock();
        ASSERT_BLOCK_EQ(block_read, block);
    }

    {
        // Only read with a column that is not exist in ColumnFileTiny
        ColumnID added_colid = 100;
        String added_colname = "added_col";
        auto columns_to_read = std::make_shared<ColumnDefines>(
            ColumnDefines{ColumnDefine(added_colid, added_colname, typeFromString("Int64"))});
        auto reader = cf->getReader(dmContext(), data_from_storage_snap, columns_to_read, ReadTag::Internal);
        auto block_read = reader->readNextBlock();
        ASSERT_COLUMNS_EQ_R(
            ColumnsWithTypeAndName({
                createColumn<Int64>(std::vector<Int64>(num_rows_write, 0)),
            }),
            block_read.getColumnsWithTypeAndName());
    }
}
CATCH

INSTANTIATE_TEST_CASE_P(
    ColumnFile,
    ColumnFileTest,
    testing::Values(std::make_tuple(false, false), std::make_tuple(true, false), std::make_tuple(true, true)));

} // namespace DB::DM::tests
