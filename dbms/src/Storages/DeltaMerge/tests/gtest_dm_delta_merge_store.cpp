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
#include <Common/FailPoint.h>
#include <Common/Logger.h>
#include <Common/SyncPoint/SyncPoint.h>
#include <DataStreams/BlocksListBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Poco/File.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/PKSquashingBlockInputStream.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <fmt/format.h>

#include <cstdint>
#include <future>
#include <memory>
#include <vector>

#include "MultiSegmentTestUtil.h"
#include "dm_basic_include.h"

namespace DB
{
namespace FailPoints
{
extern const char pause_before_dt_background_delta_merge[];
extern const char pause_until_dt_background_delta_merge[];
extern const char force_triggle_background_merge_delta[];
extern const char force_triggle_foreground_flush[];
extern const char force_set_segment_ingest_packs_fail[];
extern const char segment_merge_after_ingest_packs[];
extern const char force_set_segment_physical_split[];
} // namespace FailPoints

namespace DM
{
extern DMFilePtr writeIntoNewDMFile(DMContext & dm_context, //
                                    const ColumnDefinesPtr & schema_snap,
                                    const BlockInputStreamPtr & input_stream,
                                    UInt64 file_id,
                                    const String & parent_path,
                                    DMFileBlockOutputStream::Flags flags);
namespace tests
{
// Simple test suit for DeltaMergeStore.
class DeltaMergeStoreTest : public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        store = reload();
    }

    DeltaMergeStorePtr
    reload(const ColumnDefinesPtr & pre_define_columns = {}, bool is_common_handle = false, size_t rowkey_column_size = 1)
    {
        TiFlashStorageTestBasic::reload();
        ColumnDefinesPtr cols;
        if (!pre_define_columns)
            cols = DMTestEnv::getDefaultColumns(is_common_handle ? DMTestEnv::PkType::CommonHandle : DMTestEnv::PkType::HiddenTiDBRowID);
        else
            cols = pre_define_columns;

        ColumnDefine handle_column_define = (*cols)[0];

        DeltaMergeStorePtr s = std::make_shared<DeltaMergeStore>(*db_context,
                                                                 false,
                                                                 "test",
                                                                 "DeltaMergeStoreTest",
                                                                 100,
                                                                 *cols,
                                                                 handle_column_define,
                                                                 is_common_handle,
                                                                 rowkey_column_size,
                                                                 DeltaMergeStore::Settings());
        return s;
    }

protected:
    DeltaMergeStorePtr store;
};

enum TestMode
{
    V1_BlockOnly,
    V2_BlockOnly,
    V2_FileOnly,
    V2_Mix,
};

String testModeToString(const ::testing::TestParamInfo<TestMode> & info)
{
    const auto mode = info.param;
    switch (mode)
    {
    case TestMode::V1_BlockOnly:
        return "V1_BlockOnly";
    case TestMode::V2_BlockOnly:
        return "V2_BlockOnly";
    case TestMode::V2_FileOnly:
        return "V2_FileOnly";
    case TestMode::V2_Mix:
        return "V2_Mix";
    default:
        return "Unknown";
    }
}

// Read write test suit for DeltaMergeStore.
// We will instantiate test cases for different `TestMode`
// to test with different pack types.
class DeltaMergeStoreRWTest
    : public DB::base::TiFlashStorageTestBasic
    , public testing::WithParamInterface<TestMode>
{
public:
    DeltaMergeStoreRWTest()
    {
        mode = GetParam();

        switch (mode)
        {
        case TestMode::V1_BlockOnly:
            setStorageFormat(1);
            break;
        case TestMode::V2_BlockOnly:
        case TestMode::V2_FileOnly:
        case TestMode::V2_Mix:
            setStorageFormat(2);
            break;
        }
    }

    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        store = reload();
    }

    DeltaMergeStorePtr
    reload(const ColumnDefinesPtr & pre_define_columns = {}, bool is_common_handle = false, size_t rowkey_column_size = 1)
    {
        TiFlashStorageTestBasic::reload();
        ColumnDefinesPtr cols;
        if (!pre_define_columns)
            cols = DMTestEnv::getDefaultColumns(is_common_handle ? DMTestEnv::PkType::CommonHandle : DMTestEnv::PkType::HiddenTiDBRowID);
        else
            cols = pre_define_columns;

        ColumnDefine handle_column_define = (*cols)[0];

        DeltaMergeStorePtr s = std::make_shared<DeltaMergeStore>(*db_context,
                                                                 false,
                                                                 "test",
                                                                 "DeltaMergeStoreRWTest",
                                                                 101,
                                                                 *cols,
                                                                 handle_column_define,
                                                                 is_common_handle,
                                                                 rowkey_column_size,
                                                                 DeltaMergeStore::Settings());
        return s;
    }

    std::pair<RowKeyRange, PageIds> genDMFile(DMContext & context, const Block & block)
    {
        auto input_stream = std::make_shared<OneBlockInputStream>(block);
        auto [store_path, file_id] = store->preAllocateIngestFile();

        DMFileBlockOutputStream::Flags flags;
        flags.setSingleFile(DMTestEnv::getPseudoRandomNumber() % 2);

        auto dmfile = writeIntoNewDMFile(
            context,
            std::make_shared<ColumnDefines>(store->getTableColumns()),
            input_stream,
            file_id,
            store_path,
            flags);


        store->preIngestFile(store_path, file_id, dmfile->getBytesOnDisk());

        auto & pk_column = block.getByPosition(0).column;
        auto min_pk = pk_column->getInt(0);
        auto max_pk = pk_column->getInt(block.rows() - 1);
        HandleRange range(min_pk, max_pk + 1);

        return {RowKeyRange::fromHandleRange(range), {file_id}};
    }

protected:
    TestMode mode;
    DeltaMergeStorePtr store;

    constexpr static const char * TRACING_NAME = "DeltaMergeStoreRWTest";
};

TEST_F(DeltaMergeStoreTest, Create)
try
{
    // create table
    ASSERT_NE(store, nullptr);

    {
        // check handle column of store
        const auto & h = store->getHandle();
        ASSERT_EQ(h.name, EXTRA_HANDLE_COLUMN_NAME);
        ASSERT_EQ(h.id, EXTRA_HANDLE_COLUMN_ID);
        ASSERT_TRUE(h.type->equals(*EXTRA_HANDLE_COLUMN_INT_TYPE));
    }
    {
        // check column structure of store
        const auto & cols = store->getTableColumns();
        // version & tag column added
        ASSERT_EQ(cols.size(), 3UL);
    }
}
CATCH

TEST_F(DeltaMergeStoreTest, OpenWithExtraColumns)
try
{
    const ColumnDefine col_str_define(2, "col2", std::make_shared<DataTypeString>());
    const ColumnDefine col_i8_define(3, "i8", std::make_shared<DataTypeInt8>());
    {
        auto table_column_defines = DMTestEnv::getDefaultColumns();
        table_column_defines->emplace_back(col_str_define);
        table_column_defines->emplace_back(col_i8_define);
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
CATCH

TEST_F(DeltaMergeStoreTest, AddExtraColumn)
try
{
    auto * log = &Poco::Logger::get(GET_GTEST_FULL_NAME);
    for (const auto & pk_type : {
             DMTestEnv::PkType::HiddenTiDBRowID,
             DMTestEnv::PkType::CommonHandle,
             DMTestEnv::PkType::PkIsHandleInt64,
             DMTestEnv::PkType::PkIsHandleInt32,
         })
    {
        LOG_FMT_INFO(log, "Test case for {} begin.", DMTestEnv::PkTypeToString(pk_type));

        auto cols = DMTestEnv::getDefaultColumns(pk_type);
        store = reload(cols, (pk_type == DMTestEnv::PkType::CommonHandle), 1);

        ASSERT_EQ(store->isCommonHandle(), pk_type == DMTestEnv::PkType::CommonHandle) << DMTestEnv::PkTypeToString(pk_type);
        ASSERT_EQ(DeltaMergeStore::pkIsHandle(store->getHandle()),
                  (pk_type == DMTestEnv::PkType::PkIsHandleInt64 || pk_type == DMTestEnv::PkType::PkIsHandleInt32))
            << DMTestEnv::PkTypeToString(pk_type);

        const size_t nrows = 20;
        const auto & handle = store->getHandle();
        auto block1 = DMTestEnv::prepareSimpleWriteBlock(0,
                                                         nrows,
                                                         false,
                                                         /*tso*/ 2,
                                                         /*pk_name*/ handle.name,
                                                         handle.id,
                                                         handle.type,
                                                         store->isCommonHandle(),
                                                         store->getRowKeyColumnSize());
        block1 = DeltaMergeStore::addExtraColumnIfNeed(*db_context, store->getHandle(), std::move(block1));
        ASSERT_EQ(block1.rows(), nrows);
        ASSERT_TRUE(block1.has(EXTRA_HANDLE_COLUMN_NAME));
        for (const auto & c : block1)
            ASSERT_EQ(c.column->size(), nrows);

        // Make a block that is overlapped with `block1` and it should be squashed by `PKSquashingBlockInputStream`
        size_t nrows_2 = 2;
        auto block2 = DMTestEnv::prepareSimpleWriteBlock(nrows - 1,
                                                         nrows - 1 + nrows_2,
                                                         false,
                                                         /*tso*/ 4,
                                                         /*pk_name*/ handle.name,
                                                         handle.id,
                                                         handle.type,
                                                         store->isCommonHandle(),
                                                         store->getRowKeyColumnSize());
        block2 = DeltaMergeStore::addExtraColumnIfNeed(*db_context, store->getHandle(), std::move(block2));
        ASSERT_EQ(block2.rows(), nrows_2);
        ASSERT_TRUE(block2.has(EXTRA_HANDLE_COLUMN_NAME));
        for (const auto & c : block2)
            ASSERT_EQ(c.column->size(), nrows_2);


        BlockInputStreamPtr stream = std::make_shared<BlocksListBlockInputStream>(BlocksList{block1, block2});
        stream = std::make_shared<PKSquashingBlockInputStream<false>>(stream, EXTRA_HANDLE_COLUMN_ID, store->isCommonHandle());

        size_t num_rows_read = 0;
        stream->readPrefix();
        while (Block block = stream->read())
        {
            num_rows_read += block.rows();
            for (auto && iter : block)
            {
                auto c = iter.column;
                ASSERT_EQ(c->size(), block.rows())
                    << "unexpected num of rows for column [name=" << iter.name << "] " << DMTestEnv::PkTypeToString(pk_type);
            }
        }
        stream->readSuffix();
        ASSERT_EQ(num_rows_read, nrows + nrows_2);

        LOG_FMT_INFO(log, "Test case for {} done.", DMTestEnv::PkTypeToString(pk_type));
    }
}
CATCH

TEST_P(DeltaMergeStoreRWTest, SimpleWriteRead)
try
{
    const ColumnDefine col_str_define(2, "col2", std::make_shared<DataTypeString>());
    const ColumnDefine col_i8_define(3, "i8", std::make_shared<DataTypeInt8>());
    {
        auto table_column_defines = DMTestEnv::getDefaultColumns();
        table_column_defines->emplace_back(col_str_define);
        table_column_defines->emplace_back(col_i8_define);

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
            block.insert(DB::tests::createColumn<String>(
                createNumberStrings(0, num_rows_write),
                col_str_define.name,
                col_str_define.id));
            // Add a column of i8:Int8 for test
            block.insert(DB::tests::createColumn<Int8>(
                createSignedNumbers(0, num_rows_write),
                col_i8_define.name,
                col_i8_define.id));
        }

        switch (mode)
        {
        case TestMode::V1_BlockOnly:
        case TestMode::V2_BlockOnly:
            store->write(*db_context, db_context->getSettingsRef(), block);
            break;
        default:
        {
            auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
            auto [range, file_ids] = genDMFile(*dm_context, block);
            store->ingestFiles(dm_context, range, file_ids, false);
            break;
        }
        }
    }

    {
        // TODO read data from more than one block
        // TODO read data from mutli streams
        // TODO read partial columns from store
        // TODO read data of max_version

        // read all columns from store
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(*db_context,
                                             db_context->getSettingsRef(),
                                             columns,
                                             {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                             /* num_streams= */ 1,
                                             /* max_version= */ std::numeric_limits<UInt64>::max(),
                                             EMPTY_FILTER,
                                             TRACING_NAME,
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
                    if (iter.name == DMTestEnv::pk_name)
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
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->readRaw(*db_context, db_context->getSettingsRef(), columns, 1)[0];

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
                    if (iter.name == DMTestEnv::pk_name)
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
CATCH

TEST_P(DeltaMergeStoreRWTest, DeleteRead)
try
{
    const size_t num_rows_write = 128;
    {
        // Create a block with sequential Int64 handle in range [0, 128)
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, 128, false);

        switch (mode)
        {
        case TestMode::V1_BlockOnly:
        case TestMode::V2_BlockOnly:
            store->write(*db_context, db_context->getSettingsRef(), block);
            break;
        default:
        {
            auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
            auto [range, file_ids] = genDMFile(*dm_context, block);
            store->ingestFiles(dm_context, range, file_ids, false);
            break;
        }
        }
    }
    // Test Reading first
    {
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(*db_context,
                                             db_context->getSettingsRef(),
                                             columns,
                                             {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                             /* num_streams= */ 1,
                                             /* max_version= */ std::numeric_limits<UInt64>::max(),
                                             EMPTY_FILTER,
                                             TRACING_NAME,
                                             /* expected_block_size= */ 1024)[0];
        size_t num_rows_read = 0;
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            for (auto && iter : block)
            {
                auto c = iter.column;
                for (Int64 i = 0; i < Int64(c->size()); ++i)
                {
                    if (iter.name == DMTestEnv::pk_name)
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
        store->deleteRange(*db_context, db_context->getSettingsRef(), RowKeyRange::fromHandleRange(range));
    }
    // Read after deletion
    {
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(*db_context,
                                             db_context->getSettingsRef(),
                                             columns,
                                             {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                             /* num_streams= */ 1,
                                             /* max_version= */ std::numeric_limits<UInt64>::max(),
                                             EMPTY_FILTER,
                                             TRACING_NAME,
                                             /* expected_block_size= */ 1024)[0];
        size_t num_rows_read = 0;
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            for (auto && iter : block)
            {
                auto c = iter.column;
                for (Int64 i = 0; i < Int64(c->size()); ++i)
                {
                    if (iter.name == DMTestEnv::pk_name)
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
CATCH

TEST_P(DeltaMergeStoreRWTest, WriteMultipleBlock)
try
{
    const size_t num_write_rows = 32;

    // Test write multi blocks without overlap
    {
        Block block1 = DMTestEnv::prepareSimpleWriteBlock(0, 1 * num_write_rows, false);
        Block block2 = DMTestEnv::prepareSimpleWriteBlock(1 * num_write_rows, 2 * num_write_rows, false);
        Block block3 = DMTestEnv::prepareSimpleWriteBlock(2 * num_write_rows, 3 * num_write_rows, false);
        switch (mode)
        {
        case TestMode::V1_BlockOnly:
        case TestMode::V2_BlockOnly:
        {
            store->write(*db_context, db_context->getSettingsRef(), block1);
            store->write(*db_context, db_context->getSettingsRef(), block2);
            store->write(*db_context, db_context->getSettingsRef(), block3);
            break;
        }
        case TestMode::V2_FileOnly:
        {
            auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
            auto [range1, file_ids1] = genDMFile(*dm_context, block1);
            auto [range2, file_ids2] = genDMFile(*dm_context, block2);
            auto [range3, file_ids3] = genDMFile(*dm_context, block3);
            auto range = range1.merge(range2).merge(range3);
            auto file_ids = file_ids1;
            file_ids.insert(file_ids.cend(), file_ids2.begin(), file_ids2.end());
            file_ids.insert(file_ids.cend(), file_ids3.begin(), file_ids3.end());
            store->ingestFiles(dm_context, range, file_ids, false);
            break;
        }
        case TestMode::V2_Mix:
        {
            auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
            auto [range1, file_ids1] = genDMFile(*dm_context, block1);
            auto [range3, file_ids3] = genDMFile(*dm_context, block3);
            auto range = range1.merge(range3);
            auto file_ids = file_ids1;
            file_ids.insert(file_ids.cend(), file_ids3.begin(), file_ids3.end());
            store->ingestFiles(dm_context, range, file_ids, false);

            store->write(*db_context, db_context->getSettingsRef(), block2);
            break;
        }
        }

        store->flushCache(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));
    }

    {
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(*db_context,
                                             db_context->getSettingsRef(),
                                             columns,
                                             {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                             /* num_streams= */ 1,
                                             /* max_version= */ std::numeric_limits<UInt64>::max(),
                                             EMPTY_FILTER,
                                             TRACING_NAME,
                                             /* expected_block_size= */ 1024)[0];
        size_t num_rows_read = 0;
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            for (auto && iter : block)
            {
                auto c = iter.column;
                for (Int64 i = 0; i < Int64(c->size()); ++i)
                {
                    if (iter.name == DMTestEnv::pk_name)
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
        UInt64 tso1 = 1;
        UInt64 tso2 = 100;
        Block block1 = DMTestEnv::prepareSimpleWriteBlock(0, 1 * num_write_rows, false, tso1);
        Block block2 = DMTestEnv::prepareSimpleWriteBlock(1 * num_write_rows, 2 * num_write_rows, false, tso1);
        Block block3 = DMTestEnv::prepareSimpleWriteBlock(num_write_rows / 2, num_write_rows / 2 + num_write_rows, false, tso2);

        switch (mode)
        {
        case TestMode::V1_BlockOnly:
        case TestMode::V2_BlockOnly:
        {
            store->write(*db_context, db_context->getSettingsRef(), block1);
            store->write(*db_context, db_context->getSettingsRef(), block2);
            store->write(*db_context, db_context->getSettingsRef(), block3);
            break;
        }
        case TestMode::V2_FileOnly:
        {
            auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
            auto [range1, file_ids1] = genDMFile(*dm_context, block1);
            auto [range2, file_ids2] = genDMFile(*dm_context, block2);
            auto [range3, file_ids3] = genDMFile(*dm_context, block3);
            auto range = range1.merge(range2).merge(range3);
            auto file_ids = file_ids1;
            file_ids.insert(file_ids.cend(), file_ids2.begin(), file_ids2.end());
            file_ids.insert(file_ids.cend(), file_ids3.begin(), file_ids3.end());
            store->ingestFiles(dm_context, range, file_ids, false);
            break;
        }
        case TestMode::V2_Mix:
        {
            store->write(*db_context, db_context->getSettingsRef(), block2);

            auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
            auto [range1, file_ids1] = genDMFile(*dm_context, block1);
            auto [range3, file_ids3] = genDMFile(*dm_context, block3);
            auto range = range1.merge(range3);
            auto file_ids = file_ids1;
            file_ids.insert(file_ids.cend(), file_ids3.begin(), file_ids3.end());
            store->ingestFiles(dm_context, range, file_ids, false);
            break;
        }
        }

        store->flushCache(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));
    }

    store->compact(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));

    // Read without version
    {
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(*db_context,
                                             db_context->getSettingsRef(),
                                             columns,
                                             {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                             /* num_streams= */ 1,
                                             /* max_version= */ std::numeric_limits<UInt64>::max(),
                                             EMPTY_FILTER,
                                             TRACING_NAME,
                                             /* expected_block_size= */ 1024)[0];
        size_t num_rows_read = 0;
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            for (auto && iter : block)
            {
                auto c = iter.column;
                for (Int64 i = 0; i < Int64(c->size()); ++i)
                {
                    if (iter.name == DMTestEnv::pk_name)
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
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(*db_context,
                                             db_context->getSettingsRef(),
                                             columns,
                                             {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                             /* num_streams= */ 1,
                                             /* max_version= */ UInt64(1),
                                             EMPTY_FILTER,
                                             TRACING_NAME,
                                             /* expected_block_size= */ 1024)[0];
        size_t num_rows_read = 0;
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            for (auto && iter : block)
            {
                auto c = iter.column;
                for (Int64 i = 0; i < Int64(c->size()); ++i)
                {
                    if (iter.name == DMTestEnv::pk_name)
                    {
                        ASSERT_EQ(c->getInt(i), i);
                    }
                }
            }
        }

        ASSERT_EQ(num_rows_read, 2 * num_write_rows);
    }
}
CATCH

// DEPRECATED:
//   This test case strongly depends on implementation of `shouldSplit()` and `shouldMerge()`.
//   The machanism of them may be changed one day. So uncomment the test if need.
TEST_P(DeltaMergeStoreRWTest, DISABLED_WriteLargeBlock)
try
{
    DB::Settings settings = db_context->getSettings();
    // Mock dm_segment_rows for test
    // if rows > 8 will split
    // if left->rows < 2 && right->rows + left->rows < 4 will merge
    settings.dt_segment_limit_rows = 4;

    {
        store->check(*db_context);
    }

    {
        // Write 7 rows that would not trigger a split
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, 8, false);
        store->write(*db_context, settings, block);
    }

    {
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(*db_context,
                                             settings,
                                             columns,
                                             {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                             /* num_streams= */ 1,
                                             /* max_version= */ std::numeric_limits<UInt64>::max(),
                                             EMPTY_FILTER,
                                             TRACING_NAME,
                                             /* expected_block_size= */ 1024)[0];
        size_t num_rows_read = 0;
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            for (auto & iter : block)
            {
                auto c = iter.column;
                for (Int64 i = 0; i < Int64(c->size()); i++)
                {
                    if (iter.name == DMTestEnv::pk_name)
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
        store->write(*db_context, settings, block);
    }

    // Now there is 2 segments
    // segment1: 0, 1, 2, 3
    // segment2: 4, 5, 6, 7, 8
    {
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(*db_context,
                                             settings,
                                             columns,
                                             {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                             /* num_streams= */ 1,
                                             /* max_version= */ std::numeric_limits<UInt64>::max(),
                                             EMPTY_FILTER,
                                             TRACING_NAME,
                                             /* expected_block_size= */ 1024)[0];
        size_t num_rows_read = 0;
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
                    if (iter.name == DMTestEnv::pk_name && block_num == 0)
                    {
                        EXPECT_EQ(c->getInt(i), i);
                    }
                    else if (iter.name == DMTestEnv::pk_name && block_num == 1)
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
CATCH

TEST_P(DeltaMergeStoreRWTest, ReadWithSpecifyTso)
try
{
    const UInt64 tso1 = 4;
    const size_t num_rows_tso1 = 128;
    {
        // write to store
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_tso1, false, tso1);
        store->write(*db_context, db_context->getSettingsRef(), block);
    }

    const UInt64 tso2 = 890;
    const size_t num_rows_tso2 = 256;
    {
        // write to store
        Block block = DMTestEnv::prepareSimpleWriteBlock(num_rows_tso1, num_rows_tso1 + num_rows_tso2, false, tso2);
        store->write(*db_context, db_context->getSettingsRef(), block);
    }

    {
        // read all data of max_version
        const auto & columns = store->getTableColumns();
        BlockInputStreams ins = store->read(*db_context,
                                            db_context->getSettingsRef(),
                                            columns,
                                            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                            /* num_streams= */ 1,
                                            /* max_version= */ std::numeric_limits<UInt64>::max(),
                                            EMPTY_FILTER,
                                            TRACING_NAME,
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
        const auto & columns = store->getTableColumns();
        BlockInputStreams ins = store->read(*db_context,
                                            db_context->getSettingsRef(),
                                            columns,
                                            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                            /* num_streams= */ 1,
                                            /* max_version= */ tso2,
                                            EMPTY_FILTER,
                                            TRACING_NAME,
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
        const auto & columns = store->getTableColumns();
        BlockInputStreams ins = store->read(*db_context,
                                            db_context->getSettingsRef(),
                                            columns,
                                            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                            /* num_streams= */ 1,
                                            /* max_version= */ tso1,
                                            EMPTY_FILTER,
                                            TRACING_NAME,
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
        const auto & columns = store->getTableColumns();
        BlockInputStreams ins = store->read(*db_context,
                                            db_context->getSettingsRef(),
                                            columns,
                                            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                            /* num_streams= */ 1,
                                            /* max_version= */ tso1 - 1,
                                            EMPTY_FILTER,
                                            TRACING_NAME,
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
CATCH

TEST_P(DeltaMergeStoreRWTest, Ingest)
try
{
    if (mode == TestMode::V1_BlockOnly)
        return;

    const UInt64 tso1 = 4;
    const size_t num_rows_before_ingest = 128;
    // Write to store [0, 128)
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_before_ingest, false, tso1);
        store->write(*db_context, db_context->getSettingsRef(), block);
    }

    const UInt64 tso2 = 10;
    const UInt64 tso3 = 18;

    {
        // Prepare DTFiles for ingesting
        auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());

        auto [range1, file_ids1] = genDMFile(*dm_context, DMTestEnv::prepareSimpleWriteBlock(32, 48, false, tso2));
        auto [range2, file_ids2] = genDMFile(*dm_context, DMTestEnv::prepareSimpleWriteBlock(80, 256, false, tso3));

        auto file_ids = file_ids1;
        file_ids.insert(file_ids.cend(), file_ids2.begin(), file_ids2.end());
        auto ingest_range = RowKeyRange::fromHandleRange(HandleRange{32, 256});
        // verify that ingest_range must not less than range1.merge(range2)
        ASSERT_ROWKEY_RANGE_EQ(ingest_range, range1.merge(range2).merge(ingest_range));

        store->ingestFiles(dm_context, ingest_range, file_ids, /*clear_data_in_range*/ true);
    }


    // After ingesting, the data in [32, 128) should be overwrite by the data in ingested files.
    {
        // Read all data <= tso1
        // We can only get [0, 32) with tso1
        const auto & columns = store->getTableColumns();
        BlockInputStreams ins = store->read(*db_context,
                                            db_context->getSettingsRef(),
                                            columns,
                                            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                            /* num_streams= */ 1,
                                            /* max_version= */ tso1,
                                            EMPTY_FILTER,
                                            TRACING_NAME,
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1UL);
        BlockInputStreamPtr in = ins[0];

        size_t num_rows_read = 0;
        in->readPrefix();
        Int64 expect_pk = 0;
        UInt64 expect_tso = tso1;
        while (Block block = in->read())
        {
            ASSERT_TRUE(block.has(DMTestEnv::pk_name));
            ASSERT_TRUE(block.has(VERSION_COLUMN_NAME));
            auto pk_c = block.getByName(DMTestEnv::pk_name);
            auto v_c = block.getByName(VERSION_COLUMN_NAME);
            for (size_t i = 0; i < block.rows(); ++i)
            {
                // std::cerr << "pk:" << pk_c.column->getInt(i) << ", ver:" << v_c.column->getInt(i) << std::endl;
                ASSERT_EQ(pk_c.column->getInt(i), expect_pk++);
                ASSERT_EQ(v_c.column->getUInt(i), expect_tso);
            }
            num_rows_read += block.rows();
        }
        in->readSuffix();
        EXPECT_EQ(num_rows_read, 32UL) << "Data [32, 128) before ingest should be erased, should only get [0, 32)";
    }

    {
        // Read all data between [tso, tso2)
        const auto & columns = store->getTableColumns();
        BlockInputStreams ins = store->read(*db_context,
                                            db_context->getSettingsRef(),
                                            columns,
                                            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                            /* num_streams= */ 1,
                                            /* max_version= */ tso2 - 1,
                                            EMPTY_FILTER,
                                            TRACING_NAME,
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1UL);
        BlockInputStreamPtr in = ins[0];

        size_t num_rows_read = 0;
        in->readPrefix();
        Int64 expect_pk = 0;
        UInt64 expect_tso = tso1;
        while (Block block = in->read())
        {
            ASSERT_TRUE(block.has(DMTestEnv::pk_name));
            ASSERT_TRUE(block.has(VERSION_COLUMN_NAME));
            auto pk_c = block.getByName(DMTestEnv::pk_name);
            auto v_c = block.getByName(VERSION_COLUMN_NAME);
            for (size_t i = 0; i < block.rows(); ++i)
            {
                // std::cerr << "pk:" << pk_c.column->getInt(i) << ", ver:" << v_c.column->getInt(i) << std::endl;
                ASSERT_EQ(pk_c.column->getInt(i), expect_pk++);
                ASSERT_EQ(v_c.column->getUInt(i), expect_tso);
            }
            num_rows_read += block.rows();
        }
        in->readSuffix();
        EXPECT_EQ(num_rows_read, 32UL) << "Data [32, 128) after ingest with tso less than: " << tso2
                                       << " are erased, should only get [0, 32)";
    }

    {
        // Read all data between [tso2, tso3)
        const auto & columns = store->getTableColumns();
        BlockInputStreams ins = store->read(*db_context,
                                            db_context->getSettingsRef(),
                                            columns,
                                            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                            /* num_streams= */ 1,
                                            /* max_version= */ tso3 - 1,
                                            EMPTY_FILTER,
                                            TRACING_NAME,
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1UL);
        BlockInputStreamPtr in = ins[0];

        size_t num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
        }
        in->readSuffix();
        EXPECT_EQ(num_rows_read, 32UL + 16) << "The rows number after ingest with tso less than " << tso3 << " is not match";
    }

    {
        // Read all data between [tso2, tso3)
        const auto & columns = store->getTableColumns();
        BlockInputStreams ins = store->read(*db_context,
                                            db_context->getSettingsRef(),
                                            columns,
                                            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                            /* num_streams= */ 1,
                                            /* max_version= */ std::numeric_limits<UInt64>::max(),
                                            EMPTY_FILTER,
                                            TRACING_NAME,
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1UL);
        BlockInputStreamPtr in = ins[0];

        size_t num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
            num_rows_read += block.rows();
        in->readSuffix();
        EXPECT_EQ(num_rows_read, 32UL + (48 - 32) + (256UL - 80)) << "The rows number after ingest is not match";
    }

    {
        // Read with two point get, issue 1616
        auto range0 = RowKeyRange::fromHandleRange(HandleRange(32, 33));
        auto range1 = RowKeyRange::fromHandleRange(HandleRange(40, 41));
        const auto & columns = store->getTableColumns();
        BlockInputStreams ins = store->read(*db_context,
                                            db_context->getSettingsRef(),
                                            columns,
                                            {range0, range1},
                                            /* num_streams= */ 1,
                                            /* max_version= */ std::numeric_limits<UInt64>::max(),
                                            EMPTY_FILTER,
                                            TRACING_NAME,
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1UL);
        BlockInputStreamPtr in = ins[0];

        size_t num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
            num_rows_read += block.rows();
        in->readSuffix();
        EXPECT_EQ(num_rows_read, 2UL) << "The rows number of two point get is not match";
    }
}
CATCH

TEST_P(DeltaMergeStoreRWTest, IngestWithFail)
try
{
    if (mode == TestMode::V1_BlockOnly)
        return;

    const UInt64 tso1 = 4;
    const size_t num_rows_before_ingest = 128;
    // Write to store [0, 128)
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_before_ingest, false, tso1);
        store->write(*db_context, db_context->getSettingsRef(), block);

        auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
        store->flushCache(dm_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));

        SegmentPtr seg;
        std::tie(std::ignore, seg) = *store->segments.begin();
        store->segmentSplit(*dm_context, seg, /*is_foreground*/ true);
    }

    const UInt64 tso2 = 10;

    {
        // Prepare DTFiles for ingesting
        auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
        auto [ingest_range, file_ids] = genDMFile(*dm_context, DMTestEnv::prepareSimpleWriteBlock(32, 128, false, tso2));
        // Enable failpoint for testing
        FailPointHelper::enableFailPoint(FailPoints::force_set_segment_ingest_packs_fail);
        FailPointHelper::enableFailPoint(FailPoints::segment_merge_after_ingest_packs);
        store->ingestFiles(dm_context, ingest_range, file_ids, /*clear_data_in_range*/ true);
    }


    // After ingesting, the data in [32, 128) should be overwrite by the data in ingested files.
    {
        // Read all data <= tso1
        // We can only get [0, 32) with tso1
        const auto & columns = store->getTableColumns();
        BlockInputStreams ins = store->read(*db_context,
                                            db_context->getSettingsRef(),
                                            columns,
                                            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                            /* num_streams= */ 1,
                                            /* max_version= */ tso1,
                                            EMPTY_FILTER,
                                            TRACING_NAME,
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1);
        BlockInputStreamPtr in = ins[0];

        size_t num_rows_read = 0;
        in->readPrefix();
        Int64 expect_pk = 0;
        UInt64 expect_tso = tso1;
        while (Block block = in->read())
        {
            ASSERT_TRUE(block.has(DMTestEnv::pk_name));
            ASSERT_TRUE(block.has(VERSION_COLUMN_NAME));
            auto pk_c = block.getByName(DMTestEnv::pk_name);
            auto v_c = block.getByName(VERSION_COLUMN_NAME);
            for (size_t i = 0; i < block.rows(); ++i)
            {
                // std::cerr << "pk:" << pk_c.column->getInt(i) << ", ver:" << v_c.column->getInt(i) << std::endl;
                ASSERT_EQ(pk_c.column->getInt(i), expect_pk++);
                ASSERT_EQ(v_c.column->getUInt(i), expect_tso);
            }
            num_rows_read += block.rows();
        }
        in->readSuffix();
        EXPECT_EQ(num_rows_read, 32) << "Data [32, 128) before ingest should be erased, should only get [0, 32)";
    }

    {
        // Read all data between [tso, tso2)
        const auto & columns = store->getTableColumns();
        BlockInputStreams ins = store->read(*db_context,
                                            db_context->getSettingsRef(),
                                            columns,
                                            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                            /* num_streams= */ 1,
                                            /* max_version= */ tso2 - 1,
                                            EMPTY_FILTER,
                                            TRACING_NAME,
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1);
        BlockInputStreamPtr in = ins[0];

        size_t num_rows_read = 0;
        in->readPrefix();
        Int64 expect_pk = 0;
        UInt64 expect_tso = tso1;
        while (Block block = in->read())
        {
            ASSERT_TRUE(block.has(DMTestEnv::pk_name));
            ASSERT_TRUE(block.has(VERSION_COLUMN_NAME));
            auto pk_c = block.getByName(DMTestEnv::pk_name);
            auto v_c = block.getByName(VERSION_COLUMN_NAME);
            for (size_t i = 0; i < block.rows(); ++i)
            {
                // std::cerr << "pk:" << pk_c.column->getInt(i) << ", ver:" << v_c.column->getInt(i) << std::endl;
                ASSERT_EQ(pk_c.column->getInt(i), expect_pk++);
                ASSERT_EQ(v_c.column->getUInt(i), expect_tso);
            }
            num_rows_read += block.rows();
        }
        in->readSuffix();
        EXPECT_EQ(num_rows_read, 32) << "Data [32, 128) after ingest with tso less than: " << tso2
                                     << " are erased, should only get [0, 32)";
    }

    {
        // Read all data between [tso2, tso3)
        const auto & columns = store->getTableColumns();
        BlockInputStreams ins = store->read(*db_context,
                                            db_context->getSettingsRef(),
                                            columns,
                                            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                            /* num_streams= */ 1,
                                            /* max_version= */ std::numeric_limits<UInt64>::max(),
                                            EMPTY_FILTER,
                                            TRACING_NAME,
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1);
        BlockInputStreamPtr in = ins[0];

        size_t num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
            num_rows_read += block.rows();
        in->readSuffix();
        EXPECT_EQ(num_rows_read, 32 + 128 - 32) << "The rows number after ingest is not match";
    }
}
CATCH

TEST_P(DeltaMergeStoreRWTest, IngestEmptyFileLists)
try
{
    if (mode == TestMode::V1_BlockOnly)
        return;

    /// If users create an empty table with TiFlash replica, we will apply Region
    /// snapshot without any rows, which make it ingest with an empty DTFile list.
    /// Test whether we can clean the original data if `clear_data_in_range` is true.

    const UInt64 tso1 = 4;
    const size_t num_rows_before_ingest = 128;
    // Write to store [0, 128)
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_before_ingest, false, tso1);
        store->write(*db_context, db_context->getSettingsRef(), block);
    }

    // Test that if we ingest a empty file list, the data in range will be removed.
    // The ingest range is [32, 256)
    {
        auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());

        PageIds file_ids;
        auto ingest_range = RowKeyRange::fromHandleRange(HandleRange{32, 256});
        store->ingestFiles(dm_context, ingest_range, file_ids, /*clear_data_in_range*/ true);
    }


    // After ingesting, the data in [32, 128) should be overwrite by the data in ingested files.
    {
        // Read all data <= tso1
        // We can only get [0, 32) with tso1
        const auto & columns = store->getTableColumns();
        BlockInputStreams ins = store->read(*db_context,
                                            db_context->getSettingsRef(),
                                            columns,
                                            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                            /* num_streams= */ 1,
                                            /* max_version= */ tso1,
                                            EMPTY_FILTER,
                                            TRACING_NAME,
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1);
        BlockInputStreamPtr in = ins[0];

        size_t num_rows_read = 0;
        in->readPrefix();
        Int64 expect_pk = 0;
        UInt64 expect_tso = tso1;
        while (Block block = in->read())
        {
            ASSERT_TRUE(block.has(DMTestEnv::pk_name));
            ASSERT_TRUE(block.has(VERSION_COLUMN_NAME));
            auto pk_c = block.getByName(DMTestEnv::pk_name);
            auto v_c = block.getByName(VERSION_COLUMN_NAME);
            for (size_t i = 0; i < block.rows(); ++i)
            {
                // std::cerr << "pk:" << pk_c.column->getInt(i) << ", ver:" << v_c.column->getInt(i) << std::endl;
                ASSERT_EQ(pk_c.column->getInt(i), expect_pk++);
                ASSERT_EQ(v_c.column->getUInt(i), expect_tso);
            }
            num_rows_read += block.rows();
        }
        in->readSuffix();
        EXPECT_EQ(num_rows_read, 32) << "Data [32, 128) before ingest should be erased, should only get [0, 32)";
    }

    {
        // Read all data
        const auto & columns = store->getTableColumns();
        BlockInputStreams ins = store->read(*db_context,
                                            db_context->getSettingsRef(),
                                            columns,
                                            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                            /* num_streams= */ 1,
                                            /* max_version= */ std::numeric_limits<UInt64>::max(),
                                            EMPTY_FILTER,
                                            TRACING_NAME,
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1);
        BlockInputStreamPtr in = ins[0];

        size_t num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
            num_rows_read += block.rows();
        in->readSuffix();
        EXPECT_EQ(num_rows_read, 32) << "The rows number after ingest is not match";
    }
}
CATCH

TEST_P(DeltaMergeStoreRWTest, Split)
try
{
    // set some params to smaller threshold so that we can trigger split more frequently
    auto settings = db_context->getSettings();
    settings.dt_segment_limit_rows = 11;
    settings.dt_segment_delta_limit_rows = 7;
    settings.dt_segment_delta_cache_limit_rows = 4;
    settings.dt_segment_stable_pack_rows = 10;

    size_t num_rows_write_in_total = 0;

    const size_t num_rows_per_write = 5;
    while (true)
    {
        {
            // write to store
            Block block = DMTestEnv::prepareSimpleWriteBlock(
                num_rows_write_in_total + 1,
                num_rows_write_in_total + 1 + num_rows_per_write,
                false);

            auto write_as_file = [&]() {
                auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
                auto [range, file_ids] = genDMFile(*dm_context, block);
                store->ingestFiles(dm_context, range, file_ids, false);
            };

            switch (mode)
            {
            case TestMode::V1_BlockOnly:
            case TestMode::V2_BlockOnly:
                store->write(*db_context, settings, block);
                break;
            case TestMode::V2_FileOnly:
                write_as_file();
                break;
            case TestMode::V2_Mix:
            {
                if ((std::rand() % 2) == 0)
                {
                    store->write(*db_context, settings, block);
                }
                else
                {
                    write_as_file();
                }
                break;
            }
            }

            store->flushCache(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));
            num_rows_write_in_total += num_rows_per_write;
        }

        {
            // Let's reload the store to check the persistence system.
            // Note: store must be released before load another, because some background task could be still running.
            store.reset();
            store = reload();

            // read all columns from store
            const auto & columns = store->getTableColumns();
            BlockInputStreams ins = store->read(*db_context,
                                                db_context->getSettingsRef(),
                                                //                                                settings,
                                                columns,
                                                {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                                /* num_streams= */ 1,
                                                /* max_version= */ std::numeric_limits<UInt64>::max(),
                                                EMPTY_FILTER,
                                                TRACING_NAME,
                                                /* expected_block_size= */ 1024);
            ASSERT_EQ(ins.size(), 1UL);
            BlockInputStreamPtr in = ins[0];

            LOG_FMT_TRACE(&Poco::Logger::get(GET_GTEST_FULL_NAME), "start to check data of [1,{}]", num_rows_write_in_total);

            size_t num_rows_read = 0;
            in->readPrefix();
            Int64 expected_row_pk = 1;
            while (Block block = in->read())
            {
                num_rows_read += block.rows();
                for (auto && iter : block)
                {
                    auto c = iter.column;
                    if (iter.name == DMTestEnv::pk_name)
                    {
                        for (size_t i = 0; i < c->size(); ++i)
                        {
                            auto expected = expected_row_pk++;
                            auto value = c->getInt(i);
                            if (value != expected)
                            {
                                // Convenient for debug.
                                EXPECT_EQ(expected, value);
                            }
                        }
                    }
                }
            }
            in->readSuffix();
            ASSERT_EQ(num_rows_read, num_rows_write_in_total);

            LOG_FMT_TRACE(&Poco::Logger::get(GET_GTEST_FULL_NAME), "done checking data of [1,{}]", num_rows_write_in_total);
        }

        // Reading with a large number of small DTFile ingested will greatly slow down the testing
        if (num_rows_write_in_total >= 200)
            break;
    }
}
CATCH

TEST_P(DeltaMergeStoreRWTest, DDLChangeInt8ToInt32)
try
{
    const String col_name_ddl = "i8";
    const ColId col_id_ddl = 2;
    const DataTypePtr col_type_before_ddl = DataTypeFactory::instance().get("Int8");
    const DataTypePtr col_type_after_ddl = DataTypeFactory::instance().get("Int32");
    {
        auto table_column_defines = DMTestEnv::getDefaultColumns();
        ColumnDefine cd(col_id_ddl, col_name_ddl, col_type_before_ddl);
        table_column_defines->emplace_back(cd);
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
            // Add a column of i8:Int8 for test
            block.insert(DB::tests::createColumn<Int8>(
                createSignedNumbers(0, num_rows_write),
                col_name_ddl,
                col_id_ddl));
        }
        store->write(*db_context, db_context->getSettingsRef(), block);
    }

    {
        // DDL change col from i8 -> i32
        AlterCommands commands;
        {
            AlterCommand com;
            com.type = AlterCommand::MODIFY_COLUMN;
            com.data_type = col_type_after_ddl;
            com.column_name = col_name_ddl;
            com.column_id = col_id_ddl;
            commands.emplace_back(std::move(com));
        }
        ColumnID ignored = 0;
        store->applyAlters(commands, std::nullopt, ignored, *db_context);
    }

    {
        // read all columns from store
        const auto & columns = store->getTableColumns();
        BlockInputStreams ins = store->read(*db_context,
                                            db_context->getSettingsRef(),
                                            columns,
                                            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                            /* num_streams= */ 1,
                                            /* max_version= */ std::numeric_limits<UInt64>::max(),
                                            EMPTY_FILTER,
                                            TRACING_NAME,
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1UL);
        BlockInputStreamPtr & in = ins[0];
        {
            // check col type
            const Block head = in->getHeader();
            const auto & col = head.getByName(col_name_ddl);
            ASSERT_EQ(col.name, col_name_ddl);
            ASSERT_EQ(col.column_id, col_id_ddl);
            ASSERT_TRUE(col.type->equals(*col_type_after_ddl));
        }

        size_t num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            for (size_t i = 0; i < block.rows(); ++i)
            {
                for (auto && iter : block)
                {
                    auto c = iter.column;
                    if (iter.name == DMTestEnv::pk_name)
                    {
                        // printf("pk:%lld\n", c->getInt(i));
                        EXPECT_EQ(c->getInt(i), Int64(i));
                    }
                    else if (iter.name == col_name_ddl)
                    {
                        // printf("%s:%lld\n", col_name_ddl.c_str(), c->getInt(i));
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
CATCH


TEST_P(DeltaMergeStoreRWTest, DDLDropColumn)
try
{
    const String col_name_to_drop = "i8";
    const ColId col_id_to_drop = 2;
    const DataTypePtr col_type_to_drop = DataTypeFactory::instance().get("Int8");
    {
        auto table_column_defines = DMTestEnv::getDefaultColumns();
        ColumnDefine cd(col_id_to_drop, col_name_to_drop, col_type_to_drop);
        table_column_defines->emplace_back(cd);
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
            // Add a column of i8:Int8 for test
            block.insert(DB::tests::createColumn<Int8>(
                createSignedNumbers(0, num_rows_write),
                col_name_to_drop,
                col_id_to_drop));
        }
        store->write(*db_context, db_context->getSettingsRef(), block);
    }

    {
        // DDL change delete col i8
        AlterCommands commands;
        {
            AlterCommand com;
            com.type = AlterCommand::DROP_COLUMN;
            com.data_type = col_type_to_drop;
            com.column_name = col_name_to_drop;
            com.column_id = col_id_to_drop;
            commands.emplace_back(std::move(com));
        }
        ColumnID ignored = 0;
        store->applyAlters(commands, std::nullopt, ignored, *db_context);
    }

    {
        // read all columns from store
        const auto & columns = store->getTableColumns();
        BlockInputStreams ins = store->read(*db_context,
                                            db_context->getSettingsRef(),
                                            columns,
                                            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                            /* num_streams= */ 1,
                                            /* max_version= */ std::numeric_limits<UInt64>::max(),
                                            EMPTY_FILTER,
                                            TRACING_NAME,
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
                    if (iter.name == DMTestEnv::pk_name)
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
CATCH

TEST_P(DeltaMergeStoreRWTest, DDLAddColumn)
try
{
    const String col_name_c1 = "i8";
    const ColId col_id_c1 = 2;
    const DataTypePtr col_type_c1 = DataTypeFactory::instance().get("Int8");

    const String col_name_to_add = "i32";
    const ColId col_id_to_add = 3;
    const DataTypePtr col_type_to_add = DataTypeFactory::instance().get("Int32");
    {
        auto table_column_defines = DMTestEnv::getDefaultColumns();
        ColumnDefine cd(col_id_c1, col_name_c1, col_type_c1);
        table_column_defines->emplace_back(cd);
        store = reload(table_column_defines);
    }

    const size_t num_rows_write = 128;
    {
        // write to store with column c1
        Block block;
        {
            block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
            // Add a column of i8:Int8 for test
            block.insert(DB::tests::createColumn<Int8>(
                createSignedNumbers(0, num_rows_write),
                col_name_c1,
                col_id_c1));
        }
        store->write(*db_context, db_context->getSettingsRef(), block);
    }

    {
        // DDL change add col i32
        AlterCommands commands;
        {
            AlterCommand com;
            com.type = AlterCommand::ADD_COLUMN;
            com.data_type = col_type_to_add;
            com.column_name = col_name_to_add;
            commands.emplace_back(std::move(com));
        }
        ColumnID col_to_add = col_id_to_add;
        store->applyAlters(commands, std::nullopt, col_to_add, *db_context);
    }

    {
        // read all columns from store
        const auto & columns = store->getTableColumns();
        BlockInputStreams ins = store->read(*db_context,
                                            db_context->getSettingsRef(),
                                            columns,
                                            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                            /* num_streams= */ 1,
                                            /* max_version= */ std::numeric_limits<UInt64>::max(),
                                            EMPTY_FILTER,
                                            TRACING_NAME,
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
                    if (iter.name == DMTestEnv::pk_name)
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
CATCH

TEST_P(DeltaMergeStoreRWTest, DDLAddColumnFloat64)
try
{
    const String col_name_to_add = "f64";
    const ColId col_id_to_add = 2;
    const DataTypePtr col_type_to_add = DataTypeFactory::instance().get("Float64");

    // write some rows before DDL
    size_t num_rows_write = 1;
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        store->write(*db_context, db_context->getSettingsRef(), block);
    }

    // DDL add column f64 with default value
    {
        AlterCommands commands;
        {
            AlterCommand com;
            com.type = AlterCommand::ADD_COLUMN;
            com.data_type = col_type_to_add;
            com.column_name = col_name_to_add;

            // mock default value
            // actual ddl is like: ADD COLUMN `f64` Float64 DEFAULT 1.123456
            auto cast = std::make_shared<ASTFunction>();
            {
                cast->name = "CAST";
                ASTPtr arg = std::make_shared<ASTLiteral>(Field(static_cast<Float64>(1.123456)));
                cast->arguments = std::make_shared<ASTExpressionList>();
                cast->children.push_back(cast->arguments);
                cast->arguments->children.push_back(arg);
                cast->arguments->children.push_back(ASTPtr()); // dummy alias
            }
            com.default_expression = cast;
            commands.emplace_back(std::move(com));
        }
        ColumnID col_to_add = col_id_to_add;
        store->applyAlters(commands, std::nullopt, col_to_add, *db_context);
    }

    // try read
    {
        auto in = store->read(*db_context,
                              db_context->getSettingsRef(),
                              store->getTableColumns(),
                              {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                              /* num_streams= */ 1,
                              /* max_version= */ std::numeric_limits<UInt64>::max(),
                              EMPTY_FILTER,
                              TRACING_NAME,
                              /* expected_block_size= */ 1024)[0];

        in->readPrefix();
        size_t num_rows_read = 0;
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            ASSERT_TRUE(block.has(col_name_to_add));
            const auto & col = block.getByName(col_name_to_add);
            ASSERT_DATATYPE_EQ(col.type, col_type_to_add);
            ASSERT_EQ(col.name, col_name_to_add);
            for (size_t i = 0; i < block.rows(); ++i)
            {
                Field tmp;
                col.column->get(i, tmp);
                // There is some loss of precision during the convertion, so we just do a rough comparison
                EXPECT_FLOAT_EQ(tmp.get<Float64>(), 1.123456);
            }
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}
CATCH

TEST_P(DeltaMergeStoreRWTest, DDLAddColumnFloatDecimal64)
try
{
    const String col_name_to_add = "f64";
    const ColId col_id_to_add = 2;
    const DataTypePtr col_type_to_add = DataTypeFactory::instance().get("Float64");

    // write some rows before DDL
    size_t num_rows_write = 1;
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        store->write(*db_context, db_context->getSettingsRef(), block);
    }

    // DDL add column f64 with default value
    {
        AlterCommands commands;
        {
            AlterCommand com;
            com.type = AlterCommand::ADD_COLUMN;
            com.data_type = col_type_to_add;
            com.column_name = col_name_to_add;

            // mock default value
            // actual ddl is like: ADD COLUMN `f64` Float64 DEFAULT 1.123456
            auto cast = std::make_shared<ASTFunction>();
            {
                cast->name = "CAST";
                ASTPtr arg = std::make_shared<ASTLiteral>(toField(DecimalField(Decimal64(1123456), 6)));
                cast->arguments = std::make_shared<ASTExpressionList>();
                cast->children.push_back(cast->arguments);
                cast->arguments->children.push_back(arg);
                cast->arguments->children.push_back(ASTPtr()); // dummy alias
            }
            com.default_expression = cast;
            commands.emplace_back(std::move(com));
        }
        ColumnID col_to_add = col_id_to_add;
        store->applyAlters(commands, std::nullopt, col_to_add, *db_context);
    }

    // try read
    {
        auto in = store->read(*db_context,
                              db_context->getSettingsRef(),
                              store->getTableColumns(),
                              {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                              /* num_streams= */ 1,
                              /* max_version= */ std::numeric_limits<UInt64>::max(),
                              EMPTY_FILTER,
                              TRACING_NAME,
                              /* expected_block_size= */ 1024)[0];

        in->readPrefix();
        size_t num_rows_read = 0;
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            ASSERT_TRUE(block.has(col_name_to_add));
            const auto & col = block.getByName(col_name_to_add);
            ASSERT_DATATYPE_EQ(col.type, col_type_to_add);
            ASSERT_EQ(col.name, col_name_to_add);
            for (size_t i = 0; i < block.rows(); ++i)
            {
                Field tmp;
                col.column->get(i, tmp);
                // There is some loss of precision during the convertion, so we just do a rough comparison
                EXPECT_FLOAT_EQ(tmp.get<Float64>(), 1.123456);
            }
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}
CATCH

TEST_P(DeltaMergeStoreRWTest, DDLAddColumnFloat32)
try
{
    const String col_name_to_add = "f32";
    const ColId col_id_to_add = 2;
    const DataTypePtr col_type_to_add = DataTypeFactory::instance().get("Float32");

    // write some rows before DDL
    size_t num_rows_write = 1;
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        store->write(*db_context, db_context->getSettingsRef(), block);
    }

    // DDL add column f32 with default value
    {
        AlterCommands commands;
        {
            AlterCommand com;
            com.type = AlterCommand::ADD_COLUMN;
            com.data_type = col_type_to_add;
            com.column_name = col_name_to_add;

            // mock default value
            // actual ddl is like: ADD COLUMN `f32` Float32 DEFAULT 1.125
            auto cast = std::make_shared<ASTFunction>();
            {
                cast->name = "CAST";
                ASTPtr arg = std::make_shared<ASTLiteral>(toField(DecimalField(Decimal32(1125), 3)));
                cast->arguments = std::make_shared<ASTExpressionList>();
                cast->children.push_back(cast->arguments);
                cast->arguments->children.push_back(arg);
                cast->arguments->children.push_back(ASTPtr()); // dummy alias
            }
            com.default_expression = cast;
            commands.emplace_back(std::move(com));
        }
        ColumnID col_to_add = col_id_to_add;
        store->applyAlters(commands, std::nullopt, col_to_add, *db_context);
    }

    // try read
    {
        auto in = store->read(*db_context,
                              db_context->getSettingsRef(),
                              store->getTableColumns(),
                              {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                              /* num_streams= */ 1,
                              /* max_version= */ std::numeric_limits<UInt64>::max(),
                              EMPTY_FILTER,
                              TRACING_NAME,
                              /* expected_block_size= */ 1024)[0];

        in->readPrefix();
        size_t num_rows_read = 0;
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            ASSERT_TRUE(block.has(col_name_to_add));
            const auto & col = block.getByName(col_name_to_add);
            ASSERT_DATATYPE_EQ(col.type, col_type_to_add);
            ASSERT_EQ(col.name, col_name_to_add);
            for (size_t i = 0; i < block.rows(); ++i)
            {
                Field tmp;
                col.column->get(i, tmp);
                // There is some loss of precision during the convertion, so we just do a rough comparison
                EXPECT_FLOAT_EQ(tmp.get<Float64>(), 1.125);
            }
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}
CATCH

TEST_P(DeltaMergeStoreRWTest, DDLAddColumnInt8)
try
{
    const String col_name_to_add = "Int8";
    const ColId col_id_to_add = 2;
    const DataTypePtr col_type_to_add = DataTypeFactory::instance().get("Int8");

    // write some rows before DDL
    size_t num_rows_write = 1;
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        store->write(*db_context, db_context->getSettingsRef(), block);
    }

    // DDL add column Int8 with default value
    {
        AlterCommands commands;
        {
            AlterCommand com;
            com.type = AlterCommand::ADD_COLUMN;
            com.data_type = col_type_to_add;
            com.column_name = col_name_to_add;

            // mock default value
            // actual ddl is like: ADD COLUMN `Int8` Int8 DEFAULT 1
            auto cast = std::make_shared<ASTFunction>();
            {
                cast->name = "CAST";
                ASTPtr arg = std::make_shared<ASTLiteral>(Field(static_cast<Int64>(1)));
                cast->arguments = std::make_shared<ASTExpressionList>();
                cast->children.push_back(cast->arguments);
                cast->arguments->children.push_back(arg);
                cast->arguments->children.push_back(ASTPtr()); // dummy alias
            }
            com.default_expression = cast;
            commands.emplace_back(std::move(com));
        }
        ColumnID col_to_add = col_id_to_add;
        store->applyAlters(commands, std::nullopt, col_to_add, *db_context);
    }

    // try read
    {
        auto in = store->read(*db_context,
                              db_context->getSettingsRef(),
                              store->getTableColumns(),
                              {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                              /* num_streams= */ 1,
                              /* max_version= */ std::numeric_limits<UInt64>::max(),
                              EMPTY_FILTER,
                              TRACING_NAME,
                              /* expected_block_size= */ 1024)[0];

        in->readPrefix();
        size_t num_rows_read = 0;
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            ASSERT_TRUE(block.has(col_name_to_add));
            const auto & col = block.getByName(col_name_to_add);
            ASSERT_DATATYPE_EQ(col.type, col_type_to_add);
            ASSERT_EQ(col.name, col_name_to_add);
            for (size_t i = 0; i < block.rows(); ++i)
            {
                Field tmp;
                col.column->get(i, tmp);
                // There is some loss of precision during the convertion, so we just do a rough comparison
                EXPECT_FLOAT_EQ(tmp.get<Int8>(), 1);
            }
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}
CATCH

TEST_P(DeltaMergeStoreRWTest, DDLAddColumnUInt8)
try
{
    const String col_name_to_add = "UInt8";
    const ColId col_id_to_add = 2;
    const DataTypePtr col_type_to_add = DataTypeFactory::instance().get("UInt8");

    // write some rows before DDL
    size_t num_rows_write = 1;
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        store->write(*db_context, db_context->getSettingsRef(), block);
    }

    // DDL add column UInt8 with default value
    {
        AlterCommands commands;
        {
            AlterCommand com;
            com.type = AlterCommand::ADD_COLUMN;
            com.data_type = col_type_to_add;
            com.column_name = col_name_to_add;

            // mock default value
            // actual ddl is like: ADD COLUMN `UInt8` UInt8 DEFAULT 1
            auto cast = std::make_shared<ASTFunction>();
            {
                cast->name = "CAST";
                ASTPtr arg = std::make_shared<ASTLiteral>(Field(static_cast<UInt64>(1)));
                cast->arguments = std::make_shared<ASTExpressionList>();
                cast->children.push_back(cast->arguments);
                cast->arguments->children.push_back(arg);
                cast->arguments->children.push_back(ASTPtr()); // dummy alias
            }
            com.default_expression = cast;
            commands.emplace_back(std::move(com));
        }
        ColumnID col_to_add = col_id_to_add;
        store->applyAlters(commands, std::nullopt, col_to_add, *db_context);
    }

    // try read
    {
        auto in = store->read(*db_context,
                              db_context->getSettingsRef(),
                              store->getTableColumns(),
                              {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                              /* num_streams= */ 1,
                              /* max_version= */ std::numeric_limits<UInt64>::max(),
                              EMPTY_FILTER,
                              TRACING_NAME,
                              /* expected_block_size= */ 1024)[0];

        in->readPrefix();
        size_t num_rows_read = 0;
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            ASSERT_TRUE(block.has(col_name_to_add));
            const auto & col = block.getByName(col_name_to_add);
            ASSERT_DATATYPE_EQ(col.type, col_type_to_add);
            ASSERT_EQ(col.name, col_name_to_add);
            for (size_t i = 0; i < block.rows(); ++i)
            {
                Field tmp;
                col.column->get(i, tmp);
                // There is some loss of precision during the convertion, so we just do a rough comparison
                EXPECT_FLOAT_EQ(tmp.get<UInt64>(), 1);
            }
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}
CATCH

TEST_P(DeltaMergeStoreRWTest, DDLAddColumnDateTime)
try
{
    const String col_name_to_add = "dt";
    const ColId col_id_to_add = 2;
    const DataTypePtr col_type_to_add = DataTypeFactory::instance().get("MyDateTime");

    MyDateTime mydatetime_val(1999, 9, 9, 12, 34, 56, 0);
    const UInt64 mydatetime_uint = mydatetime_val.toPackedUInt();

    // write some rows before DDL
    size_t num_rows_write = 1;
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        store->write(*db_context, db_context->getSettingsRef(), block);
    }

    // DDL add column date with default value
    {
        AlterCommands commands;
        {
            AlterCommand com;
            com.type = AlterCommand::ADD_COLUMN;
            com.data_type = col_type_to_add;
            com.column_name = col_name_to_add;

            // mock default value
            // actual ddl is like: ADD COLUMN `date` MyDateTime DEFAULT '<packed int of mydatetime>'
            com.default_expression = makeASTFunction(
                "CAST",
                std::make_shared<ASTLiteral>(toField(mydatetime_uint)),
                ASTPtr() // dummy alias
            );
            commands.emplace_back(std::move(com));
        }
        ColumnID col_to_add = col_id_to_add;
        store->applyAlters(commands, std::nullopt, col_to_add, *db_context);
    }

    // try read
    {
        auto in = store->read(*db_context,
                              db_context->getSettingsRef(),
                              store->getTableColumns(),
                              {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                              /* num_streams= */ 1,
                              /* max_version= */ std::numeric_limits<UInt64>::max(),
                              EMPTY_FILTER,
                              TRACING_NAME,
                              /* expected_block_size= */ 1024)[0];

        size_t num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            ASSERT_TRUE(block.has(col_name_to_add));
            const auto & col = block.getByName(col_name_to_add);
            ASSERT_DATATYPE_EQ(col.type, col_type_to_add);
            ASSERT_EQ(col.name, col_name_to_add);
            for (size_t i = 0; i < block.rows(); i++)
            {
                EXPECT_EQ((*col.column)[i].get<UInt64>(), mydatetime_uint); // Timestamp for '1999-09-09 12:34:56'
            }
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}
CATCH

TEST_P(DeltaMergeStoreRWTest, DDLAddColumnString)
try
{
    const String col_name_to_add = "string";
    const ColId col_id_to_add = 2;
    const DataTypePtr col_type_to_add = DataTypeFactory::instance().get("String");

    // write some rows before DDL
    size_t num_rows_write = 1;
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        store->write(*db_context, db_context->getSettingsRef(), block);
    }

    // DDL add column string with default value
    {
        AlterCommands commands;
        {
            AlterCommand com;
            com.type = AlterCommand::ADD_COLUMN;
            com.data_type = col_type_to_add;
            com.column_name = col_name_to_add;

            // mock default value
            // actual ddl is like: ADD COLUMN `string` String DEFAULT 'test_add_string_col'
            auto cast = std::make_shared<ASTFunction>();
            {
                cast->name = "CAST";
                ASTPtr arg = std::make_shared<ASTLiteral>(Field(String("test_add_string_col")));
                cast->arguments = std::make_shared<ASTExpressionList>();
                cast->children.push_back(cast->arguments);
                cast->arguments->children.push_back(arg);
                cast->arguments->children.push_back(ASTPtr()); // dummy alias
            }
            com.default_expression = cast;
            commands.emplace_back(std::move(com));
        }
        ColumnID col_to_add = col_id_to_add;
        store->applyAlters(commands, std::nullopt, col_to_add, *db_context);
    }

    // try read
    {
        auto in = store->read(*db_context,
                              db_context->getSettingsRef(),
                              store->getTableColumns(),
                              {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                              /* num_streams= */ 1,
                              /* max_version= */ std::numeric_limits<UInt64>::max(),
                              EMPTY_FILTER,
                              TRACING_NAME,
                              /* expected_block_size= */ 1024)[0];

        in->readPrefix();
        size_t num_rows_read = 0;
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            ASSERT_TRUE(block.has(col_name_to_add));
            const auto & col = block.getByName(col_name_to_add);
            ASSERT_DATATYPE_EQ(col.type, col_type_to_add);
            ASSERT_EQ(col.name, col_name_to_add);
            for (size_t i = 0; i < block.rows(); ++i)
            {
                Field tmp;
                col.column->get(i, tmp);
                EXPECT_EQ(tmp.get<String>(), String("test_add_string_col"));
            }
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}
CATCH

TEST_P(DeltaMergeStoreRWTest, DDLRenameColumn)
try
{
    const String col_name_before_ddl = "i8";
    const String col_name_after_ddl = "i8_tmp";
    const ColId col_id_ddl = 2;
    const DataTypePtr col_type = DataTypeFactory::instance().get("Int32");
    {
        auto table_column_defines = DMTestEnv::getDefaultColumns();
        ColumnDefine cd(col_id_ddl, col_name_before_ddl, col_type);
        table_column_defines->emplace_back(cd);
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
            // Add a column of i8:Int8 for test
            block.insert(DB::tests::createColumn<Int8>(
                createSignedNumbers(0, num_rows_write),
                col_name_before_ddl,
                col_id_ddl));
        }
        store->write(*db_context, db_context->getSettingsRef(), block);
    }

    {
        // DDL change col name from col_name_before_ddl -> col_name_after_ddl
        AlterCommands commands;
        {
            AlterCommand com;
            com.type = AlterCommand::RENAME_COLUMN;
            com.data_type = col_type;
            com.column_name = col_name_before_ddl;
            com.new_column_name = col_name_after_ddl;
            com.column_id = col_id_ddl;
            commands.emplace_back(std::move(com));
        }
        ColumnID ignored = 0;
        store->applyAlters(commands, std::nullopt, ignored, *db_context);
    }

    {
        // read all columns from store
        const auto & columns = store->getTableColumns();
        BlockInputStreams ins = store->read(*db_context,
                                            db_context->getSettingsRef(),
                                            columns,
                                            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                            /* num_streams= */ 1,
                                            /* max_version= */ std::numeric_limits<UInt64>::max(),
                                            EMPTY_FILTER,
                                            TRACING_NAME,
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1UL);
        BlockInputStreamPtr & in = ins[0];
        {
            // check col rename is success
            const Block head = in->getHeader();
            const auto & col = head.getByName(col_name_after_ddl);
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
                    if (iter.name == DMTestEnv::pk_name)
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
CATCH

// Test rename pk column when pk_is_handle = true.
TEST_P(DeltaMergeStoreRWTest, DDLRenamePKColumn)
try
{
    const String col_name_before_ddl = "pk1";
    const String col_name_after_ddl = "pk2";
    const ColId col_id_ddl = 1;
    const DataTypePtr col_type = DataTypeFactory::instance().get("Int32");
    {
        auto table_column_defines = DMTestEnv::getDefaultColumns();
        ColumnDefine cd(col_id_ddl, col_name_before_ddl, col_type);
        // Use this column as pk
        (*table_column_defines)[0] = cd;
        store = reload(table_column_defines);
    }

    {
        // check column structure
        const auto & cols = store->getTableColumns();
        ASSERT_EQ(cols.size(), 3UL);
        const auto & str_col = cols[0];
        ASSERT_EQ(str_col.name, col_name_before_ddl);
        ASSERT_EQ(str_col.id, col_id_ddl);
        ASSERT_TRUE(str_col.type->equals(*col_type));
    }
    {
        // check pk name
        auto pks_desc = store->getPrimarySortDescription();
        ASSERT_EQ(pks_desc.size(), 1UL);
        auto pk = pks_desc[0];
        ASSERT_EQ(pk.column_name, col_name_before_ddl);
    }

    const size_t num_rows_write = 128;
    {
        // write to store
        Block block = DMTestEnv::prepareSimpleWriteBlock(
            0,
            num_rows_write,
            false,
            /*tso=*/2,
            col_name_before_ddl,
            col_id_ddl,
            col_type);
        store->write(*db_context, db_context->getSettingsRef(), block);
    }

    {
        // DDL change pk col name from col_name_before_ddl -> col_name_after_ddl
        AlterCommands commands;
        {
            AlterCommand com;
            com.type = AlterCommand::RENAME_COLUMN;
            com.data_type = col_type;
            com.column_name = col_name_before_ddl;
            com.new_column_name = col_name_after_ddl;
            com.column_id = col_id_ddl;
            commands.emplace_back(std::move(com));
        }
        ColumnID ignored = 0;
        TiDB::TableInfo table_info;
        {
            static const String json_table_info = R"(
{"cols":[{"comment":"","default":null,"default_bit":null,"id":1,"name":{"L":"pk2","O":"pk2"},"offset":0,"origin_default":null,"state":5,"type":{"Charset":"binary","Collate":"binary","Decimal":0,"Elems":null,"Flag":4099,"Flen":11,"Tp":3}}],"comment":"","id":45,"name":{"L":"t","O":"t"},"partition":null,"pk_is_handle":true,"schema_version":23,"state":5,"update_timestamp":417906423650844680}
        )";
            table_info.deserialize(json_table_info);
            ASSERT_TRUE(table_info.pk_is_handle);
        }
        store->applyAlters(commands, table_info, ignored, *db_context);
    }

    {
        // check pk name after ddl
        auto pks_desc = store->getPrimarySortDescription();
        ASSERT_EQ(pks_desc.size(), 1UL);
        auto pk = pks_desc[0];
        ASSERT_EQ(pk.column_name, col_name_after_ddl);
    }

    {
        // read all columns from store
        const auto & columns = store->getTableColumns();
        BlockInputStreams ins = store->read(*db_context,
                                            db_context->getSettingsRef(),
                                            columns,
                                            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                            /* num_streams= */ 1,
                                            /* max_version= */ std::numeric_limits<UInt64>::max(),
                                            EMPTY_FILTER,
                                            TRACING_NAME,
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1UL);
        BlockInputStreamPtr & in = ins[0];
        {
            // check col rename is success
            const Block head = in->getHeader();
            const auto & col = head.getByName(col_name_after_ddl);
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
                    if (iter.name == col_name_after_ddl)
                    {
                        //printf("col2:%s\n", c->getDataAt(i).data);
                        EXPECT_EQ(c->getInt(i), Int64(i));
                    }
                }
            }
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }

    {
        // write and read with new pk name after ddl
        {
            // Then write new block with new pk name
            Block block = DMTestEnv::prepareSimpleWriteBlock(
                num_rows_write,
                num_rows_write * 2,
                false,
                /*tso=*/2,
                col_name_after_ddl,
                col_id_ddl,
                col_type);
            store->write(*db_context, db_context->getSettingsRef(), block);
        }
        {
            // read all columns from store
            const auto & columns = store->getTableColumns();
            BlockInputStreams ins = store->read(*db_context,
                                                db_context->getSettingsRef(),
                                                columns,
                                                {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                                /* num_streams= */ 1,
                                                /* max_version= */ std::numeric_limits<UInt64>::max(),
                                                EMPTY_FILTER,
                                                TRACING_NAME,
                                                /* expected_block_size= */ 1024);
            ASSERT_EQ(ins.size(), 1UL);
            BlockInputStreamPtr & in = ins[0];
            {
                // check col rename is success
                const Block head = in->getHeader();
                const auto & col = head.getByName(col_name_after_ddl);
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
                        if (iter.name == col_name_after_ddl)
                        {
                            //printf("col2:%s\n", c->getDataAt(i).data);
                            EXPECT_EQ(c->getInt(i), Int64(i));
                        }
                    }
                }
            }
            in->readSuffix();
            ASSERT_EQ(num_rows_read, num_rows_write * 2);
        }
    }
}
CATCH

TEST_P(DeltaMergeStoreRWTest, DDLIssue1341)
try
{
    // issue 1341: Background task may use a wrong schema to compact data

    const String col_name_to_add = "f32";
    const ColId col_id_to_add = 2;
    const DataTypePtr col_type_to_add = DataTypeFactory::instance().get("Float32");
    const auto col_default_value = toField(DecimalField(Decimal32(1125), 3)); // 1.125

    // write some rows before DDL
    size_t num_rows_write = 1;
    {
        // Enable pause before delta-merge
        FailPointHelper::enableFailPoint(FailPoints::pause_before_dt_background_delta_merge);
        // Enable pause until delta-merge is done
        FailPointHelper::enableFailPoint(FailPoints::pause_until_dt_background_delta_merge);
        FailPointHelper::enableFailPoint(FailPoints::force_triggle_background_merge_delta);

        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        store->write(*db_context, db_context->getSettingsRef(), block);
    }

    // DDL add column f32 with default value
    {
        AlterCommands commands;
        {
            AlterCommand com;
            com.type = AlterCommand::ADD_COLUMN;
            com.data_type = col_type_to_add;
            com.column_name = col_name_to_add;

            // mock default value
            // actual ddl is like: ADD COLUMN `f32` Float32 DEFAULT 1.125
            auto cast = std::make_shared<ASTFunction>();
            {
                cast->name = "CAST";
                ASTPtr arg = std::make_shared<ASTLiteral>(toField(DecimalField(Decimal32(1125), 3)));
                cast->arguments = std::make_shared<ASTExpressionList>();
                cast->children.push_back(cast->arguments);
                cast->arguments->children.push_back(arg);
                cast->arguments->children.push_back(ASTPtr()); // dummy alias
            }
            com.default_expression = cast;
            commands.emplace_back(std::move(com));
        }
        ColumnID col_to_add = col_id_to_add;
        store->applyAlters(commands, std::nullopt, col_to_add, *db_context);
    }

    // try read
    {
        auto in = store->read(*db_context,
                              db_context->getSettingsRef(),
                              store->getTableColumns(),
                              {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                              /* num_streams= */ 1,
                              /* max_version= */ std::numeric_limits<UInt64>::max(),
                              EMPTY_FILTER,
                              TRACING_NAME,
                              /* expected_block_size= */ 1024)[0];

        in->readPrefix();
        size_t num_rows_read = 0;
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            ASSERT_TRUE(block.has(col_name_to_add));
            const auto & col = block.getByName(col_name_to_add);
            ASSERT_DATATYPE_EQ(col.type, col_type_to_add);
            ASSERT_EQ(col.name, col_name_to_add);
            for (size_t i = 0; i < block.rows(); ++i)
            {
                Field tmp;
                col.column->get(i, tmp);
                // There is some loss of precision during the convertion, so we just do a rough comparison
                EXPECT_FLOAT_EQ(std::abs(tmp.get<Float64>()), 1.125);
            }
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }

    {
        // write and triggle flush
        FailPointHelper::enableFailPoint(FailPoints::force_triggle_foreground_flush);

        Block block = DMTestEnv::prepareSimpleWriteBlock(num_rows_write, num_rows_write * 2, false);
        {
            // Add a column of float for test
            auto col = DB::tests::createColumn<Float32>(
                std::vector<Float64>(num_rows_write, 3.1415),
                col_name_to_add,
                col_id_to_add);
            col.default_value = col_default_value;
            block.insert(std::move(col));
        }
        store->write(*db_context, db_context->getSettingsRef(), block);
    }

    // disable pause so that delta-merge can continue
    FailPointHelper::disableFailPoint(FailPoints::pause_before_dt_background_delta_merge);
    // wait till delta-merge is done
    FAIL_POINT_PAUSE(FailPoints::pause_until_dt_background_delta_merge);
    {
        auto in = store->read(*db_context,
                              db_context->getSettingsRef(),
                              store->getTableColumns(),
                              {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                              /* num_streams= */ 1,
                              /* max_version= */ std::numeric_limits<UInt64>::max(),
                              EMPTY_FILTER,
                              TRACING_NAME,
                              /* expected_block_size= */ 1024)[0];

        in->readPrefix();
        while (Block block = in->read())
        {
            ASSERT_EQ(block.rows(), num_rows_write * 2);
            ASSERT_TRUE(block.has(col_name_to_add));
            const auto & col = block.getByName(col_name_to_add);
            ASSERT_DATATYPE_EQ(col.type, col_type_to_add);
            ASSERT_EQ(col.name, col_name_to_add);
            Field tmp;
            tmp = (*col.column)[0];
            EXPECT_FLOAT_EQ(tmp.get<Float64>(), 1.125); // fill with default value
            tmp = (*col.column)[1];
            EXPECT_FLOAT_EQ(tmp.get<Float64>(), 3.1415); // keep the value we inserted
        }
        in->readSuffix();
    }
}
CATCH

TEST_F(DeltaMergeStoreTest, CreateWithCommonHandle)
try
{
    auto table_column_defines = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::CommonHandle);
    dropDataOnDisk(getTemporaryPath());
    store = reload(table_column_defines, true, 2);
    {
        // check handle column of store
        auto & h = store->getHandle();
        ASSERT_EQ(h.name, EXTRA_HANDLE_COLUMN_NAME);
        ASSERT_EQ(h.id, EXTRA_HANDLE_COLUMN_ID);
        ASSERT_TRUE(h.type->equals(*EXTRA_HANDLE_COLUMN_STRING_TYPE));
    }
    {
        // check column structure of store
        auto & cols = store->getTableColumns();
        // version & tag column added
        ASSERT_EQ(cols.size(), 3UL);
    }
}
CATCH

TEST_P(DeltaMergeStoreRWTest, SimpleWriteReadCommonHandle)
try
{
    const ColumnDefine col_str_define(2, "col2", std::make_shared<DataTypeString>());
    const ColumnDefine col_i8_define(3, "i8", std::make_shared<DataTypeInt8>());
    size_t rowkey_column_size = 2;
    {
        auto table_column_defines = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::CommonHandle);
        table_column_defines->emplace_back(col_str_define);
        table_column_defines->emplace_back(col_i8_define);

        dropDataOnDisk(getTemporaryPath());
        store = reload(table_column_defines, true, rowkey_column_size);
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
            block = DMTestEnv::prepareSimpleWriteBlock(0,
                                                       num_rows_write,
                                                       false,
                                                       2,
                                                       EXTRA_HANDLE_COLUMN_NAME,
                                                       EXTRA_HANDLE_COLUMN_ID,
                                                       EXTRA_HANDLE_COLUMN_STRING_TYPE,
                                                       true,
                                                       rowkey_column_size);
            // Add a column of col2:String for test
            block.insert(DB::tests::createColumn<String>(
                createNumberStrings(0, num_rows_write),
                col_str_define.name,
                col_str_define.id));
            // Add a column of i8:Int8 for test
            block.insert(DB::tests::createColumn<Int8>(
                createSignedNumbers(0, num_rows_write),
                col_i8_define.name,
                col_i8_define.id));
        }
        store->write(*db_context, db_context->getSettingsRef(), block);
    }

    {
        // TODO read data from more than one block
        // TODO read data from mutli streams
        // TODO read partial columns from store
        // TODO read data of max_version

        // read all columns from store
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(*db_context,
                                             db_context->getSettingsRef(),
                                             columns,
                                             {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                             /* num_streams= */ 1,
                                             /* max_version= */ std::numeric_limits<UInt64>::max(),
                                             EMPTY_FILTER,
                                             TRACING_NAME,
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
                    if (iter.name == DMTestEnv::pk_name)
                    {
                        DMTestEnv::verifyClusteredIndexValue(c->operator[](i).get<String>(), i, rowkey_column_size);
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
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->readRaw(*db_context, db_context->getSettingsRef(), columns, 1)[0];

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
                    if (iter.name == DMTestEnv::pk_name)
                    {
                        DMTestEnv::verifyClusteredIndexValue(c->operator[](i).get<String>(), i, rowkey_column_size);
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
CATCH

TEST_P(DeltaMergeStoreRWTest, WriteMultipleBlockWithCommonHandle)
try
{
    const size_t num_write_rows = 32;
    const size_t rowkey_column_size = 2;
    auto table_column_defines = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::CommonHandle);

    {
        dropDataOnDisk(getTemporaryPath());
        store = reload(table_column_defines, true, rowkey_column_size);
    }

    // Test write multi blocks without overlap
    {
        Block block1 = DMTestEnv::prepareSimpleWriteBlock(0,
                                                          1 * num_write_rows,
                                                          false,
                                                          2,
                                                          EXTRA_HANDLE_COLUMN_NAME,
                                                          EXTRA_HANDLE_COLUMN_ID,
                                                          EXTRA_HANDLE_COLUMN_STRING_TYPE,
                                                          true,
                                                          rowkey_column_size);
        Block block2 = DMTestEnv::prepareSimpleWriteBlock(1 * num_write_rows,
                                                          2 * num_write_rows,
                                                          false,
                                                          2,
                                                          EXTRA_HANDLE_COLUMN_NAME,
                                                          EXTRA_HANDLE_COLUMN_ID,
                                                          EXTRA_HANDLE_COLUMN_STRING_TYPE,
                                                          true,
                                                          rowkey_column_size);
        Block block3 = DMTestEnv::prepareSimpleWriteBlock(2 * num_write_rows,
                                                          3 * num_write_rows,
                                                          false,
                                                          2,
                                                          EXTRA_HANDLE_COLUMN_NAME,
                                                          EXTRA_HANDLE_COLUMN_ID,
                                                          EXTRA_HANDLE_COLUMN_STRING_TYPE,
                                                          true,
                                                          rowkey_column_size);
        store->write(*db_context, db_context->getSettingsRef(), block1);
        store->write(*db_context, db_context->getSettingsRef(), block2);
        store->write(*db_context, db_context->getSettingsRef(), block3);

        store->flushCache(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));
    }

    {
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(*db_context,
                                             db_context->getSettingsRef(),
                                             columns,
                                             {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                             /* num_streams= */ 1,
                                             /* max_version= */ std::numeric_limits<UInt64>::max(),
                                             EMPTY_FILTER,
                                             TRACING_NAME,
                                             /* expected_block_size= */ 1024)[0];
        size_t num_rows_read = 0;
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            for (auto && iter : block)
            {
                auto c = iter.column;
                for (Int64 i = 0; i < Int64(c->size()); ++i)
                {
                    if (iter.name == DMTestEnv::pk_name)
                    {
                        DMTestEnv::verifyClusteredIndexValue(c->operator[](i).get<String>(), i, rowkey_column_size);
                    }
                }
            }
        }

        ASSERT_EQ(num_rows_read, 3 * num_write_rows);
    }

    store = reload(table_column_defines, true, rowkey_column_size);

    // Test write multi blocks with overlap
    {
        UInt64 tso1 = 1;
        UInt64 tso2 = 100;
        Block block1 = DMTestEnv::prepareSimpleWriteBlock(0,
                                                          1 * num_write_rows,
                                                          false,
                                                          tso1,
                                                          EXTRA_HANDLE_COLUMN_NAME,
                                                          EXTRA_HANDLE_COLUMN_ID,
                                                          EXTRA_HANDLE_COLUMN_STRING_TYPE,
                                                          true,
                                                          rowkey_column_size);
        Block block2 = DMTestEnv::prepareSimpleWriteBlock(1 * num_write_rows,
                                                          2 * num_write_rows,
                                                          false,
                                                          tso1,
                                                          EXTRA_HANDLE_COLUMN_NAME,
                                                          EXTRA_HANDLE_COLUMN_ID,
                                                          EXTRA_HANDLE_COLUMN_STRING_TYPE,
                                                          true,
                                                          rowkey_column_size);
        Block block3 = DMTestEnv::prepareSimpleWriteBlock(num_write_rows / 2,
                                                          num_write_rows / 2 + num_write_rows,
                                                          false,
                                                          tso2,
                                                          EXTRA_HANDLE_COLUMN_NAME,
                                                          EXTRA_HANDLE_COLUMN_ID,
                                                          EXTRA_HANDLE_COLUMN_STRING_TYPE,
                                                          true,
                                                          rowkey_column_size);
        store->write(*db_context, db_context->getSettingsRef(), block1);
        store->write(*db_context, db_context->getSettingsRef(), block2);
        store->write(*db_context, db_context->getSettingsRef(), block3);

        store->flushCache(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));
    }

    store->compact(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));

    // Read without version
    {
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(*db_context,
                                             db_context->getSettingsRef(),
                                             columns,
                                             {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                             /* num_streams= */ 1,
                                             /* max_version= */ std::numeric_limits<UInt64>::max(),
                                             EMPTY_FILTER,
                                             TRACING_NAME,
                                             /* expected_block_size= */ 1024)[0];
        size_t num_rows_read = 0;
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            for (auto && iter : block)
            {
                auto c = iter.column;
                for (Int64 i = 0; i < Int64(c->size()); ++i)
                {
                    if (iter.name == DMTestEnv::pk_name)
                    {
                        DMTestEnv::verifyClusteredIndexValue(c->operator[](i).get<String>(), i, rowkey_column_size);
                    }
                }
            }
        }

        ASSERT_EQ(num_rows_read, 3 * num_write_rows);
    }
    // Read with version
    {
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(*db_context,
                                             db_context->getSettingsRef(),
                                             columns,
                                             {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                             /* num_streams= */ 1,
                                             /* max_version= */ UInt64(1),
                                             EMPTY_FILTER,
                                             TRACING_NAME,
                                             /* expected_block_size= */ 1024)[0];
        size_t num_rows_read = 0;
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            for (auto && iter : block)
            {
                auto c = iter.column;
                for (Int64 i = 0; i < Int64(c->size()); ++i)
                {
                    if (iter.name == DMTestEnv::pk_name)
                    {
                        DMTestEnv::verifyClusteredIndexValue(c->operator[](i).get<String>(), i, rowkey_column_size);
                    }
                }
            }
        }

        ASSERT_EQ(num_rows_read, 2 * num_write_rows);
    }
}
CATCH

TEST_P(DeltaMergeStoreRWTest, DeleteReadWithCommonHandle)
try
{
    const size_t num_rows_write = 128;
    size_t rowkey_column_size = 2;
    {
        // Create a block with sequential Int64 handle in range [0, 128)
        auto table_column_difines = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::CommonHandle);

        dropDataOnDisk(getTemporaryPath());
        store = reload(table_column_difines, true, rowkey_column_size);

        Block block = DMTestEnv::prepareSimpleWriteBlock(
            0,
            128,
            false,
            2,
            EXTRA_HANDLE_COLUMN_NAME,
            EXTRA_HANDLE_COLUMN_ID,
            EXTRA_HANDLE_COLUMN_STRING_TYPE,
            true,
            rowkey_column_size);
        store->write(*db_context, db_context->getSettingsRef(), block);
    }
    // Test Reading first
    {
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(*db_context,
                                             db_context->getSettingsRef(),
                                             columns,
                                             {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                             /* num_streams= */ 1,
                                             /* max_version= */ std::numeric_limits<UInt64>::max(),
                                             EMPTY_FILTER,
                                             TRACING_NAME,
                                             /* expected_block_size= */ 1024)[0];
        size_t num_rows_read = 0;
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            for (auto && iter : block)
            {
                auto c = iter.column;
                for (Int64 i = 0; i < Int64(c->size()); ++i)
                {
                    if (iter.name == DMTestEnv::pk_name)
                    {
                        DMTestEnv::verifyClusteredIndexValue(c->operator[](i).get<String>(), i, rowkey_column_size);
                    }
                }
            }
        }

        ASSERT_EQ(num_rows_read, num_rows_write);
    }
    // Delete range [0, 64)
    const size_t num_deleted_rows = 64;
    {
        WriteBufferFromOwnString ss;
        DB::EncodeUInt(static_cast<UInt8>(TiDB::CodecFlagInt), ss);
        DB::EncodeInt64(Int64(0), ss);
        DB::EncodeUInt(static_cast<UInt8>(TiDB::CodecFlagInt), ss);
        DB::EncodeInt64(Int64(0), ss);
        RowKeyValue start(true, std::make_shared<String>(ss.releaseStr()));

        ss.restart();
        DB::EncodeUInt(static_cast<UInt8>(TiDB::CodecFlagInt), ss);
        DB::EncodeInt64(Int64(num_deleted_rows), ss);
        DB::EncodeUInt(static_cast<UInt8>(TiDB::CodecFlagInt), ss);
        DB::EncodeInt64(Int64(num_deleted_rows), ss);
        RowKeyValue end(true, std::make_shared<String>(ss.str()));
        RowKeyRange range(start, end, true, 2);
        store->deleteRange(*db_context, db_context->getSettingsRef(), range);
    }
    // Read after deletion
    {
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(*db_context,
                                             db_context->getSettingsRef(),
                                             columns,
                                             {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                             /* num_streams= */ 1,
                                             /* max_version= */ std::numeric_limits<UInt64>::max(),
                                             EMPTY_FILTER,
                                             TRACING_NAME,
                                             /* expected_block_size= */ 1024)[0];
        size_t num_rows_read = 0;
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            for (auto && iter : block)
            {
                auto c = iter.column;
                for (Int64 i = 0; i < Int64(c->size()); ++i)
                {
                    if (iter.name == DMTestEnv::pk_name)
                    {
                        // Range after deletion is [64, 128)
                        DMTestEnv::verifyClusteredIndexValue(
                            c->operator[](i).get<String>(),
                            i + Int64(num_deleted_rows),
                            rowkey_column_size);
                    }
                }
            }
        }

        ASSERT_EQ(num_rows_read, num_rows_write - num_deleted_rows);
    }
}
CATCH

TEST_P(DeltaMergeStoreRWTest, DisableSmallColumnCache)
try
{
    auto settings = db_context->getSettings();

    size_t num_rows_write_in_total = 0;
    const size_t num_rows_per_write = 5;
    while (true)
    {
        {
            // write to store
            Block block = DMTestEnv::prepareSimpleWriteBlock(
                num_rows_write_in_total + 1,
                num_rows_write_in_total + 1 + num_rows_per_write,
                false);

            store->write(*db_context, settings, block);

            store->flushCache(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));
            num_rows_write_in_total += num_rows_per_write;
            auto segment_stats = store->getSegmentStats();
            size_t delta_cache_size = 0;
            for (auto & stat : segment_stats)
            {
                delta_cache_size += stat.delta_cache_size;
            }
            EXPECT_EQ(delta_cache_size, 0);
        }

        {
            // Let's reload the store to check the persistence system.
            // Note: store must be released before load another, because some background task could be still running.
            store.reset();
            store = reload();

            // read all columns from store
            const auto & columns = store->getTableColumns();
            BlockInputStreams ins = store->read(*db_context,
                                                db_context->getSettingsRef(),
                                                //                                                settings,
                                                columns,
                                                {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                                /* num_streams= */ 1,
                                                /* max_version= */ std::numeric_limits<UInt64>::max(),
                                                EMPTY_FILTER,
                                                TRACING_NAME,
                                                /* expected_block_size= */ 1024);
            ASSERT_EQ(ins.size(), 1UL);
            BlockInputStreamPtr in = ins[0];

            LOG_FMT_TRACE(&Poco::Logger::get(GET_GTEST_FULL_NAME), "start to check data of [1,{}]", num_rows_write_in_total);

            size_t num_rows_read = 0;
            in->readPrefix();
            Int64 expected_row_pk = 1;
            while (Block block = in->read())
            {
                num_rows_read += block.rows();
                for (auto && iter : block)
                {
                    auto c = iter.column;
                    if (iter.name == DMTestEnv::pk_name)
                    {
                        for (size_t i = 0; i < c->size(); ++i)
                        {
                            auto expected = expected_row_pk++;
                            auto value = c->getInt(i);
                            if (value != expected)
                            {
                                // Convenient for debug.
                                EXPECT_EQ(expected, value);
                            }
                        }
                    }
                }
            }
            in->readSuffix();
            ASSERT_EQ(num_rows_read, num_rows_write_in_total);

            LOG_FMT_TRACE(&Poco::Logger::get(GET_GTEST_FULL_NAME), "done checking data of [1,{}]", num_rows_write_in_total);
        }

        // Reading with a large number of small DTFile ingested will greatly slow down the testing
        if (num_rows_write_in_total >= 200)
            break;
    }
}
CATCH

INSTANTIATE_TEST_CASE_P(
    TestMode,
    DeltaMergeStoreRWTest,
    testing::Values(TestMode::V1_BlockOnly, TestMode::V2_BlockOnly, TestMode::V2_FileOnly, TestMode::V2_Mix),
    testModeToString);


class DeltaMergeStoreMergeDeltaBySegmentTest
    : public DB::base::TiFlashStorageTestBasic
    , public testing::WithParamInterface<std::tuple<UInt64 /* PageStorage version */, DMTestEnv::PkType>>
{
public:
    DeltaMergeStoreMergeDeltaBySegmentTest()
    {
        log = &Poco::Logger::get(DB::base::TiFlashStorageTestBasic::getCurrentFullTestName());
        std::tie(ps_ver, pk_type) = GetParam();
    }

    void SetUp() override
    {
        try
        {
            setStorageFormat(ps_ver);
            TiFlashStorageTestBasic::SetUp();

            setupDMStore();

            // Split into 4 segments.
            helper = std::make_unique<MultiSegmentTestUtil>(*db_context);
            helper->prepareSegments(store, 50, pk_type);
        }
        CATCH
    }

    void setupDMStore()
    {
        auto cols = DMTestEnv::getDefaultColumns(pk_type);
        store = std::make_shared<DeltaMergeStore>(*db_context,
                                                  false,
                                                  "test",
                                                  DB::base::TiFlashStorageTestBasic::getCurrentFullTestName(),
                                                  101,
                                                  *cols,
                                                  (*cols)[0],
                                                  pk_type == DMTestEnv::PkType::CommonHandle,
                                                  1,
                                                  DeltaMergeStore::Settings());
        dm_context = store->newDMContext(*db_context, db_context->getSettingsRef(), DB::base::TiFlashStorageTestBasic::getCurrentFullTestName());
    }

protected:
    std::unique_ptr<MultiSegmentTestUtil> helper;
    DeltaMergeStorePtr store;
    DMContextPtr dm_context;

    UInt64 ps_ver;
    DMTestEnv::PkType pk_type;

    [[maybe_unused]] Poco::Logger * log;
};

INSTANTIATE_TEST_CASE_P(
    ByPsVerAndPkType,
    DeltaMergeStoreMergeDeltaBySegmentTest,
    ::testing::Combine(
        ::testing::Values(2, 3),
        ::testing::Values(DMTestEnv::PkType::HiddenTiDBRowID, DMTestEnv::PkType::CommonHandle, DMTestEnv::PkType::PkIsHandleInt64)),
    [](const testing::TestParamInfo<std::tuple<UInt64 /* PageStorage version */, DMTestEnv::PkType>> & info) {
        const auto [ps_ver, pk_type] = info.param;
        return fmt::format("PsV{}_{}", ps_ver, DMTestEnv::PkTypeToString(pk_type));
    });


// The given key is the boundary of the segment.
TEST_P(DeltaMergeStoreMergeDeltaBySegmentTest, BoundaryKey)
try
{
    {
        // Write data to first 3 segments.
        auto newly_written_rows = helper->rows_by_segments[0] + helper->rows_by_segments[1] + helper->rows_by_segments[2];
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, newly_written_rows, false, pk_type, 5 /* new tso */);
        store->write(*db_context, db_context->getSettingsRef(), block);
        store->flushCache(dm_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));

        helper->expected_delta_rows[0] += helper->rows_by_segments[0];
        helper->expected_delta_rows[1] += helper->rows_by_segments[1];
        helper->expected_delta_rows[2] += helper->rows_by_segments[2];
        helper->verifyExpectedRowsForAllSegments();
    }
    if (store->isCommonHandle())
    {
        // Specifies MAX_KEY. nullopt should be returned.
        auto result = store->mergeDeltaBySegment(*db_context, RowKeyValue::COMMON_HANDLE_MAX_KEY, DeltaMergeStore::TaskRunThread::Foreground);
        ASSERT_EQ(result, std::nullopt);
    }
    else
    {
        // Specifies MAX_KEY. nullopt should be returned.
        auto result = store->mergeDeltaBySegment(*db_context, RowKeyValue::INT_HANDLE_MAX_KEY, DeltaMergeStore::TaskRunThread::Foreground);
        ASSERT_EQ(result, std::nullopt);
    }
    std::optional<RowKeyRange> result_1;
    {
        // Specifies MIN_KEY. In this case, the first segment should be processed.
        if (store->isCommonHandle())
        {
            result_1 = store->mergeDeltaBySegment(*db_context, RowKeyValue::COMMON_HANDLE_MIN_KEY, DeltaMergeStore::TaskRunThread::Foreground);
        }
        else
        {
            result_1 = store->mergeDeltaBySegment(*db_context, RowKeyValue::INT_HANDLE_MIN_KEY, DeltaMergeStore::TaskRunThread::Foreground);
        }
        // The returned range is the same as first segment's range.
        ASSERT_NE(result_1, std::nullopt);
        ASSERT_EQ(*result_1, store->segments.begin()->second->getRowKeyRange());

        helper->expected_stable_rows[0] += helper->expected_delta_rows[0];
        helper->expected_delta_rows[0] = 0;
        helper->verifyExpectedRowsForAllSegments();
    }
    {
        // Compact the first segment again, nothing should change.
        auto result = store->mergeDeltaBySegment(*db_context, result_1->start, DeltaMergeStore::TaskRunThread::Foreground);
        ASSERT_EQ(*result, *result_1);

        helper->verifyExpectedRowsForAllSegments();
    }
    std::optional<RowKeyRange> result_2;
    {
        // Compact again using the end key just returned. The second segment should be processed.
        result_2 = store->mergeDeltaBySegment(*db_context, result_1->end, DeltaMergeStore::TaskRunThread::Foreground);
        ASSERT_NE(result_2, std::nullopt);
        ASSERT_EQ(*result_2, std::next(store->segments.begin())->second->getRowKeyRange());

        helper->expected_stable_rows[1] += helper->expected_delta_rows[1];
        helper->expected_delta_rows[1] = 0;
        helper->verifyExpectedRowsForAllSegments();
    }
}
CATCH

TEST_P(DeltaMergeStoreMergeDeltaBySegmentTest, InvalidKey)
{
    // Expect exceptions when invalid key is given.
    EXPECT_ANY_THROW({
        if (store->isCommonHandle())
        {
            // For common handle, give int handle key and have a try
            store->mergeDeltaBySegment(*db_context, RowKeyValue::INT_HANDLE_MIN_KEY, DeltaMergeStore::TaskRunThread::Foreground);
        }
        else
        {
            // For int handle, give common handle key and have a try
            store->mergeDeltaBySegment(*db_context, RowKeyValue::COMMON_HANDLE_MIN_KEY, DeltaMergeStore::TaskRunThread::Foreground);
        }
    });
}


// Give the last segment key.
TEST_P(DeltaMergeStoreMergeDeltaBySegmentTest, LastSegment)
try
{
    std::optional<RowKeyRange> result;
    {
        auto it = std::next(store->segments.begin(), 3);
        ASSERT_NE(it, store->segments.end());
        auto seg = it->second;

        result = store->mergeDeltaBySegment(*db_context, seg->getRowKeyRange().start, DeltaMergeStore::TaskRunThread::Foreground);
        ASSERT_NE(result, std::nullopt);
        helper->verifyExpectedRowsForAllSegments();
    }
    {
        // As we are the last segment, compact "next segment" should result in failure. A nullopt is returned.
        auto result2 = store->mergeDeltaBySegment(*db_context, result->end, DeltaMergeStore::TaskRunThread::Foreground);
        ASSERT_EQ(result2, std::nullopt);
        helper->verifyExpectedRowsForAllSegments();
    }
}
CATCH


// The given key is not the boundary of the segment.
TEST_P(DeltaMergeStoreMergeDeltaBySegmentTest, NonBoundaryKey)
try
{
    {
        // Write data to first 3 segments.
        auto newly_written_rows = helper->rows_by_segments[0] + helper->rows_by_segments[1] + helper->rows_by_segments[2];
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, newly_written_rows, false, pk_type, 5 /* new tso */);
        store->write(*db_context, db_context->getSettingsRef(), block);
        store->flushCache(dm_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));

        helper->expected_delta_rows[0] += helper->rows_by_segments[0];
        helper->expected_delta_rows[1] += helper->rows_by_segments[1];
        helper->expected_delta_rows[2] += helper->rows_by_segments[2];
        helper->verifyExpectedRowsForAllSegments();
    }
    {
        // Compact segment[1] by giving a prefix-next key.
        auto range = std::next(store->segments.begin())->second->getRowKeyRange();
        auto compact_key = range.start.toPrefixNext();

        auto result = store->mergeDeltaBySegment(*db_context, compact_key, DeltaMergeStore::TaskRunThread::Foreground);
        ASSERT_NE(result, std::nullopt);

        helper->expected_stable_rows[1] += helper->expected_delta_rows[1];
        helper->expected_delta_rows[1] = 0;
        helper->verifyExpectedRowsForAllSegments();
    }
}
CATCH


// Verify that unflushed data will also be compacted.
TEST_P(DeltaMergeStoreMergeDeltaBySegmentTest, Flush)
try
{
    {
        // Write data to first 3 segments and flush.
        auto newly_written_rows = helper->rows_by_segments[0] + helper->rows_by_segments[1] + helper->rows_by_segments[2];
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, newly_written_rows, false, pk_type, 5 /* new tso */);
        store->write(*db_context, db_context->getSettingsRef(), block);
        store->flushCache(dm_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));

        helper->expected_delta_rows[0] += helper->rows_by_segments[0];
        helper->expected_delta_rows[1] += helper->rows_by_segments[1];
        helper->expected_delta_rows[2] += helper->rows_by_segments[2];
        helper->verifyExpectedRowsForAllSegments();

        auto segment1 = std::next(store->segments.begin())->second;
        ASSERT_EQ(segment1->getDelta()->getUnsavedRows(), 0);
    }
    {
        // Write new data to segment[1] without flush.
        auto newly_written_rows = helper->rows_by_segments[1];
        Block block = DMTestEnv::prepareSimpleWriteBlock(helper->rows_by_segments[0], helper->rows_by_segments[0] + newly_written_rows, false, pk_type, 10 /* new tso */);
        store->write(*db_context, db_context->getSettingsRef(), block);

        helper->expected_delta_rows[1] += helper->rows_by_segments[1];
        helper->verifyExpectedRowsForAllSegments();

        auto segment1 = std::next(store->segments.begin())->second;
        ASSERT_GT(segment1->getDelta()->getUnsavedRows(), 0);
    }
    {
        auto segment1 = std::next(store->segments.begin())->second;
        auto result = store->mergeDeltaBySegment(*db_context, segment1->getRowKeyRange().start, DeltaMergeStore::TaskRunThread::Foreground);
        ASSERT_NE(result, std::nullopt);

        segment1 = std::next(store->segments.begin())->second;
        ASSERT_EQ(*result, segment1->getRowKeyRange());

        helper->expected_stable_rows[1] += helper->expected_delta_rows[1];
        helper->expected_delta_rows[1] = 0;
        helper->verifyExpectedRowsForAllSegments();

        ASSERT_EQ(segment1->getDelta()->getUnsavedRows(), 0);
    }
}
CATCH


// There is another flush cache executing for the same segment.
TEST_P(DeltaMergeStoreMergeDeltaBySegmentTest, DISABLED_RetryByFlushCache)
try
{
    // In release-6.1, as https://github.com/pingcap/tiflash/commit/6da631c99c918bfffcf183128306a5e6bd35c7f7
    // is not cherry picked, when a flush is in commit stage, another flush will be just
    // waiting, instead of returning false immediately. The SyncPoint in this test case
    // is not suitable any more.

    {
        // Write new data to segment[1] without flush.
        auto newly_written_rows = helper->rows_by_segments[1];
        Block block = DMTestEnv::prepareSimpleWriteBlock(helper->rows_by_segments[0], helper->rows_by_segments[0] + newly_written_rows, false, pk_type, 10 /* new tso */);
        store->write(*db_context, db_context->getSettingsRef(), block);
        helper->expected_delta_rows[1] += helper->rows_by_segments[1];
        helper->verifyExpectedRowsForAllSegments();
    }

    auto sp_flush_commit = SyncPointCtl::enableInScope("before_ColumnFileFlushTask::commit");
    auto sp_merge_delta_retry = SyncPointCtl::enableInScope("before_DeltaMergeStore::mergeDeltaBySegment|retry_segment");

    // Start a flush and suspend it before flushCommit.
    auto th_flush = std::async([&]() {
        auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef(), "test");
        auto segment1 = std::next(store->segments.begin())->second;
        auto result = segment1->flushCache(*dm_context);
        ASSERT_TRUE(result);
        ASSERT_EQ(segment1->getDelta()->getUnsavedRows(), 0);
        // There should be still rows in the delta layer.
        ASSERT_GT(segment1->getDelta()->getRows(), 0);
        helper->verifyExpectedRowsForAllSegments();
    });
    sp_flush_commit.waitAndPause();

    // Start a mergeDelta. It should hit retry immediately due to a flush is in progress.
    auto th_merge_delta = std::async([&]() {
        auto segment1 = std::next(store->segments.begin())->second;
        auto result = store->mergeDeltaBySegment(*db_context, segment1->getRowKeyRange().start, DeltaMergeStore::TaskRunThread::Foreground);
        ASSERT_NE(result, std::nullopt);
        // All rows in the delta layer should be merged into the stable layer.
        helper->expected_stable_rows[1] += helper->expected_delta_rows[1];
        helper->expected_delta_rows[1] = 0;
        helper->verifyExpectedRowsForAllSegments();
    });
    sp_merge_delta_retry.waitAndPause();

    // Let's finish the flush.
    sp_flush_commit.next();
    th_flush.wait();

    // Proceed the mergeDelta retry. Retry should succeed without triggering any new flush.
    sp_merge_delta_retry.next();
    th_merge_delta.wait();
}
CATCH


// The segment is splitted during the execution.
TEST_P(DeltaMergeStoreMergeDeltaBySegmentTest, RetryBySplit)
try
{
    auto sp_split_prepare = SyncPointCtl::enableInScope("before_Segment::prepareSplit");
    auto sp_merge_delta_retry = SyncPointCtl::enableInScope("before_DeltaMergeStore::mergeDeltaBySegment|retry_segment");

    // Start a split and suspend it during prepareSplit to simulate a long-running split.
    auto th_split = std::async([&] {
        auto old_rows_by_segments = helper->rows_by_segments;
        ASSERT_EQ(4, old_rows_by_segments.size());

        // Split segment1 into 2.
        auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef(), "test");
        auto segment1 = std::next(store->segments.begin())->second;
        auto result = store->segmentSplit(*dm_context, segment1, /*is_foreground*/ true);
        ASSERT_NE(result.second, nullptr);

        helper->resetExpectedRows();
        ASSERT_EQ(5, helper->rows_by_segments.size());
        ASSERT_EQ(old_rows_by_segments[0], helper->rows_by_segments[0]);
        ASSERT_EQ(old_rows_by_segments[1], helper->rows_by_segments[1] + helper->rows_by_segments[2]);
        ASSERT_EQ(old_rows_by_segments[2], helper->rows_by_segments[3]);
        ASSERT_EQ(old_rows_by_segments[3], helper->rows_by_segments[4]);
    });
    sp_split_prepare.waitAndPause();

    // Start a mergeDelta. As there is a split in progress, we would expect several retries.
    auto th_merge_delta = std::async([&] {
        // mergeDeltaBySegment for segment1
        auto segment1 = std::next(store->segments.begin())->second;
        auto result = store->mergeDeltaBySegment(*db_context, segment1->getRowKeyRange().start, DeltaMergeStore::TaskRunThread::Foreground);
        ASSERT_NE(result, std::nullopt);

        // Although original segment1 has been split into 2, we still expect only segment1's delta
        // was merged.
        ASSERT_EQ(5, helper->rows_by_segments.size());
        helper->expected_stable_rows[1] += helper->expected_delta_rows[1];
        helper->expected_delta_rows[1] = 0;
        helper->verifyExpectedRowsForAllSegments();
    });
    sp_merge_delta_retry.waitAndNext();
    sp_merge_delta_retry.waitAndNext();
    sp_merge_delta_retry.waitAndPause();

    // Proceed and finish the split.
    sp_split_prepare.next();
    th_split.wait();
    {
        // Write to the new segment1 + segment2 after split.
        auto newly_written_rows = helper->rows_by_segments[1] + helper->rows_by_segments[2];
        Block block = DMTestEnv::prepareSimpleWriteBlock(helper->rows_by_segments[0], helper->rows_by_segments[0] + newly_written_rows, false, pk_type, 10 /* new tso */);
        store->write(*db_context, db_context->getSettingsRef(), block);
        helper->expected_delta_rows[1] += helper->rows_by_segments[1];
        helper->expected_delta_rows[2] += helper->rows_by_segments[2];
        helper->verifyExpectedRowsForAllSegments();
    }

    // This time the retry should succeed without any future retries.
    sp_merge_delta_retry.next();
    th_merge_delta.wait();
}
CATCH


} // namespace tests
} // namespace DM
} // namespace DB
