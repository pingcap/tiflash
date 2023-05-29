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

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/MyTime.h>
#include <Common/SyncPoint/SyncPoint.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/PKSquashingBlockInputStream.h>
#include <Storages/DeltaMerge/ReadThread/UnorderedInputStream.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/DeltaMerge/tests/gtest_dm_delta_merge_store_test_basic.h>
#include <Storages/PathPool.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/InputStreamTestUtils.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <common/logger_useful.h>
#include <common/types.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <future>
#include <iterator>
#include <random>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
} // namespace ErrorCodes

namespace FailPoints
{
extern const char pause_before_dt_background_delta_merge[];
extern const char pause_until_dt_background_delta_merge[];
extern const char force_triggle_background_merge_delta[];
extern const char force_triggle_foreground_flush[];
extern const char force_set_segment_ingest_packs_fail[];
extern const char segment_merge_after_ingest_packs[];
extern const char force_set_segment_physical_split[];
extern const char force_set_page_file_write_errno[];
} // namespace FailPoints

namespace DM
{
namespace tests
{
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
        ASSERT_EQ(cols.size(), 3);
    }
}
CATCH

TEST_F(DeltaMergeStoreTest, ShutdownInMiddleDTFileGC)
try
{
    // create table
    ASSERT_NE(store, nullptr);

    auto global_page_storage = TiFlashTestEnv::getGlobalContext().getGlobalStoragePool();

    // Start a PageStorage gc and suspend it before clean external page
    auto sp_gc = SyncPointCtl::enableInScope("before_PageStorageImpl::cleanExternalPage_execute_callbacks");
    auto th_gc = std::async([&]() {
        if (global_page_storage)
            global_page_storage->gc();
    });
    sp_gc.waitAndPause();

    {
        // check column structure of store
        const auto & cols = store->getTableColumns();
        // version & tag column added
        ASSERT_EQ(cols.size(), 3);
    }

    auto provider = db_context->getFileProvider();
    const auto paths = getAllStorePaths();
    auto get_num_stable_files = [&]() -> size_t {
        size_t total_num_dtfiles = 0;
        auto options = DMFile::ListOptions{.only_list_can_gc = true, .clean_up = false};
        for (const auto & root_path : paths)
        {
            auto file_ids = DMFile::listAllInPath(provider, root_path, options);
            total_num_dtfiles += file_ids.size();
        }
        return total_num_dtfiles;
    };

    const size_t num_before_shutdown = get_num_stable_files();
    ASSERT_GT(num_before_shutdown, 0);

    // shutdown the table in the middle of page storage gc, but not dropped
    store->shutdown();

    const size_t num_after_shutdown = get_num_stable_files();
    ASSERT_EQ(num_before_shutdown, num_after_shutdown);

    sp_gc.next(); // continue the page storage gc
    th_gc.get();

    const size_t num_after_gc = get_num_stable_files();
    ASSERT_EQ(num_before_shutdown, num_after_gc);
}
CATCH

TEST_F(DeltaMergeStoreTest, DroppedInMiddleDTFileGC)
try
{
    // create table
    ASSERT_NE(store, nullptr);

    auto global_page_storage = TiFlashTestEnv::getGlobalContext().getGlobalStoragePool();

    // Start a PageStorage gc and suspend it before clean external page
    auto sp_gc = SyncPointCtl::enableInScope("before_PageStorageImpl::cleanExternalPage_execute_callbacks");
    auto th_gc = std::async([&]() {
        if (global_page_storage)
            global_page_storage->gc();
    });
    sp_gc.waitAndPause();

    {
        // check column structure of store
        const auto & cols = store->getTableColumns();
        // version & tag column added
        ASSERT_EQ(cols.size(), 3);
    }

    // drop table in the middle of page storage gc
    store->shutdown();
    store = nullptr;

    sp_gc.next(); // continue the page storage gc
    th_gc.get();

    // Mark: ensure this test won't run into crash by nullptr
}
CATCH

TEST_F(DeltaMergeStoreTest, DroppedInMiddleDTFileRemoveCallback)
try
{
    // create table
    ASSERT_NE(store, nullptr);

    auto global_page_storage = TiFlashTestEnv::getGlobalContext().getGlobalStoragePool();

    // Start a PageStorage gc and suspend it before removing dtfiles
    auto sp_gc = SyncPointCtl::enableInScope("before_DeltaMergeStore::callbacks_remover_remove");
    auto th_gc = std::async([&]() {
        if (global_page_storage)
            global_page_storage->gc();
    });
    sp_gc.waitAndPause();

    {
        // check column structure of store
        const auto & cols = store->getTableColumns();
        // version & tag column added
        ASSERT_EQ(cols.size(), 3);
    }

    // drop table and files in the middle of page storage gc
    store->drop();
    store = nullptr;

    sp_gc.next(); // continue removing dtfiles
    th_gc.get();
}
CATCH

TEST_F(DeltaMergeStoreTest, CreateInMiddleDTFileGC)
try
{
    // create table
    ASSERT_NE(store, nullptr);

    auto global_page_storage = TiFlashTestEnv::getGlobalContext().getGlobalStoragePool();

    // Start a PageStorage gc and suspend it before clean external page
    auto sp_gc = SyncPointCtl::enableInScope("before_PageStorageImpl::cleanExternalPage_execute_callbacks");
    auto th_gc = std::async([&]() {
        if (global_page_storage)
            global_page_storage->gc();
    });
    sp_gc.waitAndPause();

    DeltaMergeStorePtr new_store;
    ColumnDefinesPtr new_cols;
    {
        new_cols = DMTestEnv::getDefaultColumns();
        ColumnDefine handle_column_define = (*new_cols)[0];
        new_store = std::make_shared<DeltaMergeStore>(*db_context,
                                                      false,
                                                      "test",
                                                      "t_200",
                                                      NullspaceID,
                                                      200,
                                                      true,
                                                      *new_cols,
                                                      handle_column_define,
                                                      false,
                                                      1,
                                                      DeltaMergeStore::Settings());
        auto block = DMTestEnv::prepareSimpleWriteBlock(0, 100, false);
        new_store->write(*db_context, db_context->getSettingsRef(), block);
        new_store->flushCache(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));
    }

    sp_gc.next(); // continue the page storage gc
    th_gc.get();

    BlockInputStreamPtr in = new_store->read(*db_context,
                                             db_context->getSettingsRef(),
                                             *new_cols,
                                             {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                             /* num_streams= */ 1,
                                             /* max_version= */ std::numeric_limits<UInt64>::max(),
                                             EMPTY_FILTER,
                                             std::vector<RuntimeFilterPtr>{},
                                             0,
                                             "",
                                             /* keep_order= */ false,
                                             /* is_fast_scan= */ false,
                                             /* expected_block_size= */ 1024)[0];
    ASSERT_INPUTSTREAM_NROWS(in, 100);
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
        SCOPED_TRACE(fmt::format("Test case for {}", DMTestEnv::PkTypeToString(pk_type)));
        LOG_INFO(log, "Test case for {} begin.", DMTestEnv::PkTypeToString(pk_type));

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
        ASSERT_NO_THROW({ block1.checkNumberOfRows(); });

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
        ASSERT_NO_THROW({ block2.checkNumberOfRows(); });


        BlockInputStreamPtr stream = std::make_shared<BlocksListBlockInputStream>(BlocksList{block1, block2});
        stream = std::make_shared<PKSquashingBlockInputStream<false>>(stream, EXTRA_HANDLE_COLUMN_ID, store->isCommonHandle());
        ASSERT_INPUTSTREAM_NROWS(stream, nrows + nrows_2);

        LOG_INFO(log, "Test case for {} done.", DMTestEnv::PkTypeToString(pk_type));
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
                                             std::vector<RuntimeFilterPtr>{},
                                             0,
                                             TRACING_NAME,
                                             /* keep_order= */ false,
                                             /* is_fast_scan= */ false,
                                             /* expected_block_size= */ 1024)[0];
        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name, col_str_define.name, col_i8_define.name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
                createColumn<String>(createNumberStrings(0, num_rows_write)),
                createColumn<Int8>(createSignedNumbers(0, num_rows_write)),
            }));
    }

    {
        // test readRaw
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->readRaw(*db_context, db_context->getSettingsRef(), columns, 1, /* keep_order= */ false)[0];
        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name, col_str_define.name, col_i8_define.name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
                createColumn<String>(createNumberStrings(0, num_rows_write)),
                createColumn<Int8>(createSignedNumbers(0, num_rows_write)),
            }));
    }
}
CATCH

TEST_P(DeltaMergeStoreRWTest, SimpleReadCheckTableScanContext)
try
{
    const ColumnDefine col_a_define(2, "col_a", std::make_shared<DataTypeInt64>());
    {
        auto table_column_defines = DMTestEnv::getDefaultColumns();
        table_column_defines->emplace_back(col_a_define);

        store = reload(table_column_defines);
    }

    const size_t num_rows_write = 50000;
    {
        // write to store
        Block block;
        {
            block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
            block.insert(DB::tests::createColumn<Int64>(
                createSignedNumbers(0, num_rows_write),
                col_a_define.name,
                col_a_define.id));
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

    // merge into stable layer
    store->flushCache(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));
    store->compact(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));
    store->mergeDeltaAll(*db_context);

    const auto & columns = store->getTableColumns();
    auto scan_context = std::make_shared<ScanContext>();
    auto in = store->read(*db_context,
                          db_context->getSettingsRef(),
                          columns,
                          {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                          /* num_streams= */ 1,
                          /* max_version= */ std::numeric_limits<UInt64>::max(),
                          EMPTY_FILTER,
                          std::vector<RuntimeFilterPtr>{},
                          0,
                          TRACING_NAME,
                          /* keep_order= */ false,
                          /* is_fast_scan= */ false,
                          /* expected_block_size= */ 1024,
                          /* read_segments */ {},
                          /* extra_table_id_index */ InvalidColumnID,
                          /* scan_context */ scan_context)[0];
    in->readPrefix();
    while (in->read()) {};
    in->readSuffix();

    ASSERT_EQ(scan_context->total_dmfile_scanned_packs, 7);
    ASSERT_EQ(scan_context->total_dmfile_scanned_rows, 50000);
    ASSERT_EQ(scan_context->total_dmfile_skipped_packs, 0);
    ASSERT_EQ(scan_context->total_dmfile_skipped_rows, 0);

    auto filter = createGreater(Attr{col_a_define.name, col_a_define.id, DataTypeFactory::instance().get("Int64")}, Field(static_cast<Int64>(10000)), 0);
    scan_context = std::make_shared<ScanContext>();
    in = store->read(*db_context,
                     db_context->getSettingsRef(),
                     columns,
                     {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                     /* num_streams= */ 1,
                     /* max_version= */ std::numeric_limits<UInt64>::max(),
                     std::make_shared<PushDownFilter>(filter),
                     std::vector<RuntimeFilterPtr>{},
                     0,
                     TRACING_NAME,
                     /* keep_order= */ false,
                     /* is_fast_scan= */ false,
                     /* expected_block_size= */ 1024,
                     /* read_segments */ {},
                     /* extra_table_id_index */ InvalidColumnID,
                     /* scan_context */ scan_context)[0];

    in->readPrefix();
    while (in->read()) {};
    in->readSuffix();

    ASSERT_EQ(scan_context->total_dmfile_scanned_packs, 6);
    ASSERT_EQ(scan_context->total_dmfile_scanned_rows, 41808);
    ASSERT_EQ(scan_context->total_dmfile_skipped_packs, 1);
    ASSERT_EQ(scan_context->total_dmfile_skipped_rows, 8192);
}
CATCH


TEST_P(DeltaMergeStoreRWTest, WriteCrashBeforeWalWithoutCache)
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
        db_context->getSettingsRef().dt_segment_delta_cache_limit_rows = 8;
        FailPointHelper::enableFailPoint(FailPoints::force_set_page_file_write_errno);
        ASSERT_THROW(store->write(*db_context, db_context->getSettingsRef(), block), DB::Exception);
        try
        {
            store->write(*db_context, db_context->getSettingsRef(), block);
        }
        catch (DB::Exception & e)
        {
            if (e.code() != ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR)
                throw;
        }
    }
    FailPointHelper::disableFailPoint(FailPoints::force_set_page_file_write_errno);

    {
        // read all columns from store
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(*db_context,
                                             db_context->getSettingsRef(),
                                             columns,
                                             {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                             /* num_streams= */ 1,
                                             /* max_version= */ std::numeric_limits<UInt64>::max(),
                                             EMPTY_FILTER,
                                             std::vector<RuntimeFilterPtr>{},
                                             0,
                                             TRACING_NAME,
                                             /* keep_order= */ false,
                                             /* is_fast_scan= */ false,
                                             /* expected_block_size= */ 1024)[0];
        ASSERT_INPUTSTREAM_NROWS(in, 0);
    }
}
CATCH

TEST_P(DeltaMergeStoreRWTest, WriteCrashBeforeWalWithCache)
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

        FailPointHelper::enableFailPoint(FailPoints::force_set_page_file_write_errno);
        store->write(*db_context, db_context->getSettingsRef(), block);
        ASSERT_THROW(store->flushCache(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())), DB::Exception);
        try
        {
            store->flushCache(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));
        }
        catch (DB::Exception & e)
        {
            if (e.code() != ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR)
                throw;
        }
    }
    FailPointHelper::disableFailPoint(FailPoints::force_set_page_file_write_errno);

    {
        // read all columns from store
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(*db_context,
                                             db_context->getSettingsRef(),
                                             columns,
                                             {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                             /* num_streams= */ 1,
                                             /* max_version= */ std::numeric_limits<UInt64>::max(),
                                             EMPTY_FILTER,
                                             std::vector<RuntimeFilterPtr>{},
                                             0,
                                             TRACING_NAME,
                                             /* keep_order= */ false,
                                             /* is_fast_scan= */ false,
                                             /* expected_block_size= */ 1024)[0];
        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name, col_str_define.name, col_i8_define.name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
                createColumn<String>(createNumberStrings(0, num_rows_write)),
                createColumn<Int8>(createSignedNumbers(0, num_rows_write)),
            }));
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
                                             std::vector<RuntimeFilterPtr>{},
                                             0,
                                             TRACING_NAME,
                                             /* keep_order= */ false,
                                             /* is_fast_scan= */ false,
                                             /* expected_block_size= */ 1024)[0];
        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
            }));
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
                                             std::vector<RuntimeFilterPtr>{},
                                             0,
                                             TRACING_NAME,
                                             /* keep_order= */ false,
                                             /* is_fast_scan= */ false,
                                             /* expected_block_size= */ 1024)[0];
        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                // Range after deletion is [64, 128)
                createColumn<Int64>(createNumbers<Int64>(num_deleted_rows, num_rows_write)),
            }));
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
                                             std::vector<RuntimeFilterPtr>{},
                                             0,
                                             TRACING_NAME,
                                             /* keep_order= */ false,
                                             /* is_fast_scan= */ false,
                                             /* expected_block_size= */ 1024)[0];
        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, 3 * num_write_rows)),
            }));
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
            store->ingestFiles(dm_context, range1, {file_ids1}, false);
            auto [range2, file_ids2] = genDMFile(*dm_context, block2);
            store->ingestFiles(dm_context, range2, {file_ids2}, false);
            auto [range3, file_ids3] = genDMFile(*dm_context, block3);
            store->ingestFiles(dm_context, range3, {file_ids3}, false);
            break;
        }
        case TestMode::V2_Mix:
        {
            store->write(*db_context, db_context->getSettingsRef(), block2);

            auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
            auto [range1, file_ids1] = genDMFile(*dm_context, block1);
            store->ingestFiles(dm_context, range1, {file_ids1}, false);
            auto [range3, file_ids3] = genDMFile(*dm_context, block3);
            store->ingestFiles(dm_context, range3, {file_ids3}, false);
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
                                             std::vector<RuntimeFilterPtr>{},
                                             0,
                                             TRACING_NAME,
                                             /* keep_order= */ false,
                                             /* is_fast_scan= */ false,
                                             /* expected_block_size= */ 1024)[0];
        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, 3 * num_write_rows)),
            }));
    }
    // Read with version
    {
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(*db_context,
                                             db_context->getSettingsRef(),
                                             columns,
                                             {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                             /* num_streams= */ 1,
                                             /* max_version= */ static_cast<UInt64>(1),
                                             EMPTY_FILTER,
                                             std::vector<RuntimeFilterPtr>{},
                                             0,
                                             TRACING_NAME,
                                             /* keep_order= */ false,
                                             /* is_fast_scan= */ false,
                                             /* expected_block_size= */ 1024)[0];
        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, 2 * num_write_rows)),
            }));
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
                                             std::vector<RuntimeFilterPtr>{},
                                             0,
                                             TRACING_NAME,
                                             /* keep_order= */ false,
                                             /* is_fast_scan= */ false,
                                             /* expected_block_size= */ 1024)[0];
        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, 8)),
            }));
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
                                             std::vector<RuntimeFilterPtr>{},
                                             0,
                                             TRACING_NAME,
                                             /* keep_order= */ false,
                                             /* is_fast_scan= */ false,
                                             /* expected_block_size= */ 1024)[0];
        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, 9)),
            }));
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
                                            std::vector<RuntimeFilterPtr>{},
                                            0,
                                            TRACING_NAME,
                                            /* keep_order= */ false,
                                            /* is_fast_scan= */ false,
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1UL);
        BlockInputStreamPtr in = ins[0];
        ASSERT_INPUTSTREAM_NROWS(in, num_rows_tso1 + num_rows_tso2);
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
                                            std::vector<RuntimeFilterPtr>{},
                                            0,
                                            TRACING_NAME,
                                            /* keep_order= */ false,
                                            /* is_fast_scan= */ false,
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1UL);
        BlockInputStreamPtr in = ins[0];
        ASSERT_INPUTSTREAM_NROWS(in, num_rows_tso1 + num_rows_tso2);
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
                                            std::vector<RuntimeFilterPtr>{},
                                            0,
                                            TRACING_NAME,
                                            /* keep_order= */ false,
                                            /* is_fast_scan= */ false,
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1UL);
        BlockInputStreamPtr in = ins[0];
        ASSERT_INPUTSTREAM_NROWS(in, num_rows_tso1);
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
                                            std::vector<RuntimeFilterPtr>{},
                                            0,
                                            TRACING_NAME,
                                            /* keep_order= */ false,
                                            /* is_fast_scan= */ false,
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1UL);
        BlockInputStreamPtr in = ins[0];
        ASSERT_INPUTSTREAM_NROWS(in, 0);
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
                                            std::vector<RuntimeFilterPtr>{},
                                            0,
                                            TRACING_NAME,
                                            /* keep_order= */ false,
                                            /* is_fast_scan= */ false,
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1UL);
        BlockInputStreamPtr in = ins[0];
        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name, VERSION_COLUMN_NAME}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, 32)),
                createColumn<UInt64>(std::vector<UInt64>(32, tso1)),
            }))
            << "Data [32, 128) before ingest should be erased, should only get [0, 32)";
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
                                            std::vector<RuntimeFilterPtr>{},
                                            0,
                                            TRACING_NAME,
                                            /* keep_order= */ false,
                                            /* is_fast_scan= */ false,
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1UL);
        BlockInputStreamPtr in = ins[0];
        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name, VERSION_COLUMN_NAME}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, 32)),
                createColumn<UInt64>(std::vector<UInt64>(32, tso1)),
            }))
            << fmt::format("Data [32, 128) after ingest with tso less than: {} are erased, should only get [0, 32)", tso2);
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
                                            std::vector<RuntimeFilterPtr>{},
                                            0,
                                            TRACING_NAME,
                                            /* keep_order= */ false,
                                            /* is_fast_scan= */ false,
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1UL);
        BlockInputStreamPtr in = ins[0];
        ASSERT_INPUTSTREAM_NROWS(in, 32 + 16) << fmt::format("The rows number after ingest with tso less than {} is not match", tso3);
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
                                            std::vector<RuntimeFilterPtr>{},
                                            0,
                                            TRACING_NAME,
                                            /* keep_order= */ false,
                                            /* is_fast_scan= */ false,
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1UL);
        BlockInputStreamPtr in = ins[0];
        ASSERT_INPUTSTREAM_NROWS(in, 32 + (48 - 32) + (256 - 80)) << "The rows number after ingest is not match";
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
                                            std::vector<RuntimeFilterPtr>{},
                                            0,
                                            TRACING_NAME,
                                            /* keep_order= */ false,
                                            /* is_fast_scan= */ false,
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1UL);
        BlockInputStreamPtr in = ins[0];
        ASSERT_INPUTSTREAM_NROWS(in, 2) << "The rows number of two point get is not match";
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
        store->segmentSplit(*dm_context, seg, DeltaMergeStore::SegmentSplitReason::ForegroundWrite);
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
                                            std::vector<RuntimeFilterPtr>{},
                                            0,
                                            TRACING_NAME,
                                            /* keep_order= */ false,
                                            /* is_fast_scan= */ false,
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1);
        BlockInputStreamPtr in = ins[0];
        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name, VERSION_COLUMN_NAME}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, 32)),
                createColumn<UInt64>(std::vector<UInt64>(32, tso1)),
            }))
            << "Data [32, 128) before ingest should be erased, should only get [0, 32)";
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
                                            std::vector<RuntimeFilterPtr>{},
                                            0,
                                            TRACING_NAME,
                                            /* keep_order= */ false,
                                            /* is_fast_scan= */ false,
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1);
        BlockInputStreamPtr in = ins[0];
        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name, VERSION_COLUMN_NAME}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, 32)),
                createColumn<UInt64>(std::vector<UInt64>(32, tso1)),
            }))
            << fmt::format("Data [32, 128) after ingest with tso less than: {} are erased, should only get [0, 32)", tso2);
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
                                            std::vector<RuntimeFilterPtr>{},
                                            0,
                                            TRACING_NAME,
                                            /* keep_order= */ false,
                                            /* is_fast_scan= */ false,
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1);
        BlockInputStreamPtr in = ins[0];
        ASSERT_INPUTSTREAM_NROWS(in, 32 + 128 - 32) << "The rows number after ingest is not match";
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
    std::default_random_engine random;
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
                if ((random() % 2) == 0)
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
                                                std::vector<RuntimeFilterPtr>{},
                                                0,
                                                TRACING_NAME,
                                                /* keep_order= */ false,
                                                /* is_fast_scan= */ false,
                                                /* expected_block_size= */ 1024);
            ASSERT_EQ(ins.size(), 1UL);
            BlockInputStreamPtr in = ins[0];

            LOG_TRACE(&Poco::Logger::get(GET_GTEST_FULL_NAME), "start to check data of [1,{}]", num_rows_write_in_total);
            ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
                in,
                Strings({DMTestEnv::pk_name}),
                createColumns({
                    createColumn<Int64>(createNumbers<Int64>(1, num_rows_write_in_total + 1)),
                }));

            LOG_TRACE(&Poco::Logger::get(GET_GTEST_FULL_NAME), "done checking data of [1,{}]", num_rows_write_in_total);
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
                                            std::vector<RuntimeFilterPtr>{},
                                            0,
                                            TRACING_NAME,
                                            /* keep_order= */ false,
                                            /* is_fast_scan= */ false,
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
        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name, col_name_ddl}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
                createColumn<Int32>(createSignedNumbers(0, num_rows_write)),
            }));
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
                                            std::vector<RuntimeFilterPtr>{},
                                            0,
                                            TRACING_NAME,
                                            /* keep_order= */ false,
                                            /* is_fast_scan= */ false,
                                            /* expected_block_size= */ 1024);
        ASSERT_EQ(ins.size(), 1UL);
        BlockInputStreamPtr & in = ins[0];
        {
            const Block head = in->getHeader();
            ASSERT_FALSE(head.has(col_name_to_drop));
        }
        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
            }));
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
                                            std::vector<RuntimeFilterPtr>{},
                                            0,
                                            TRACING_NAME,
                                            /* keep_order= */ false,
                                            /* is_fast_scan= */ false,
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
        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name, col_name_c1, col_name_to_add}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
                createColumn<Int8>(createSignedNumbers(0, num_rows_write)),
                createColumn<Int32>(std::vector<Int64>(num_rows_write, 0)),
            }));
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
                              std::vector<RuntimeFilterPtr>{},
                              0,
                              TRACING_NAME,
                              /* keep_order= */ false,
                              /* is_fast_scan= */ false,
                              /* expected_block_size= */ 1024)[0];

        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name, col_name_to_add}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
                createColumn<Float64>(std::vector<Float64>(num_rows_write, 1.123456)),
            }));
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
                              std::vector<RuntimeFilterPtr>{},
                              0,
                              TRACING_NAME,
                              /* keep_order= */ false,
                              /* is_fast_scan= */ false,
                              /* expected_block_size= */ 1024)[0];

        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name, col_name_to_add}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
                createColumn<Float64>(std::vector<Float64>(num_rows_write, 1.123456)),
            }));
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
                              std::vector<RuntimeFilterPtr>{},
                              0,
                              TRACING_NAME,
                              /* keep_order= */ false,
                              /* is_fast_scan= */ false,
                              /* expected_block_size= */ 1024)[0];
        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name, col_name_to_add}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
                createColumn<Float32>(std::vector<Float64>(num_rows_write, 1.125)),
            }));
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
                              std::vector<RuntimeFilterPtr>{},
                              0,
                              TRACING_NAME,
                              /* keep_order= */ false,
                              /* is_fast_scan= */ false,
                              /* expected_block_size= */ 1024)[0];
        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name, col_name_to_add}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
                createColumn<Int8>(std::vector<Int64>(num_rows_write, 1)),
            }));
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
                              std::vector<RuntimeFilterPtr>{},
                              0,
                              TRACING_NAME,
                              /* keep_order= */ false,
                              /* is_fast_scan= */ false,
                              /* expected_block_size= */ 1024)[0];

        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name, col_name_to_add}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
                createColumn<UInt8>(std::vector<UInt64>(num_rows_write, 1)),
            }));
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
                              std::vector<RuntimeFilterPtr>{},
                              0,
                              TRACING_NAME,
                              /* keep_order= */ false,
                              /* is_fast_scan= */ false,
                              /* expected_block_size= */ 1024)[0];

        std::vector<DataTypeMyDateTime::FieldType> datetime_data(
            num_rows_write,
            MyDateTime(1999, 9, 9, 12, 34, 56, 0).toPackedUInt());

        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name, col_name_to_add}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
                createColumn<MyDateTime>(/*data_type_args=*/std::make_tuple(0), datetime_data),
            }));
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
                              std::vector<RuntimeFilterPtr>{},
                              0,
                              TRACING_NAME,
                              /* keep_order= */ false,
                              /* is_fast_scan= */ false,
                              /* expected_block_size= */ 1024)[0];
        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name, col_name_to_add}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
                createColumn<String>(Strings(num_rows_write, "test_add_string_col")),
            }));
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
                                            std::vector<RuntimeFilterPtr>{},
                                            0,
                                            TRACING_NAME,
                                            /* keep_order= */ false,
                                            /* is_fast_scan= */ false,
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

        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name, col_name_after_ddl}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
                createColumn<Int32>(createSignedNumbers(0, num_rows_write)),
            }));
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
                                            std::vector<RuntimeFilterPtr>{},
                                            0,
                                            TRACING_NAME,
                                            /* keep_order= */ false,
                                            /* is_fast_scan= */ false,
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
        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({col_name_after_ddl}),
            createColumns({
                createColumn<Int32>(createNumbers<Int64>(0, num_rows_write)),
            }));
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
                                                std::vector<RuntimeFilterPtr>{},
                                                0,
                                                TRACING_NAME,
                                                /* keep_order= */ false,
                                                /* is_fast_scan= */ false,
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
            ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
                in,
                Strings({col_name_after_ddl}),
                createColumns({
                    createColumn<Int32>(createNumbers<Int64>(0, num_rows_write * 2)),
                }));
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
                              std::vector<RuntimeFilterPtr>{},
                              0,
                              TRACING_NAME,
                              /* keep_order= */ false,
                              /* is_fast_scan= */ false,
                              /* expected_block_size= */ 1024)[0];
        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name, col_name_to_add}),
            createColumns({
                createColumn<Int64>(std::vector<Int64>{0}),
                createColumn<Float32>(std::vector<Float64>{1.125}),
            }));
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
                              std::vector<RuntimeFilterPtr>{},
                              0,
                              TRACING_NAME,
                              /* keep_order= */ false,
                              /* is_fast_scan= */ false,
                              /* expected_block_size= */ 1024)[0];

        // FIXME!!!
        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name, col_name_to_add}),
            createColumns({
                createColumn<Int64>(std::vector<Int64>{0, 1}),
                createColumn<Float32>(std::vector<Float64>{1.125, 3.1415}),
            }));
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
        const auto & h = store->getHandle();
        ASSERT_EQ(h.name, EXTRA_HANDLE_COLUMN_NAME);
        ASSERT_EQ(h.id, EXTRA_HANDLE_COLUMN_ID);
        ASSERT_TRUE(h.type->equals(*EXTRA_HANDLE_COLUMN_STRING_TYPE));
    }
    {
        // check column structure of store
        const auto & cols = store->getTableColumns();
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
    const size_t rowkey_column_size = 2;
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
                                             std::vector<RuntimeFilterPtr>{},
                                             0,
                                             TRACING_NAME,
                                             /* keep_order= */ false,
                                             /* is_fast_scan= */ false,
                                             /* expected_block_size= */ 1024)[0];

        // mock common handle
        auto common_handle_coldata = []() {
            auto tmp = createNumbers<Int64>(0, num_rows_write);
            Strings res;
            std::transform(tmp.begin(), tmp.end(), std::back_inserter(res), [](Int64 v) { return genMockCommonHandle(v, rowkey_column_size); });
            return res;
        }();
        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name, col_i8_define.name, col_str_define.name}),
            createColumns({
                createColumn<String>(common_handle_coldata),
                createColumn<Int8>(createSignedNumbers(0, num_rows_write)),
                createColumn<String>(createNumberStrings(0, num_rows_write)),
            }));
    }

    {
        // test readRaw
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->readRaw(*db_context, db_context->getSettingsRef(), columns, 1, /* keep_order= */ false)[0];
        // mock common handle
        auto common_handle_coldata = []() {
            auto tmp = createNumbers<Int64>(0, num_rows_write);
            Strings res;
            std::transform(tmp.begin(), tmp.end(), std::back_inserter(res), [](Int64 v) { return genMockCommonHandle(v, rowkey_column_size); });
            return res;
        }();
        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name, col_i8_define.name, col_str_define.name}),
            createColumns({
                createColumn<String>(common_handle_coldata),
                createColumn<Int8>(createSignedNumbers(0, num_rows_write)),
                createColumn<String>(createNumberStrings(0, num_rows_write)),
            }));
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
                                             std::vector<RuntimeFilterPtr>{},
                                             0,
                                             TRACING_NAME,
                                             /* keep_order= */ false,
                                             /* is_fast_scan= */ false,
                                             /* expected_block_size= */ 1024)[0];
        // mock common handle
        auto common_handle_coldata = []() {
            auto tmp = createNumbers<Int64>(0, 3 * num_write_rows);
            Strings res;
            std::transform(tmp.begin(), tmp.end(), std::back_inserter(res), [](Int64 v) { return genMockCommonHandle(v, rowkey_column_size); });
            return res;
        }();
        ASSERT_EQ(common_handle_coldata.size(), 3 * num_write_rows);
        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<String>(common_handle_coldata),
            }));
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
                                             std::vector<RuntimeFilterPtr>{},
                                             0,
                                             TRACING_NAME,
                                             /* keep_order= */ false,
                                             /* is_fast_scan= */ false,
                                             /* expected_block_size= */ 1024)[0];
        // mock common handle
        auto common_handle_coldata = []() {
            auto tmp = createNumbers<Int64>(0, 3 * num_write_rows);
            Strings res;
            std::transform(tmp.begin(), tmp.end(), std::back_inserter(res), [](Int64 v) { return genMockCommonHandle(v, rowkey_column_size); });
            return res;
        }();
        ASSERT_EQ(common_handle_coldata.size(), 3 * num_write_rows);
        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<String>(common_handle_coldata),
            }));
    }
    // Read with version
    {
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(*db_context,
                                             db_context->getSettingsRef(),
                                             columns,
                                             {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                             /* num_streams= */ 1,
                                             /* max_version= */ static_cast<UInt64>(1),
                                             EMPTY_FILTER,
                                             std::vector<RuntimeFilterPtr>{},
                                             0,
                                             TRACING_NAME,
                                             /* keep_order= */ false,
                                             /* is_fast_scan= */ false,
                                             /* expected_block_size= */ 1024)[0];
        // mock common handle
        auto common_handle_coldata = []() {
            auto tmp = createNumbers<Int64>(0, 2 * num_write_rows);
            Strings res;
            std::transform(tmp.begin(), tmp.end(), std::back_inserter(res), [](Int64 v) { return genMockCommonHandle(v, rowkey_column_size); });
            return res;
        }();
        ASSERT_EQ(common_handle_coldata.size(), 2 * num_write_rows);
        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<String>(common_handle_coldata),
            }));
    }
}
CATCH

TEST_P(DeltaMergeStoreRWTest, DeleteReadWithCommonHandle)
try
{
    const size_t num_rows_write = 128;
    const size_t rowkey_column_size = 2;
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
                                             std::vector<RuntimeFilterPtr>{},
                                             0,
                                             TRACING_NAME,
                                             /* keep_order= */ false,
                                             /* is_fast_scan= */ false,
                                             /* expected_block_size= */ 1024)[0];
        // mock common handle
        auto common_handle_coldata = []() {
            auto tmp = createNumbers<Int64>(0, num_rows_write);
            Strings res;
            std::transform(tmp.begin(), tmp.end(), std::back_inserter(res), [](Int64 v) { return genMockCommonHandle(v, rowkey_column_size); });
            return res;
        }();
        ASSERT_EQ(common_handle_coldata.size(), num_rows_write);
        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<String>(common_handle_coldata),
            }));
    }
    // Delete range [0, 64)
    const size_t num_deleted_rows = 64;
    {
        RowKeyValue start(true, std::make_shared<String>(genMockCommonHandle(0, rowkey_column_size)));
        RowKeyValue end(true, std::make_shared<String>(genMockCommonHandle(num_deleted_rows, rowkey_column_size)));
        RowKeyRange range(start, end, true, rowkey_column_size);
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
                                             std::vector<RuntimeFilterPtr>{},
                                             0,
                                             TRACING_NAME,
                                             /* keep_order= */ false,
                                             /* is_fast_scan= */ false,
                                             /* expected_block_size= */ 1024)[0];
        // mock common handle, data range after deletion is [64, 128)
        auto common_handle_coldata = []() {
            auto tmp = createNumbers<Int64>(num_deleted_rows, num_rows_write);
            Strings res;
            std::transform(tmp.begin(), tmp.end(), std::back_inserter(res), [](Int64 v) { return genMockCommonHandle(v, rowkey_column_size); });
            return res;
        }();
        ASSERT_EQ(common_handle_coldata.size(), num_rows_write - num_deleted_rows);
        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<String>(common_handle_coldata),
            }));
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
            auto segment_stats = store->getSegmentsStats();
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
                                                std::vector<RuntimeFilterPtr>{},
                                                0,
                                                TRACING_NAME,
                                                /* keep_order= */ false,
                                                /* is_fast_scan= */ false,
                                                /* expected_block_size= */ 1024);
            ASSERT_EQ(ins.size(), 1UL);
            BlockInputStreamPtr in = ins[0];

            LOG_TRACE(&Poco::Logger::get(GET_GTEST_FULL_NAME), "start to check data of [1,{}]", num_rows_write_in_total);

            ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
                in,
                Strings({DMTestEnv::pk_name}),
                createColumns({
                    createColumn<Int64>(createNumbers<Int64>(1, num_rows_write_in_total + 1)),
                }));
            LOG_TRACE(&Poco::Logger::get(GET_GTEST_FULL_NAME), "done checking data of [1,{}]", num_rows_write_in_total);
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
                                                  NullspaceID,
                                                  101,
                                                  true,
                                                  *cols,
                                                  (*cols)[0],
                                                  pk_type == DMTestEnv::PkType::CommonHandle,
                                                  1,
                                                  DeltaMergeStore::Settings());
        dm_context = store->newDMContext(*db_context, db_context->getSettingsRef(), DB::base::TiFlashStorageTestBasic::getCurrentFullTestName());
    }

protected:
    std::unique_ptr<MultiSegmentTestUtil> helper{};
    DeltaMergeStorePtr store;
    DMContextPtr dm_context;

    UInt64 ps_ver{};
    DMTestEnv::PkType pk_type;
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
        auto result = store->mergeDeltaBySegment(*db_context, RowKeyValue::COMMON_HANDLE_MAX_KEY);
        ASSERT_EQ(result, std::nullopt);
    }
    else
    {
        // Specifies MAX_KEY. nullopt should be returned.
        auto result = store->mergeDeltaBySegment(*db_context, RowKeyValue::INT_HANDLE_MAX_KEY);
        ASSERT_EQ(result, std::nullopt);
    }
    std::optional<RowKeyRange> result_1;
    {
        // Specifies MIN_KEY. In this case, the first segment should be processed.
        if (store->isCommonHandle())
        {
            result_1 = store->mergeDeltaBySegment(*db_context, RowKeyValue::COMMON_HANDLE_MIN_KEY);
        }
        else
        {
            result_1 = store->mergeDeltaBySegment(*db_context, RowKeyValue::INT_HANDLE_MIN_KEY);
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
        auto result = store->mergeDeltaBySegment(*db_context, result_1->start);
        ASSERT_EQ(*result, *result_1);

        helper->verifyExpectedRowsForAllSegments();
    }
    std::optional<RowKeyRange> result_2;
    {
        // Compact again using the end key just returned. The second segment should be processed.
        result_2 = store->mergeDeltaBySegment(*db_context, result_1->end);
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
            store->mergeDeltaBySegment(*db_context, RowKeyValue::INT_HANDLE_MIN_KEY);
        }
        else
        {
            // For int handle, give common handle key and have a try
            store->mergeDeltaBySegment(*db_context, RowKeyValue::COMMON_HANDLE_MIN_KEY);
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

        result = store->mergeDeltaBySegment(*db_context, seg->getRowKeyRange().start);
        ASSERT_NE(result, std::nullopt);
        helper->verifyExpectedRowsForAllSegments();
    }
    {
        // As we are the last segment, compact "next segment" should result in failure. A nullopt is returned.
        auto result2 = store->mergeDeltaBySegment(*db_context, result->end);
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

        auto result = store->mergeDeltaBySegment(*db_context, compact_key);
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
        auto result = store->mergeDeltaBySegment(*db_context, segment1->getRowKeyRange().start);
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
TEST_P(DeltaMergeStoreMergeDeltaBySegmentTest, RetryByFlushCache)
try
{
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
        auto result = store->mergeDeltaBySegment(*db_context, segment1->getRowKeyRange().start);
        ASSERT_NE(result, std::nullopt);
        // All rows in the delta layer should be merged into the stable layer.
        helper->expected_stable_rows[1] += helper->expected_delta_rows[1];
        helper->expected_delta_rows[1] = 0;
        helper->verifyExpectedRowsForAllSegments();
    });
    sp_merge_delta_retry.waitAndPause();

    // Let's finish the flush.
    sp_flush_commit.next();
    th_flush.get();

    // Proceed the mergeDelta retry. Retry should succeed without triggering any new flush.
    sp_merge_delta_retry.next();
    th_merge_delta.get();
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
        auto result = store->segmentSplit(*dm_context, segment1, DeltaMergeStore::SegmentSplitReason::ForegroundWrite);
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
        auto result = store->mergeDeltaBySegment(*db_context, segment1->getRowKeyRange().start);
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
    th_split.get();
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
    th_merge_delta.get();
}
CATCH


} // namespace tests
} // namespace DM
} // namespace DB
