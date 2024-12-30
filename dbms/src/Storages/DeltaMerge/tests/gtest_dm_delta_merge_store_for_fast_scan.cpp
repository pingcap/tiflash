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
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/DeltaMerge/tests/gtest_dm_delta_merge_store_test_basic.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/InputStreamTestUtils.h>
/// This test file is mainly test on the correctness of read in fast mode.
/// Because the basic functions are tested in gtest_dm_delta_merge_store.cpp, we will not cover it here.

namespace DB::DM::tests
{

TEST_P(DeltaMergeStoreRWTest, TestFastScanWithOnlyInsertWithoutRangeFilter)
{
    /// test under only insert data (no update, no delete) with all range

    const ColumnDefine col_str_define(2, "col2", std::make_shared<DataTypeString>());
    const ColumnDefine col_i8_define(3, "i8", std::make_shared<DataTypeInt8>());
    {
        auto table_column_defines = DMTestEnv::getDefaultColumns();
        table_column_defines->emplace_back(col_str_define);
        table_column_defines->emplace_back(col_i8_define);

        store = reload(table_column_defines);
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
        case TestMode::PageStorageV2_MemoryOnly:
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
        // read all columns from store with all range in fast mode
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(
            *db_context,
            db_context->getSettingsRef(),
            columns,
            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
            /* num_streams= */ 1,
            /* start_ts= */ std::numeric_limits<UInt64>::max(),
            EMPTY_FILTER,
            std::vector<RuntimeFilterPtr>{},
            0,
            TRACING_NAME,
            /* keep_order= */ false,
            /* is_fast_scan= */ true,
            /* expected_block_size= */ 1024)[0];
        ASSERT_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name, col_str_define.name, col_i8_define.name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
                createColumn<String>(createNumberStrings(0, num_rows_write)),
                createColumn<Int8>(createSignedNumbers(0, num_rows_write)),
            }));
    }
}

TEST_P(DeltaMergeStoreRWTest, TestFastScanWithOnlyInsertWithRangeFilter)
{
    /// test under only insert data (no update, no delete) with range filter

    const ColumnDefine col_str_define(2, "col2", std::make_shared<DataTypeString>());
    const ColumnDefine col_i8_define(3, "i8", std::make_shared<DataTypeInt8>());
    {
        auto table_column_defines = DMTestEnv::getDefaultColumns();
        table_column_defines->emplace_back(col_str_define);
        table_column_defines->emplace_back(col_i8_define);

        store = reload(table_column_defines);
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
        case TestMode::PageStorageV2_MemoryOnly:
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
        // read all columns from store with row key range in fast mode
        auto read_nums_limit = 64;
        WriteBufferFromOwnString start_key_ss;
        DB::EncodeInt64(0, start_key_ss);

        WriteBufferFromOwnString end_key_ss;
        DB::EncodeInt64(read_nums_limit, end_key_ss);

        const auto & columns = store->getTableColumns();
        RowKeyRanges key_ranges{RowKeyRange(
            RowKeyValue(false, std::make_shared<String>(start_key_ss.releaseStr()), /*int_val_*/ 0),
            RowKeyValue(false, std::make_shared<String>(end_key_ss.releaseStr()), /*int_val_*/ read_nums_limit),
            false,
            store->getRowKeyColumnSize())};
        BlockInputStreamPtr in = store->read(
            *db_context,
            db_context->getSettingsRef(),
            columns,
            key_ranges,
            /* num_streams= */ 1,
            /* start_ts= */ std::numeric_limits<UInt64>::max(),
            EMPTY_FILTER,
            std::vector<RuntimeFilterPtr>{},
            0,
            TRACING_NAME,
            /* keep_order= */ false,
            /* is_fast_scan= */ true,
            /* expected_block_size= */ 1024)[0];
        ASSERT_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name, col_str_define.name, col_i8_define.name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, read_nums_limit)),
                createColumn<String>(createNumberStrings(0, read_nums_limit)),
                createColumn<Int8>(createSignedNumbers(0, read_nums_limit)),
            }));
    }
}

TEST_P(DeltaMergeStoreRWTest, TestFastScanWithOnlyInsertWithMultipleBlockWithoutFlushCache)
try
{
    const size_t num_write_rows = 32;
    auto table_column_defines = DMTestEnv::getDefaultColumns();
    store = reload(table_column_defines);

    //Test write multi blocks without overlap and do not compact
    {
        Block block1 = DMTestEnv::prepareSimpleWriteBlock(0, 1 * num_write_rows, false);
        Block block2 = DMTestEnv::prepareSimpleWriteBlock(1 * num_write_rows, 2 * num_write_rows, false);
        Block block3 = DMTestEnv::prepareSimpleWriteBlock(2 * num_write_rows, 3 * num_write_rows, false);
        switch (mode)
        {
        case TestMode::PageStorageV2_MemoryOnly:
        case TestMode::Current_MemoryOnly:
        {
            store->write(*db_context, db_context->getSettingsRef(), block1);
            store->write(*db_context, db_context->getSettingsRef(), block2);
            store->write(*db_context, db_context->getSettingsRef(), block3);
            break;
        }
        case TestMode::PageStorageV2_DiskOnly:
        case TestMode::Current_DiskOnly:
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
        case TestMode::PageStorageV2_MemoryAndDisk: // disk + memory
        case TestMode::Current_MemoryAndDisk:
        {
            auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
            auto [range1, file_ids1] = genDMFile(*dm_context, block1);
            auto [range3, file_ids3] = genDMFile(*dm_context, block3);
            auto range = range1.merge(range3);
            auto file_ids = file_ids1;
            file_ids.insert(file_ids.cend(), file_ids3.begin(), file_ids3.end());
            store->ingestFiles(dm_context, range, file_ids, false); // in disk
            store->write(*db_context, db_context->getSettingsRef(), block2);

            break;
        }
        }
    }

    {
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(
            *db_context,
            db_context->getSettingsRef(),
            columns,
            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
            /* num_streams= */ 1,
            /* start_ts= */ std::numeric_limits<UInt64>::max(),
            EMPTY_FILTER,
            std::vector<RuntimeFilterPtr>{},
            0,
            TRACING_NAME,
            /* keep_order= */ false,
            /* is_fast_scan= */ true,
            /* expected_block_size= */ 1024)[0];
        switch (mode)
        {
        case TestMode::PageStorageV2_MemoryOnly:
        case TestMode::Current_MemoryOnly:
        {
            ASSERT_INPUTSTREAM_COLS_UR(
                in,
                Strings({DMTestEnv::pk_name}),
                createColumns({createColumn<Int64>(createNumbers<Int64>(0, 3 * num_write_rows))}));
            break;
        }
        case TestMode::PageStorageV2_DiskOnly:
        case TestMode::Current_DiskOnly:
        {
            auto pk_coldata = []() {
                std::vector<Int64> res;
                // first [0, 32)
                auto tmp = createNumbers<Int64>(0, num_write_rows);
                res.insert(res.end(), tmp.begin(), tmp.end());
                // then [32, 64)
                tmp = createNumbers<Int64>(num_write_rows, 2 * num_write_rows);
                res.insert(res.end(), tmp.begin(), tmp.end());
                // then [64, 96)
                tmp = createNumbers<Int64>(2 * num_write_rows, 3 * num_write_rows);
                res.insert(res.end(), tmp.begin(), tmp.end());
                return res;
            }();
            ASSERT_EQ(pk_coldata.size(), 3 * num_write_rows);
            ASSERT_INPUTSTREAM_COLS_UR(
                in,
                Strings({DMTestEnv::pk_name}),
                createColumns({createColumn<Int64>(pk_coldata)}));
            break;
        }
        case TestMode::PageStorageV2_MemoryAndDisk:
        case TestMode::Current_MemoryAndDisk:
        {
            // persist first, then memory, finally stable
            auto pk_coldata = []() {
                std::vector<Int64> res;
                // first [0, 32)
                auto tmp = createNumbers<Int64>(0, num_write_rows);
                res.insert(res.end(), tmp.begin(), tmp.end());
                // then [64, 96)
                tmp = createNumbers<Int64>(2 * num_write_rows, 3 * num_write_rows);
                res.insert(res.end(), tmp.begin(), tmp.end());
                // then [32, 64)
                tmp = createNumbers<Int64>(num_write_rows, 2 * num_write_rows);
                res.insert(res.end(), tmp.begin(), tmp.end());
                return res;
            }();
            ASSERT_EQ(pk_coldata.size(), 3 * num_write_rows);
            ASSERT_INPUTSTREAM_COLS_UR(
                in,
                Strings({DMTestEnv::pk_name}),
                createColumns({createColumn<Int64>(pk_coldata)}));
            break;
        }
        }
    }
}
CATCH

TEST_P(DeltaMergeStoreRWTest, TestFastScanWithOnlyInsertWithMultipleBlockWithoutCompact)
try
{
    const size_t num_write_rows = 32;
    auto table_column_defines = DMTestEnv::getDefaultColumns();
    store = reload(table_column_defines);

    //Test write multi blocks without overlap and do not compact
    {
        Block block1 = DMTestEnv::prepareSimpleWriteBlock(0, 1 * num_write_rows, false);
        Block block2 = DMTestEnv::prepareSimpleWriteBlock(1 * num_write_rows, 2 * num_write_rows, false);
        Block block3 = DMTestEnv::prepareSimpleWriteBlock(2 * num_write_rows, 3 * num_write_rows, false);
        switch (mode)
        {
        case TestMode::PageStorageV2_MemoryOnly:
        case TestMode::Current_MemoryOnly:
        {
            store->write(*db_context, db_context->getSettingsRef(), block1);
            store->write(*db_context, db_context->getSettingsRef(), block2);
            store->write(*db_context, db_context->getSettingsRef(), block3);
            break;
        }
        case TestMode::PageStorageV2_DiskOnly:
        case TestMode::Current_DiskOnly:
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
        case TestMode::PageStorageV2_MemoryAndDisk:
        case TestMode::Current_MemoryAndDisk:
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
        BlockInputStreamPtr in = store->read(
            *db_context,
            db_context->getSettingsRef(),
            columns,
            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
            /* num_streams= */ 1,
            /* start_ts= */ std::numeric_limits<UInt64>::max(),
            EMPTY_FILTER,
            std::vector<RuntimeFilterPtr>{},
            0,
            TRACING_NAME,
            /* keep_order= */ false,
            /* is_fast_scan= */ true,
            /* expected_block_size= */ 1024)[0];
        switch (mode)
        {
        case TestMode::PageStorageV2_MemoryOnly:
        case TestMode::PageStorageV2_DiskOnly:
        case TestMode::Current_MemoryOnly:
        case TestMode::Current_DiskOnly:
        {
            ASSERT_INPUTSTREAM_COLS_UR(
                in,
                Strings({DMTestEnv::pk_name}),
                createColumns({createColumn<Int64>(createNumbers<Int64>(0, 3 * num_write_rows))}));
            break;
        }
        case TestMode::PageStorageV2_MemoryAndDisk:
        case TestMode::Current_MemoryAndDisk:
        {
            auto pk_coldata = []() {
                std::vector<Int64> res;
                // first [0, 32)
                auto tmp = createNumbers<Int64>(0, num_write_rows);
                res.insert(res.end(), tmp.begin(), tmp.end());
                // then [64, 96)
                tmp = createNumbers<Int64>(2 * num_write_rows, 3 * num_write_rows);
                res.insert(res.end(), tmp.begin(), tmp.end());
                // then [32, 64)
                tmp = createNumbers<Int64>(num_write_rows, 2 * num_write_rows);
                res.insert(res.end(), tmp.begin(), tmp.end());
                return res;
            }();
            ASSERT_EQ(pk_coldata.size(), 3 * num_write_rows);
            ASSERT_INPUTSTREAM_COLS_UR(
                in,
                Strings({DMTestEnv::pk_name}),
                createColumns({createColumn<Int64>(pk_coldata)}));
            break;
        }
        }
    }
}
CATCH

TEST_P(DeltaMergeStoreRWTest, TestFastScanWithOnlyInsertWithMultipleBlockWithCompact)
try
{
    const size_t num_write_rows = 32;
    auto table_column_defines = DMTestEnv::getDefaultColumns();
    store = reload(table_column_defines);

    //Test write multi blocks without overlap and do not compact
    {
        Block block1 = DMTestEnv::prepareSimpleWriteBlock(0, 1 * num_write_rows, false);
        Block block2 = DMTestEnv::prepareSimpleWriteBlock(1 * num_write_rows, 2 * num_write_rows, false);
        Block block3 = DMTestEnv::prepareSimpleWriteBlock(2 * num_write_rows, 3 * num_write_rows, false);
        switch (mode)
        {
        case TestMode::PageStorageV2_MemoryOnly:
        case TestMode::Current_MemoryOnly:
        {
            store->write(*db_context, db_context->getSettingsRef(), block1);
            store->write(*db_context, db_context->getSettingsRef(), block2);
            store->write(*db_context, db_context->getSettingsRef(), block3);
            break;
        }
        case TestMode::PageStorageV2_DiskOnly:
        case TestMode::Current_DiskOnly:
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
        case TestMode::PageStorageV2_MemoryAndDisk:
        case TestMode::Current_MemoryAndDisk:
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

    store->compact(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));

    {
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(
            *db_context,
            db_context->getSettingsRef(),
            columns,
            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
            /* num_streams= */ 1,
            /* start_ts= */ std::numeric_limits<UInt64>::max(),
            EMPTY_FILTER,
            std::vector<RuntimeFilterPtr>{},
            0,
            TRACING_NAME,
            /* keep_order= */ false,
            /* is_fast_scan= */ true,
            /* expected_block_size= */ 1024)[0];
        switch (mode)
        {
        case TestMode::PageStorageV2_MemoryOnly:
        case TestMode::PageStorageV2_DiskOnly:
        case TestMode::Current_MemoryOnly:
        case TestMode::Current_DiskOnly:
        {
            ASSERT_INPUTSTREAM_COLS_UR(
                in,
                Strings({DMTestEnv::pk_name}),
                createColumns({createColumn<Int64>(createNumbers<Int64>(0, 3 * num_write_rows))}));
            break;
        }
        case TestMode::PageStorageV2_MemoryAndDisk:
        case TestMode::Current_MemoryAndDisk:
        {
            auto pk_coldata = []() {
                std::vector<Int64> res;
                // first [0, 32)
                auto tmp = createNumbers<Int64>(0, num_write_rows);
                res.insert(res.end(), tmp.begin(), tmp.end());
                // then [64, 96)
                tmp = createNumbers<Int64>(2 * num_write_rows, 3 * num_write_rows);
                res.insert(res.end(), tmp.begin(), tmp.end());
                // then [32, 64)
                tmp = createNumbers<Int64>(num_write_rows, 2 * num_write_rows);
                res.insert(res.end(), tmp.begin(), tmp.end());
                return res;
            }();
            ASSERT_EQ(pk_coldata.size(), 3 * num_write_rows);
            ASSERT_INPUTSTREAM_COLS_UR(
                in,
                Strings({DMTestEnv::pk_name}),
                createColumns({createColumn<Int64>(pk_coldata)}));
            break;
        }
        }
    }
}
CATCH

TEST_P(DeltaMergeStoreRWTest, TestFastScanWithOnlyInsertWithMultipleBlockWithCompactAndMergeDelta)
try
{
    const size_t num_write_rows = 32;
    auto table_column_defines = DMTestEnv::getDefaultColumns();
    store = reload(table_column_defines);

    //Test write multi blocks without overlap and do not compact
    {
        Block block1 = DMTestEnv::prepareSimpleWriteBlock(0, 1 * num_write_rows, false);
        Block block2 = DMTestEnv::prepareSimpleWriteBlock(1 * num_write_rows, 2 * num_write_rows, false);
        Block block3 = DMTestEnv::prepareSimpleWriteBlock(2 * num_write_rows, 3 * num_write_rows, false);
        switch (mode)
        {
        case TestMode::PageStorageV2_MemoryOnly:
        case TestMode::Current_MemoryOnly:
        {
            store->write(*db_context, db_context->getSettingsRef(), block1);
            store->write(*db_context, db_context->getSettingsRef(), block2);
            store->write(*db_context, db_context->getSettingsRef(), block3);
            break;
        }
        case TestMode::PageStorageV2_DiskOnly:
        case TestMode::Current_DiskOnly:
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
        case TestMode::PageStorageV2_MemoryAndDisk:
        case TestMode::Current_MemoryAndDisk:
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

    store->compact(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));

    store->mergeDeltaAll(*db_context);

    {
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(
            *db_context,
            db_context->getSettingsRef(),
            columns,
            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
            /* num_streams= */ 1,
            /* start_ts= */ std::numeric_limits<UInt64>::max(),
            EMPTY_FILTER,
            std::vector<RuntimeFilterPtr>{},
            0,
            TRACING_NAME,
            /* keep_order= */ false,
            /* is_fast_scan= */ true,
            /* expected_block_size= */ 1024)[0];
        ASSERT_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name}),
            createColumns({createColumn<Int64>(createNumbers<Int64>(0, 3 * num_write_rows))}));
    }
}
CATCH

// Insert + Update
TEST_P(DeltaMergeStoreRWTest, TestFastScanWithMultipleBlockWithOverlap)
try
{
    const size_t num_write_rows = 32;

    // Test write multi blocks with overlap and do compact
    {
        UInt64 tso1 = 1;
        UInt64 tso2 = 100;
        // [0,32)
        Block block1 = DMTestEnv::prepareSimpleWriteBlock(0, 1 * num_write_rows, false, tso1);
        // [32,64)
        Block block2 = DMTestEnv::prepareSimpleWriteBlock(1 * num_write_rows, 2 * num_write_rows, false, tso1);
        // [16,48), overlap
        Block block3
            = DMTestEnv::prepareSimpleWriteBlock(num_write_rows / 2, num_write_rows / 2 + num_write_rows, false, tso2);

        switch (mode)
        {
        case TestMode::PageStorageV2_MemoryOnly:
        case TestMode::Current_MemoryOnly:
        {
            store->write(*db_context, db_context->getSettingsRef(), block1);
            store->write(*db_context, db_context->getSettingsRef(), block2);
            store->write(*db_context, db_context->getSettingsRef(), block3);
            break;
        }
        case TestMode::PageStorageV2_DiskOnly:
        case TestMode::Current_DiskOnly:
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
        case TestMode::PageStorageV2_MemoryAndDisk:
        case TestMode::Current_MemoryAndDisk:
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

        // in MemoryOnly mode, flush cache will make sort
        store->flushCache(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));
    }

    store->compact(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));

    {
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(
            *db_context,
            db_context->getSettingsRef(),
            columns,
            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
            /* num_streams= */ 1,
            /* start_ts= */ std::numeric_limits<UInt64>::max(),
            EMPTY_FILTER,
            std::vector<RuntimeFilterPtr>{},
            0,
            TRACING_NAME,
            /* keep_order= */ false,
            /* is_fast_scan= */ true,
            /* expected_block_size= */ 1024)[0];

        switch (mode)
        {
        case TestMode::PageStorageV2_MemoryOnly:
        case TestMode::Current_MemoryOnly:
        {
            auto pk_coldata = []() {
                std::vector<Int64> res;
                // first [0, 32)
                auto tmp = createNumbers<Int64>(0, num_write_rows);
                res.insert(res.end(), tmp.begin(), tmp.end());
                // then [32, 64)
                tmp = createNumbers<Int64>(num_write_rows, 2 * num_write_rows);
                res.insert(res.end(), tmp.begin(), tmp.end());
                // then [32/2, 32 * 1.5)
                tmp = createNumbers<Int64>(num_write_rows / 2, 1.5 * num_write_rows);
                res.insert(res.end(), tmp.begin(), tmp.end());
                // the pk is sorted by flush cache
                std::sort(res.begin(), res.end());
                return res;
            }();
            ASSERT_EQ(pk_coldata.size(), 3 * num_write_rows);
            ASSERT_INPUTSTREAM_COLS_UR(
                in,
                Strings({DMTestEnv::pk_name}),
                createColumns({createColumn<Int64>(pk_coldata)}));
            break;
        }
        case TestMode::PageStorageV2_DiskOnly:
        case TestMode::Current_DiskOnly:
        {
            auto pk_coldata = []() {
                std::vector<Int64> res;
                // first [0, 128)
                auto tmp = createNumbers<Int64>(0, num_write_rows);
                res.insert(res.end(), tmp.begin(), tmp.end());
                // then [128, 256)
                tmp = createNumbers<Int64>(num_write_rows, 2 * num_write_rows);
                res.insert(res.end(), tmp.begin(), tmp.end());
                // then [128/2, 128 * 1.5)
                tmp = createNumbers<Int64>(num_write_rows / 2, 1.5 * num_write_rows);
                res.insert(res.end(), tmp.begin(), tmp.end());
                return res;
            }();
            ASSERT_EQ(pk_coldata.size(), 3 * num_write_rows);
            ASSERT_INPUTSTREAM_COLS_UR(
                in,
                Strings({DMTestEnv::pk_name}),
                createColumns({createColumn<Int64>(pk_coldata)}));
            break;
        }
        case TestMode::PageStorageV2_MemoryAndDisk:
        case TestMode::Current_MemoryAndDisk:
        {
            auto pk_coldata = []() {
                std::vector<Int64> res;
                // first [128, 256)
                auto tmp = createNumbers<Int64>(num_write_rows, 2 * num_write_rows);
                res.insert(res.end(), tmp.begin(), tmp.end());
                // then [0, 128)
                tmp = createNumbers<Int64>(0, num_write_rows);
                res.insert(res.end(), tmp.begin(), tmp.end());
                // then [128/2, 128 * 1.5)
                tmp = createNumbers<Int64>(num_write_rows / 2, 1.5 * num_write_rows);
                res.insert(res.end(), tmp.begin(), tmp.end());
                return res;
            }();
            ASSERT_EQ(pk_coldata.size(), 3 * num_write_rows);
            ASSERT_INPUTSTREAM_COLS_UR(
                in,
                Strings({DMTestEnv::pk_name}),
                createColumns({createColumn<Int64>(pk_coldata)}));
            break;
        }
        }
    }
}
CATCH

// Insert + Delete row
TEST_P(DeltaMergeStoreRWTest, TestFastScanWithDeleteRow)
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

    const size_t num_rows_write = 128;
    {
        Block block1;
        {
            block1 = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
            // Add a column of col2:String for test
            block1.insert(DB::tests::createColumn<String>(
                createNumberStrings(0, num_rows_write),
                col_str_define.name,
                col_str_define.id));
            // Add a column of i8:Int8 for test
            block1.insert(DB::tests::createColumn<Int8>(
                createSignedNumbers(0, num_rows_write),
                col_i8_define.name,
                col_i8_define.id));
        }

        Block block2;
        {
            block2 = DMTestEnv::prepareSimpleWriteBlock(
                num_rows_write,
                1.5 * num_rows_write,
                false,
                3,
                DMTestEnv::pk_name,
                MutSup::extra_handle_id,
                MutSup::getExtraHandleColumnIntType(),
                false,
                1,
                true,
                true);
            // Add a column of col2:String for test
            block2.insert(DB::tests::createColumn<String>(
                createNumberStrings(0.5 * num_rows_write, num_rows_write),
                col_str_define.name,
                col_str_define.id));
            // Add a column of i8:Int8 for test
            block2.insert(DB::tests::createColumn<Int8>(
                createSignedNumbers(0.5 * num_rows_write, num_rows_write),
                col_i8_define.name,
                col_i8_define.id));
        }

        switch (mode)
        {
        case TestMode::PageStorageV2_MemoryOnly:
            store->write(*db_context, db_context->getSettingsRef(), block1);
            store->write(*db_context, db_context->getSettingsRef(), block2);
            break;
        default:
            auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
            auto [range1, file_ids1] = genDMFile(*dm_context, block1);
            auto [range2, file_ids2] = genDMFile(*dm_context, block2);
            auto range = range1.merge(range2);
            auto file_ids = file_ids1;
            file_ids.insert(file_ids.cend(), file_ids2.begin(), file_ids2.end());
            store->ingestFiles(dm_context, range, file_ids, false);
            break;
        }
    }

    // Read after deletion
    {
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(
            *db_context,
            db_context->getSettingsRef(),
            columns,
            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
            /* num_streams= */ 1,
            /* start_ts= */ std::numeric_limits<UInt64>::max(),
            EMPTY_FILTER,
            std::vector<RuntimeFilterPtr>{},
            0,
            TRACING_NAME,
            /* keep_order= */ false,
            /* is_fast_scan= */ true,
            /* expected_block_size= */ 1024)[0];
        // filter del mark = 1， thus just read the insert data before delete
        ASSERT_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name}),
            createColumns({createColumn<Int64>(createNumbers<Int64>(0, num_rows_write))}));
    }

    store->flushCache(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));

    store->compact(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));

    store->mergeDeltaAll(*db_context);

    {
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(
            *db_context,
            db_context->getSettingsRef(),
            columns,
            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
            /* num_streams= */ 1,
            /* start_ts= */ std::numeric_limits<UInt64>::max(),
            EMPTY_FILTER,
            std::vector<RuntimeFilterPtr>{},
            0,
            TRACING_NAME,
            /* keep_order= */ false,
            /* is_fast_scan= */ true,
            /* expected_block_size= */ 1024)[0];
        ASSERT_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name}),
            createColumns({createColumn<Int64>(createNumbers<Int64>(0, num_rows_write))}));
    }
}
CATCH

// Insert + Delete Range
TEST_P(DeltaMergeStoreRWTest, TestFastScanWithDeleteRange)
try
{
    const size_t num_rows_write = 128;
    {
        // Create a block with sequential Int64 handle in range [0, 128)
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);

        switch (mode)
        {
        case TestMode::PageStorageV2_MemoryOnly:
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
        BlockInputStreamPtr in = store->read(
            *db_context,
            db_context->getSettingsRef(),
            columns,
            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
            /* num_streams= */ 1,
            /* start_ts= */ std::numeric_limits<UInt64>::max(),
            EMPTY_FILTER,
            std::vector<RuntimeFilterPtr>{},
            0,
            TRACING_NAME,
            /* keep_order= */ false,
            /* is_fast_scan= */ true,
            /* expected_block_size= */ 1024)[0];
        ASSERT_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name}),
            createColumns({createColumn<Int64>(createNumbers<Int64>(0, num_rows_write))}));
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
        BlockInputStreamPtr in = store->read(
            *db_context,
            db_context->getSettingsRef(),
            columns,
            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
            /* num_streams= */ 1,
            /* start_ts= */ std::numeric_limits<UInt64>::max(),
            EMPTY_FILTER,
            std::vector<RuntimeFilterPtr>{},
            0,
            TRACING_NAME,
            /* keep_order = */ false,
            /* is_fast_scan= */ true,
            /* expected_block_size= */ 1024)[0];
        // filter del mark = 1， thus just read the insert data before delete
        ASSERT_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name}),
            createColumns({createColumn<Int64>(createNumbers<Int64>(0, num_rows_write))}));
    }
}
CATCH

TEST_P(DeltaMergeStoreRWTest, TestFastScanWithDeleteWithMergeDelta)
try
{
    const size_t num_rows_write = 128;
    {
        // Create a block with sequential Int64 handle in range [0, 128)
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);

        switch (mode)
        {
        case TestMode::PageStorageV2_MemoryOnly:
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

    // Delete range [0, 64)
    const size_t num_deleted_rows = 64;
    {
        HandleRange range(0, num_deleted_rows);
        store->deleteRange(*db_context, db_context->getSettingsRef(), RowKeyRange::fromHandleRange(range));
    }

    store->flushCache(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));

    store->compact(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));

    store->mergeDeltaAll(*db_context);

    // Read after merge delta
    {
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(
            *db_context,
            db_context->getSettingsRef(),
            columns,
            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
            /* num_streams= */ 1,
            /* start_ts= */ std::numeric_limits<UInt64>::max(),
            EMPTY_FILTER,
            std::vector<RuntimeFilterPtr>{},
            0,
            TRACING_NAME,
            /* keep_order= */ false,
            /* is_fast_scan= */ true,
            /* expected_block_size= */ 1024)[0];
        auto pk_coldata = createNumbers<Int64>(num_deleted_rows, num_rows_write);
        ASSERT_EQ(pk_coldata.size(), num_rows_write - num_deleted_rows);
        ASSERT_INPUTSTREAM_COLS_UR(in, Strings({DMTestEnv::pk_name}), createColumns({createColumn<Int64>(pk_coldata)}));
    }
}
CATCH

// insert + update + delete and fast mode first and then normal mode, to check the mode conversion is ok
TEST_P(DeltaMergeStoreRWTest, TestFastScanComplexWithModeConversion)
try
{
    const size_t num_write_rows = 128;

    {
        UInt64 tso1 = 1;
        UInt64 tso2 = 100;
        Block block1 = DMTestEnv::prepareSimpleWriteBlock(0, 1 * num_write_rows, false, tso1);
        Block block2 = DMTestEnv::prepareSimpleWriteBlock(1 * num_write_rows, 2 * num_write_rows, false, tso1);
        Block block3
            = DMTestEnv::prepareSimpleWriteBlock(num_write_rows / 2, num_write_rows / 2 + num_write_rows, false, tso2);

        switch (mode)
        {
        case TestMode::PageStorageV2_MemoryOnly:
        case TestMode::Current_MemoryOnly:
        {
            store->write(*db_context, db_context->getSettingsRef(), block1);
            store->write(*db_context, db_context->getSettingsRef(), block2);
            store->write(*db_context, db_context->getSettingsRef(), block3);
            break;
        }
        case TestMode::PageStorageV2_DiskOnly:
        case TestMode::Current_DiskOnly:
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
        case TestMode::PageStorageV2_MemoryAndDisk:
        case TestMode::Current_MemoryAndDisk:
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

        // in MemoryOnly mode, flush cache will make sort
        store->flushCache(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));
    }

    store->compact(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));

    // Delete range [0, 64)
    const size_t num_deleted_rows = 64;
    {
        HandleRange range(0, num_deleted_rows);
        store->deleteRange(*db_context, db_context->getSettingsRef(), RowKeyRange::fromHandleRange(range));
    }

    // Read in fast mode
    {
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(
            *db_context,
            db_context->getSettingsRef(),
            columns,
            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
            /* num_streams= */ 1,
            /* start_ts= */ std::numeric_limits<UInt64>::max(),
            EMPTY_FILTER,
            std::vector<RuntimeFilterPtr>{},
            0,
            TRACING_NAME,
            /* keep_order= */ false,
            /* is_fast_scan= */ true,
            /* expected_block_size= */ 1024)[0];

        switch (mode)
        {
        case TestMode::PageStorageV2_MemoryOnly:
        case TestMode::Current_MemoryOnly:
        {
            auto pk_coldata = []() {
                std::vector<Int64> res;
                // first [0, 128)
                auto tmp = createNumbers<Int64>(0, num_write_rows);
                res.insert(res.end(), tmp.begin(), tmp.end());
                // then [128, 256)
                tmp = createNumbers<Int64>(num_write_rows, 2 * num_write_rows);
                res.insert(res.end(), tmp.begin(), tmp.end());
                // then [128/2, 128 * 1.5)
                tmp = createNumbers<Int64>(num_write_rows / 2, 1.5 * num_write_rows);
                res.insert(res.end(), tmp.begin(), tmp.end());
                // the pk is sorted by flush cache
                std::sort(res.begin(), res.end());
                return res;
            }();
            ASSERT_EQ(pk_coldata.size(), 3 * num_write_rows);
            ASSERT_INPUTSTREAM_COLS_UR(
                in,
                Strings({DMTestEnv::pk_name}),
                createColumns({createColumn<Int64>(pk_coldata)}));
            break;
        }
        case TestMode::PageStorageV2_DiskOnly:
        case TestMode::Current_DiskOnly:
        {
            auto pk_coldata = []() {
                std::vector<Int64> res;
                // first [0, 128)
                auto tmp = createNumbers<Int64>(0, num_write_rows);
                res.insert(res.end(), tmp.begin(), tmp.end());
                // then [128, 256)
                tmp = createNumbers<Int64>(num_write_rows, 2 * num_write_rows);
                res.insert(res.end(), tmp.begin(), tmp.end());
                // then [128/2, 128 * 1.5)
                tmp = createNumbers<Int64>(num_write_rows / 2, 1.5 * num_write_rows);
                res.insert(res.end(), tmp.begin(), tmp.end());
                return res;
            }();
            ASSERT_EQ(pk_coldata.size(), 3 * num_write_rows);
            ASSERT_INPUTSTREAM_COLS_UR(
                in,
                Strings({DMTestEnv::pk_name}),
                createColumns({createColumn<Int64>(pk_coldata)}));
            break;
        }
        case TestMode::PageStorageV2_MemoryAndDisk:
        case TestMode::Current_MemoryAndDisk:
        {
            auto pk_coldata = []() {
                std::vector<Int64> res;
                // first [128, 256)
                auto tmp = createNumbers<Int64>(num_write_rows, 2 * num_write_rows);
                res.insert(res.end(), tmp.begin(), tmp.end());
                // then [0, 128)
                tmp = createNumbers<Int64>(0, num_write_rows);
                res.insert(res.end(), tmp.begin(), tmp.end());
                // then [128/2, 128 * 1.5)
                tmp = createNumbers<Int64>(num_write_rows / 2, 1.5 * num_write_rows);
                res.insert(res.end(), tmp.begin(), tmp.end());
                return res;
            }();
            ASSERT_EQ(pk_coldata.size(), 3 * num_write_rows);
            ASSERT_INPUTSTREAM_COLS_UR(
                in,
                Strings({DMTestEnv::pk_name}),
                createColumns({createColumn<Int64>(pk_coldata)}));
            break;
        }
        }
    }

    // Read with version in normal case
    {
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(
            *db_context,
            db_context->getSettingsRef(),
            columns,
            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
            /* num_streams= */ 1,
            /* start_ts= */ static_cast<UInt64>(1),
            EMPTY_FILTER,
            std::vector<RuntimeFilterPtr>{},
            0,
            TRACING_NAME,
            /* keep_order= */ false,
            /* is_fast_scan= */ false,
            /* expected_block_size= */ 1024)[0];
        // Data is not guaranteed to be returned in order.
        ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name}),
            createColumns({createColumn<Int64>(createNumbers<Int64>(num_write_rows / 2, 2 * num_write_rows))}));
    }
}
CATCH

TEST_P(DeltaMergeStoreRWTest, TestFastScanForCleanRead)
try
{
    const size_t num_rows_write = 128;
    {
        // Create a block with sequential Int64 handle in range [0, 128)
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, 128, false);

        switch (mode)
        {
        case TestMode::PageStorageV2_MemoryOnly:
        case TestMode::Current_MemoryOnly:
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

    store->flushCache(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));

    store->compact(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));

    store->mergeDeltaAll(*db_context);

    // could do clean read with no optimization
    {
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(
            *db_context,
            db_context->getSettingsRef(),
            columns,
            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
            /* num_streams= */ 1,
            /* start_ts= */ std::numeric_limits<UInt64>::max(),
            EMPTY_FILTER,
            std::vector<RuntimeFilterPtr>{},
            0,
            TRACING_NAME,
            /* keep_order= */ false,
            /* is_fast_scan= */ true,
            /* expected_block_size= */ 1024)[0];
        ASSERT_INPUTSTREAM_COLS_UR(
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

    store->flushCache(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));

    store->compact(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));

    store->mergeDeltaAll(*db_context);

    // could do clean read with handle and del optimization
    {
        const auto & columns = store->getTableColumns();
        ColumnDefines real_columns;
        for (const auto & col : columns)
        {
            if (col.name != MutSup::extra_handle_column_name && col.name != MutSup::delmark_column_name)
            {
                real_columns.emplace_back(col);
            }
        }

        BlockInputStreamPtr in = store->read(
            *db_context,
            db_context->getSettingsRef(),
            real_columns,
            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
            /* num_streams= */ 1,
            /* start_ts= */ std::numeric_limits<UInt64>::max(),
            EMPTY_FILTER,
            std::vector<RuntimeFilterPtr>{},
            0,
            TRACING_NAME,
            /* keep_order= */ false,
            /* is_fast_scan= */ true,
            /* expected_block_size= */ 1024)[0];
        ASSERT_INPUTSTREAM_NROWS(in, num_rows_write - num_deleted_rows);
    }
}
CATCH


TEST_P(DeltaMergeStoreRWTest, TestFastScanWithLogicalSplit)
try
{
    constexpr auto num_write_rows = 32;
    auto table_column_defines = DMTestEnv::getDefaultColumns();
    store = reload(table_column_defines);

    //Test write multi blocks without overlap and do not compact
    {
        auto block1 = DMTestEnv::prepareSimpleWriteBlock(0, 1 * num_write_rows, false);
        auto block2 = DMTestEnv::prepareSimpleWriteBlock(1 * num_write_rows, 2 * num_write_rows, false);
        auto block3 = DMTestEnv::prepareSimpleWriteBlock(2 * num_write_rows, 3 * num_write_rows, false);
        switch (mode)
        {
        case TestMode::PageStorageV2_MemoryOnly:
        case TestMode::Current_MemoryOnly:
        {
            store->write(*db_context, db_context->getSettingsRef(), block1);
            store->write(*db_context, db_context->getSettingsRef(), block2);
            store->write(*db_context, db_context->getSettingsRef(), block3);
            break;
        }
        case TestMode::PageStorageV2_DiskOnly:
        case TestMode::Current_DiskOnly:
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
        case TestMode::PageStorageV2_MemoryAndDisk:
        case TestMode::Current_MemoryAndDisk:
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

    store->compact(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));

    store->mergeDeltaAll(*db_context);

    auto fastscan_rows = [&]() {
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(
            *db_context,
            db_context->getSettingsRef(),
            columns,
            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
            /* num_streams= */ 1,
            /* start_ts= */ std::numeric_limits<UInt64>::max(),
            EMPTY_FILTER,
            std::vector<RuntimeFilterPtr>{},
            0,
            TRACING_NAME,
            /* keep_order= */ false,
            /* is_fast_scan= */ true,
            /* expected_block_size= */ 1024)[0];
        size_t rows = 0;
        in->readPrefix();
        while (true)
        {
            auto b = in->read();
            if (!b)
            {
                break;
            }
            rows += b.rows();
        }
        return rows;
    };

    auto before_split = fastscan_rows();

    ASSERT_EQ(store->segments.size(), 1);
    auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
    auto old = store->segments.begin()->second;
    auto [left, right] = store->segmentSplit(
        *dm_context,
        old,
        DeltaMergeStore::SegmentSplitReason::ForegroundWrite,
        std::nullopt,
        DeltaMergeStore::SegmentSplitMode::Logical);
    ASSERT_NE(left, nullptr);
    ASSERT_NE(right, nullptr);
    ASSERT_EQ(store->segments.size(), 2);

    auto after_split = fastscan_rows();

    ASSERT_EQ(before_split, after_split);
}
CATCH

} // namespace DB::DM::tests
