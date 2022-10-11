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
#include <Storages/DeltaMerge/File/DMFileReader.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/PKSquashingBlockInputStream.h>
#include <Storages/DeltaMerge/ReadThread/ColumnSharingCache.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/DeltaMerge/tests/gtest_dm_delta_merge_store_test_basic.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/InputStreamTestUtils.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <common/logger_useful.h>
#include <common/types.h>

#include <algorithm>
#include <future>
#include <iterator>

namespace DB
{
namespace FailPoints
{
extern const char exception_in_merged_task_init[];
} // namespace FailPoints

namespace DM
{
namespace tests
{
TEST_P(DeltaMergeStoreRWTest, ExceptionInMergedTaskInit)
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
        store->write(*db_context, db_context->getSettingsRef(), block);
    }
    FailPointHelper::enableFailPoint(FailPoints::exception_in_merged_task_init);
    for (int i = 0; i < 100; i++)
    {
        // read all columns from store
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in1 = store->read(*db_context,
                                              db_context->getSettingsRef(),
                                              columns,
                                              {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                              /* num_streams= */ 1,
                                              /* max_version= */ std::numeric_limits<UInt64>::max(),
                                              EMPTY_FILTER,
                                              TRACING_NAME,
                                              /* keep_order= */ false,
                                              /* is_fast_scan= */ false,
                                              /* expected_block_size= */ 1024)[0];
        BlockInputStreamPtr in2 = store->read(*db_context,
                                              db_context->getSettingsRef(),
                                              columns,
                                              {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                              /* num_streams= */ 1,
                                              /* max_version= */ std::numeric_limits<UInt64>::max(),
                                              EMPTY_FILTER,
                                              TRACING_NAME,
                                              /* keep_order= */ false,
                                              /* is_fast_scan= */ false,
                                              /* expected_block_size= */ 1024)[0];
        try
        {
            auto b = in1->read();
        }
        catch (Exception & e)
        {
            ASSERT_EQ(e.code(), ErrorCodes::FAIL_POINT_ERROR);
        }

        try
        {
            auto b = in2->read();
        }
        catch (Exception & e)
        {
            ASSERT_EQ(e.code(), ErrorCodes::FAIL_POINT_ERROR);
        }
    }
    FailPointHelper::disableFailPoint(FailPoints::exception_in_merged_task_init);
}
CATCH


TEST_P(DeltaMergeStoreRWTest, DMFileNameChangedInDMFileReadPool)
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

    constexpr size_t num_rows_write = 128;
    // Ensure stable is not empty.
    {
        auto block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        block.insert(DB::tests::createColumn<String>(
            createNumberStrings(0, num_rows_write),
            col_str_define.name,
            col_str_define.id));
        block.insert(DB::tests::createColumn<Int8>(
            createSignedNumbers(0, num_rows_write),
            col_i8_define.name,
            col_i8_define.id));
        store->write(*db_context, db_context->getSettingsRef(), block);
        ASSERT_TRUE(store->flushCache(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())));
        store->mergeDeltaAll(*db_context);
        auto stable = store->id_to_segment.begin()->second->getStable();
        ASSERT_EQ(stable->getRows(), num_rows_write);
    }

    static const size_t NUMBER_OF_BLOCKS_IN_DELTA = 10;
    static const size_t NUMBER_OF_BLOCKS_IN_STABLE = 1;
    // Ensure delta is not empty.
    {
        for (size_t i = 0; i < NUMBER_OF_BLOCKS_IN_DELTA; ++i)
        {
            auto beg = num_rows_write * (i + 1);
            auto end = beg + num_rows_write;
            auto block = DMTestEnv::prepareSimpleWriteBlock(beg, end, false);
            block.insert(DB::tests::createColumn<String>(
                createNumberStrings(beg, end),
                col_str_define.name,
                col_str_define.id));
            block.insert(DB::tests::createColumn<Int8>(
                createSignedNumbers(beg, end),
                col_i8_define.name,
                col_i8_define.id));
            store->write(*db_context, db_context->getSettingsRef(), block);
            ASSERT_TRUE(store->flushCache(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())));
        }
        auto delta = store->id_to_segment.begin()->second->getDelta();
        ASSERT_EQ(delta->getRows(), num_rows_write * NUMBER_OF_BLOCKS_IN_DELTA);
    }

    // Check DMFile
    const auto & dmfiles = store->id_to_segment.begin()->second->getStable()->getDMFiles();
    ASSERT_EQ(dmfiles.size(), 1);
    auto dmfile = dmfiles.front();
    auto readable_path = DMFile::getPathByStatus(dmfile->parentPath(), dmfile->fileId(), DMFile::Status::READABLE);
    ASSERT_EQ(dmfile->path(), readable_path);
    ASSERT_EQ(DMFileReaderPool::instance().get(readable_path), nullptr);

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
                                             /* keep_order= */ false,
                                             /* is_fast_scan= */ false,
                                             /* expected_block_size= */ 128)[0];
        auto blk = in->read();
        // DMFileReader is created and add to DMFileReaderPool.
        auto * reader = DMFileReaderPool::instance().get(readable_path);
        ASSERT_NE(reader, nullptr);
        ASSERT_EQ(reader->path(), readable_path);

        // Update DMFile.
        ASSERT_TRUE(store->flushCache(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())));
        store->mergeDeltaAll(*db_context);
        auto stable = store->id_to_segment.begin()->second->getStable();
        ASSERT_EQ(stable->getRows(), (NUMBER_OF_BLOCKS_IN_DELTA + NUMBER_OF_BLOCKS_IN_STABLE) * num_rows_write);

        dmfile->remove(db_context->getFileProvider());
        ASSERT_NE(dmfile->path(), readable_path);

        in = nullptr;
        ASSERT_EQ(DMFileReaderPool::instance().get(readable_path), nullptr);
    }
}
CATCH

} // namespace tests
} // namespace DM
} // namespace DB
