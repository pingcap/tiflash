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
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/tests/gtest_dm_delta_merge_store_test_basic.h>

/// This test file is mainly test on the correctness of read in fast mode.
/// Because the basic functions are tested in gtest_dm_delta_merge_storage.cpp, we will not cover it here.

namespace DB
{
namespace FailPoints
{
} // namespace FailPoints

namespace DM
{
namespace tests
{

TEST_P(DeltaMergeStoreRWTest, TestFastModeWithOnlyInsertWithoutRangeFilter)
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
        // read all columns from store with all range in fast mode
        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(*db_context,
                                             db_context->getSettingsRef(),
                                             columns,
                                             {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                             /* num_streams= */ 1,
                                             /* max_version= */ std::numeric_limits<UInt64>::max(),
                                             EMPTY_FILTER,
                                             TRACING_NAME,
                                             /* is_raw_read= */ true,
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

TEST_P(DeltaMergeStoreRWTest, TestFastModeWithOnlyInsertWithRangeFilter)
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
        // read all columns from store with row key range in fast mode
        WriteBufferFromOwnString start_key_ss;
        DB::EncodeInt64(0, start_key_ss);

        WriteBufferFromOwnString end_key_ss;
        DB::EncodeInt64(64, end_key_ss);


        const auto & columns = store->getTableColumns();
        BlockInputStreamPtr in = store->read(*db_context,
                                             db_context->getSettingsRef(),
                                             columns,
                                             {RowKeyRange(
                                                 RowKeyValue(false, std::make_shared<String>(start_key_ss.releaseStr()), 0),
                                                 RowKeyValue(false, std::make_shared<String>(end_key_ss.releaseStr()), 64),
                                                 false,
                                                 store->getRowKeyColumnSize())},
                                             /* num_streams= */ 1,
                                             /* max_version= */ std::numeric_limits<UInt64>::max(),
                                             EMPTY_FILTER,
                                             TRACING_NAME,
                                             /* is_raw_read= */ true,
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
        ASSERT_EQ(num_rows_read, 64);
    }
}

TEST_P(DeltaMergeStoreRWTest, TestFastModeWithOnlyInsertWithMultipleBlockWithoutFlushCache)
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
        case TestMode::V2_Mix: // disk + memory
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
        BlockInputStreamPtr in = store->read(*db_context,
                                             db_context->getSettingsRef(),
                                             columns,
                                             {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                             /* num_streams= */ 1,
                                             /* max_version= */ std::numeric_limits<UInt64>::max(),
                                             EMPTY_FILTER,
                                             TRACING_NAME,
                                             /* is_raw_read= */ true,
                                             /* expected_block_size= */ 1024)[0];
        size_t num_rows_read = 0;
        in->readPrefix();
        switch (mode)
        {
        case TestMode::V1_BlockOnly:
        case TestMode::V2_BlockOnly:
        {
            while (Block block = in->read())
            {
                for (auto && iter : block)
                {
                    auto c = iter.column;
                    for (Int64 i = 0; i < Int64(c->size()); ++i)
                    {
                        if (iter.name == DMTestEnv::pk_name)
                        {
                            ASSERT_EQ(c->getInt(i), i + num_rows_read);
                        }
                    }
                }
                num_rows_read += block.rows();
            }
            break;
        }
        case TestMode::V2_FileOnly:
        {
            while (Block block = in->read())
            {
                for (auto && iter : block)
                {
                    auto c = iter.column;
                    for (Int64 i = 0; i < Int64(c->size()); ++i)
                    {
                        if (iter.name == DMTestEnv::pk_name)
                        {
                            ASSERT_EQ(c->getInt(i), i + num_rows_read);
                        }
                    }
                }
                num_rows_read += block.rows();
            }
            break;
        }
        case TestMode::V2_Mix:
        {
            int block_index = 0;
            int begin_value = 0; // persist first, then memory, finally stable
            while (Block block = in->read())
            {
                if (block_index == 1)
                {
                    begin_value = num_write_rows * 2;
                }
                else if (block_index == 2)
                {
                    begin_value = num_write_rows;
                }
                for (auto && iter : block)
                {
                    auto c = iter.column;
                    for (Int64 i = 0; i < Int64(c->size()); ++i)
                    {
                        if (iter.name == DMTestEnv::pk_name)
                        {
                            ASSERT_EQ(c->getInt(i), i + begin_value);
                        }
                    }
                }
                num_rows_read += block.rows();
                block_index += 1;
            }
            break;
        }
        }

        in->readSuffix();
        ASSERT_EQ(num_rows_read, 3 * num_write_rows);
    }
}
CATCH

TEST_P(DeltaMergeStoreRWTest, TestFastModeWithOnlyInsertWithMultipleBlockWithoutCompact)
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
                                             /* is_raw_read= */ true,
                                             /* expected_block_size= */ 1024)[0];
        size_t num_rows_read = 0;
        in->readPrefix();
        switch (mode)
        {
        case TestMode::V1_BlockOnly:
        case TestMode::V2_BlockOnly:
        {
            while (Block block = in->read())
            {
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
                num_rows_read += block.rows();
            }
            break;
        }
        case TestMode::V2_FileOnly:
        {
            while (Block block = in->read())
            {
                for (auto && iter : block)
                {
                    auto c = iter.column;
                    for (Int64 i = 0; i < Int64(c->size()); ++i)
                    {
                        if (iter.name == DMTestEnv::pk_name)
                        {
                            ASSERT_EQ(c->getInt(i), i + num_rows_read);
                        }
                    }
                }
                num_rows_read += block.rows();
            }
            break;
        }
        case TestMode::V2_Mix:
        {
            int block_index = 0;
            int begin_value = 0;
            while (Block block = in->read())
            {
                if (block_index == 1)
                {
                    begin_value = num_write_rows * 2;
                }
                else if (block_index == 2)
                {
                    begin_value = num_write_rows;
                }
                for (auto && iter : block)
                {
                    auto c = iter.column;
                    for (Int64 i = 0; i < Int64(c->size()); ++i)
                    {
                        if (iter.name == DMTestEnv::pk_name)
                        {
                            ASSERT_EQ(c->getInt(i), i + begin_value);
                        }
                    }
                }
                num_rows_read += block.rows();
                block_index += 1;
            }
            break;
        }
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, 3 * num_write_rows);
    }
}
CATCH

TEST_P(DeltaMergeStoreRWTest, TestFastModeWithOnlyInsertWithMultipleBlockWithCompact)
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

    store->compact(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));

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
                                             /* is_raw_read= */ true,
                                             /* expected_block_size= */ 1024)[0];
        size_t num_rows_read = 0;

        in->readPrefix();
        switch (mode)
        {
        case TestMode::V1_BlockOnly:
        case TestMode::V2_BlockOnly:
        {
            while (Block block = in->read())
            {
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
                num_rows_read += block.rows();
            }
            break;
        }
        case TestMode::V2_FileOnly:
        {
            while (Block block = in->read())
            {
                for (auto && iter : block)
                {
                    auto c = iter.column;
                    for (Int64 i = 0; i < Int64(c->size()); ++i)
                    {
                        if (iter.name == DMTestEnv::pk_name)
                        {
                            ASSERT_EQ(c->getInt(i), i + num_rows_read);
                        }
                    }
                }
                num_rows_read += block.rows();
            }
            break;
        }
        case TestMode::V2_Mix:
        {
            int block_index = 0;
            int begin_value = 0;

            while (Block block = in->read())
            {
                if (block_index == 1)
                {
                    begin_value = num_write_rows * 2;
                }
                else if (block_index == 2)
                {
                    begin_value = num_write_rows;
                }
                for (auto && iter : block)
                {
                    auto c = iter.column;
                    for (Int64 i = 0; i < Int64(c->size()); ++i)
                    {
                        if (iter.name == DMTestEnv::pk_name)
                        {
                            ASSERT_EQ(c->getInt(i), i + begin_value);
                        }
                    }
                }
                num_rows_read += block.rows();
                block_index += 1;
            }
            break;
        }
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, 3 * num_write_rows);
    }
}
CATCH

TEST_P(DeltaMergeStoreRWTest, TestFastModeWithOnlyInsertWithMultipleBlockWithCompactAndMergeDelta)
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

    store->compact(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));

    store->mergeDeltaAll(*db_context);

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
                                             /* is_raw_read= */ true,
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
                        ASSERT_EQ(c->getInt(i), i);
                    }
                }
            }
        }
        in->readSuffix();

        ASSERT_EQ(num_rows_read, 3 * num_write_rows);
    }
}
CATCH

// Insert + Update
TEST_P(DeltaMergeStoreRWTest, TestFastModeWithMultipleBlockWithOverlap)
try
{
    const size_t num_write_rows = 32;

    // Test write multi blocks with overlap and do compact
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

        // in V1_BlockOnly and V2_BlockOnly mode, flush cache will make sort
        store->flushCache(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));
    }

    store->compact(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));

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
                                             /* is_raw_read= */ true,
                                             /* expected_block_size= */ 1024)[0];
        size_t num_rows_read = 0;

        in->readPrefix();
        switch (mode)
        {
        case TestMode::V1_BlockOnly:
        case TestMode::V2_BlockOnly:
        {
            while (Block block = in->read())
            {
                for (auto && iter : block)
                {
                    if (iter.name == DMTestEnv::pk_name)
                    {
                        auto c = iter.column;
                        for (Int64 i = 0; i < Int64(c->size()); ++i)
                        {
                            if (i < Int64(num_write_rows / 2))
                            {
                                ASSERT_EQ(c->getInt(i), i);
                            }
                            else if (i < Int64(2.5 * num_write_rows))
                            {
                                ASSERT_EQ(c->getInt(i), (i - num_write_rows / 2) / 2 + num_write_rows / 2);
                            }
                            else
                            {
                                ASSERT_EQ(c->getInt(i), (i - num_write_rows * 2) + num_write_rows);
                            }
                        }
                    }
                }
                num_rows_read += block.rows();
            }
            break;
        }
        case TestMode::V2_FileOnly:
        {
            auto block_index = 0;
            auto begin_value = 0;

            while (Block block = in->read())
            {
                if (block_index == 1)
                {
                    begin_value = num_write_rows;
                }
                else if (block_index == 2)
                {
                    begin_value = num_write_rows / 2;
                }
                for (auto && iter : block)
                {
                    if (iter.name == DMTestEnv::pk_name)
                    {
                        auto c = iter.column;
                        for (Int64 i = 0; i < Int64(c->size()); ++i)
                        {
                            ASSERT_EQ(c->getInt(i), i + begin_value);
                        }
                    }
                }
                num_rows_read += block.rows();
                block_index += 1;
            }
            break;
        }
        case TestMode::V2_Mix:
        {
            auto block_index = 0;
            auto begin_value = num_write_rows;

            while (Block block = in->read())
            {
                if (block_index == 1)
                {
                    begin_value = 0;
                }
                else if (block_index == 2)
                {
                    begin_value = num_write_rows / 2;
                }
                for (auto && iter : block)
                {
                    if (iter.name == DMTestEnv::pk_name)
                    {
                        auto c = iter.column;
                        for (Int64 i = 0; i < Int64(c->size()); ++i)
                        {
                            ASSERT_EQ(c->getInt(i), i + begin_value);
                        }
                    }
                }
                num_rows_read += block.rows();
                block_index += 1;
            }

            break;
        }
        }

        in->readSuffix();
        ASSERT_EQ(num_rows_read, 3 * num_write_rows);
    }
}
CATCH

// Insert + Delete row
TEST_P(DeltaMergeStoreRWTest, TestFastModeWithDeleteRow)
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
            block2 = DMTestEnv::prepareSimpleWriteBlock(num_rows_write, 1.5 * num_rows_write, false, 3, DMTestEnv::pk_name, EXTRA_HANDLE_COLUMN_ID, EXTRA_HANDLE_COLUMN_INT_TYPE, false, 1, true, true);
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
        case TestMode::V1_BlockOnly:
        case TestMode::V2_BlockOnly:
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
        BlockInputStreamPtr in = store->read(*db_context,
                                             db_context->getSettingsRef(),
                                             columns,
                                             {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                             /* num_streams= */ 1,
                                             /* max_version= */ std::numeric_limits<UInt64>::max(),
                                             EMPTY_FILTER,
                                             TRACING_NAME,
                                             /* is_raw_read= */ true,
                                             /* expected_block_size= */ 1024)[0];
        size_t num_rows_read = 0;

        in->readPrefix();
        // filter del mark = 1， thus just read the insert data before delete
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
        in->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }

    store->flushCache(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));

    store->compact(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));

    store->mergeDeltaAll(*db_context);

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
                                             /* is_raw_read= */ true,
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
                        ASSERT_EQ(c->getInt(i), i);
                    }
                }
            }
        }
        in->readSuffix();

        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}
CATCH

// Insert + Delete Range
TEST_P(DeltaMergeStoreRWTest, TestFastModeWithDeleteRange)
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
                                             /* is_raw_read= */ true,
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
                        ASSERT_EQ(c->getInt(i), i);
                    }
                }
            }
        }
        in->readSuffix();
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
                                             /* is_raw_read= */ true,
                                             /* expected_block_size= */ 1024)[0];
        size_t num_rows_read = 0;

        // filter del mark = 1， thus just read the insert data before delete
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
                        ASSERT_EQ(c->getInt(i), i);
                    }
                }
            }
        }
        in->readSuffix();

        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}
CATCH

TEST_P(DeltaMergeStoreRWTest, TestFastModeWithDeleteWithMergeDelta)
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
        BlockInputStreamPtr in = store->read(*db_context,
                                             db_context->getSettingsRef(),
                                             columns,
                                             {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                             /* num_streams= */ 1,
                                             /* max_version= */ std::numeric_limits<UInt64>::max(),
                                             EMPTY_FILTER,
                                             TRACING_NAME,
                                             /* is_raw_read= */ true,
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
                        ASSERT_EQ(c->getInt(i), i + num_deleted_rows);
                    }
                }
            }
        }
        in->readSuffix();

        ASSERT_EQ(num_rows_read, num_rows_write - num_deleted_rows);
    }
}
CATCH

// insert + update + delete and fast mode first and then normal mode, to check the mode conversion is ok
TEST_P(DeltaMergeStoreRWTest, TestFastModeComplexWithModeConversion)
try
{
    const size_t num_write_rows = 128;

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

        // in V1_BlockOnly and V2_BlockOnly mode, flush cache will make sort
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
        BlockInputStreamPtr in = store->read(*db_context,
                                             db_context->getSettingsRef(),
                                             columns,
                                             {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                             /* num_streams= */ 1,
                                             /* max_version= */ std::numeric_limits<UInt64>::max(),
                                             EMPTY_FILTER,
                                             TRACING_NAME,
                                             /* is_raw_read= */ true,
                                             /* expected_block_size= */ 1024)[0];
        size_t num_rows_read = 0;

        in->readPrefix();
        switch (mode)
        {
        case TestMode::V1_BlockOnly:
        case TestMode::V2_BlockOnly:
        {
            while (Block block = in->read())
            {
                for (auto && iter : block)
                {
                    if (iter.name == DMTestEnv::pk_name)
                    {
                        auto c = iter.column;
                        for (Int64 i = 0; i < Int64(c->size()); ++i)
                        {
                            if (i < Int64(num_write_rows / 2))
                            {
                                ASSERT_EQ(c->getInt(i), i);
                            }
                            else if (i < Int64(2.5 * num_write_rows))
                            {
                                ASSERT_EQ(c->getInt(i), (i - num_write_rows / 2) / 2 + num_write_rows / 2);
                            }
                            else
                            {
                                ASSERT_EQ(c->getInt(i), (i - num_write_rows * 2) + num_write_rows);
                            }
                        }
                    }
                }
                num_rows_read += block.rows();
            }
            break;
        }
        case TestMode::V2_FileOnly:
        {
            auto block_index = 0;
            auto begin_value = 0;
            while (Block block = in->read())
            {
                if (block_index == 1)
                {
                    begin_value = num_write_rows;
                }
                else if (block_index == 2)
                {
                    begin_value = num_write_rows / 2;
                }
                for (auto && iter : block)
                {
                    if (iter.name == DMTestEnv::pk_name)
                    {
                        auto c = iter.column;
                        for (Int64 i = 0; i < Int64(c->size()); ++i)
                        {
                            ASSERT_EQ(c->getInt(i), i + begin_value);
                        }
                    }
                }
                num_rows_read += block.rows();
                block_index += 1;
            }
            break;
        }
        case TestMode::V2_Mix:
        {
            auto block_index = 0;
            auto begin_value = num_write_rows;
            while (Block block = in->read())
            {
                if (block_index == 1)
                {
                    begin_value = 0;
                }
                else if (block_index == 2)
                {
                    begin_value = num_write_rows / 2;
                }
                for (auto && iter : block)
                {
                    if (iter.name == DMTestEnv::pk_name)
                    {
                        auto c = iter.column;
                        for (Int64 i = 0; i < Int64(c->size()); ++i)
                        {
                            ASSERT_EQ(c->getInt(i), i + begin_value);
                        }
                    }
                }
                num_rows_read += block.rows();
                block_index += 1;
            }
            break;
        }
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, 3 * num_write_rows);
    }

    // Read with version in normal case
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
                                             /* is_raw_read= */ false,
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
                        ASSERT_EQ(c->getInt(i), i + num_write_rows / 2);
                    }
                }
            }
        }
        in->readSuffix();

        ASSERT_EQ(num_rows_read, 1.5 * num_write_rows);
    }
}
CATCH

TEST_P(DeltaMergeStoreRWTest, TestFastModeForCleanRead)
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

    store->flushCache(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));

    store->compact(*db_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));

    store->mergeDeltaAll(*db_context);

    // could do clean read with no optimization
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
                                             /* is_raw_read= */ true,
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
                        ASSERT_EQ(c->getInt(i), i);
                    }
                }
            }
        }
        in->readSuffix();

        ASSERT_EQ(num_rows_read, num_rows_write);
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

    // could do clean read with handle optimization
    {
        const auto & columns = store->getTableColumns();
        ColumnDefines real_columns;
        for (auto & col : columns)
        {
            if (col.name != EXTRA_HANDLE_COLUMN_NAME)
            {
                real_columns.emplace_back(col);
            }
        }

        BlockInputStreamPtr in = store->read(*db_context,
                                             db_context->getSettingsRef(),
                                             real_columns,
                                             {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                             /* num_streams= */ 1,
                                             /* max_version= */ std::numeric_limits<UInt64>::max(),
                                             EMPTY_FILTER,
                                             TRACING_NAME,
                                             /* is_raw_read= */ true,
                                             /* expected_block_size= */ 1024)[0];
        size_t num_rows_read = 0;

        in->readPrefix();
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
        }
        in->readSuffix();

        ASSERT_EQ(num_rows_read, num_rows_write - num_deleted_rows);
    }
}
CATCH
} // namespace tests
} // namespace DM
} // namespace DB