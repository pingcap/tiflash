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

#pragma once

#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <memory>
namespace DB
{
class IBlockInputStream;
using BlockInputStreamPtr = std::shared_ptr<IBlockInputStream>;

namespace tests
{
/// helper functions for comparing the result of input stream

// Get the num of rows read from inputstream
size_t getInputStreamNRows(const BlockInputStreamPtr & stream);

// Checking the num of rows read from inputstream
::testing::AssertionResult InputStreamRowsLengthCompare(
    const char * stream_expr,
    const char * nrows_expr,
    const BlockInputStreamPtr & stream,
    const size_t num_rows_expect);
#define ASSERT_INPUTSTREAM_NROWS(val1, val2) ASSERT_PRED_FORMAT2(::DB::tests::InputStreamRowsLengthCompare, val1, val2)

// Checking the blocks read from inputstream.
// The inputstream must return the blocks strictly equal to `blocks`.
::testing::AssertionResult InputStreamVSBlocksCompare(
    const char * stream_expr,
    const char * blocks_expr,
    const BlockInputStreamPtr & stream,
    const Blocks & blocks);
#define ASSERT_INPUTSTREAM_BLOCKS(val1, val2) ASSERT_PRED_FORMAT2(::DB::tests::InputStreamVSBlocksCompare, val1, val2)

// Unrestrictly checking the blocks read from inputstream.
// Allowing the inputstream break the rows into serval smaller blocks.
::testing::AssertionResult InputStreamVSBlockUnrestrictlyCompare(
    const char * stream_expr,
    const char * block_expr,
    const BlockInputStreamPtr & stream,
    const Block & expect_block);
#define ASSERT_INPUTSTREAM_BLOCK_UR(val1, val2) \
    ASSERT_PRED_FORMAT2(::DB::tests::InputStreamVSBlockUnrestrictlyCompare, val1, val2)

// Unrestrictly checking a part of columns read from inputstream.
// Allowing the inputstream break the rows into serval smaller blocks.
// Only check the read columns with name in `colnames`
// The size of `colnames` must be the same as `colunms`
::testing::AssertionResult InputStreamVSBlockUnrestrictlyCompareColumns(
    const char * stream_expr,
    const char * colnames_expr,
    const char * columns_expr,
    const BlockInputStreamPtr & stream,
    const Strings & colnames,
    const ColumnsWithTypeAndName & columns);
#define ASSERT_INPUTSTREAM_COLS_UR(stream, colnames, columns) \
    ASSERT_PRED_FORMAT3(::DB::tests::InputStreamVSBlockUnrestrictlyCompareColumns, stream, colnames, columns)

// Similar to `InputStreamVSBlockUnrestrictlyCompareColumns` but assume inputstream's blocks are unordered.
// It is only used for normal mode. (The blocks of fast mode have overlap.)
// This function read all blocks first and sort them by handle column or column at position 0 if MutSup::extra_handle_column_name not exist.
::testing::AssertionResult UnorderedInputStreamVSBlockUnrestrictlyCompareColumns(
    const char * stream_expr,
    const char * colnames_expr,
    const char * columns_expr,
    const BlockInputStreamPtr & stream,
    const Strings & colnames,
    const ColumnsWithTypeAndName & columns);
#define ASSERT_UNORDERED_INPUTSTREAM_COLS_UR(stream, colnames, columns) \
    ASSERT_PRED_FORMAT3(::DB::tests::UnorderedInputStreamVSBlockUnrestrictlyCompareColumns, stream, colnames, columns)
} // namespace tests
} // namespace DB
