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

::testing::AssertionResult InputStreamRowsLengthCompare(
    const char * lhs_expr,
    const char * rhs_expr,
    const BlockInputStreamPtr & lhs,
    size_t num_rows_expect);
#define ASSERT_INPUTSTREAM_NROWS(val1, val2) ASSERT_PRED_FORMAT2(::DB::tests::InputStreamRowsLengthCompare, val1, val2)

::testing::AssertionResult InputStreamVSBlocksCompare(
    const char * lhs_expr,
    const char * rhs_expr,
    const BlockInputStreamPtr & lhs,
    const Blocks & rhs);
#define ASSERT_INPUTSTREAM_BLOCKS(val1, val2) ASSERT_PRED_FORMAT2(::DB::tests::InputStreamVSBlocksCompare, val1, val2)

// unrestrictly checking the blocks read from inputstream.
// Allowing the inputstream break the rows into serval smaller blocks.
::testing::AssertionResult InputStreamVSBlockUnrestrictlyCompare(
    const char * lhs_expr,
    const char * rhs_expr,
    const BlockInputStreamPtr & lhs,
    const Block & rhs);
#define ASSERT_INPUTSTREAM_BLOCK_UR(val1, val2) ASSERT_PRED_FORMAT2(::DB::tests::InputStreamVSBlockUnrestrictlyCompare, val1, val2)

// unrestrictly checking a part of columns read from inputstream.
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

} // namespace tests
} // namespace DB
