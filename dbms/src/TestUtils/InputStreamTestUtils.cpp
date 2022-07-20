#include <DataStreams/IBlockInputStream.h>
#include <TestUtils/InputStreamTestUtils.h>

#include "Core/ColumnWithTypeAndName.h"
#include "TestUtils/FunctionTestUtils.h"
#include "gtest/gtest.h"

namespace DB
{
namespace tests
{
::testing::AssertionResult InputStreamRowsLengthCompare(
    const char * lhs_expr,
    const char * rhs_expr,
    const BlockInputStreamPtr & lhs,
    const size_t num_rows_expect)
{
    RUNTIME_CHECK(lhs != nullptr, Exception, fmt::format("The first param of ASSERT_INPUTSTREAM_NROWS, `{}` is nullptr!", lhs_expr));

    size_t num_rows_read = 0;
    lhs->readPrefix();
    while (true)
    {
        try
        {
            Block block = lhs->read();
            if (!block)
                break;
            num_rows_read += block.rows();
        }
        catch (...)
        {
            return ::testing::AssertionFailure() << fmt::format("exception thrown while reading from {}. Error: {}", lhs_expr, getCurrentExceptionMessage(true, false));
        }
    }
    lhs->readSuffix();

    if (num_rows_expect == num_rows_read)
        return ::testing::AssertionSuccess();

    auto reason = fmt::format(R"r(  ({}).read() return num of rows
    Which is: {}
  {}
    Which is: {})r",
                              lhs_expr,
                              num_rows_read,
                              rhs_expr,
                              num_rows_expect);
    return ::testing::AssertionFailure() << reason;
}

::testing::AssertionResult InputStreamVSBlocksCompare(
    const char * lhs_expr,
    const char * rhs_expr,
    const BlockInputStreamPtr & lhs,
    const Blocks & rhs)
{
    RUNTIME_CHECK(lhs != nullptr, Exception, fmt::format("The first param of ASSERT_INPUTSTREAM_NROWS, `{}` is nullptr!", lhs_expr));

    size_t block_idx = 0;
    size_t num_rows_expect = 0;
    size_t num_rows_read = 0;
    lhs->readPrefix();
    while (Block block = lhs->read())
    {
        if (block_idx == rhs.size())
        {
            auto reason = fmt::format(R"r(  ({}).read() return more blocks as expected
  {} only has {} blocks)r",
                                      lhs_expr,
                                      rhs_expr,
                                      rhs.size());
            return ::testing::AssertionFailure() << reason;
        }

        if (auto res = DB::tests::blockEqual(rhs[block_idx], block); !res)
        {
            auto reason = fmt::format(R"r(
  ({}).read() return block is not equal to
  the {} block in ({}))r",
                                      lhs_expr,
                                      rhs_expr,
                                      block_idx);
            return res << reason;
        }

        // continue to compare next block
        num_rows_read += block.rows();
        num_rows_expect += rhs[block_idx].rows();
        block_idx++;
    }
    lhs->readSuffix();

    if (num_rows_expect == num_rows_read)
        return ::testing::AssertionSuccess();

    auto reason = fmt::format(R"r(  ({}).read() return num of rows
    Which is: {}
  sum( ({}).rows() )
    Which is: {})r",
                              lhs_expr,
                              num_rows_read,
                              rhs_expr,
                              num_rows_expect);
    return ::testing::AssertionFailure() << reason;
}

::testing::AssertionResult InputStreamVSBlockUnrestrictlyCompare(
    const char * lhs_expr,
    const char * rhs_expr,
    const BlockInputStreamPtr & lhs,
    const Block & rhs)
{
    RUNTIME_CHECK(lhs != nullptr, Exception, fmt::format("The first param of ASSERT_INPUTSTREAM_NROWS, `{}` is nullptr!", lhs_expr));

    size_t num_rows_expect = rhs.rows();
    size_t num_rows_read = 0;
    size_t prev_num_rows_read = 0;
    lhs->readPrefix();
    while (Block block = lhs->read())
    {
        num_rows_read += block.rows();
        // hot path, first block from inputstream and the rows is as expected
        if (prev_num_rows_read == 0 && block.rows() == num_rows_expect)
        {
            if (auto res = DB::tests::blockEqual(rhs, block); !res)
            {
                auto reason = fmt::format(R"r(
  ({}).read() return block is not equal to
  the block ({}))r",
                                          lhs_expr,
                                          rhs_expr);
                return res << reason;
            }
        }

        if (num_rows_read > num_rows_expect)
        {
            auto reason = fmt::format(R"r(
  ({}).read() return more rows({}) than expected
  ({}).rows()
    Which is: {})r",
                                      lhs_expr,
                                      num_rows_read,
                                      rhs_expr,
                                      num_rows_expect);
            return ::testing::AssertionFailure() << reason;
        }

        // else, compare the the `block` to the [prev_num_rows_read, num_rows_read) rows of `rhs`
        {
            // num of columns
            if (auto res = ::testing::internal::EqHelper<false>::Compare("", "", block.columns(), rhs.columns()); !res)
            {
                return res;
            }
            for (size_t i = 0; i < rhs.columns(); ++i)
            {
                const auto & expected_full_col = rhs.getByPosition(i);
                auto expect_col = expected_full_col.cloneEmpty();
                auto column_data = expect_col.type->createColumn();
                column_data->insertRangeFrom(*expected_full_col.column, prev_num_rows_read, num_rows_read - prev_num_rows_read);
                expect_col.column = std::move(column_data);
                const auto & actual_col = block.getByPosition(i);
                if (auto res = DB::tests::columnEqual(expect_col, actual_col); !res)
                {
                    return res;
                }
            }
        }

        prev_num_rows_read += block.rows();
    }
    lhs->readSuffix();

    if (num_rows_expect == num_rows_read)
        return ::testing::AssertionSuccess();

    auto reason = fmt::format(R"r(  ({}).read() return num of rows
    Which is: {}
  the num rows of ({})
    Which is: {})r",
                              lhs_expr,
                              num_rows_read,
                              rhs_expr,
                              num_rows_expect);
    return ::testing::AssertionFailure() << reason;
}

} // namespace tests
} // namespace DB
