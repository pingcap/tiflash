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

#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataStreams/IBlockInputStream.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/InputStreamTestUtils.h>

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
            auto read_cols_expr = fmt::format("{}.read().columns()", lhs_expr);
            auto rcols_expr = fmt::format("{}.columns()", rhs_expr);
            if (auto res = ::testing::internal::EqHelper<false>::Compare(
                    read_cols_expr.c_str(),
                    rcols_expr.c_str(),
                    block.columns(),
                    rhs.columns());
                !res)
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

    // Less rows than expected
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

::testing::AssertionResult InputStreamVSBlockUnrestrictlyCompareColumns(
    const char * stream_expr,
    const char * /*colnames_expr*/,
    const char * columns_expr,
    const BlockInputStreamPtr & stream,
    const Strings & colnames,
    const ColumnsWithTypeAndName & columns)
{
    RUNTIME_CHECK(stream != nullptr, Exception, fmt::format("The first param of ASSERT_INPUTSTREAM_COLS_UR, `{}` is nullptr!", stream_expr));
    RUNTIME_CHECK(
        colnames.size() == columns.size(),
        Exception,
        fmt::format("The length of second and thrid param of ASSERT_INPUTSTREAM_COLS_UR not match! {} != {}", colnames.size(), columns.size()));

    Block expect_block(columns);
    expect_block.checkNumberOfRows(); // check the input

    size_t num_rows_expect = expect_block.rows();
    size_t num_rows_read = 0;
    size_t prev_num_rows_read = 0;
    stream->readPrefix();
    while (Block read_block = stream->read())
    {
        num_rows_read += read_block.rows();

        if (num_rows_read > num_rows_expect)
        {
            auto reason = fmt::format(R"r(
  ({}).read() return more rows({}) than expected
  ({}).rows()
    Which is: {})r",
                                      stream_expr,
                                      num_rows_read,
                                      columns_expr,
                                      num_rows_expect);
            return ::testing::AssertionFailure() << reason;
        }

        // else, compare the the `read_block` to the [prev_num_rows_read, num_rows_read) rows of `expect_block`
        for (size_t col_idx = 0; col_idx < colnames.size(); ++col_idx)
        {
            const auto & col_name = colnames[col_idx];
            // Copy the [prev_num_rows_read, num_rows_read) of `expect_block`
            const auto & expect_full_col = expect_block.getByPosition(col_idx);
            auto expect_col = expect_full_col.cloneEmpty();
            auto column_data = expect_col.type->createColumn();
            column_data->insertRangeFrom(*expect_full_col.column, prev_num_rows_read, num_rows_read - prev_num_rows_read);
            expect_col.column = std::move(column_data);

            const auto & actual_col = read_block.getByName(col_name);
            if (auto res = DB::tests::columnEqual(expect_col, actual_col); !res)
            {
                auto expect_expr = fmt::format("expect block: {}", getColumnsContent(expect_block.getColumnsWithTypeAndName(), prev_num_rows_read, num_rows_read));
                Block actual_block_to_cmp;
                for (const auto & col_name : colnames)
                    actual_block_to_cmp.insert(read_block.getByName(col_name));
                auto actual_expr = fmt::format("actual block: {}", getColumnsContent(actual_block_to_cmp.getColumnsWithTypeAndName()));
                return res << fmt::format("\n  details: [column={}] [prev_nrows={}] [cur_nrows={}]:\n    {}\n    {}", col_name, prev_num_rows_read, num_rows_read, expect_expr, actual_expr);
            }
        }

        prev_num_rows_read += read_block.rows();
    }
    stream->readSuffix();

    if (num_rows_expect == num_rows_read)
        return ::testing::AssertionSuccess();

    // Less rows than expected
    auto reason = fmt::format(R"r(  ({}).read() return num of rows
    Which is: {}
  the num rows of ({})
    Which is: {})r",
                              stream_expr,
                              num_rows_read,
                              columns_expr,
                              num_rows_expect);
    return ::testing::AssertionFailure() << reason;
}

} // namespace tests
} // namespace DB
