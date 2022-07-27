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
size_t getInputStreamNRows(const BlockInputStreamPtr & stream)
{
    RUNTIME_CHECK(stream != nullptr, Exception(fmt::format("The inputstream is nullptr!")));

    size_t num_rows_read = 0;
    stream->readPrefix();
    while (true)
    {
        Block block = stream->read();
        // No more blocks
        if (!block)
            break;
        block.checkNumberOfRows();
        num_rows_read += block.rows();
    }
    stream->readSuffix();
    return num_rows_read;
}

::testing::AssertionResult InputStreamRowsLengthCompare(
    const char * stream_expr,
    const char * nrows_expr,
    const BlockInputStreamPtr & stream,
    const size_t num_rows_expect)
{
    RUNTIME_CHECK(stream != nullptr, Exception(fmt::format("The first param of ASSERT_INPUTSTREAM_NROWS, `{}` is nullptr!", stream_expr)));

    size_t num_rows_read = 0;
    stream->readPrefix();
    while (true)
    {
        try
        {
            Block read_block = stream->read();
            if (!read_block)
                break;
            read_block.checkNumberOfRows();
            num_rows_read += read_block.rows();
        }
        catch (...)
        {
            return ::testing::AssertionFailure() << fmt::format("exception thrown while reading from {}. Error: {}", stream_expr, getCurrentExceptionMessage(true, false));
        }
    }
    stream->readSuffix();

    if (num_rows_expect == num_rows_read)
        return ::testing::AssertionSuccess();

    auto reason = fmt::format(R"r(  ({}).read() return num of rows
    Which is: {}
  {}
    Which is: {})r",
                              stream_expr,
                              num_rows_read,
                              nrows_expr,
                              num_rows_expect);
    return ::testing::AssertionFailure() << reason;
}

::testing::AssertionResult InputStreamVSBlocksCompare(
    const char * stream_expr,
    const char * blocks_expr,
    const BlockInputStreamPtr & stream,
    const Blocks & blocks)
{
    RUNTIME_CHECK(stream != nullptr, Exception(fmt::format("The first param of ASSERT_INPUTSTREAM_NROWS, `{}` is nullptr!", stream_expr)));

    size_t block_idx = 0;
    size_t num_rows_expect = 0;
    size_t num_rows_read = 0;
    stream->readPrefix();
    while (Block read_block = stream->read())
    {
        read_block.checkNumberOfRows();

        if (block_idx == blocks.size())
        {
            auto reason = fmt::format(R"r(  ({}).read() return more blocks as expected
  {} only has {} blocks)r",
                                      stream_expr,
                                      blocks_expr,
                                      blocks.size());
            return ::testing::AssertionFailure() << reason;
        }

        blocks[block_idx].checkNumberOfRows(); // check the input
        if (auto res = DB::tests::blockEqual(blocks[block_idx], read_block); !res)
        {
            auto reason = fmt::format(R"r(
  ({}).read() return block is not equal to
  the {} block in ({}))r",
                                      stream_expr,
                                      blocks_expr,
                                      block_idx);
            return res << reason;
        }

        // continue to compare next block
        num_rows_read += read_block.rows();
        num_rows_expect += blocks[block_idx].rows();
        block_idx++;
    }
    stream->readSuffix();

    if (num_rows_expect == num_rows_read)
        return ::testing::AssertionSuccess();

    auto reason = fmt::format(R"r(  ({}).read() return num of rows
    Which is: {}
  sum( ({}).rows() )
    Which is: {})r",
                              stream_expr,
                              num_rows_read,
                              blocks_expr,
                              num_rows_expect);
    return ::testing::AssertionFailure() << reason;
}

::testing::AssertionResult InputStreamVSBlockUnrestrictlyCompare(
    const char * stream_expr,
    const char * block_expr,
    const BlockInputStreamPtr & stream,
    const Block & expect_block)
{
    RUNTIME_CHECK(stream != nullptr, Exception(fmt::format("The first param of ASSERT_INPUTSTREAM_NROWS, `{}` is nullptr!", stream_expr)));
    expect_block.checkNumberOfRows(); // check the input

    size_t num_rows_expect = expect_block.rows();
    size_t num_rows_read = 0;
    size_t prev_num_rows_read = 0;
    stream->readPrefix();
    while (Block read_block = stream->read())
    {
        read_block.checkNumberOfRows();
        num_rows_read += read_block.rows();
        // hot path, first block from inputstream and the rows is as expected
        if (prev_num_rows_read == 0 && read_block.rows() == num_rows_expect)
        {
            if (auto res = DB::tests::blockEqual(expect_block, read_block); !res)
            {
                auto reason = fmt::format(R"r(
  ({}).read() return block is not equal
    structure() == {}
  to the expect block ({})
    structure() == {})r",
                                          stream_expr,
                                          read_block.dumpJsonStructure(),
                                          block_expr,
                                          expect_block.dumpJsonStructure());
                return res << reason;
            }
        }

        if (num_rows_read > num_rows_expect)
        {
            auto reason = fmt::format(R"r(
  ({}).read() return more rows({}) than expected
  ({}).rows()
    Which is: {}
  last block is: {})r",
                                      stream_expr,
                                      num_rows_read,
                                      block_expr,
                                      num_rows_expect,
                                      getColumnsContent(read_block.getColumnsWithTypeAndName()));
            return ::testing::AssertionFailure() << reason;
        }

        // else, compare the the `block` to the [prev_num_rows_read, num_rows_read) rows of `rhs`
        {
            // num of columns
            auto read_cols_expr = fmt::format("{}.read().columns()", stream_expr);
            auto rcols_expr = fmt::format("{}.columns()", block_expr);
            if (auto res = ::testing::internal::EqHelper<false>::Compare(
                    read_cols_expr.c_str(),
                    rcols_expr.c_str(),
                    read_block.columns(),
                    expect_block.columns());
                !res)
            {
                return res;
            }
            for (size_t i = 0; i < expect_block.columns(); ++i)
            {
                const auto & actual_col = read_block.getByPosition(i);
                const auto & expected_full_col = expect_block.getByPosition(i);
                if (expected_full_col.column->isColumnConst() != actual_col.column->isColumnConst())
                {
                    // One is ColumnConst but the other is not
                    return ::testing::AssertionFailure() << fmt::format(
                               "  block[{}].isColumnConst() from actual block\n    {}\n  expect_block[{}].isColumnConst()\n    {}",
                               actual_col.name,
                               actual_col.column->isColumnConst(),
                               expected_full_col.name,
                               expected_full_col.column->isColumnConst());
                }
                else if (expected_full_col.column->isColumnConst() && actual_col.column->isColumnConst())
                {
                    if (auto res = dataTypeEqual(expected_full_col.type, actual_col.type); !res)
                        return res;
                    if (auto res = ::testing::internal::EqHelper<false>::Compare("", "", actual_col.column->size(), expected_full_col.column->size()); !res)
                    {
                        return res;
                    }
                    if (actual_col.column->compareAt(0, 0, *expected_full_col.column, -1) != 0)
                    {
                        return ::testing::AssertionFailure() << "Column Const data mismatch";
                    }
                }
                else
                {
                    auto expect_col = expected_full_col.cloneEmpty();
                    auto column_data = expect_col.type->createColumn();
                    column_data->insertRangeFrom(*expected_full_col.column, prev_num_rows_read, num_rows_read - prev_num_rows_read);
                    expect_col.column = std::move(column_data);
                    if (auto res = DB::tests::columnEqual(expect_col, actual_col); !res)
                    {
                        return res;
                    }
                }
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
                              block_expr,
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
    RUNTIME_CHECK(stream != nullptr, Exception(fmt::format("The first param of ASSERT_INPUTSTREAM_COLS_UR, `{}` is nullptr!", stream_expr)));
    RUNTIME_CHECK(
        colnames.size() == columns.size(),
        Exception(fmt::format("The length of second and thrid param of ASSERT_INPUTSTREAM_COLS_UR not match! {} != {}", colnames.size(), columns.size())));

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
    Which is: {}
  last block is: {})r",
                                      stream_expr,
                                      num_rows_read,
                                      columns_expr,
                                      num_rows_expect,
                                      getColumnsContent(read_block.getColumnsWithTypeAndName()));
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
