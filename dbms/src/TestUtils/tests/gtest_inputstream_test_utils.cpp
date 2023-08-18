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

#include <Core/Block.h>
#include <DataStreams/BlocksListBlockInputStream.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/InputStreamTestUtils.h>

namespace DB
{
namespace tests
{
TEST(InputStreamTestUtilsTest, RawFuncPass)
try
{
    ASSERT_ANY_THROW(InputStreamVSBlockUnrestrictlyCompare("", "", nullptr, Block()););

    {
        // pass
        BlocksList blocks{
            Block({createColumn<String>({"hello", "world"}, "col1"), createColumn<Int64>({123, 456}, "col2")}),
            Block(
                {createColumn<String>({"tikv", "tidb", "pd", "tiflash"}, "col1"),
                 createColumn<Int64>({1, 2, 3, 4}, "col2")}),
        };
        BlockInputStreamPtr in = std::make_unique<BlocksListBlockInputStream>(std::move(blocks));
        EXPECT_TRUE(InputStreamVSBlockUnrestrictlyCompare(
            "",
            "",
            in,
            Block({
                createColumn<String>({"hello", "world", "tikv", "tidb", "pd", "tiflash"}, "col1"),
                createColumn<Int64>({123, 456, 1, 2, 3, 4}, "col2"),
            })));
    }
    {
        // pass
        BlocksList blocks{
            Block(
                {createColumn<String>({"hello", "world", "tikv", "tidb", "pd", "tiflash"}, "col1"),
                 createColumn<Int64>({123, 456, 1, 2, 3, 4}, "col2")}),
        };
        BlockInputStreamPtr in = std::make_unique<BlocksListBlockInputStream>(std::move(blocks));
        EXPECT_TRUE(InputStreamVSBlockUnrestrictlyCompare(
            "",
            "",
            in,
            Block({
                createColumn<String>({"hello", "world", "tikv", "tidb", "pd", "tiflash"}, "col1"),
                createColumn<Int64>({123, 456, 1, 2, 3, 4}, "col2"),
            })));
    }
}
CATCH

TEST(InputStreamTestUtilsTest, RawFuncColumnNotMatch)
try
{
    {
        // column num not match
        BlocksList blocks{
            Block({createColumn<String>({"hello", "world"}, "col1"), createColumn<Int64>({123, 456}, "col2")}),
            Block(
                {createColumn<String>({"tikv", "tidb", "pd", "tiflash"}, "col1"),
                 createColumn<Int64>({1, 2, 3, 4}, "col2")})};
        auto res = InputStreamVSBlockUnrestrictlyCompare(
            "in",
            "block",
            std::make_unique<BlocksListBlockInputStream>(std::move(blocks)),
            Block({
                createColumn<String>({"hello", "world", "tikv", "tidb", "pd", "tiflash"}, "col1"),
                createColumn<Int64>({123, 456, 1, 2, 3, 4}, "col2"),
                createColumn<Int64>({123, 456, 1, 2, 3, 4}, "col3"),
            }));
        EXPECT_FALSE(res);
    }
    {
        // column num not match
        BlocksList blocks{
            Block({createColumn<String>({"hello", "world"}, "col1"), createColumn<Int64>({123, 456}, "col2")}),
            Block(
                {createColumn<String>({"tikv", "tidb", "pd", "tiflash"}, "col1"),
                 createColumn<Int64>({1, 2, 3, 4}, "col2")})};
        auto res = InputStreamVSBlockUnrestrictlyCompare(
            "in",
            "block",
            std::make_unique<BlocksListBlockInputStream>(std::move(blocks)),
            Block({
                createColumn<String>({"hello", "world", "tikv", "tidb", "pd", "tiflash"}, "col1"),
            }));
        EXPECT_FALSE(res);
    }
}
CATCH

TEST(InputStreamTestUtilsTest, RawFuncRowsNotMatch)
try
{
    {
        // rows not match
        BlocksList blocks{
            Block({createColumn<String>({"hello", "world"}, "col1"), createColumn<Int64>({123, 456}, "col2")}),
            Block(
                {createColumn<String>({"tikv", "tidb", "pd", "tiflash"}, "col1"),
                 createColumn<Int64>({1, 2, 3, 4}, "col2")})};
        auto res = InputStreamVSBlockUnrestrictlyCompare(
            "in",
            "block",
            std::make_unique<BlocksListBlockInputStream>(std::move(blocks)),
            Block({
                createColumn<String>({"hello", "world", "tikv", "tidb", "pd", "tiflash", "br"}, "col1"),
                createColumn<Int64>({123, 456, 1, 2, 3, 4, 5}, "col2"),
            }));
        EXPECT_FALSE(res);
    }
    {
        // rows not match
        BlocksList blocks{
            Block({createColumn<String>({"hello", "world"}, "col1"), createColumn<Int64>({123, 456}, "col2")}),
            Block(
                {createColumn<String>({"tikv", "tidb", "pd", "tiflash"}, "col1"),
                 createColumn<Int64>({1, 2, 3, 4}, "col2")})};
        auto res = InputStreamVSBlockUnrestrictlyCompare(
            "in",
            "block",
            std::make_unique<BlocksListBlockInputStream>(std::move(blocks)),
            Block({
                createColumn<String>({"hello", "world", "tikv", "tidb", "pd"}, "col1"),
                createColumn<Int64>({123, 456, 1, 2, 3}, "col2"),
            }));
        EXPECT_FALSE(res);
    }
}
CATCH

TEST(InputStreamTestUtilsTest, RawFuncCellNotMatch)
try
{
    ASSERT_ANY_THROW(InputStreamVSBlockUnrestrictlyCompare("", "", nullptr, Block()););

    {
        BlocksList blocks{
            Block({createColumn<String>({"hello", "world"}, "col1"), createColumn<Int64>({123, 456}, "col2")}),
            Block(
                {createColumn<String>({"tikv", "tidb", "pd", "tiflash"}, "col1"),
                 createColumn<Int64>({1, 2, 3, 4}, "col2")}),
        };
        BlockInputStreamPtr in = std::make_unique<BlocksListBlockInputStream>(std::move(blocks));
        auto res = InputStreamVSBlockUnrestrictlyCompare(
            "",
            "",
            in,
            Block({
                createColumn<String>({"hello", "world", "tikv", "tidb", "pd", "tiflash"}, "col1"),
                createColumn<Int64>({123, 456, 1, 2, 3, /*wrong value*/ 5}, "col2"),
            }));
        EXPECT_FALSE(res);
    }
    {
        // pass
        BlocksList blocks{
            Block(
                {createColumn<String>({"hello", "world", "tikv", "tidb", "pd", "tiflash"}, "col1"),
                 createColumn<Int64>({123, 456, 1, 2, 3, 4}, "col2")}),
        };
        BlockInputStreamPtr in = std::make_unique<BlocksListBlockInputStream>(std::move(blocks));
        auto res = InputStreamVSBlockUnrestrictlyCompare(
            "",
            "",
            in,
            Block({
                createColumn<String>({"hello", "world", "tikv", "tidb", "pd", /*wrong value*/ "flash"}, "col1"),
                createColumn<Int64>({123, 456, 1, 2, 3, 4}, "col2"),
            }));
        EXPECT_FALSE(res);
    }
}
CATCH

TEST(InputStreamTestUtilsTest, CompareInputStreamNRows)
try
{
    // Only check number of rows
    {
        BlocksList blocks{};
        BlockInputStreamPtr in = std::make_unique<BlocksListBlockInputStream>(std::move(blocks));
        ASSERT_INPUTSTREAM_NROWS(in, 0);
    }
    {
        BlocksList blocks{
            Block({createConstColumn<String>(100, "test_data", "col1"), createConstColumn<Int64>(100, 123, "col2")})};
        BlockInputStreamPtr in = std::make_unique<BlocksListBlockInputStream>(std::move(blocks));
        ASSERT_INPUTSTREAM_NROWS(in, 100);
    }
    {
        BlocksList blocks{
            Block({createColumn<String>({"hello", "world"}, "col1"), createColumn<Int64>({123, 456}, "col2")}),
            Block(
                {createColumn<String>({"tikv", "tidb", "pd", "tiflash"}, "col1"),
                 createColumn<Int64>({1, 2, 3, 4}, "col2")})};
        BlockInputStreamPtr in = std::make_unique<BlocksListBlockInputStream>(std::move(blocks));
        ASSERT_INPUTSTREAM_NROWS(in, 6);
    }
}
CATCH

TEST(InputStreamTestUtilsTest, CompareInputStreamBlock)
try
{
    // Check the block read
    {
        BlocksList blocks{};
        BlockInputStreamPtr in = std::make_unique<BlocksListBlockInputStream>(std::move(blocks));
        ASSERT_INPUTSTREAM_BLOCKS(in, Blocks());
    }
    {
        BlocksList blocks{
            Block({createConstColumn<String>(100, "test_data", "col1"), createConstColumn<Int64>(100, 123, "col2")})};
        BlockInputStreamPtr in = std::make_unique<BlocksListBlockInputStream>(std::move(blocks));
        ASSERT_INPUTSTREAM_BLOCKS(
            in,
            Blocks({Block{
                createConstColumn<String>(100, "test_data", "col1"),
                createConstColumn<Int64>(100, 123, "col2")}}));
    }
    {
        BlocksList blocks{
            Block({createColumn<String>({"hello", "world"}, "col1"), createColumn<Int64>({123, 456}, "col2")}),
            Block(
                {createColumn<String>({"tikv", "tidb", "pd", "tiflash"}, "col1"),
                 createColumn<Int64>({1, 2, 3, 4}, "col2")})};
        BlockInputStreamPtr in = std::make_unique<BlocksListBlockInputStream>(std::move(blocks));
        ASSERT_INPUTSTREAM_BLOCKS(
            in,
            Blocks({
                Block({createColumn<String>({"hello", "world"}, "col1"), createColumn<Int64>({123, 456}, "col2")}),
                Block(
                    {createColumn<String>({"tikv", "tidb", "pd", "tiflash"}, "col1"),
                     createColumn<Int64>({1, 2, 3, 4}, "col2")}),
            }));
    }

    // unrestrictly check
    {
        //the input stream return smaller blocks
        BlocksList blocks{
            Block({createColumn<String>({"hello", "world"}, "col1"), createColumn<Int64>({123, 456}, "col2")}),
            Block(
                {createColumn<String>({"tikv", "tidb", "pd", "tiflash"}, "col1"),
                 createColumn<Int64>({1, 2, 3, 4}, "col2")})};
        BlockInputStreamPtr in = std::make_unique<BlocksListBlockInputStream>(std::move(blocks));
        ASSERT_INPUTSTREAM_BLOCK_UR(
            in,
            Block({//
                   createColumn<String>({"hello", "world", "tikv", "tidb", "pd", "tiflash"}, "col1"),
                   createColumn<Int64>({123, 456, 1, 2, 3, 4}, "col2")}));
    }
    {
        //the input stream return excatly the same block
        BlocksList blocks{Block(
            {createColumn<String>({"hello", "world", "tikv", "tidb", "pd", "tiflash"}, "col1"),
             createColumn<Int64>({123, 456, 1, 2, 3, 4}, "col2")})};
        BlockInputStreamPtr in = std::make_unique<BlocksListBlockInputStream>(std::move(blocks));
        ASSERT_INPUTSTREAM_BLOCK_UR(
            in,
            Block({//
                   createColumn<String>({"hello", "world", "tikv", "tidb", "pd", "tiflash"}, "col1"),
                   createColumn<Int64>({123, 456, 1, 2, 3, 4}, "col2")}));
    }
}
CATCH

TEST(InputStreamTestUtilsTest, CompareInputStreamColumns)
try
{
    // unrestrictly check a part of columns
    {
        //the input stream return smaller blocks, only check col1
        BlocksList blocks{
            Block({createColumn<String>({"hello", "world"}, "col1"), createColumn<Int64>({123, 456}, "col2")}),
            Block(
                {createColumn<String>({"tikv", "tidb", "pd", "tiflash"}, "col1"),
                 createColumn<Int64>({1, 2, 3, 4}, "col2")})};
        BlockInputStreamPtr in = std::make_unique<BlocksListBlockInputStream>(std::move(blocks));
        ASSERT_INPUTSTREAM_COLS_UR(
            in,
            Strings({"col1"}),
            createColumns({createColumn<String>({"hello", "world", "tikv", "tidb", "pd", "tiflash"})}));
    }
    {
        //the input stream return smaller blocks, only check col2
        BlocksList blocks{
            Block({createColumn<String>({"hello", "world"}, "col1"), createColumn<Int64>({123, 456}, "col2")}),
            Block(
                {createColumn<String>({"tikv", "tidb", "pd", "tiflash"}, "col1"),
                 createColumn<Int64>({1, 2, 3, 4}, "col2")})};
        BlockInputStreamPtr in = std::make_unique<BlocksListBlockInputStream>(std::move(blocks));
        ASSERT_INPUTSTREAM_COLS_UR(in, Strings({"col2"}), createColumns({createColumn<Int64>({123, 456, 1, 2, 3, 4})}));
    }
}
CATCH

} // namespace tests
} // namespace DB
