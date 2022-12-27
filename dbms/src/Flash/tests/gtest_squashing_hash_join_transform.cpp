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

#include <DataStreams/SquashingHashJoinBlockTransform.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>


namespace DB
{
namespace tests
{
class SquashingHashJoinBlockTransformTest : public ::testing::Test
{
public:
    void SetUp() override {}
    static ColumnWithTypeAndName toVec(const std::vector<Int64> & v)
    {
        return createColumn<Int64>(v);
    }

    static void check(Blocks blocks, UInt64 max_block_size)
    {
        for (size_t i = 0; i < blocks.size(); ++i)
        {
            ASSERT(blocks[i].rows() <= max_block_size);
        }
    }
};

TEST_F(SquashingHashJoinBlockTransformTest, testALL)
try
{
    std::vector<Int64> block_size{1, 5, 10, 99, 999, 9999, 39999, DEFAULT_BLOCK_SIZE};
    size_t merge_block_count = 10000;

    for (auto size : block_size)
    {
        Int64 expect_rows = 0;
        Blocks test_blocks;

        for (size_t i = 0; i < merge_block_count; ++i)
        {
            size_t rand_block_size = std::rand() % size + 1;
            expect_rows += rand_block_size;
            std::vector<Int64> values;
            for (size_t j = 0; j < rand_block_size; ++j)
            {
                values.push_back(1);
            }
            Block block{toVec(values)};
            test_blocks.push_back(block);
        }
        test_blocks.push_back(Block{});

        Blocks final_blocks;
        size_t index = 0;
        Int64 actual_rows = 0;
        SquashingHashJoinBlockTransform squashing_transform(size);
        while (!squashing_transform.isJoinFinished())
        {
            while (squashing_transform.needAppendBlock())
            {
                Block result_block = test_blocks[index++];
                squashing_transform.appendBlock(result_block);
            }
            final_blocks.push_back(squashing_transform.getFinalOutputBlock());
            actual_rows += final_blocks.back().rows();
        }
        check(final_blocks, std::min(size, expect_rows));
        ASSERT(actual_rows == expect_rows);
    }
}
CATCH

} // namespace tests
} // namespace DB