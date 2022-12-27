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
            if (i == (blocks.size() - 1))
            {
                ASSERT(blocks[i].rows() <= max_block_size);
                break;
            }
            ASSERT(blocks[i].rows() == max_block_size);
        }
    }
};

TEST_F(SquashingHashJoinBlockTransformTest, testALL)
try
{
    Int64 rows = 30000;
    Blocks test_blocks;
    for (Int64 i = 0; i < rows; ++i)
    {
        Block block{toVec({i})};
        test_blocks.push_back(block);
    }
    test_blocks.push_back(Block{});

    std::vector<Int64> block_size{1, 5, 10, 99, 999, 9999, 39999, DEFAULT_BLOCK_SIZE};

    for (auto size : block_size)
    {
        Blocks final_blocks;
        size_t index = 0;
        SquashingHashJoinBlockTransform squashing_transform(size);
        while (!squashing_transform.isJoinFinished())
        {
            while (squashing_transform.needAppendBlock())
            {
                Block result_block = test_blocks[index++];
                squashing_transform.appendBlock(result_block);
            }
            final_blocks.push_back(squashing_transform.getFinalOutputBlock());
        }
        check(final_blocks, std::min(size, rows));
    }
}
CATCH

} // namespace tests
} // namespace DB