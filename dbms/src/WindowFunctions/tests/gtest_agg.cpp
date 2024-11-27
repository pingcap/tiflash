// Copyright 2024 PingCAP, Ltd.
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

#include <Common/Decimal.h>
#include <Interpreters/Context.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/WindowTestUtils.h>
#include <TestUtils/mockExecutor.h>


namespace DB::tests
{
class WindowAggFuncTest : public DB::tests::WindowTest
{
public:
    const ASTPtr value_col = col(VALUE_COL_NAME);

    void initializeContext() override
    {
        ExecutorTest::initializeContext();
    }
};

TEST_F(WindowAggFuncTest, windowAggSumTests)
try
{
    {
        // rows frame
        MockWindowFrame frame;
        frame.type = tipb::WindowFrameType::Rows;
        frame.start = mock::MockWindowFrameBound(tipb::WindowBoundType::Preceding, false, 0);
        frame.end = mock::MockWindowFrameBound(tipb::WindowBoundType::Following, false, 3);
        std::vector<Int64> frame_start_offset{0, 1, 3, 10};

        std::vector<std::vector<Int64>> res{
            {0, 15, 14, 12, 8, 26, 41, 38, 28, 15, 18, 32, 49, 75, 66, 51, 31},
            {0, 15, 15, 14, 12, 26, 41, 41, 38, 28, 18, 33, 52, 80, 75, 66, 51},
            {0, 15, 15, 15, 15, 26, 41, 41, 41, 41, 18, 33, 53, 84, 83, 80, 75},
            {0, 15, 15, 15, 15, 26, 41, 41, 41, 41, 18, 33, 53, 84, 84, 84, 84}};

        for (size_t i = 0; i < frame_start_offset.size(); ++i)
        {
            frame.start = mock::MockWindowFrameBound(tipb::WindowBoundType::Preceding, false, frame_start_offset[i]);

            executeFunctionAndAssert(
                toVec<Int64>(res[i]),
                Sum(value_col),
                {toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3}),
                 toVec<Int64>(/*order*/ {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31}),
                 toVec<Int64>(/*value*/ {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31})},
                frame);
        }
    }

    // TODO uncomment these test after range frame is merged
    // {
    //     // range frame
    //     MockWindowFrame frame;
    //     frame.type = tipb::WindowFrameType::Rows;
    //     frame.start = buildRangeFrameBound(tipb::WindowBoundType::Preceding, tipb::RangeCmpDataType::Int, ORDER_COL_NAME, false, 0);
    //     frame.end = buildRangeFrameBound(tipb::WindowBoundType::Following, tipb::RangeCmpDataType::Int, ORDER_COL_NAME, true, 3);
    //     std::vector<Int64> frame_start_offset{0, 1, 3, 10};

    //     std::vector<std::vector<Int64>> res_not_null{
    //         {0, 7, 6, 4, 8, 3, 3, 23, 28, 15, 4, 8, 5, 9, 15, 20, 31},
    //         {0, 7, 7, 4, 8, 3, 3, 23, 28, 15, 4, 8, 5, 9, 15, 20, 31},
    //         {0, 7, 7, 7, 8, 3, 3, 23, 38, 28, 4, 9, 8, 9, 15, 20, 31},
    //         {0, 7, 7, 7, 15, 3, 3, 26, 41, 38, 4, 9, 9, 18, 29, 35, 31}};

    //     for (size_t i = 0; i < frame_start_offset.size(); ++i)
    //     {
    //         frame.start = buildRangeFrameBound(tipb::WindowBoundType::Preceding, tipb::RangeCmpDataType::Int, ORDER_COL_NAME, false, 0);

    //         executeFunctionAndAssert(
    //             toVec<Int64>(res_not_null[i]),
    //             Sum(value_col),
    //             {toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3}),
    //             toVec<Int64>(/*order*/ {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31}),
    //             toVec<Int64>(/*value*/ {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31})},
    //             frame);
    //     }
    // }
}
CATCH

TEST_F(WindowAggFuncTest, windowAggCountTests)
try
{
    {
        // rows frame
        MockWindowFrame frame;
        frame.type = tipb::WindowFrameType::Rows;
        frame.start = mock::MockWindowFrameBound(tipb::WindowBoundType::Preceding, false, 0);
        frame.end = mock::MockWindowFrameBound(tipb::WindowBoundType::Following, false, 3);
        std::vector<Int64> frame_start_offset{0, 1, 3, 10};

        std::vector<std::vector<Int64>> res{
            {1, 4, 3, 2, 1, 4, 4, 3, 2, 1, 4, 4, 4, 4, 3, 2, 1},
            {1, 4, 4, 3, 2, 4, 5, 4, 3, 2, 4, 5, 5, 5, 4, 3, 2},
            {1, 4, 4, 4, 4, 4, 5, 5, 5, 4, 4, 5, 6, 7, 6, 5, 4},
            {1, 4, 4, 4, 4, 4, 5, 5, 5, 5, 4, 5, 6, 7, 7, 7, 7}};

        for (size_t i = 0; i < frame_start_offset.size(); ++i)
        {
            frame.start = mock::MockWindowFrameBound(tipb::WindowBoundType::Preceding, false, frame_start_offset[i]);

            executeFunctionAndAssert(
                toVec<Int64>(res[i]),
                Count(value_col),
                {toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3}),
                 toVec<Int64>(/*order*/ {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31}),
                 toVec<Int64>(/*value*/ {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31})},
                frame);
        }
    }
    // TODO add range frame tests after that is merged
}
CATCH
} // namespace DB::tests
