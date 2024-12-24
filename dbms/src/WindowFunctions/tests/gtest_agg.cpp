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
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/WindowTestUtils.h>
#include <TestUtils/mockExecutor.h>

#include <optional>

namespace DB::tests
{
class WindowAggFuncTest : public DB::tests::WindowTest
{
public:
    const ASTPtr value_col = col(VALUE_COL_NAME);

    void initializeContext() override { ExecutorTest::initializeContext(); }

protected:
    static std::vector<Int64> partition;
    static std::vector<Int64> order;
    static std::vector<Int64> int_value;
};

// TODO uncomment them
std::vector<Int64> WindowAggFuncTest::partition = {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 4, 5, 6, 6, 6};
std::vector<Int64> WindowAggFuncTest::order =     {0, 0, 1, 3, 6, 0, 1, 4, 7, 8, 0, 4, 6, 10, 20, 40, 41, 0, 0, 0, 10, 30};
std::vector<Int64> WindowAggFuncTest::int_value
    = {0, -1, 0, 4, 6, 2, 0, -4, -2, 1, 7, -3, 9, -9, -3, 2, 1, 4, -5, 2, 5, 0};

// std::vector<Int64> WindowAggFuncTest::partition = {2, 2};
// std::vector<Int64> WindowAggFuncTest::order =     {7, 8};
// std::vector<Int64> WindowAggFuncTest::int_value
//     = {-2, 1};

// TODO test frame position list:
// 1. prev_start = frame_start, prev_end = frame_end
// 2. prev_start < frame_start, prev_end = frame_end
// 3. prev_start < frame_start, prev_end < frame_end
// 4. prev_end <= frame_start

// TODO decrease and add test list:
// 1. decrease = 0
// 2. decrease < add
// 3. decrease = add
// 4. decrease > add
// 5. add = 0

// TODO maybe we need to reset null flag for AggregateFunctionNull
// TODO test duplicate order by values in range frame
TEST_F(WindowAggFuncTest, Sum)
try
{
    {
    // {
    //     // rows frame
    //     MockWindowFrame frame;
    //     frame.type = tipb::WindowFrameType::Rows;
    //     frame.end = mock::MockWindowFrameBound(tipb::WindowBoundType::Following, false, 0);
    //     std::vector<Int64> frame_start_offset{0, 1, 3, 10};
    //     std::vector<Int64> frame_end_offset{0, 1, 3, 10};

    //     std::vector<std::vector<std::optional<Int64>>> res{
    //         {0, -1, 0, 4, 6, 2, 0, -4, -2, 1, 7, -3, 9, -9, -3, 2, 1, 4, -5, 2, 5, 0},
    //         {0, -1, -1, 4, 10, 2, 2, -4, -6, -1, 7, 4, 6, 0, -12, -1, 3, 4, -5, 2, 7, 5},
    //         {0, -1, -1, 3, 9, 2, 2, -2, -4, -5, 7, 4, 13, 4, -6, -1, -9, 4, -5, 2, 7, 7},
    //         {0, -1, -1, 3, 9, 2, 2, -2, -4, -3, 7, 4, 13, 4, 1, 3, 4, 4, -5, 2, 7, 7}};

    //     for (size_t i = 0; i < frame_start_offset.size(); ++i)
    //     {
    //         std::cout << "--------- i: " << i << std::endl;
    //         frame.start = mock::MockWindowFrameBound(tipb::WindowBoundType::Preceding, false, frame_start_offset[i]);

    //         executeFunctionAndAssert(
    //             toNullableVec<Int64>(res[i]),
    //             Sum(value_col),
    //             {toVec<Int64>(partition), toVec<Int64>(order), toVec<Int64>(int_value)},
    //             frame);
    //     }

    //     res
    //         = {{0, -1, 0, 4, 6, 2, 0, -4, -2, 1, 7, -3, 9, -9, -3, 2, 1, 4, -5, 2, 5, 0},
    //            {0, -1, 4, 10, 6, 2, -4, -6, -1, 1, 4, 6, 0, -12, -1, 3, 1, 4, -5, 7, 5, 0},
    //            {0, 9, 10, 10, 6, -4, -5, -5, -1, 1, 4, -6, -1, -9, 0, 3, 1, 4, -5, 7, 5, 0},
    //            {0, 9, 10, 10, 6, -3, -5, -5, -1, 1, 4, -3, 0, -9, 0, 3, 1, 4, -5, 7, 5, 0}};

    //     frame.start = mock::MockWindowFrameBound(tipb::WindowBoundType::Preceding, false, 0);
    //     for (size_t i = 0; i < frame_end_offset.size(); ++i)
    //     {
    //         std::cout << "--------- i: " << i << std::endl;
    //         frame.end = mock::MockWindowFrameBound(tipb::WindowBoundType::Following, false, frame_end_offset[i]);

    //         executeFunctionAndAssert(
    //             toNullableVec<Int64>(res[i]),
    //             Sum(value_col),
    //             {toVec<Int64>(partition), toVec<Int64>(order), toVec<Int64>(int_value)},
    //             frame);
    //     }
    // }
    }

    {
        // range frame
        MockWindowFrame frame;
        frame.type = tipb::WindowFrameType::Ranges;
        frame.start = buildRangeFrameBound<Int64>(tipb::WindowBoundType::Preceding, tipb::RangeCmpDataType::Int, ORDER_COL_NAME, false, 0);
        frame.end = buildRangeFrameBound<Int64>(tipb::WindowBoundType::Following, tipb::RangeCmpDataType::Int, ORDER_COL_NAME, true, 0);
        // std::vector<Int64> frame_end_offset{0, 1, 3, 10}; // TODO uncomment it
        std::vector<Int64> frame_end_offset{0, 1, 3, 10};

        // TODO uncomment it
        std::vector<std::vector<std::optional<Int64>>> res{
            {0, -1, 0, 4, 6, 2, 0, -4, -2, 1, 7, -3, 9, -9, -3, 2, 1, 4, -5, 2, 5, 0},
            {0, -1, 0, 4, 6, 2, 0, -4, -1, 1, 7, -3, 9, -9, -3, 3, 1, 4, -5, 2, 5, 0},
            {0, 3, 4, 10, 6, 2, -4, -6, -1, 1, 7, 6, 9, -9, -3, 3, 1, 4, -5, 2, 5, 0},
            {0, 9, 10, 10, 6, -3, -5, -5, -1, 1, 4, -3, 0, -12, -3, 3, 1, 4, -5, 7, 5, 0}
        };

        // std::vector<std::vector<std::optional<Int64>>> res{
        //     // {0, -1, 0, 4, 6, 2, 0, -4, -2, 1, 7, -3, 9, -9, -3, 2, 1, 4, -5, 2, 5, 0},
        //     {-1, 1},
        //     // {0, 3, 4, 10, 6, 2, -4, -6, -1, 1, 7, 6, 9, -9, -3, 3, 1, 4, -5, 2, 5, 0},
        //     // {0, 9, 10, 10, 6, -3, -5, -5, -1, 1, 4, -3, 0, -12, -3, 3, 1, 4, -5, 7, 5, 0}
        // };

        for (size_t i = 0; i < frame_end_offset.size(); i++)
        {
            std::cout << "-------- i: " << i << std::endl;
            frame.end = buildRangeFrameBound<Int64>(tipb::WindowBoundType::Following, tipb::RangeCmpDataType::Int, ORDER_COL_NAME, true, frame_end_offset[i]);

            executeFunctionAndAssert(
                toNullableVec<Int64>(res[i]),
                Sum(value_col),
                {toVec<Int64>(partition),
                toVec<Int64>(order),
                toVec<Int64>(int_value)},
                frame);
        }
    }
}
CATCH

TEST_F(WindowAggFuncTest, Count)
try
{
    // TODO add tests
}
CATCH

TEST_F(WindowAggFuncTest, Avg)
try
{
    // TODO add tests
}
CATCH

TEST_F(WindowAggFuncTest, Min)
try
{
    // TODO add tests
}
CATCH

TEST_F(WindowAggFuncTest, Max)
try
{
    // TODO add tests
}
CATCH
} // namespace DB::tests
