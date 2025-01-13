// Copyright 2025 PingCAP, Ltd.
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
#include <Interpreters/WindowDescription.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/WindowTestUtils.h>
#include <TestUtils/mockExecutor.h>
#include <gtest/gtest.h>

#include <optional>

namespace DB::tests
{
struct TestCase
{
    TestCase(
        const ASTPtr & ast_func_,
        const std::vector<Int64> & start_offsets_,
        const std::vector<Int64> & end_offsets_,
        const std::vector<std::vector<std::optional<Int64>>> & results_,
        const std::vector<std::vector<std::optional<Float64>>> & float_results_,
        bool is_range_frame_,
        bool is_input_value_nullable_,
        bool is_return_type_int_ = true)
        : ast_func(ast_func_)
        , start_offsets(start_offsets_)
        , end_offsets(end_offsets_)
        , results(results_)
        , float_results(float_results_)
        , is_range_frame(is_range_frame_)
        , is_input_value_nullable(is_input_value_nullable_)
        , is_return_type_int(is_return_type_int_)
        , test_case_num(start_offsets.size())
    {
        assert(test_case_num == end_offsets.size());
        assert((test_case_num == results.size()) || (test_case_num == float_results.size()));
    }

    ASTPtr ast_func;
    std::vector<Int64> start_offsets;
    std::vector<Int64> end_offsets;
    std::vector<std::vector<std::optional<Int64>>> results;
    std::vector<std::vector<std::optional<Float64>>> float_results;
    bool is_range_frame;
    bool is_input_value_nullable;
    bool is_return_type_int;
    size_t test_case_num;
};

class WindowAggFuncTest : public DB::tests::WindowTest
{
public:
    const ASTPtr value_col = col(VALUE_COL_NAME);

    void initializeContext() override { ExecutorTest::initializeContext(); }

    void executeTest(const TestCase & test);

protected:
    static std::vector<Int64> partition;
    static std::vector<Int64> order;
    static std::vector<Int64> int_value;
    static std::vector<std::optional<Int64>> int_nullable_value;
};

std::vector<Int64> WindowAggFuncTest::partition = {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 4, 5, 6, 6, 6};
std::vector<Int64> WindowAggFuncTest::order = {0, 0, 1, 3, 6, 0, 1, 4, 7, 8, 0, 4, 6, 10, 20, 40, 41, 0, 0, 0, 10, 30};
std::vector<Int64> WindowAggFuncTest::int_value
    = {0, -1, 0, 4, 6, 2, 0, -4, -2, 1, 7, -3, 9, -9, -3, 2, 1, 4, -5, 2, 5, 0};
std::vector<std::optional<Int64>> WindowAggFuncTest::int_nullable_value
    = {0, -1, {}, {}, 6, 2, {}, -4, {}, {}, 7, {}, 9, {}, {}, {}, {}, 4, {}, 2, {}, 0};

void WindowAggFuncTest::executeTest(const TestCase & test)
{
    MockWindowFrame frame;
    if (test.is_range_frame)
        frame.type = tipb::WindowFrameType::Ranges;
    else
        frame.type = tipb::WindowFrameType::Rows;

    for (size_t i = 0; i < test.test_case_num; i++)
    {
        if (test.is_range_frame)
        {
            frame.start = buildRangeFrameBound<Int64>(
                tipb::WindowBoundType::Preceding,
                tipb::RangeCmpDataType::Int,
                ORDER_COL_NAME,
                false,
                test.start_offsets[i]);
            frame.end = buildRangeFrameBound<Int64>(
                tipb::WindowBoundType::Following,
                tipb::RangeCmpDataType::Int,
                ORDER_COL_NAME,
                true,
                test.end_offsets[i]);
        }
        else
        {
            frame.start = mock::MockWindowFrameBound(tipb::WindowBoundType::Preceding, false, test.start_offsets[i]);
            frame.end = mock::MockWindowFrameBound(tipb::WindowBoundType::Following, false, test.end_offsets[i]);
        }

        ColumnWithTypeAndName value_col_with_type_and_name;
        if (test.is_input_value_nullable)
            value_col_with_type_and_name = toNullableVec<Int64>(int_nullable_value);
        else
            value_col_with_type_and_name = toVec<Int64>(int_value);

        ColumnWithTypeAndName res;
        if (test.is_return_type_int)
            res = toNullableVec<Int64>(test.results[i]);
        else
            res = toNullableVec<Float64>(test.float_results[i]);

        executeFunctionAndAssert(
            res,
            test.ast_func,
            {toVec<Int64>(partition), toVec<Int64>(order), value_col_with_type_and_name},
            frame);
    }
}


// TODO decide if we need decrease at compile time
// TODO test duplicate order by values in range frame in ft
TEST_F(WindowAggFuncTest, Sum)
try
{
    std::vector<Int64> start_offset{0, 1, 3, 10, 0, 0, 0, 3};
    std::vector<Int64> end_offset{0, 0, 0, 0, 1, 3, 10, 3};
    std::vector<std::vector<std::optional<Int64>>> res;

    res
        = {{0, -1, 0, 4, 6, 2, 0, -4, -2, 1, 7, -3, 9, -9, -3, 2, 1, 4, -5, 2, 5, 0},
           {0, -1, -1, 4, 10, 2, 2, -4, -6, -1, 7, 4, 6, 0, -12, -1, 3, 4, -5, 2, 7, 5},
           {0, -1, -1, 3, 9, 2, 2, -2, -4, -5, 7, 4, 13, 4, -6, -1, -9, 4, -5, 2, 7, 7},
           {0, -1, -1, 3, 9, 2, 2, -2, -4, -3, 7, 4, 13, 4, 1, 3, 4, 4, -5, 2, 7, 7},
           {0, -1, 4, 10, 6, 2, -4, -6, -1, 1, 4, 6, 0, -12, -1, 3, 1, 4, -5, 7, 5, 0},
           {0, 9, 10, 10, 6, -4, -5, -5, -1, 1, 4, -6, -1, -9, 0, 3, 1, 4, -5, 7, 5, 0},
           {0, 9, 10, 10, 6, -3, -5, -5, -1, 1, 4, -3, 0, -9, 0, 3, 1, 4, -5, 7, 5, 0},
           {0, 9, 9, 9, 9, -4, -3, -3, -3, -5, 4, 1, 3, 4, -3, 0, -9, 4, -5, 7, 7, 7}};

    executeTest(TestCase(Sum(value_col), start_offset, end_offset, res, {}, false, false));

    res
        = {{0, -1, 0, 4, 6, 2, 0, -4, -2, 1, 7, -3, 9, -9, -3, 2, 1, 4, -5, 2, 5, 0},
           {0, -1, -1, 4, 6, 2, 2, -4, -2, -1, 7, -3, 9, -9, -3, 2, 3, 4, -5, 2, 5, 0},
           {0, -1, -1, 3, 10, 2, 2, -4, -6, -1, 7, -3, 6, -9, -3, 2, 3, 4, -5, 2, 5, 0},
           {0, -1, -1, 3, 9, 2, 2, -2, -4, -3, 7, 4, 13, 4, -12, 2, 3, 4, -5, 2, 7, 0},
           {0, -1, 0, 4, 6, 2, 0, -4, -1, 1, 7, -3, 9, -9, -3, 3, 1, 4, -5, 2, 5, 0},
           {0, 3, 4, 10, 6, 2, -4, -6, -1, 1, 7, 6, 9, -9, -3, 3, 1, 4, -5, 2, 5, 0},
           {0, 9, 10, 10, 6, -3, -5, -5, -1, 1, 4, -3, 0, -12, -3, 3, 1, 4, -5, 7, 5, 0},
           {0, 3, 3, 9, 10, 2, -2, -6, -5, -1, 7, 6, 6, -9, -3, 3, 3, 4, -5, 2, 5, 0}};
    executeTest(TestCase(Sum(value_col), start_offset, end_offset, res, {}, true, false));

    res
        = {{0, -1, {}, {}, 6, 2, {}, -4, {}, {}, 7, {}, 9, {}, {}, {}, {}, 4, {}, 2, {}, 0},
           {0, -1, -1, {}, 6, 2, 2, -4, {}, {}, 7, {}, 9, {}, {}, {}, {}, 4, {}, 2, {}, 0},
           {0, -1, -1, -1, 6, 2, 2, -4, -4, {}, 7, {}, 9, {}, {}, {}, {}, 4, {}, 2, {}, 0},
           {0, -1, -1, -1, 5, 2, 2, -2, -2, -2, 7, 7, 16, 16, {}, {}, {}, 4, {}, 2, 2, 0},
           {0, -1, {}, {}, 6, 2, {}, -4, {}, {}, 7, {}, 9, {}, {}, {}, {}, 4, {}, 2, {}, 0},
           {0, -1, {}, 6, 6, 2, -4, -4, {}, {}, 7, 9, 9, {}, {}, {}, {}, 4, {}, 2, {}, 0},
           {0, 5, 6, 6, 6, -2, -4, -4, {}, {}, 16, 9, 9, {}, {}, {}, {}, 4, {}, 2, {}, 0},
           {0, -1, -1, 5, 6, 2, -2, -4, -4, {}, 7, 9, 9, {}, {}, {}, {}, 4, {}, 2, {}, 0}};
    executeTest(TestCase(Sum(value_col), start_offset, end_offset, res, {}, true, true));
}
CATCH

TEST_F(WindowAggFuncTest, Count)
try
{
    std::vector<Int64> start_offset{0, 1, 3, 10, 0, 0, 0, 3};
    std::vector<Int64> end_offset{0, 0, 0, 0, 1, 3, 10, 3};
    std::vector<std::vector<std::optional<Int64>>> res;

    res
        = {{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
           {1, 1, 2, 2, 2, 1, 2, 2, 2, 2, 1, 2, 2, 2, 2, 2, 2, 1, 1, 1, 2, 2},
           {1, 1, 2, 3, 4, 1, 2, 3, 4, 4, 1, 2, 3, 4, 4, 4, 4, 1, 1, 1, 2, 3},
           {1, 1, 2, 3, 4, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 6, 7, 1, 1, 1, 2, 3},
           {1, 2, 2, 2, 1, 2, 2, 2, 2, 1, 2, 2, 2, 2, 2, 2, 1, 1, 1, 2, 2, 1},
           {1, 4, 3, 2, 1, 4, 4, 3, 2, 1, 4, 4, 4, 4, 3, 2, 1, 1, 1, 3, 2, 1},
           {1, 4, 3, 2, 1, 5, 4, 3, 2, 1, 7, 6, 5, 4, 3, 2, 1, 1, 1, 3, 2, 1},
           {1, 4, 4, 4, 4, 4, 5, 5, 5, 4, 4, 5, 6, 7, 6, 5, 4, 1, 1, 3, 3, 3}};

    executeTest(TestCase(Count(value_col), start_offset, end_offset, res, {}, false, false));

    res
        = {{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
           {1, 1, 2, 1, 1, 1, 2, 1, 1, 2, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1},
           {1, 1, 2, 3, 2, 1, 2, 2, 2, 2, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 1, 1},
           {1, 1, 2, 3, 4, 1, 2, 3, 4, 5, 1, 2, 3, 4, 2, 1, 2, 1, 1, 1, 2, 1},
           {1, 2, 1, 1, 1, 2, 1, 1, 2, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1},
           {1, 3, 2, 2, 1, 2, 2, 2, 2, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1},
           {1, 4, 3, 2, 1, 5, 4, 3, 2, 1, 4, 3, 2, 2, 1, 2, 1, 1, 1, 2, 1, 1},
           {1, 3, 3, 4, 2, 2, 3, 3, 3, 2, 1, 2, 2, 1, 1, 2, 2, 1, 1, 1, 1, 1}};
    executeTest(TestCase(Count(value_col), start_offset, end_offset, res, {}, true, false));

    res
        = {{1, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1, 0, 1, 0, 0, 0, 0, 1, 0, 1, 0, 1},
           {1, 1, 1, 0, 1, 1, 1, 1, 0, 0, 1, 0, 1, 0, 0, 0, 0, 1, 0, 1, 0, 1},
           {1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 0, 1, 0, 0, 0, 0, 1, 0, 1, 0, 1},
           {1, 1, 1, 1, 2, 1, 1, 2, 2, 2, 1, 1, 2, 2, 0, 0, 0, 1, 0, 1, 1, 1},
           {1, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1, 0, 1, 0, 0, 0, 0, 1, 0, 1, 0, 1},
           {1, 1, 0, 1, 1, 1, 1, 1, 0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 0, 1, 0, 1},
           {1, 2, 1, 1, 1, 2, 1, 1, 0, 0, 2, 1, 1, 0, 0, 0, 0, 1, 0, 1, 0, 1},
           {1, 1, 1, 2, 1, 1, 2, 1, 1, 0, 1, 1, 1, 0, 0, 0, 0, 1, 0, 1, 0, 1}};
    executeTest(TestCase(Count(value_col), start_offset, end_offset, res, {}, true, true));
}
CATCH

TEST_F(WindowAggFuncTest, Avg)
try
{
    std::vector<Int64> start_offset;
    std::vector<Int64> end_offset;
    std::vector<std::vector<std::optional<Float64>>> res;

    start_offset = {0, 1, 0};
    end_offset = {0, 0, 1};
    res
        = {{0, -1, 0, 4, 6, 2, 0, -4, -2, 1, 7, -3, 9, -9, -3, 2, 1, 4, -5, 2, 5, 0},
           {0, -1, -0.5, 2, 5, 2, 1, -2, -3, -0.5, 7, 2, 3, 0, -6, -0.5, 1.5, 4, -5, 2, 3.5, 2.5},
           {0, -0.5, 2, 5, 6, 1, -2, -3, -0.5, 1, 2, 3, 0, -6, -0.5, 1.5, 1, 4, -5, 3.5, 2.5, 0}};

    executeTest(TestCase(Avg(value_col), start_offset, end_offset, {}, res, false, false, false));
}
CATCH

TEST_F(WindowAggFuncTest, Min)
try
{
    std::vector<Int64> start_offset{0, 1, 3, 10, 0, 0, 0, 3};
    std::vector<Int64> end_offset{0, 0, 0, 0, 1, 3, 10, 3};
    std::vector<std::vector<std::optional<Int64>>> res;

    res
        = {{0, -1, 0, 4, 6, 2, 0, -4, -2, 1, 7, -3, 9, -9, -3, 2, 1, 4, -5, 2, 5, 0},
           {0, -1, -1, 0, 4, 2, 0, -4, -4, -2, 7, -3, -3, -9, -9, -3, 1, 4, -5, 2, 2, 0},
           {0, -1, -1, -1, -1, 2, 0, -4, -4, -4, 7, -3, -3, -9, -9, -9, -9, 4, -5, 2, 2, 0},
           {0, -1, -1, -1, -1, 2, 0, -4, -4, -4, 7, -3, -3, -9, -9, -9, -9, 4, -5, 2, 2, 0},
           {0, -1, 0, 4, 6, 0, -4, -4, -2, 1, -3, -3, -9, -9, -3, 1, 1, 4, -5, 2, 0, 0},
           {0, -1, 0, 4, 6, -4, -4, -4, -2, 1, -9, -9, -9, -9, -3, 1, 1, 4, -5, 0, 0, 0},
           {0, -1, 0, 4, 6, -4, -4, -4, -2, 1, -9, -9, -9, -9, -3, 1, 1, 4, -5, 0, 0, 0},
           {0, -1, -1, -1, -1, -4, -4, -4, -4, -4, -9, -9, -9, -9, -9, -9, -9, 4, -5, 0, 0, 0}};
    executeTest(TestCase(MinForWindow(value_col), start_offset, end_offset, res, {}, false, false));

    res
        = {{0, -1, 0, 4, 6, 2, 0, -4, -2, 1, 7, -3, 9, -9, -3, 2, 1, 4, -5, 2, 5, 0},
           {0, -1, -1, 4, 6, 2, 0, -4, -2, -2, 7, -3, 9, -9, -3, 2, 1, 4, -5, 2, 5, 0},
           {0, -1, -1, -1, 4, 2, 0, -4, -4, -2, 7, -3, -3, -9, -3, 2, 1, 4, -5, 2, 5, 0},
           {0, -1, -1, -1, -1, 2, 0, -4, -4, -4, 7, -3, -3, -9, -9, 2, 1, 4, -5, 2, 2, 0},
           {0, -1, 0, 4, 6, 0, 0, -4, -2, 1, 7, -3, 9, -9, -3, 1, 1, 4, -5, 2, 5, 0},
           {0, -1, 0, 4, 6, 0, -4, -4, -2, 1, 7, -3, 9, -9, -3, 1, 1, 4, -5, 2, 5, 0},
           {0, -1, 0, 4, 6, -4, -4, -4, -2, 1, -9, -9, -9, -9, -3, 1, 1, 4, -5, 2, 5, 0},
           {0, -1, -1, -1, 4, 0, -4, -4, -4, -2, 7, -3, -3, -9, -3, 1, 1, 4, -5, 2, 5, 0}};
    executeTest(TestCase(MinForWindow(value_col), start_offset, end_offset, res, {}, true, false));

    res
        = {{0, -1, {}, {}, 6, 2, {}, -4, {}, {}, 7, {}, 9, {}, {}, {}, {}, 4, {}, 2, {}, 0},
           {0, -1, -1, {}, 6, 2, 2, -4, {}, {}, 7, {}, 9, {}, {}, {}, {}, 4, {}, 2, {}, 0},
           {0, -1, -1, -1, 6, 2, 2, -4, -4, {}, 7, {}, 9, {}, {}, {}, {}, 4, {}, 2, {}, 0},
           {0, -1, -1, -1, -1, 2, 2, -4, -4, -4, 7, 7, 7, 7, {}, {}, {}, 4, {}, 2, 2, 0},
           {0, -1, {}, {}, 6, 2, {}, -4, {}, {}, 7, {}, 9, {}, {}, {}, {}, 4, {}, 2, {}, 0},
           {0, -1, {}, 6, 6, 2, -4, -4, {}, {}, 7, 9, 9, {}, {}, {}, {}, 4, {}, 2, {}, 0},
           {0, -1, 6, 6, 6, -4, -4, -4, {}, {}, 7, 9, 9, {}, {}, {}, {}, 4, {}, 2, {}, 0},
           {0, -1, -1, -1, 6, 2, -4, -4, -4, {}, 7, 9, 9, {}, {}, {}, {}, 4, {}, 2, {}, 0}};
    executeTest(TestCase(MinForWindow(value_col), start_offset, end_offset, res, {}, true, true));
}
CATCH

TEST_F(WindowAggFuncTest, Max)
try
{
    std::vector<Int64> start_offset{0, 1, 3, 10, 0, 0, 0, 3};
    std::vector<Int64> end_offset{0, 0, 0, 0, 1, 3, 10, 3};
    std::vector<std::vector<std::optional<Int64>>> res;

    res
        = {{0, -1, 0, 4, 6, 2, 0, -4, -2, 1, 7, -3, 9, -9, -3, 2, 1, 4, -5, 2, 5, 0},
           {0, -1, 0, 4, 6, 2, 2, 0, -2, 1, 7, 7, 9, 9, -3, 2, 2, 4, -5, 2, 5, 5},
           {0, -1, 0, 4, 6, 2, 2, 2, 2, 1, 7, 7, 9, 9, 9, 9, 2, 4, -5, 2, 5, 5},
           {0, -1, 0, 4, 6, 2, 2, 2, 2, 2, 7, 7, 9, 9, 9, 9, 9, 4, -5, 2, 5, 5},
           {0, 0, 4, 6, 6, 2, 0, -2, 1, 1, 7, 9, 9, -3, 2, 2, 1, 4, -5, 5, 5, 0},
           {0, 6, 6, 6, 6, 2, 1, 1, 1, 1, 9, 9, 9, 2, 2, 2, 1, 4, -5, 5, 5, 0},
           {0, 6, 6, 6, 6, 2, 1, 1, 1, 1, 9, 9, 9, 2, 2, 2, 1, 4, -5, 5, 5, 0},
           {0, 6, 6, 6, 6, 2, 2, 2, 2, 1, 9, 9, 9, 9, 9, 9, 2, 4, -5, 5, 5, 5}};
    executeTest(TestCase(MaxForWindow(value_col), start_offset, end_offset, res, {}, false, false));

    res
        = {{0, -1, 0, 4, 6, 2, 0, -4, -2, 1, 7, -3, 9, -9, -3, 2, 1, 4, -5, 2, 5, 0},
           {0, -1, 0, 4, 6, 2, 2, -4, -2, 1, 7, -3, 9, -9, -3, 2, 2, 4, -5, 2, 5, 0},
           {0, -1, 0, 4, 6, 2, 2, 0, -2, 1, 7, -3, 9, -9, -3, 2, 2, 4, -5, 2, 5, 0},
           {0, -1, 0, 4, 6, 2, 2, 2, 2, 2, 7, 7, 9, 9, -3, 2, 2, 4, -5, 2, 5, 0},
           {0, 0, 0, 4, 6, 2, 0, -4, 1, 1, 7, -3, 9, -9, -3, 2, 1, 4, -5, 2, 5, 0},
           {0, 4, 4, 6, 6, 2, 0, -2, 1, 1, 7, 9, 9, -9, -3, 2, 1, 4, -5, 2, 5, 0},
           {0, 6, 6, 6, 6, 2, 1, 1, 1, 1, 9, 9, 9, -3, -3, 2, 1, 4, -5, 5, 5, 0},
           {0, 4, 4, 6, 6, 2, 2, 0, 1, 1, 7, 9, 9, -9, -3, 2, 2, 4, -5, 2, 5, 0}};
    executeTest(TestCase(MaxForWindow(value_col), start_offset, end_offset, res, {}, true, false));

    res
        = {{0, -1, {}, {}, 6, 2, {}, -4, {}, {}, 7, {}, 9, {}, {}, {}, {}, 4, {}, 2, {}, 0},
           {0, -1, -1, {}, 6, 2, 2, -4, {}, {}, 7, {}, 9, {}, {}, {}, {}, 4, {}, 2, {}, 0},
           {0, -1, -1, -1, 6, 2, 2, -4, -4, {}, 7, {}, 9, {}, {}, {}, {}, 4, {}, 2, {}, 0},
           {0, -1, -1, -1, 6, 2, 2, 2, 2, 2, 7, 7, 9, 9, {}, {}, {}, 4, {}, 2, 2, 0},
           {0, -1, {}, {}, 6, 2, {}, -4, {}, {}, 7, {}, 9, {}, {}, {}, {}, 4, {}, 2, {}, 0},
           {0, -1, {}, 6, 6, 2, -4, -4, {}, {}, 7, 9, 9, {}, {}, {}, {}, 4, {}, 2, {}, 0},
           {0, 6, 6, 6, 6, 2, -4, -4, {}, {}, 9, 9, 9, {}, {}, {}, {}, 4, {}, 2, {}, 0},
           {0, -1, -1, 6, 6, 2, 2, -4, -4, {}, 7, 9, 9, {}, {}, {}, {}, 4, {}, 2, {}, 0}};
    executeTest(TestCase(MaxForWindow(value_col), start_offset, end_offset, res, {}, true, true));
}
CATCH

TEST_F(WindowAggFuncTest, initNeedDecrease)
try
{
    WindowDescription desc;
    desc.frame = WindowFrame();
    desc.initNeedDecrease(true);
    ASSERT_FALSE(desc.need_decrease);

    std::vector<WindowFrame::FrameType> frame_types;
    for (auto type : frame_types)
    {
        desc.frame = WindowFrame(
            type,
            WindowFrame::BoundaryType::Unbounded,
            0,
            true,
            WindowFrame::BoundaryType::Offset,
            1,
            false);
        desc.initNeedDecrease(true);
        ASSERT_FALSE(desc.need_decrease);

        desc.frame = WindowFrame(
            type,
            WindowFrame::BoundaryType::Offset,
            0,
            true,
            WindowFrame::BoundaryType::Unbounded,
            0,
            false);
        desc.initNeedDecrease(true);
        ASSERT_TRUE(desc.need_decrease);

        desc.frame = WindowFrame(
            type,
            WindowFrame::BoundaryType::Unbounded,
            0,
            true,
            WindowFrame::BoundaryType::Unbounded,
            0,
            false);
        desc.initNeedDecrease(true);
        ASSERT_FALSE(desc.need_decrease);
    }

    desc.frame = WindowFrame(
        WindowFrame::FrameType::Ranges,
        WindowFrame::BoundaryType::Offset,
        0,
        true,
        WindowFrame::BoundaryType::Offset,
        0,
        true);
    desc.initNeedDecrease(true);
    ASSERT_TRUE(desc.need_decrease);

    desc.frame = WindowFrame(
        WindowFrame::FrameType::Ranges,
        WindowFrame::BoundaryType::Offset,
        0,
        false,
        WindowFrame::BoundaryType::Offset,
        0,
        false);
    desc.initNeedDecrease(true);
    ASSERT_TRUE(desc.need_decrease);

    desc.frame = WindowFrame(
        WindowFrame::FrameType::Ranges,
        WindowFrame::BoundaryType::Offset,
        0,
        true,
        WindowFrame::BoundaryType::Offset,
        0,
        false);
    desc.initNeedDecrease(true);
    ASSERT_TRUE(desc.need_decrease);

    desc.frame = WindowFrame(
        WindowFrame::FrameType::Rows,
        WindowFrame::BoundaryType::Offset,
        0,
        true,
        WindowFrame::BoundaryType::Offset,
        0,
        false);
    desc.initNeedDecrease(true);
    ASSERT_FALSE(desc.need_decrease);

    desc.frame = WindowFrame(
        WindowFrame::FrameType::Rows,
        WindowFrame::BoundaryType::Offset,
        0,
        true,
        WindowFrame::BoundaryType::Offset,
        0,
        true);
    desc.initNeedDecrease(true);
    ASSERT_FALSE(desc.need_decrease);

    desc.frame = WindowFrame(
        WindowFrame::FrameType::Rows,
        WindowFrame::BoundaryType::Offset,
        0,
        false,
        WindowFrame::BoundaryType::Offset,
        0,
        false);
    desc.initNeedDecrease(true);
    ASSERT_FALSE(desc.need_decrease);

    desc.frame = WindowFrame(
        WindowFrame::FrameType::Rows,
        WindowFrame::BoundaryType::Offset,
        3,
        true,
        WindowFrame::BoundaryType::Offset,
        3,
        true);
    desc.initNeedDecrease(true);
    ASSERT_FALSE(desc.need_decrease);

    desc.frame = WindowFrame(
        WindowFrame::FrameType::Rows,
        WindowFrame::BoundaryType::Offset,
        3,
        false,
        WindowFrame::BoundaryType::Offset,
        3,
        false);
    desc.initNeedDecrease(true);
    ASSERT_FALSE(desc.need_decrease);

    desc.frame = WindowFrame(
        WindowFrame::FrameType::Rows,
        WindowFrame::BoundaryType::Offset,
        3,
        true,
        WindowFrame::BoundaryType::Offset,
        3,
        false);
    desc.initNeedDecrease(true);
    ASSERT_TRUE(desc.need_decrease);

    desc.frame = WindowFrame(
        WindowFrame::FrameType::Rows,
        WindowFrame::BoundaryType::Offset,
        2,
        true,
        WindowFrame::BoundaryType::Offset,
        1,
        false);
    desc.initNeedDecrease(true);
    ASSERT_TRUE(desc.need_decrease);

    desc.frame = WindowFrame(
        WindowFrame::FrameType::Rows,
        WindowFrame::BoundaryType::Offset,
        0,
        true,
        WindowFrame::BoundaryType::Offset,
        1,
        false);
    desc.initNeedDecrease(true);
    ASSERT_TRUE(desc.need_decrease);

    desc.frame = WindowFrame(
        WindowFrame::FrameType::Rows,
        WindowFrame::BoundaryType::Offset,
        1,
        true,
        WindowFrame::BoundaryType::Offset,
        0,
        false);
    desc.initNeedDecrease(true);
    ASSERT_TRUE(desc.need_decrease);
}
CATCH

} // namespace DB::tests
