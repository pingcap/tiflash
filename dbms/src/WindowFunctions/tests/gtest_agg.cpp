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
struct TestCase
{
    TestCase(
        const String & agg_func_name_,
        const std::vector<Int64> & start_offsets_,
        const std::vector<Int64> & end_offsets_,
        const std::vector<std::vector<std::optional<Int64>>> & results_,
        bool is_range_frame_,
        bool is_input_value_nullable_)
        : agg_func_name(agg_func_name_)
        , start_offsets(start_offsets_)
        , end_offsets(end_offsets_)
        , results(results_)
        , is_range_frame(is_range_frame_)
        , is_input_value_nullable(is_input_value_nullable_)
        , test_case_num(start_offsets.size())
    {
        assert(test_case_num == end_offsets.size());
        assert(test_case_num == results.size());
    }

    String agg_func_name;
    std::vector<Int64> start_offsets;
    std::vector<Int64> end_offsets;
    std::vector<std::vector<std::optional<Int64>>> results;
    bool is_range_frame;
    bool is_input_value_nullable;
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

        executeFunctionAndAssert(
            toNullableVec<Int64>(test.results[i]),
            Sum(value_col),
            {toVec<Int64>(partition), toVec<Int64>(order), value_col_with_type_and_name},
            frame);
    }
}

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

// TODO decide if we need decrease at compile time
// TODO test duplicate order by values in range frame in ft
TEST_F(WindowAggFuncTest, Sum)
try
{
    std::vector<Int64> start_offset;
    std::vector<Int64> end_offset;
    std::vector<std::vector<std::optional<Int64>>> res;

    start_offset = {0, 1, 3, 10, 0, 0, 0, 3};
    end_offset = {0, 0, 0, 0, 1, 3, 10, 3};
    res
        = {{0, -1, 0, 4, 6, 2, 0, -4, -2, 1, 7, -3, 9, -9, -3, 2, 1, 4, -5, 2, 5, 0},
           {0, -1, -1, 4, 10, 2, 2, -4, -6, -1, 7, 4, 6, 0, -12, -1, 3, 4, -5, 2, 7, 5},
           {0, -1, -1, 3, 9, 2, 2, -2, -4, -5, 7, 4, 13, 4, -6, -1, -9, 4, -5, 2, 7, 7},
           {0, -1, -1, 3, 9, 2, 2, -2, -4, -3, 7, 4, 13, 4, 1, 3, 4, 4, -5, 2, 7, 7},
           {0, -1, 4, 10, 6, 2, -4, -6, -1, 1, 4, 6, 0, -12, -1, 3, 1, 4, -5, 7, 5, 0},
           {0, 9, 10, 10, 6, -4, -5, -5, -1, 1, 4, -6, -1, -9, 0, 3, 1, 4, -5, 7, 5, 0},
           {0, 9, 10, 10, 6, -3, -5, -5, -1, 1, 4, -3, 0, -9, 0, 3, 1, 4, -5, 7, 5, 0},
           {0, 9, 9, 9, 9, -4, -3, -3, -3, -5, 4, 1, 3, 4, -3, 0, -9, 4, -5, 7, 7, 7}};

    executeTest(TestCase("sum", start_offset, end_offset, res, false, false));

    start_offset = {0, 1, 3, 10, 0, 0, 0, 3};
    end_offset = {0, 0, 0, 0, 1, 3, 10, 3};
    res
        = {{0, -1, 0, 4, 6, 2, 0, -4, -2, 1, 7, -3, 9, -9, -3, 2, 1, 4, -5, 2, 5, 0},
           {0, -1, -1, 4, 6, 2, 2, -4, -2, -1, 7, -3, 9, -9, -3, 2, 3, 4, -5, 2, 5, 0},
           {0, -1, -1, 3, 10, 2, 2, -4, -6, -1, 7, -3, 6, -9, -3, 2, 3, 4, -5, 2, 5, 0},
           {0, -1, -1, 3, 9, 2, 2, -2, -4, -3, 7, 4, 13, 4, -12, 2, 3, 4, -5, 2, 7, 0},
           {0, -1, 0, 4, 6, 2, 0, -4, -1, 1, 7, -3, 9, -9, -3, 3, 1, 4, -5, 2, 5, 0},
           {0, 3, 4, 10, 6, 2, -4, -6, -1, 1, 7, 6, 9, -9, -3, 3, 1, 4, -5, 2, 5, 0},
           {0, 9, 10, 10, 6, -3, -5, -5, -1, 1, 4, -3, 0, -12, -3, 3, 1, 4, -5, 7, 5, 0},
           {0, 3, 3, 9, 10, 2, -2, -6, -5, -1, 7, 6, 6, -9, -3, 3, 3, 4, -5, 2, 5, 0}};
    executeTest(TestCase("sum", start_offset, end_offset, res, true, false));

    start_offset = {0, 1, 3, 10, 0, 0, 0, 3};
    end_offset = {0, 0, 0, 0, 1, 3, 10, 3};
    res
        = {{0, -1, {}, {}, 6, 2, {}, -4, {}, {}, 7, {}, 9, {}, {}, {}, {}, 4, {}, 2, {}, 0},
           {0, -1, -1, {}, 6, 2, 2, -4, {}, {}, 7, {}, 9, {}, {}, {}, {}, 4, {}, 2, {}, 0},
           {0, -1, -1, -1, 6, 2, 2, -4, -4, {}, 7, {}, 9, {}, {}, {}, {}, 4, {}, 2, {}, 0},
           {0, -1, -1, -1, 5, 2, 2, -2, -2, -2, 7, 7, 16, 16, {}, {}, {}, 4, {}, 2, 2, 0},
           {0, -1, {}, {}, 6, 2, {}, -4, {}, {}, 7, {}, 9, {}, {}, {}, {}, 4, {}, 2, {}, 0},
           {0, -1, {}, 6, 6, 2, -4, -4, {}, {}, 7, 9, 9, {}, {}, {}, {}, 4, {}, 2, {}, 0},
           {0, 5, 6, 6, 6, -2, -4, -4, {}, {}, 16, 9, 9, {}, {}, {}, {}, 4, {}, 2, {}, 0},
           {0, -1, -1, 5, 6, 2, -2, -4, -4, {}, 7, 9, 9, {}, {}, {}, {}, 4, {}, 2, {}, 0}};
    executeTest(TestCase("sum", start_offset, end_offset, res, true, true));
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
