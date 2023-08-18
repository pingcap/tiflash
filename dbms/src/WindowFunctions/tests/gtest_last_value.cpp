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

#include <Debug/MockExecutor/WindowBinder.h>
#include <Interpreters/Context.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/WindowTestUtils.h>
#include <TestUtils/mockExecutor.h>

#include <optional>
#include <utility>

namespace DB::tests
{
class LastValue : public DB::tests::WindowTest
{
public:
    const ASTPtr value_col = col(VALUE_COL_NAME);

    void initializeContext() override { ExecutorTest::initializeContext(); }

    template <typename IntType>
    void testInt()
    {
        MockWindowFrame unbounded_type_frame{
            tipb::WindowFrameType::Rows,
            mock::MockWindowFrameBound(tipb::WindowBoundType::Preceding, true, 0),
            mock::MockWindowFrameBound(tipb::WindowBoundType::Following, true, 0)};

        executeFunctionAndAssert(
            toNullableVec<IntType>({1, 5, 5, 5, 5, 10, 10, 10, 10, 10, 13, 13, 13}),
            LastValue(value_col),
            {toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
             toVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
             toVec<IntType>(/*value*/ {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13})},
            unbounded_type_frame);

        executeFunctionAndAssert(
            toNullableVec<IntType>({{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}}),
            LastValue(value_col),
            {toNullableVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
             toNullableVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
             toNullableVec<IntType>(/*value*/ {{}, 2, 3, 4, {}, 6, 7, 8, 9, {}, 11, 12, {}})},
            unbounded_type_frame);
    }

    template <typename FloatType>
    void testFloat()
    {
        MockWindowFrame unbounded_type_frame{
            tipb::WindowFrameType::Rows,
            mock::MockWindowFrameBound(tipb::WindowBoundType::Preceding, true, 0),
            mock::MockWindowFrameBound(tipb::WindowBoundType::Following, true, 0)};

        executeFunctionAndAssert(
            toNullableVec<FloatType>({1, 5, 5, 5, 5, 10, 10, 10, 10, 10, 13, 13, 13}),
            LastValue(value_col),
            {toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
             toVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
             toVec<FloatType>(/*value*/ {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13})},
            unbounded_type_frame);

        executeFunctionAndAssert(
            toNullableVec<FloatType>({{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}}),
            LastValue(value_col),
            {toNullableVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
             toNullableVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
             toNullableVec<FloatType>(/*value*/ {{}, 2, 3, 4, {}, 6, 7, 8, 9, {}, 11, 12, {}})},
            unbounded_type_frame);
    }
};

TEST_F(LastValue, lastValue)
try
{
    {
        // frame type: unbounded
        executeFunctionAndAssert(
            toNullableVec<String>({"1", "5", "5", "5", "5", "10", "10", "10", "10", "10", "13", "13", "13"}),
            LastValue(value_col),
            {toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
             toVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
             toVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13"})});

        executeFunctionAndAssert(
            toNullableVec<String>({{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}}),
            LastValue(value_col),
            {toNullableVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
             toNullableVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
             toNullableVec<String>(/*value*/ {{}, "2", "3", "4", {}, "6", "7", "8", "9", {}, "11", "12", {}})});
    }

    {
        // frame type: offset
        MockWindowFrame frame;
        frame.type = tipb::WindowFrameType::Rows;
        frame.end = mock::MockWindowFrameBound(tipb::WindowBoundType::Following, false, 0);

        std::vector<Int64> frame_start_offset{0, 1, 3, 10};
        std::vector<std::vector<std::optional<String>>> res_not_null{
            {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13"},
            {"1", "3", "4", "5", "5", "7", "8", "9", "10", "10", "12", "13", "13"},
            {"1", "5", "5", "5", "5", "9", "10", "10", "10", "10", "13", "13", "13"},
            {"1", "5", "5", "5", "5", "10", "10", "10", "10", "10", "13", "13", "13"},
        };
        std::vector<std::vector<std::optional<String>>> res_null{
            {{}, "2", "3", "4", {}, "6", "7", "8", "9", {}, "11", "12", {}},
            {{}, "3", "4", {}, {}, "7", "8", "9", {}, {}, "12", {}, {}},
            {{}, {}, {}, {}, {}, "9", {}, {}, {}, {}, {}, {}, {}},
            {{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}},
        };

        for (size_t i = 0; i < frame_start_offset.size(); ++i)
        {
            frame.end = mock::MockWindowFrameBound(tipb::WindowBoundType::Following, false, frame_start_offset[i]);
            executeFunctionAndAssert(
                toNullableVec<String>(res_not_null[i]),
                LastValue(value_col),
                {toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
                 toVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
                 toVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13"})},
                frame);

            executeFunctionAndAssert(
                toNullableVec<String>(res_null[i]),
                LastValue(value_col),
                {toNullableVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
                 toNullableVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
                 toNullableVec<String>(/*value*/ {{}, "2", "3", "4", {}, "6", "7", "8", "9", {}, "11", "12", {}})},
                frame);
        }

        // The following are <preceding, preceding> tests
        frame.start = mock::MockWindowFrameBound(tipb::WindowBoundType::Preceding, false, 2);
        frame.end = mock::MockWindowFrameBound(tipb::WindowBoundType::Preceding, false, 1);
        executeFunctionAndAssert(
            toNullableVec<String>({{}, {}, "2", "3", "4", {}, "6", "7", "8", "9", {}, "11", "12"}),
            LastValue(value_col),
            {toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
             toVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
             toVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13"})},
            frame);

        // The following are <following, folloing> tests
        frame.start = mock::MockWindowFrameBound(tipb::WindowBoundType::Following, false, 1);
        frame.end = mock::MockWindowFrameBound(tipb::WindowBoundType::Following, false, 2);
        executeFunctionAndAssert(
            toNullableVec<String>({{}, "4", "5", "5", {}, "8", "9", "10", "10", {}, "13", "13", {}}),
            LastValue(value_col),
            {toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
             toVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
             toVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13"})},
            frame);
    }

    // TODO support unsigned int.
    testInt<Int8>();
    testInt<Int16>();
    testInt<Int32>();
    testInt<Int64>();

    testFloat<Float32>();
    testFloat<Float64>();
}
CATCH

} // namespace DB::tests
