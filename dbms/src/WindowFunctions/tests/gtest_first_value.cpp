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

#include <Common/Decimal.h>
#include <Core/Field.h>
#include <Debug/MockExecutor/WindowBinder.h>
#include <Interpreters/Context.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/WindowTestUtils.h>
#include <TestUtils/mockExecutor.h>
#include <common/types.h>
#include <tipb/executor.pb.h>

#include <memory>
#include <optional>

namespace DB::tests
{
class FirstValue : public DB::tests::WindowTest
{
public:
    const ASTPtr value_col = col(VALUE_COL_NAME);

    void initializeContext() override
    {
        ExecutorTest::initializeContext();
    }

    template <typename IntType>
    void testInt()
    {
        executeFunctionAndAssert(
            toVec<IntType>({1, 2, 2, 2, 2, 6, 6, 6, 6, 6, 11, 11, 11}),
            FirstValue(value_col),
            {toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
             toVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
             toVec<IntType>(/*value*/ {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13})});

        executeFunctionAndAssert(
            toNullableVec<IntType>({{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}}),
            FirstValue(value_col),
            {toNullableVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
             toNullableVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
             toNullableVec<IntType>(/*value*/ {{}, {}, 3, 4, 5, {}, 7, 8, 9, 10, {}, 12, 13})});
    }

    template <typename FloatType>
    void testFloat()
    {
        executeFunctionAndAssert(
            toVec<FloatType>({1, 2, 2, 2, 2, 6, 6, 6, 6, 6, 11, 11, 11}),
            FirstValue(value_col),
            {toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
             toVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
             toVec<FloatType>(/*value*/ {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13})});

        executeFunctionAndAssert(
            toNullableVec<FloatType>({{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}}),
            FirstValue(value_col),
            {toNullableVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
             toNullableVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
             toNullableVec<FloatType>(/*value*/ {{}, {}, 3, 4, 5, {}, 7, 8, 9, 10, {}, 12, 13})});
    }
};

TEST_F(FirstValue, firstValueWithRowsFrameType)
try
{
    {
        // boundary type: unbounded
        executeFunctionAndAssert(
            toVec<String>({"1", "2", "2", "2", "2", "6", "6", "6", "6", "6", "11", "11", "11"}),
            FirstValue(value_col),
            {toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
             toVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
             toVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13"})});

        executeFunctionAndAssert(
            toNullableVec<String>({{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}}),
            FirstValue(value_col),
            {toNullableVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
             toNullableVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
             toNullableVec<String>(/*value*/ {{}, {}, "3", "4", "5", {}, "7", "8", "9", "10", {}, "12", "13"})});
    }

    {
        // boundary type: offset
        MockWindowFrame frame;
        frame.type = tipb::WindowFrameType::Rows;
        frame.start = mock::MockWindowFrameBound(tipb::WindowBoundType::Following, false, 0);

        std::vector<Int64> frame_start_offset{0, 1, 3, 10};
        std::vector<std::vector<String>> res_not_null{
            {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13"},
            {"1", "2", "2", "3", "4", "6", "6", "7", "8", "9", "11", "11", "12"},
            {"1", "2", "2", "2", "2", "6", "6", "6", "6", "7", "11", "11", "11"},
            {"1", "2", "2", "2", "2", "6", "6", "6", "6", "6", "11", "11", "11"}};
        std::vector<std::vector<std::optional<String>>> res_null{
            {{}, {}, "3", "4", "5", {}, "7", "8", "9", "10", {}, "12", "13"},
            {{}, {}, {}, "3", "4", {}, {}, "7", "8", "9", {}, {}, "12"},
            {{}, {}, {}, {}, {}, {}, {}, {}, {}, "7", {}, {}, {}},
            {{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}}};

        for (size_t i = 0; i < frame_start_offset.size(); ++i)
        {
            frame.start = mock::MockWindowFrameBound(tipb::WindowBoundType::Preceding, false, frame_start_offset[i]);

            executeFunctionAndAssert(
                toVec<String>(res_not_null[i]),
                FirstValue(value_col),
                {toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
                 toVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
                 toVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13"})},
                frame);

            executeFunctionAndAssert(
                toNullableVec<String>(res_null[i]),
                FirstValue(value_col),
                {toNullableVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
                 toNullableVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
                 toNullableVec<String>(/*value*/ {{}, {}, "3", "4", "5", {}, "7", "8", "9", "10", {}, "12", "13"})},
                frame);
        }
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
