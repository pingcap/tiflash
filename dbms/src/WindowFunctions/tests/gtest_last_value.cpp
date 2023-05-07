// Copyright 2023 PingCAP, Ltd.
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

#include <Interpreters/Context.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <optional>
#include <utility>
#include <TestUtils/mockExecutor.h>

namespace DB::tests
{
// TODO Tests with frame should be added
class LastValue : public DB::tests::ExecutorTest
{
    static const size_t max_concurrency_level = 10;

public:
    static constexpr auto value_col_name = "first_value";
    const ASTPtr value_col = col(value_col_name);

    void initializeContext() override
    {
        ExecutorTest::initializeContext();
    }

    void executeWithConcurrencyAndBlockSize(const std::shared_ptr<tipb::DAGRequest> & request, const ColumnsWithTypeAndName & expect_columns)
    {
        std::vector<size_t> block_sizes{1, 2, 3, 4, DEFAULT_BLOCK_SIZE};
        for (auto block_size : block_sizes)
        {
            context.context->setSetting("max_block_size", Field(static_cast<UInt64>(block_size)));
            ASSERT_COLUMNS_EQ_R(expect_columns, executeStreams(request));
            ASSERT_COLUMNS_EQ_UR(expect_columns, executeStreams(request, 2));
            ASSERT_COLUMNS_EQ_UR(expect_columns, executeStreams(request, max_concurrency_level));
        }
    }

    void executeFunctionAndAssert(
        const ColumnWithTypeAndName & result,
        const ASTPtr & function,
        const ColumnsWithTypeAndName & input,
        const MockWindowFrame & frame)
    {
        ColumnsWithTypeAndName actual_input = input;
        assert(actual_input.size() == 3);
        TiDB::TP value_tp = dataTypeToTP(actual_input[2].type);

        actual_input[0].name = "partition";
        actual_input[1].name = "order";
        actual_input[2].name = value_col_name;
        context.addMockTable(
            {"test_db", "test_table_for_last_value"},
            {{"partition", TiDB::TP::TypeLongLong, actual_input[0].type->isNullable()},
             {"order", TiDB::TP::TypeLongLong, actual_input[1].type->isNullable()},
             {value_col_name, value_tp, actual_input[2].type->isNullable()}},
            actual_input);

        auto request = context
                           .scan("test_db", "test_table_for_last_value")
                           .sort({{"partition", false}, {"order", false}}, true)
                           .window(function, {"order", false}, {"partition", false}, frame)
                           .build(context);

        ColumnsWithTypeAndName expect = input;
        expect.push_back(result);
        executeWithConcurrencyAndBlockSize(request, expect);
    }

    template <typename IntType>
    void testInt()
    {
        // TODO test with bounded_type_frame
        MockWindowFrame unbounded_type_frame{
            tipb::WindowFrameType::Rows,
            std::make_tuple(tipb::WindowBoundType::Preceding, true, 0),
            std::make_tuple(tipb::WindowBoundType::Following, true, 0)};

        executeFunctionAndAssert(
            toVec<IntType>({1, 5, 5, 5, 5, 10, 10, 10, 10, 10, 13, 13, 13}),
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
        // TODO test with bounded_type_frame
        MockWindowFrame unbounded_type_frame{
            tipb::WindowFrameType::Rows,
            std::make_tuple(tipb::WindowBoundType::Preceding, true, 0),
            std::make_tuple(tipb::WindowBoundType::Following, true, 0)};

        executeFunctionAndAssert(
            toVec<FloatType>({1, 5, 5, 5, 5, 10, 10, 10, 10, 10, 13, 13, 13}),
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
    // TODO test with bounded_type_frame
    MockWindowFrame unbounded_type_frame{
        tipb::WindowFrameType::Rows,
        std::make_tuple(tipb::WindowBoundType::Preceding, true, 0),
        std::make_tuple(tipb::WindowBoundType::Following, true, 0)};

    executeFunctionAndAssert(
        toVec<String>({"1", "5", "5", "5", "5", "10", "10", "10", "10", "10", "13", "13", "13"}),
        LastValue(value_col),
        {toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
         toVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
         toVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13"})},
         unbounded_type_frame);

    executeFunctionAndAssert(
        toNullableVec<String>({{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}}),
        LastValue(value_col),
        {toNullableVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
         toNullableVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
         toNullableVec<String>(/*value*/ {{}, "2", "3", "4", {}, "6", "7", "8", "9", {}, "11", "12", {}})},
         unbounded_type_frame);

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