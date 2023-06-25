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

#include <Debug/MockExecutor/WindowBinder.h>
#include <Interpreters/Context.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/WindowTestUtils.h>
#include <TestUtils/mockExecutor.h>
#include <tipb/executor.pb.h>

#include <memory>
#include <optional>

namespace DB::tests
{
class FirstValue : public DB::tests::WindowTest
{
    static const size_t MAX_CONCURRENCY_LEVEL = 10;
    static constexpr auto PARTITION_COL_NAME = "partition";
    static constexpr auto ORDER_COL_NAME = "order";
    static constexpr auto VALUE_COL_NAME = "first_value";

public:
    const ASTPtr value_col = col(VALUE_COL_NAME);

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
            ASSERT_COLUMNS_EQ_UR(expect_columns, executeStreams(request, MAX_CONCURRENCY_LEVEL));
        }
    }

    void executeFunctionAndAssert(
        const ColumnWithTypeAndName & result,
        const ASTPtr & function,
        const ColumnsWithTypeAndName & input,
        MockWindowFrame mock_frame = MockWindowFrame())
    {
        ColumnsWithTypeAndName actual_input = input;
        assert(actual_input.size() == 3);
        TiDB::TP value_tp = dataTypeToTP(actual_input[2].type);

        actual_input[0].name = PARTITION_COL_NAME;
        actual_input[1].name = ORDER_COL_NAME;
        actual_input[2].name = VALUE_COL_NAME;
        context.addMockTable(
            {"test_db", "test_table_for_first_value"},
            {{PARTITION_COL_NAME, TiDB::TP::TypeLongLong, actual_input[0].type->isNullable()},
             {ORDER_COL_NAME, TiDB::TP::TypeLongLong, actual_input[1].type->isNullable()},
             {VALUE_COL_NAME, value_tp, actual_input[2].type->isNullable()}},
            actual_input);

        auto request = context
                           .scan("test_db", "test_table_for_first_value")
                           .sort({{PARTITION_COL_NAME, false}, {ORDER_COL_NAME, false}}, true)
                           .window(function, {ORDER_COL_NAME, false}, {PARTITION_COL_NAME, false}, mock_frame)
                           .build(context);

        ColumnsWithTypeAndName expect = input;
        expect.push_back(result);
        executeWithConcurrencyAndBlockSize(request, expect);
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

    template <typename Type>
    void testIntForRangeFrame()
    {
        MockWindowFrame mock_frame;
        mock_frame.type = tipb::WindowFrameType::Ranges;
        mock_frame.start = mock::MockWindowFrameBound(tipb::WindowBoundType::Preceding, false, 0);
        mock_frame.end = mock::MockWindowFrameBound(tipb::WindowBoundType::Following, false, 0);

        std::vector<Int64> frame_start_range{0, 1, 3, 10};
        std::vector<std::vector<Int64>> res{
            {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31},
            {0, 1, 1, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31},
            {0, 1, 1, 1, 8, 0, 0, 10, 10, 13, 1, 1, 3, 9, 15, 20, 31},
            {0, 1, 1, 1, 1, 0, 0, 0, 3, 10, 1, 1, 1, 1, 5, 15, 31}};

        for (size_t i = 0; i < frame_start_range.size(); ++i)
        {
            mock_frame.start = buildRangeFrameBound(tipb::WindowBoundType::Preceding, tipb::RangeCmpDataType::Int, ORDER_COL_NAME, frame_start_range[i]);
            std::cout << "ddebug1111111111" << std::endl;
            executeFunctionAndAssert(
                toVec<Type>(res[i]),
                FirstValue(value_col),
                {toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3}),
                 toVec<Type>(/*order*/ {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31}),
                 toVec<Int64>(/*value*/ {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31})},
                mock_frame);
            std::cout << "ddebug222222222222" << std::endl;
        }
    }

    template <typename Type>
    void testDecimalType()
    {}
};

TEST_F(FirstValue, firstValueWithRowsFrameType)
try
{
    {
        // boundry type: unbounded
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
        // boundry type: offset
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

TEST_F(FirstValue, firstValueWithRangeFrameType)
try
{
    // TODO support unsigned int.
    testIntForRangeFrame<Int8>();
}
CATCH

} // namespace DB::tests
