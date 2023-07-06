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
#include <TestUtils/mockExecutor.h>

namespace DB::tests
{
class WindowAggFuncTest : public DB::tests::ExecutorTest
{
    static const size_t max_concurrency_level = 10;

public:
    static constexpr auto VALUE_COL_NAME = "window_agg";
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
            ASSERT_COLUMNS_EQ_UR(expect_columns, executeStreams(request, max_concurrency_level));
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

        actual_input[0].name = "partition";
        actual_input[1].name = "order";
        actual_input[2].name = VALUE_COL_NAME;
        context.addMockTable(
            {"test_db", "test_table_for_first_value"},
            {{"partition", TiDB::TP::TypeLongLong, actual_input[0].type->isNullable()},
             {"order", TiDB::TP::TypeLongLong, actual_input[1].type->isNullable()},
             {VALUE_COL_NAME, value_tp, actual_input[2].type->isNullable()}},
            actual_input);

        auto request = context
                           .scan("test_db", "test_table_for_first_value")
                           .sort({{"partition", false}, {"order", false}}, true)
                           .window(function, {"order", false}, {"partition", false}, mock_frame)
                           .build(context);

        ColumnsWithTypeAndName expect = input;
        expect.push_back(result);
        executeWithConcurrencyAndBlockSize(request, expect);
    }
};

TEST_F(WindowAggFuncTest, windowAggTests)
try
{
    MockWindowFrame frame;
    frame.type = tipb::WindowFrameType::Rows;
    frame.start = std::make_tuple(tipb::WindowBoundType::Preceding, false, 0);
    std::vector<Int64> frame_start_offset{0, 1, 3, 10};

    std::vector<std::vector<Int64>> res_not_null{
        {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
        {1, 2, 5, 7, 9, 6, 13, 15, 17, 19, 11, 23, 25},
        {1, 2, 5, 9, 14, 6, 13, 21, 30, 34, 11, 23, 36},
        {1, 2, 5, 9, 14, 6, 13, 21, 30, 40, 11, 23, 36}};

    for (size_t i = 0; i < frame_start_offset.size(); ++i)
    {
        frame.start = std::make_tuple(tipb::WindowBoundType::Preceding, false, frame_start_offset[i]);

        executeFunctionAndAssert(
            toVec<Int64>(res_not_null[i]),
            Sum(value_col),
            {toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
             toVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
             toVec<Int64>(/*value*/ {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13})},
            frame);
    }
}
CATCH
} // namespace DB::tests
