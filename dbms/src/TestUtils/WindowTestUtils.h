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

#pragma once

#include <Debug/MockExecutor/WindowBinder.h>
#include <Interpreters/Context.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/mockExecutor.h>
#include <tipb/executor.pb.h>
#include <tipb/expression.pb.h>

#include <memory>

namespace DB::tests
{
static inline ASTPtr buildPlusFunction(
    tipb::RangeCmpDataType cmp_data_type,
    const String & order_by_col_name,
    const Field & range_val_field)
{
    switch (cmp_data_type)
    {
    case tipb::RangeCmpDataType::Int:
    case tipb::RangeCmpDataType::DateTime:
    case tipb::RangeCmpDataType::Duration:
        return plusInt(col(order_by_col_name), lit(range_val_field));
    case tipb::RangeCmpDataType::Float:
        return plusReal(col(order_by_col_name), lit(range_val_field));
    case tipb::RangeCmpDataType::Decimal:
        return plusDecimal(col(order_by_col_name), lit(range_val_field));
    default:
        throw Exception("Invalid tipb::RangeCmpDataType");
    }
}

static inline ASTPtr buildMinusFunction(
    tipb::RangeCmpDataType cmp_data_type,
    const String & order_by_col_name,
    const Field & range_val_field)
{
    switch (cmp_data_type)
    {
    case tipb::RangeCmpDataType::Int:
    case tipb::RangeCmpDataType::DateTime:
    case tipb::RangeCmpDataType::Duration:
        return minusInt(col(order_by_col_name), lit(range_val_field));
    case tipb::RangeCmpDataType::Float:
        return minusReal(col(order_by_col_name), lit(range_val_field));
    case tipb::RangeCmpDataType::Decimal:
        return minusDecimal(col(order_by_col_name), lit(range_val_field));
    default:
        throw Exception("Invalid tipb::RangeCmpDataType");
    }
}

class WindowTest : public ExecutorTest
{
protected:
    static const size_t MAX_CONCURRENCY_LEVEL = 10;
    static constexpr auto PARTITION_COL_NAME = "partition";
    static constexpr auto ORDER_COL_NAME = "order";
    static constexpr auto VALUE_COL_NAME = "first_value";

    template <typename T>
    mock::MockWindowFrameBound buildRangeFrameBound(
        tipb::WindowBoundType bound_type,
        tipb::RangeCmpDataType cmp_data_type,
        const String & order_by_col_name,
        bool is_plus,
        T range_val)
    {
        mock::BuildRangeFrameHelper helper;
        if (is_plus)
        {
            auto f = Field(range_val);
            helper.range_aux_func = buildPlusFunction(cmp_data_type, order_by_col_name, f);
        }
        else
            helper.range_aux_func = buildMinusFunction(cmp_data_type, order_by_col_name, Field(range_val));
        helper.context = context.context;
        return mock::MockWindowFrameBound(bound_type, cmp_data_type, helper);
    }

    // TODO Sometimes we only need to validate the correctness of value type, it's needless to configure
    // concurrency and block size and it will cause more useless tests. We should modify this function
    // so that caller could configure it and choose block_size and concurrency.
    void executeWithConcurrencyAndBlockSize(
        const std::shared_ptr<tipb::DAGRequest> & request,
        const ColumnsWithTypeAndName & expect_columns,
        bool is_restrict = true)
    {
        std::vector<size_t> block_sizes{1, 2, 3, 4, DEFAULT_BLOCK_SIZE};
        for (auto block_size : block_sizes)
        {
            context.context->setSetting("max_block_size", Field(static_cast<UInt64>(block_size)));
            if (is_restrict)
                ASSERT_COLUMNS_EQ_R(expect_columns, executeStreams(request));
            else
                ASSERT_COLUMNS_EQ_UR(expect_columns, executeStreams(request));
            ASSERT_COLUMNS_EQ_UR(expect_columns, executeStreams(request, 2));
            ASSERT_COLUMNS_EQ_UR(expect_columns, executeStreams(request, MAX_CONCURRENCY_LEVEL));
        }
    }

    void executeFunctionAndAssert(
        const ColumnWithTypeAndName & result,
        const ASTPtr & function,
        const ColumnsWithTypeAndName & input,
        MockWindowFrame mock_frame = MockWindowFrame(),
        bool is_restrict = true)
    {
        ColumnsWithTypeAndName actual_input = input;
        assert(actual_input.size() == 3);
        TiDB::TP partition_tp = dataTypeToTP(actual_input[0].type);
        TiDB::TP order_tp = dataTypeToTP(actual_input[1].type);
        TiDB::TP value_tp = dataTypeToTP(actual_input[2].type);

        actual_input[0].name = PARTITION_COL_NAME;
        actual_input[1].name = ORDER_COL_NAME;
        actual_input[2].name = VALUE_COL_NAME;
        context.addMockTable(
            {"test_db", "test_table_for_window"},
            {{PARTITION_COL_NAME, partition_tp, actual_input[0].type->isNullable()},
             {ORDER_COL_NAME, order_tp, actual_input[1].type->isNullable()},
             {VALUE_COL_NAME, value_tp, actual_input[2].type->isNullable()}},
            actual_input);

        auto request = context.scan("test_db", "test_table_for_window")
                           .sort({{PARTITION_COL_NAME, false}, {ORDER_COL_NAME, false}}, true)
                           .window(function, {ORDER_COL_NAME, false}, {PARTITION_COL_NAME, false}, mock_frame)
                           .build(context);

        ColumnsWithTypeAndName expect = input;
        expect.push_back(result);
        executeWithConcurrencyAndBlockSize(request, expect, is_restrict);
    }
};
} // namespace DB::tests
