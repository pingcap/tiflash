// Copyright 2022 PingCAP, Ltd.
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

#include <Columns/ColumnNullable.h>
#include <Core/ColumnNumbers.h>
#include <DataTypes/DataTypeNothing.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzerHelper.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <TestUtils/ColumnsToTiPBExpr.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <fmt/core.h>

namespace DB
{
namespace tests
{
template <typename ExpectedT, typename ActualT, typename ExpectedDisplayT, typename ActualDisplayT>
::testing::AssertionResult assertEqual(
    const char * expected_expr,
    const char * actual_expr,
    const ExpectedT & expected_v,
    const ActualT & actual_v,
    const ExpectedDisplayT & expected_display,
    const ActualDisplayT & actual_display,
    const String & title = "")
{
    if (expected_v != actual_v)
    {
        auto expected_str = fmt::format("\n{}: {}", expected_expr, expected_display);
        auto actual_str = fmt::format("\n{}: {}", actual_expr, actual_display);
        return ::testing::AssertionFailure() << title << expected_str << actual_str;
    }
    return ::testing::AssertionSuccess();
}


#define ASSERT_EQUAL_WITH_TEXT(expected_value, actual_value, title, expected_display, actual_display)                                             \
    do                                                                                                                                            \
    {                                                                                                                                             \
        auto result = assertEqual(#expected_value, #actual_value, (expected_value), (actual_value), (expected_display), (actual_display), title); \
        if (!result)                                                                                                                              \
            return result;                                                                                                                        \
    } while (false)

#define ASSERT_EQUAL(expected_value, actual_value, title)                                                             \
    do                                                                                                                \
    {                                                                                                                 \
        auto expected_v = (expected_value);                                                                           \
        auto actual_v = (actual_value);                                                                               \
        auto result = assertEqual(#expected_value, #actual_value, expected_v, actual_v, expected_v, actual_v, title); \
        if (!result)                                                                                                  \
            return result;                                                                                            \
    } while (false)

::testing::AssertionResult dataTypeEqual(
    const DataTypePtr & expected,
    const DataTypePtr & actual)
{
    ASSERT_EQUAL(expected->getName(), actual->getName(), "DataType name mismatch");
    return ::testing::AssertionSuccess();
}

::testing::AssertionResult columnEqual(
    const ColumnPtr & expected,
    const ColumnPtr & actual)
{
     for (size_t i = 0, size = actual->size(); i < size; ++i)
    {
        auto actual_field = (*actual)[i];
        std::cout << "actual res[" << i << "]: " << actual_field.toString() << std::endl;        
    }

    ASSERT_EQUAL(expected->getName(), actual->getName(), "Column name mismatch");
    ASSERT_EQUAL(expected->size(), actual->size(), "Column size mismatch");

    for (size_t i = 0, size = expected->size(); i < size; ++i)
    {
        auto expected_field = (*expected)[i];
        auto actual_field = (*actual)[i];

        ASSERT_EQUAL_WITH_TEXT(expected_field, actual_field, fmt::format("Value {} mismatch", i), expected_field.toString(), actual_field.toString());
    }
    return ::testing::AssertionSuccess();
}

::testing::AssertionResult columnEqual(
    const ColumnWithTypeAndName & expected,
    const ColumnWithTypeAndName & actual)
{
    auto ret = dataTypeEqual(expected.type, actual.type);
    if (!ret)
        return ret;

    return columnEqual(expected.column, actual.column);
}

void blockEqual(
    const Block & expected,
    const Block & actual)
{
    size_t columns = actual.columns();
    ASSERT_TRUE(expected.columns() == columns);

    for (size_t i = 0; i < columns; ++i)
    {
        const auto & expected_col = expected.getByPosition(i);
        const auto & actual_col = actual.getByPosition(i);
        ASSERT_TRUE(actual_col.type->getName() == expected_col.type->getName());
        ASSERT_COLUMN_EQ(expected_col.column, actual_col.column);
    }
}

std::pair<ExpressionActionsPtr, String> buildFunction(
    Context & context,
    const String & func_name,
    const ColumnNumbers & argument_column_numbers,
    const ColumnsWithTypeAndName & columns,
    const TiDB::TiDBCollatorPtr & collator)
{
    tipb::Expr tipb_expr = columnsToTiPBExpr(func_name, argument_column_numbers, columns, collator);
    NamesAndTypes source_columns;
    for (size_t index : argument_column_numbers)
        source_columns.emplace_back(columns[index].name, columns[index].type);
    DAGExpressionAnalyzer analyzer(source_columns, context);
    ExpressionActionsChain chain;
    auto & last_step = analyzer.initAndGetLastStep(chain);
    auto result_name = DB::DAGExpressionAnalyzerHelper::buildFunction(&analyzer, tipb_expr, last_step.actions);
    last_step.required_output.push_back(result_name);
    chain.finalize();
    return std::make_pair(last_step.actions, result_name);
}

ColumnsWithTypeAndName toColumnsWithUniqueName(const ColumnsWithTypeAndName & columns)
{
    ColumnsWithTypeAndName columns_with_distinct_name = columns;
    std::string base_name = "col";
    for (size_t i = 0; i < columns.size(); ++i)
    {
        columns_with_distinct_name[i].name = fmt::format("{}_{}", base_name, i);
    }
    return columns_with_distinct_name;
}

ColumnWithTypeAndName executeFunction(
    Context & context,
    const String & func_name,
    const ColumnsWithTypeAndName & columns,
    const TiDB::TiDBCollatorPtr & collator,
    bool raw_function_test)
{
    ColumnNumbers argument_column_numbers;
    for (size_t i = 0; i < columns.size(); ++i)
        argument_column_numbers.push_back(i);
    return executeFunction(context, func_name, argument_column_numbers, columns, collator, raw_function_test);
}

ColumnWithTypeAndName executeFunction(
    Context & context,
    const String & func_name,
    const ColumnNumbers & argument_column_numbers,
    const ColumnsWithTypeAndName & columns,
    const TiDB::TiDBCollatorPtr & collator,
    bool raw_function_test)
{
    if (raw_function_test)
    {
        auto & factory = FunctionFactory::instance();
        Block block(columns);
        ColumnsWithTypeAndName arguments;
        for (size_t i = 0; i < argument_column_numbers.size(); ++i)
            arguments.push_back(columns.at(i));
        auto bp = factory.tryGet(func_name, context);
        if (!bp)
            throw TiFlashTestException(fmt::format("Function {} not found!", func_name));
        auto func = bp->build(arguments, collator);
        block.insert({nullptr, func->getReturnType(), "res"});
        func->execute(block, argument_column_numbers, columns.size());
        return block.getByPosition(columns.size());
    }
    auto columns_with_unique_name = toColumnsWithUniqueName(columns);
    auto [actions, result_name] = buildFunction(context, func_name, argument_column_numbers, columns_with_unique_name, collator);
    Block block(columns_with_unique_name);
    actions->execute(block);
    return block.getByName(result_name);
}

DataTypePtr getReturnTypeForFunction(
    Context & context,
    const String & func_name,
    const ColumnsWithTypeAndName & columns,
    const TiDB::TiDBCollatorPtr & collator,
    bool raw_function_test)
{
    if (raw_function_test)
    {
        auto & factory = FunctionFactory::instance();

        Block block(columns);
        ColumnNumbers cns;
        for (size_t i = 0; i < columns.size(); ++i)
            cns.push_back(i);

        auto bp = factory.tryGet(func_name, context);
        if (!bp)
            throw TiFlashTestException(fmt::format("Function {} not found!", func_name));
        auto func = bp->build(columns, collator);
        return func->getReturnType();
    }
    else
    {
        ColumnNumbers argument_column_numbers;
        for (size_t i = 0; i < columns.size(); ++i)
            argument_column_numbers.push_back(i);
        auto columns_with_unique_name = toColumnsWithUniqueName(columns);
        auto [actions, result_name] = buildFunction(context, func_name, argument_column_numbers, columns_with_unique_name, collator);
        return actions->getSampleBlock().getByName(result_name).type;
    }
}

ColumnWithTypeAndName createOnlyNullColumnConst(size_t size, const String & name)
{
    DataTypePtr data_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>());
    return {data_type->createColumnConst(size, Null()), data_type, name};
}

ColumnWithTypeAndName createOnlyNullColumn(size_t size, const String & name)
{
    DataTypePtr data_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>());
    auto col = data_type->createColumn();
    for (size_t i = 0; i < size; i++)
        col->insert(Null());
    return {std::move(col), data_type, name};
}

} // namespace tests
} // namespace DB
