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

#include <TestUtils/AggregationTestUtils.h>

namespace DB::tests
{
void AggregationTest::SetUpTestCase()
{
    auto register_func = [](std::function<void()> func) {
        try
        {
            func();
        }
        catch (DB::Exception &)
        {
            // Maybe another test has already registered, ignore exception here.
        }
    };

    register_func(DB::registerAggregateFunctions);
}

::testing::AssertionResult AggregationTest::checkAggReturnType(const String & agg_name, const DataTypes & data_types, const DataTypePtr & expect_type)
{
    AggregateFunctionPtr agg_ptr = DB::AggregateFunctionFactory::instance().get(agg_name, data_types, {});
    const DataTypePtr & ret_type = agg_ptr->getReturnType();
    if (ret_type->equals(*expect_type))
        return ::testing::AssertionSuccess();
    return ::testing::AssertionFailure() << "Expect type: " << expect_type->getName() << " Actual type: " << ret_type->getName();
}

void AggregationTest::executeAggFunctionAndAssert(const std::vector<String> & func_names, const ColumnWithTypeAndName & column, const ColumnsWithTypeAndName & expected_cols)
{
    String db_name = "test_agg_function";
    String table_name = "test_table_agg";
    std::vector<ASTPtr> agg_funcs;
    for (const auto & func_name : func_names)
        agg_funcs.push_back(aggFunctionBuilder(func_name, column.name));

    MockColumnInfoVec column_infos;
    column_infos.push_back({column.name, dataTypeToTP(column.type)});
    context.addMockTable(db_name, table_name, column_infos, {column});

    auto request = context.scan(db_name, table_name)
                       .aggregation(agg_funcs, {})
                       .build(context);

    checkResult(request, expected_cols);
}

void AggregationTest::executeGroupByAndAssert(const ColumnsWithTypeAndName & cols, const ColumnsWithTypeAndName & expected_cols)
{
    RUNTIME_CHECK_MSG(cols.size() == expected_cols.size(), "number of group_by columns don't match number of expected columns");

    String db_name = "test_group";
    String table_name = "test_table_group";
    MockAstVec group_by_cols;
    MockColumnNameVec proj_names;
    MockColumnInfoVec column_infos;
    for (const auto & col : cols)
    {
        group_by_cols.push_back(col(col.name));
        proj_names.push_back(col.name);
        column_infos.push_back({col.name, dataTypeToTP(col.type)});
    }

    context.addMockTable(db_name, table_name, column_infos, cols);

    auto request = context.scan(db_name, table_name)
                       .aggregation({}, group_by_cols)
                       .project(proj_names)
                       .build(context);

    checkResult(request, expected_cols);
}

void AggregationTest::checkResult(std::shared_ptr<tipb::DAGRequest> request, const ColumnsWithTypeAndName & expected_cols)
{
    for (size_t i = 1; i <= 10; ++i)
        ASSERT_COLUMNS_EQ_UR(expected_cols, executeStreams(request, i)) << "expected_cols: " << getColumnsContent(expected_cols) << ", actual_cols: " << getColumnsContent(executeStreams(request, i));
}

ASTPtr AggregationTest::aggFunctionBuilder(const String & func_name, const String & col_name)
{
    ASTPtr func;
    String func_name_lowercase = Poco::toLower(func_name);

    // TODO support more agg functions.
    if (func_name_lowercase == "max")
        func = Max(col(col_name));
    else if (func_name_lowercase == "min")
        func = Min(col(col_name));
    else if (func_name_lowercase == "count")
        func = Count(col(col_name));
    else if (func_name_lowercase == "sum")
        func = Sum(col(col_name));
    else
        throw Exception(fmt::format("Unsupported agg function {}", func_name), ErrorCodes::LOGICAL_ERROR);
    return func;
}
} // namespace DB::tests
