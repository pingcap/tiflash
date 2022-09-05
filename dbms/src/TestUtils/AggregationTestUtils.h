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

#pragma once

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <TestUtils/ExecutorTestUtils.h>

namespace DB::tests
{

class AggregationTest : public ExecutorTest
{
public:
    static ::testing::AssertionResult checkAggReturnType(const String & agg_name, const DataTypes & data_types, const DataTypePtr & expect_type);

    // Test one aggregation functions without group by.
    void executeAggFunctionAndAssert(const std::vector<String> & func_names, const ColumnWithTypeAndName & column, const ColumnsWithTypeAndName & expected_cols);

    // Prequisite: add a mock table in MockDAGRequestContext.
    // func_names: agg functions which are going to execute.
    // col_names: columns each agg function executed on.
    void executeAggFunctionAndAssertWithTable(const String & db_name, const String & table_name, const std::vector<String> & func_names, const std::vector<String> & col_names, const ColumnsWithTypeAndName & expected_cols);

    // Test group by columns
    void executeGroupByAndAssert(const ColumnsWithTypeAndName & cols, const ColumnsWithTypeAndName & expected_cols);
    // Prequisite: add a mock table in MockDAGRequestContext.
    void executeGroupByAndAssertWithTable(const String & db_name, const String & table_name, const std::vector<String> & group_by_cols, const ColumnsWithTypeAndName & expected_cols);

    static void SetUpTestCase();

private:
    void checkResult(std::shared_ptr<tipb::DAGRequest> request, const ColumnsWithTypeAndName & expected_cols);
    ASTPtr aggFunctionBuilder(const String & func_name, const String & col_name);
};

} // namespace DB::tests
