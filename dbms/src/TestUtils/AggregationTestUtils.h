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
#include <TestUtils/TiFlashTestEnv.h>
#include <gtest/gtest.h>

namespace DB::tests
{

class AggregationTest : public ExecutorTest
{
public:
    static ::testing::AssertionResult checkAggReturnType(const String & agg_name, const DataTypes & data_types, const DataTypePtr & expect_type)
    {
        AggregateFunctionPtr agg_ptr = DB::AggregateFunctionFactory::instance().get(agg_name, data_types, {});
        const DataTypePtr & ret_type = agg_ptr->getReturnType();
        if (ret_type->equals(*expect_type))
            return ::testing::AssertionSuccess();
        return ::testing::AssertionFailure() << "Expect type: " << expect_type->getName() << " Actual type: " << ret_type->getName();
    }

    void executeAggFunctionAndAssert(ASTPtr func, String func_name, const ColumnsWithTypeAndName & column, const ColumnsWithTypeAndName & expected_cols)
    {
        context.addMockTableColumnData("test_db", "test_table", column);
        auto request = context.scan("test_db", "test_table")
                           .aggregation(func, {})
                           .project({func_name})
                           .build(context);

        for (size_t i = 1; i <= 10; ++i)
            ASSERT_COLUMNS_EQ_UR(expected_cols, executeStreams(request, i));
    }

    void executeGroupByAndAssert(const ColumnsWithTypeAndName & cols, const ColumnsWithTypeAndName & expected_cols)
    {
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

        for (size_t i = 1; i <= 10; ++i)
            ASSERT_COLUMNS_EQ_UR(expected_cols, executeStreams(request, i)) << "expected_cols: " << getColumnsContent(expected_cols) << ", actual_cols: " << getColumnsContent(executeStreams(request, i));
    }
     
    // todo refine..
    void executeGroupByAndAssertWithTable(const String & db_name, const String & table_name, const ColumnsWithTypeAndName & expected_cols)
    {
        MockAstVec group_by_cols;
        MockColumnNameVec proj_names;
        for (const auto & col : expected_cols)
        {
            group_by_cols.push_back(col(col.name));
            proj_names.push_back(col.name);
        }
  
        auto request = context.scan(db_name, table_name)
                           .aggregation({}, group_by_cols)
                           .project(proj_names)
                           .build(context);

        for (size_t i = 1; i <= 10; ++i)
            ASSERT_COLUMNS_EQ_UR(expected_cols, executeStreams(request, i)) << "expected_cols: " << getColumnsContent(expected_cols) << ", actual_cols: " << getColumnsContent(executeStreams(request, i));
    }

    static void SetUpTestCase();
};

} // namespace DB::tests
