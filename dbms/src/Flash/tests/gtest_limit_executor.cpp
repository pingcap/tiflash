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

#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/mockExecutor.h>

namespace DB
{
namespace tests
{

class ExecutorLimitTestRunner : public DB::tests::ExecutorTest
{
public:
    using ColDataType = std::optional<typename TypeTraits<String>::FieldType>;
    using ColumnWithData = std::vector<ColDataType>;

    void initializeContext() override
    {
        ExecutorTest::initializeContext();

        context.addMockTable({db_name, table_name},
                             {{col_name, TiDB::TP::TypeString}},
                             {toNullableVec<String>(col_name, col0)});
    }

    std::shared_ptr<tipb::DAGRequest> buildDAGRequest(size_t limit_num)
    {
        return context.scan(db_name, table_name).limit(limit_num).build(context);
    }

    /// Prepare some names
    const String db_name{"test_db"};
    const String table_name{"projection_test_table"};
    const String col_name{"limit_col"};
    const ColumnWithData col0{"col0-0", {}, "col0-2", "col0-3", {}, "col0-5", "col0-6", "col0-7"};
};

TEST_F(ExecutorLimitTestRunner, Limit)
try
{
    std::shared_ptr<tipb::DAGRequest> request;
    ColumnsWithTypeAndName expect_cols;

    /// Check limit result with various parameters
    const size_t col_data_num = col0.size();
    for (size_t limit_num = 0; limit_num <= col_data_num + 3; ++limit_num)
    {
        if (limit_num == col_data_num + 3)
            limit_num = INT_MAX;
        request = buildDAGRequest(limit_num);

        if (limit_num == 0)
            expect_cols = {};
        else if (limit_num > col_data_num)
            expect_cols = {toNullableVec<String>(col_name, ColumnWithData(col0.begin(), col0.end()))};
        else
            expect_cols = {toNullableVec<String>(col_name, ColumnWithData(col0.begin(), col0.begin() + limit_num))};

        WRAP_FOR_DIS_ENABLE_PLANNER_BEGIN
        ASSERT_COLUMNS_EQ_R(executeStreams(request), expect_cols);
        WRAP_FOR_DIS_ENABLE_PLANNER_END

        executeAndAssertRowsEqual(request, std::min(limit_num, col_data_num));
    }
}
CATCH

TEST_F(ExecutorLimitTestRunner, RawQuery)
try
{
    String query = "select * from test_db.projection_test_table limit 1";
    auto cols = {toNullableVec<String>(col_name, ColumnWithData(col0.begin(), col0.begin() + 1))};
    ASSERT_COLUMNS_EQ_R(executeRawQuery(query, 1), cols);
}
CATCH

} // namespace tests
} // namespace DB
