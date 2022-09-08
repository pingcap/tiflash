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

    static const size_t max_concurrency_level = 10;

    void executeWithConcurrency(const std::shared_ptr<tipb::DAGRequest> & request, size_t expect_rows)
    {
        WRAP_FOR_DIS_ENABLE_PLANNER_BEGIN
        for (size_t i = 1; i <= max_concurrency_level; i += 2)
        {
            ASSERT_EQ(expect_rows, Block(executeStreams(request, i)).rows());
        }
        WRAP_FOR_DIS_ENABLE_PLANNER_END
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
        executeWithConcurrency(request, std::min(limit_num, col_data_num));
    }
}
CATCH

} // namespace tests
} // namespace DB
