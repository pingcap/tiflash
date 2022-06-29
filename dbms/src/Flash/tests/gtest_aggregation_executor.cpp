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

class ExecutorAggTestRunner : public DB::tests::ExecutorTest
{
public:
    using ColStringType = std::optional<typename TypeTraits<String>::FieldType>;
    using ColInt32Type = std::optional<typename TypeTraits<Int32>::FieldType>;
    using ColumnWithString = std::vector<ColStringType>;
    using ColumnWithInt32 = std::vector<ColInt32Type>;

    void initializeContext() override
    {
        ExecutorTest::initializeContext();

        context.addMockTable({db_name, table_name},
                             {{col_name[0], TiDB::TP::TypeLong},
                              {col_name[1], TiDB::TP::TypeString},
                              {col_name[2], TiDB::TP::TypeString},
                              {col_name[3], TiDB::TP::TypeLong}},
                             {toNullableVec<Int32>(col_name[0], col_age),
                              toNullableVec<String>(col_name[1], col_gender),
                              toNullableVec<String>(col_name[2], col_country),
                              toNullableVec<Int32>(col_name[3], col_salary)});
    }

    std::shared_ptr<tipb::DAGRequest> buildDAGRequest(MockAsts agg_funcs, MockAsts group_by_exprs, MockOrderByItems order_by_items, MockColumnNames proj)
    {
        /// We can filter the group by column with project operator.
        /// topN is applied to get stable results in concurrency environment.
        return context.scan(db_name, table_name).aggregation(agg_funcs, group_by_exprs).topN(order_by_items, 100).project(proj).build(context);
    }

    /// Prepare some data and names
    const String db_name{"test_db"};
    const String table_name{"clerk"};
    const std::vector<String> col_name{"age", "gender", "country", "salary"};
    ColumnWithInt32 col_age{30, {}, 27, 32, 25, 36, {}, 22, 34};
    ColumnWithString col_gender{"male", "female", "female", "male", "female", "female", "male", "female", "male", };
    ColumnWithString col_country{"russia", "korea", "usa", "usa", "usa", "china", "china", "china", "china"};
    ColumnWithInt32 col_salary{1000, 1300, 0, {}, -200, 900, -999, 2000, -300};
};

TEST_F(ExecutorAggTestRunner, Aggregation)
try
{
    std::shared_ptr<tipb::DAGRequest> request;
    auto agg_func0 = Max(col(col_name[0])); /// select max(age) from clerk group by country order by max(age) DESC limit 100;
    auto agg_func1 = Max(col(col_name[3])); /// select max(salary) from clerk group by country, gender order by max(salary) DESC limit 100;

    auto group_by_expr0 = col(col_name[2]);
    auto group_by_expr10 = col(col_name[2]);
    auto group_by_expr11 = col(col_name[1]);

    /// Prepare some data for test
    std::vector<ColumnsWithTypeAndName> expect_cols{
        {toNullableVec<Int32>("max(age)", ColumnWithInt32{36, 32, 30, {}})},
        {toNullableVec<Int32>("max(salary)", ColumnWithInt32{2000, 1300, 1000, 0, -300, {}})}
    };
    std::vector<MockAsts> group_by_exprs{{group_by_expr0}, {group_by_expr10, group_by_expr11}};
    std::vector<MockColumnNames> projections{{"max(age)"}, {"max(salary)"}};
    std::vector<MockOrderByItems> order_by_items{{MockOrderByItem("max(age)", true)}, {MockOrderByItem("max(salary)", true)}};
    std::vector<MockAsts> agg_funcs{{agg_func0}, {agg_func1}};
    const size_t test_num = expect_cols.size();

    /// Start to test
    for (size_t i = 0; i < test_num; ++i)
    {
        request = buildDAGRequest(agg_funcs[i], group_by_exprs[i], order_by_items[i], projections[i]);
        executeStreamsWithMultiConcurrency(request, expect_cols[i]);
    }

    // TODO more aggregation functions...
}
CATCH

} // namespace tests
} // namespace DB
