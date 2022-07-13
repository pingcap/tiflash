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
    using ColStringNullableType = std::optional<typename TypeTraits<String>::FieldType>;
    using ColInt32NullableType = std::optional<typename TypeTraits<Int32>::FieldType>;
    using ColUInt64Type = typename TypeTraits<UInt64>::FieldType;
    using ColumnWithNullableString = std::vector<ColStringNullableType>;
    using ColumnWithNullableInt32 = std::vector<ColInt32NullableType>;
    using ColumnWithUInt64 = std::vector<ColUInt64Type>;

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

    std::shared_ptr<tipb::DAGRequest> buildDAGRequest(MockAstVec agg_funcs, MockAstVec group_by_exprs, MockOrderByItemVec order_by_items, MockColumnNameVec proj)
    {
        /// We can filter the group by column with project operator.
        /// topN is applied to get stable results in concurrency environment.
        return context.scan(db_name, table_name).aggregation(agg_funcs, group_by_exprs).topN(order_by_items, 100).project(proj).build(context);
    }

    void executeWithConcurrency(const std::shared_ptr<tipb::DAGRequest> & request, const ColumnsWithTypeAndName & expect_columns)
    {
        for (size_t i = 1; i < max_concurrency; i += step)
            ASSERT_COLUMNS_EQ_R(expect_columns, executeStreams(request, i));
    }

    size_t max_concurrency = 10;
    size_t step = 2;

    /// Prepare some data and names
    const String db_name{"test_db"};
    const String table_name{"clerk"};
    const std::vector<String> col_name{"age", "gender", "country", "salary"};
    ColumnWithNullableInt32 col_age{30, {}, 27, 32, 25, 36, {}, 22, 34};
    ColumnWithNullableString col_gender{"male", "female", "female", "male", "female", "female", "male", "female", "male", };
    ColumnWithNullableString col_country{"russia", "korea", "usa", "usa", "usa", "china", "china", "china", "china"};
    ColumnWithNullableInt32 col_salary{1000, 1300, 0, {}, -200, 900, -999, 2000, -300};
};

TEST_F(ExecutorAggTestRunner, AggregationMaxAndMin)
try
{
    std::shared_ptr<tipb::DAGRequest> request;
    auto agg_func0 = Max(col(col_name[0])); /// select max(age) from clerk group by country order by max(age) DESC limit 100;
    auto agg_func1 = Max(col(col_name[3])); /// select max(salary) from clerk group by country, gender order by max(salary) DESC limit 100;

    auto group_by_expr0 = col(col_name[2]);
    auto group_by_expr10 = col(col_name[2]);
    auto group_by_expr11 = col(col_name[1]);

    /// Prepare some data for max function test
    std::vector<ColumnsWithTypeAndName> expect_cols{
        {toNullableVec<Int32>("max(age)", ColumnWithNullableInt32{36, 32, 30, {}})},
        {toNullableVec<Int32>("max(salary)", ColumnWithNullableInt32{2000, 1300, 1000, 0, -300, {}})}
    };
    std::vector<MockAstVec> group_by_exprs{{group_by_expr0}, {group_by_expr10, group_by_expr11}};
    std::vector<MockColumnNameVec> projections{{"max(age)"}, {"max(salary)"}};
    std::vector<MockOrderByItemVec> order_by_items{{MockOrderByItem("max(age)", true)}, {MockOrderByItem("max(salary)", true)}};
    std::vector<MockAstVec> agg_funcs{{agg_func0}, {agg_func1}};
    size_t test_num = expect_cols.size();

    /// Start to test max function
    for (size_t i = 0; i < test_num; ++i)
    {
        request = buildDAGRequest(agg_funcs[i], group_by_exprs[i], order_by_items[i], projections[i]);
        executeWithConcurrency(request, expect_cols[i]);
    }

    /// Min function tests

    agg_func0 = Min(col(col_name[0])); /// select min(age) from clerk group by country order by min(age) DESC limit 100;
    agg_func1 = Min(col(col_name[3])); /// select min(salary) from clerk group by country, gender order by min(salary) DESC limit 100;

    expect_cols = {
        {toNullableVec<Int32>("min(age)", ColumnWithNullableInt32{30, 25, 22, {}})},
        {toNullableVec<Int32>("min(salary)", ColumnWithNullableInt32{1300, 1000, 900, -200, -999, {}})}
    };
    projections = {{"min(age)"}, {"min(salary)"}};
    order_by_items = {{MockOrderByItem("min(age)", true)}, {MockOrderByItem("min(salary)", true)}};
    agg_funcs = {{agg_func0}, {agg_func1}};
    test_num = expect_cols.size();

    /// Start to test min function
    for (size_t i = 0; i < test_num; ++i)
    {
        request = buildDAGRequest(agg_funcs[i], group_by_exprs[i], order_by_items[i], projections[i]);
        executeWithConcurrency(request, expect_cols[i]);
    }
}
CATCH

TEST_F(ExecutorAggTestRunner, AggregationCount)
try
{
    /// Prepare some data
    std::shared_ptr<tipb::DAGRequest> request;
    auto agg_func0 = Count(col(col_name[0])); /// select count(age) from clerk group by country order by count(age) DESC limit 100;
    auto agg_func1 = Count(col(col_name[1])); /// select count(gender) from clerk group by country, gender order by count(gender) DESC limit 100;
    std::vector<MockAstVec> agg_funcs = {{agg_func0}, {agg_func1}};

    auto group_by_expr0 = col(col_name[2]);
    auto group_by_expr10 = col(col_name[2]);
    auto group_by_expr11 = col(col_name[1]);

    std::vector<ColumnsWithTypeAndName> expect_cols {
        {toVec<UInt64>("count(age)", ColumnWithUInt64{3, 3, 1, 0})},
        {toVec<UInt64>("count(gender)", ColumnWithUInt64{2, 2, 2, 1, 1, 1})}
    };
    std::vector<MockAstVec> group_by_exprs{{group_by_expr0}, {group_by_expr10, group_by_expr11}};
    std::vector<MockColumnNameVec> projections{{"count(age)"}, {"count(gender)"}};
    std::vector<MockOrderByItemVec> order_by_items{{MockOrderByItem("count(age)", true)}, {MockOrderByItem("count(gender)", true)}};
    size_t test_num = expect_cols.size();

    /// Start to test
    for (size_t i = 0; i < test_num; ++i)
    {
        request = buildDAGRequest({agg_funcs[i]}, group_by_exprs[i], order_by_items[i], projections[i]);
        executeWithConcurrency(request, expect_cols[i]);
    }
}
CATCH

// TODO more aggregation functions...

} // namespace tests
} // namespace DB
