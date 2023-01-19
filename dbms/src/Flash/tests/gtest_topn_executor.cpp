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

class TopNExecutorTestRunner : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();

        context.addMockTable({db_name, table_single_name},
                             {{single_col_name, TiDB::TP::TypeString}},
                             {toNullableVec<String>(single_col_name, col0)});

        context.addMockTable({db_name, table_name},
                             {{col_name[0], TiDB::TP::TypeLong},
                              {col_name[1], TiDB::TP::TypeString},
                              {col_name[2], TiDB::TP::TypeString},
                              {col_name[3], TiDB::TP::TypeLong}},
                             {toNullableVec<Int32>(col_name[0], col_age),
                              toNullableVec<String>(col_name[1], col_gender),
                              toNullableVec<String>(col_name[2], col_country),
                              toNullableVec<Int32>(col_name[3], col_salary)});

        /// table with 200 rows
        {
            // with 15 types of key.
            std::vector<std::optional<TypeTraits<int>::FieldType>> key(200);
            for (size_t i = 0; i < 200; ++i)
                key[i] = i % 15;
            context.addMockTable(
                {"test_db", "big_table_1"},
                {{"key", TiDB::TP::TypeLong}},
                {toNullableVec<Int32>("key", key)});
        }
        {
            // with 200 types of key.
            std::vector<std::optional<TypeTraits<int>::FieldType>> key(200);
            for (size_t i = 0; i < 200; ++i)
                key[i] = i;
            context.addMockTable(
                {"test_db", "big_table_2"},
                {{"key", TiDB::TP::TypeLong}},
                {toNullableVec<Int32>("key", key)});
        }
        {
            // with 1 types of key.
            std::vector<std::optional<TypeTraits<int>::FieldType>> key(200);
            for (size_t i = 0; i < 200; ++i)
                key[i] = 0;
            context.addMockTable(
                {"test_db", "big_table_3"},
                {{"key", TiDB::TP::TypeLong}},
                {toNullableVec<Int32>("key", key)});
        }
    }

    std::shared_ptr<tipb::DAGRequest> buildDAGRequest(const String & table_name, const String & col_name, bool is_desc, int limit_num)
    {
        return context.scan(db_name, table_name).topN(col_name, is_desc, limit_num).build(context);
    }

    std::shared_ptr<tipb::DAGRequest> buildDAGRequest(const String & table_name, MockOrderByItemVec order_by_items, int limit, MockAstVec func_proj_ast = {}, MockColumnNameVec out_proj_ast = {})
    {
        if (func_proj_ast.empty())
            return context.scan(db_name, table_name).topN(order_by_items, limit).build(context);
        else
            return context.scan(db_name, table_name).project(func_proj_ast).topN(order_by_items, limit).project(out_proj_ast).build(context);
    }

    /// Prepare some names
    const String db_name{"test_db"};

    const String table_single_name{"topn_single_table"}; /// For single column test
    const String single_col_name{"single_col"};
    ColumnWithNullableString col0{"col0-0", "col0-1", "col0-2", {}, "col0-4", {}, "col0-6", "col0-7"};

    const String table_name{"clerk"};
    const std::vector<String> col_name{"age", "gender", "country", "salary"};
    ColumnWithNullableInt32 col_age{{}, 27, 32, 36, {}, 34};
    ColumnWithNullableString col_gender{"female", "female", "male", "female", "male", "male"};
    ColumnWithNullableString col_country{"korea", "usa", "usa", "china", "china", "china"};
    ColumnWithNullableInt32 col_salary{1300, 0, {}, 900, {}, -300};
};

TEST_F(TopNExecutorTestRunner, TopN)
try
{
    std::shared_ptr<tipb::DAGRequest> request;
    std::vector<ColumnsWithTypeAndName> expect_cols;

    {
        /// Test single column
        size_t col_data_num = col0.size();
        for (size_t i = 1; i <= 1; ++i)
        {
            bool is_desc;
            is_desc = static_cast<bool>(i); /// Set descent or ascent
            if (is_desc)
                sort(col0.begin(), col0.end(), std::greater<ColStringNullableType>()); /// Sort col0 for the following comparison
            else
                sort(col0.begin(), col0.end());

            for (size_t limit_num = 0; limit_num <= col_data_num + 5; ++limit_num)
            {
                request = buildDAGRequest(table_single_name, single_col_name, is_desc, limit_num);

                expect_cols.clear();
                if (limit_num == 0 || limit_num > col_data_num)
                    expect_cols.push_back({toNullableVec<String>(single_col_name, ColumnWithNullableString(col0.begin(), col0.end()))});
                else
                    expect_cols.push_back({toNullableVec<String>(single_col_name, ColumnWithNullableString(col0.begin(), col0.begin() + limit_num))});

                executeAndAssertColumnsEqual(request, expect_cols.back());
            }
        }
    }

    {
        /// Test multi-columns
        expect_cols = {{toNullableVec<Int32>(col_name[0], ColumnWithNullableInt32{36, 34, 32, 27, {}, {}}),
                        toNullableVec<String>(col_name[1], ColumnWithNullableString{"female", "male", "male", "female", "male", "female"}),
                        toNullableVec<String>(col_name[2], ColumnWithNullableString{"china", "china", "usa", "usa", "china", "korea"}),
                        toNullableVec<Int32>(col_name[3], ColumnWithNullableInt32{900, -300, {}, 0, {}, 1300})},
                       {toNullableVec<Int32>(col_name[0], ColumnWithNullableInt32{32, {}, 34, 27, 36, {}}),
                        toNullableVec<String>(col_name[1], ColumnWithNullableString{"male", "male", "male", "female", "female", "female"}),
                        toNullableVec<String>(col_name[2], ColumnWithNullableString{"usa", "china", "china", "usa", "china", "korea"}),
                        toNullableVec<Int32>(col_name[3], ColumnWithNullableInt32{{}, {}, -300, 0, 900, 1300})},
                       {toNullableVec<Int32>(col_name[0], ColumnWithNullableInt32{34, {}, 32, 36, {}, 27}),
                        toNullableVec<String>(col_name[1], ColumnWithNullableString{"male", "male", "male", "female", "female", "female"}),
                        toNullableVec<String>(col_name[2], ColumnWithNullableString{"china", "china", "usa", "china", "korea", "usa"}),
                        toNullableVec<Int32>(col_name[3], ColumnWithNullableInt32{-300, {}, {}, 900, 1300, 0})}};

        std::vector<MockOrderByItemVec> order_by_items{
            /// select * from clerk order by age DESC, gender DESC;
            {MockOrderByItem(col_name[0], true), MockOrderByItem(col_name[1], true)},
            /// select * from clerk order by gender DESC, salary ASC;
            {MockOrderByItem(col_name[1], true), MockOrderByItem(col_name[3], false)},
            /// select * from clerk order by gender DESC, country ASC, salary DESC;
            {MockOrderByItem(col_name[1], true), MockOrderByItem(col_name[2], false), MockOrderByItem(col_name[3], true)}};

        size_t test_num = expect_cols.size();

        for (size_t i = 0; i < test_num; ++i)
        {
            request = buildDAGRequest(table_name, order_by_items[i], 100);
            executeAndAssertColumnsEqual(request, expect_cols[i]);
        }
    }
}
CATCH

TEST_F(TopNExecutorTestRunner, TopNFunction)
try
{
    std::shared_ptr<tipb::DAGRequest> request;
    std::vector<ColumnsWithTypeAndName> expect_cols;
    MockColumnNameVec output_projection{col_name[0], col_name[1], col_name[2], col_name[3]};
    MockAstVec func_projection; // Do function operation for topn
    MockOrderByItemVec order_by_items;
    ASTPtr col0_ast = col(col_name[0]);
    ASTPtr col1_ast = col(col_name[1]);
    ASTPtr col2_ast = col(col_name[2]);
    ASTPtr col3_ast = col(col_name[3]);
    ASTPtr func_ast;

    {
        /// "and" function
        expect_cols = {{toNullableVec<Int32>(col_name[0], ColumnWithNullableInt32{{}, {}, 32, 27, 36, 34}),
                        toNullableVec<String>(col_name[1], ColumnWithNullableString{"female", "male", "male", "female", "female", "male"}),
                        toNullableVec<String>(col_name[2], ColumnWithNullableString{"korea", "china", "usa", "usa", "china", "china"}),
                        toNullableVec<Int32>(col_name[3], ColumnWithNullableInt32{1300, {}, {}, 0, 900, -300})}};

        {
            /// select * from clerk order by age and salary ASC limit 100;
            order_by_items = {MockOrderByItem("and(age, salary)", false)};
            func_ast = And(col(col_name[0]), col(col_name[3]));
            func_projection = {col0_ast, col1_ast, col2_ast, col3_ast, func_ast};

            request = buildDAGRequest(table_name, order_by_items, 100, func_projection, output_projection);
            executeAndAssertColumnsEqual(request, expect_cols.back());
        }
    }

    {
        /// "equal" function
        expect_cols = {{toNullableVec<Int32>(col_name[0], ColumnWithNullableInt32{27, 36, 34, 32, {}, {}}),
                        toNullableVec<String>(col_name[1], ColumnWithNullableString{"female", "female", "male", "male", "female", "male"}),
                        toNullableVec<String>(col_name[2], ColumnWithNullableString{"usa", "china", "china", "usa", "korea", "china"}),
                        toNullableVec<Int32>(col_name[3], ColumnWithNullableInt32{0, 900, -300, {}, 1300, {}})}};

        {
            /// select age, salary from clerk order by age = salary DESC limit 100;
            order_by_items = {MockOrderByItem("equals(age, salary)", true)};
            func_ast = eq(col(col_name[0]), col(col_name[3]));
            func_projection = {col0_ast, col1_ast, col2_ast, col3_ast, func_ast};

            request = buildDAGRequest(table_name, order_by_items, 100, func_projection, output_projection);
            executeAndAssertColumnsEqual(request, expect_cols.back());
        }
    }

    {
        /// "greater" function
        expect_cols = {{toNullableVec<Int32>(col_name[0], ColumnWithNullableInt32{{}, 32, {}, 36, 27, 34}),
                        toNullableVec<String>(col_name[1], ColumnWithNullableString{"female", "male", "male", "female", "female", "male"}),
                        toNullableVec<String>(col_name[2], ColumnWithNullableString{"korea", "usa", "china", "china", "usa", "china"}),
                        toNullableVec<Int32>(col_name[3], ColumnWithNullableInt32{1300, {}, {}, 900, 0, -300})}};

        {
            /// select age, gender, country, salary from clerk order by age > salary ASC limit 100;
            order_by_items = {MockOrderByItem("greater(age, salary)", false)};
            func_ast = gt(col(col_name[0]), col(col_name[3]));
            func_projection = {col0_ast, col1_ast, col2_ast, col3_ast, func_ast};

            request = buildDAGRequest(table_name, order_by_items, 100, func_projection, output_projection);
            executeAndAssertColumnsEqual(request, expect_cols.back());
        }
    }

    /// TODO more functions...
}
CATCH

TEST_F(TopNExecutorTestRunner, BigTable)
try
{
    std::vector<String> tables{"big_table_1", "big_table_2", "big_table_3"};
    for (const auto & table : tables)
    {
        std::vector<size_t> limits{0, 1, 10, 20, 199, 200, 300};
        for (auto limit_num : limits)
        {
            auto request = context
                               .scan("test_db", table)
                               .topN("key", false, limit_num)
                               .build(context);
            auto expect = executeStreams(request, 1);
            executeAndAssertColumnsEqual(request, expect);
        }
    }
}
CATCH

} // namespace tests
} // namespace DB
