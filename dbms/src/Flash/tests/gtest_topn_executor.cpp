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

#include <Interpreters/Context.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTOrderByElement.h>
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

        context.addMockTable(
            {db_name, table_single_name},
            {{single_col_name, TiDB::TP::TypeString}},
            {toNullableVec<String>(single_col_name, col0)});

        context.addMockTable(
            {db_name, table_name},
            {{col_name[0], TiDB::TP::TypeLong},
             {col_name[1], TiDB::TP::TypeString},
             {col_name[2], TiDB::TP::TypeString},
             {col_name[3], TiDB::TP::TypeLong}},
            {toNullableVec<Int32>(col_name[0], col_age),
             toNullableVec<String>(col_name[1], col_gender),
             toNullableVec<String>(col_name[2], col_country),
             toNullableVec<Int32>(col_name[3], col_salary)});
        context.addMockTable(
            {db_name, empty_name},
            {{col_name[0], TiDB::TP::TypeLong},
             {col_name[1], TiDB::TP::TypeString},
             {col_name[2], TiDB::TP::TypeString},
             {col_name[3], TiDB::TP::TypeLong}},
            {toNullableVec<Int32>(col_name[0], {}),
             toNullableVec<String>(col_name[1], {}),
             toNullableVec<String>(col_name[2], {}),
             toNullableVec<Int32>(col_name[3], {})});

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

    std::shared_ptr<tipb::DAGRequest> buildDAGRequest(
        const String & table_name,
        const String & col_name,
        bool is_desc,
        int limit_num)
    {
        return context.scan(db_name, table_name).topN(col_name, is_desc, limit_num).build(context);
    }

    std::shared_ptr<tipb::DAGRequest> buildDAGRequest(
        const String & table_name,
        MockOrderByItemVec order_by_items,
        int limit,
        MockAstVec func_proj_ast = {},
        MockAstVec out_proj_ast = {})
    {
        if (func_proj_ast.empty())
            return context.scan(db_name, table_name).topN(order_by_items, limit).build(context);
        else
            return context.scan(db_name, table_name)
                .project(func_proj_ast)
                .topN(order_by_items, limit)
                .project(out_proj_ast)
                .build(context);
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

    // empty table
    const String empty_name{"empty_table"};
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
            bool is_desc = static_cast<bool>(i); /// Set descent or ascent
            for (size_t limit_num = 0; limit_num <= col_data_num + 5; ++limit_num)
            {
                request = buildDAGRequest(table_single_name, single_col_name, is_desc, limit_num);
                SortInfos sort_infos{{0, is_desc}};
                executeAndAssertSortedBlocks(request, sort_infos);
            }
        }
    }

    {
        /// Test multi-columns
        std::vector<MockOrderByItemVec> order_by_items{
            /// select * from clerk order by age DESC, gender DESC;
            {MockOrderByItem(col_name[0], true), MockOrderByItem(col_name[1], true)},
            /// select * from clerk order by gender DESC, salary ASC;
            {MockOrderByItem(col_name[1], true), MockOrderByItem(col_name[3], false)},
            /// select * from clerk order by gender DESC, country ASC, salary DESC;
            {MockOrderByItem(col_name[1], true),
             MockOrderByItem(col_name[2], false),
             MockOrderByItem(col_name[3], true)}};

        std::vector<SortInfos> infos{
            {{0, true}, {1, true}},
            {{1, true}, {3, false}},
            {{1, true}, {2, false}, {3, true}}};

        for (size_t i = 0; i < order_by_items.size(); ++i)
        {
            request = buildDAGRequest(table_name, order_by_items[i], 100);
            executeAndAssertSortedBlocks(request, infos[i]);
        }
    }
}
CATCH

TEST_F(TopNExecutorTestRunner, TopNFunction)
try
{
    std::shared_ptr<tipb::DAGRequest> request;
    std::vector<MockAstVec> output_projections{
        {col(col_name[0]), col(col_name[1]), col(col_name[2]), col(col_name[3]), And(col("age"), col("salary"))},
        {col(col_name[0]), col(col_name[1]), col(col_name[2]), col(col_name[3]), eq(col("age"), col("salary"))},
        {col(col_name[0]), col(col_name[1]), col(col_name[2]), col(col_name[3]), gt(col("age"), col("salary"))}};

    MockAstVec func_projection; // Do function operation for topn
    MockOrderByItemVec order_by_items;
    ASTPtr col0_ast = col(col_name[0]);
    ASTPtr col1_ast = col(col_name[1]);
    ASTPtr col2_ast = col(col_name[2]);
    ASTPtr col3_ast = col(col_name[3]);
    ASTPtr func_ast;

    /// "and" function
    {
        /// select * from clerk order by age and salary ASC limit 100;
        order_by_items = {MockOrderByItem("and(age, salary)", false)};
        func_ast = And(col(col_name[0]), col(col_name[3]));
        func_projection = {col0_ast, col1_ast, col2_ast, col3_ast, func_ast};
        request = buildDAGRequest(table_name, order_by_items, 100, func_projection, output_projections[0]);
        SortInfos sort_infos{{4, false}};
        executeAndAssertSortedBlocks(request, sort_infos);
    }


    /// "equal" function
    {
        /// select age, salary from clerk order by age = salary DESC limit 100;
        order_by_items = {MockOrderByItem("equals(age, salary)", true)};
        func_ast = eq(col(col_name[0]), col(col_name[3]));
        func_projection = {col0_ast, col1_ast, col2_ast, col3_ast, func_ast};

        request = buildDAGRequest(table_name, order_by_items, 100, func_projection, output_projections[1]);
        SortInfos sort_infos{{4, true}};
        executeAndAssertSortedBlocks(request, sort_infos);
    }

    {
        /// "greater" function
        {
            /// select age, gender, country, salary from clerk order by age > salary ASC limit 100;
            order_by_items = {MockOrderByItem("greater(age, salary)", false)};
            func_ast = gt(col(col_name[0]), col(col_name[3]));
            func_projection = {col0_ast, col1_ast, col2_ast, col3_ast, func_ast};

            request = buildDAGRequest(table_name, order_by_items, 100, func_projection, output_projections[2]);
            SortInfos sort_infos{{4, false}};
            executeAndAssertSortedBlocks(request, sort_infos);
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
            auto request = context.scan(db_name, table).topN("key", false, limit_num).build(context);
            SortInfos sort_infos{{0, false}};
            executeAndAssertSortedBlocks(request, sort_infos);
        }
    }
}
CATCH

TEST_F(TopNExecutorTestRunner, Empty)
try
{
    for (size_t i = 0; i < col_name.size(); ++i)
    {
        auto request = context.scan(db_name, empty_name).topN(col_name[i], false, 100).build(context);
        SortInfos sort_infos{{i, false}};
        executeAndAssertSortedBlocks(request, sort_infos);
    }
}
CATCH

TEST_F(TopNExecutorTestRunner, SortByConst)
try
{
    // case1: order by key, 1 limit 50
    auto order_by_items = std::make_shared<ASTExpressionList>();
    {
        ASTPtr locale_node;
        auto order_by_item = std::make_shared<ASTOrderByElement>(-1, -1, false, locale_node);
        order_by_item->children.push_back(std::make_shared<ASTIdentifier>("key"));
        order_by_items->children.push_back(order_by_item);
    }
    {
        ASTPtr locale_node;
        auto order_by_item = std::make_shared<ASTOrderByElement>(-1, -1, false, locale_node);
        order_by_item->children.push_back(lit(Field(static_cast<UInt64>(1))));
        order_by_items->children.push_back(order_by_item);
    }
    auto request = context.scan("test_db", "big_table_2")
                       .topN(order_by_items, lit(Field(static_cast<UInt64>(50))))
                       .build(context);
    SortInfos sort_infos{{0, true}};
    executeAndAssertSortedBlocks(request, sort_infos);

    // case2: order by 1 limit 10
    order_by_items = std::make_shared<ASTExpressionList>();
    {
        ASTPtr locale_node;
        auto order_by_item = std::make_shared<ASTOrderByElement>(-1, -1, false, locale_node);
        order_by_item->children.push_back(lit(Field(static_cast<UInt64>(1))));
        order_by_items->children.push_back(order_by_item);
    }
    request = context.scan("test_db", "big_table_2")
                  .topN(order_by_items, lit(Field(static_cast<UInt64>(10))))
                  .build(context);
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(35)));
    std::vector<size_t> concurrencies{1, 5, 10};
    for (auto concurrency : concurrencies)
    {
        size_t total_rows = 0;
        auto result = getExecuteStreamsReturnBlocks(request, concurrency);
        for (const auto & block : result)
        {
            // Every block is cut by the limit of topn.
            ASSERT_TRUE(block.rows() <= 10);
            total_rows += block.rows();
        }
        // The total number of rows must >= the limit of topn, because the table rows > the limit of topn.
        ASSERT_TRUE(total_rows >= 10);
    }
}
CATCH

} // namespace tests
} // namespace DB
