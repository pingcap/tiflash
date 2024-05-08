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

#include <Flash/Statistics/traverseExecutors.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/mockExecutor.h>

#include <functional>
#include <queue>
#include <set>

namespace DB
{
namespace tests
{

/// Note: These tests are for the correctness of the test framework
class ExecutorCollation : public DB::tests::ExecutorTest
{
public:
    using ColStringNullableType = std::optional<typename TypeTraits<String>::FieldType>;
    using ColStringType = std::optional<typename TypeTraits<String>::FieldType>;
    using ColUInt64Type = typename TypeTraits<UInt64>::FieldType;

    using ColumnWithNullableString = std::vector<ColStringNullableType>;
    using ColumnWithString = std::vector<ColStringType>;
    using ColumnWithUInt64 = std::vector<ColUInt64Type>;

    void initializeContext() override
    {
        ExecutorTest::initializeContext();

        context.addMockTable(
            {db_name, table_name},
            {{col_name, TiDB::TP::TypeString}},
            {toNullableVec<String>(col_name, col)});

        context.addMockTable(
            {db_name, chinese_table},
            {{chinese_col_name, TiDB::TP::TypeString}},
            {toNullableVec<String>(chinese_col_name, chinese_col)});

        context.addMockTable(
            join_table,
            "t1",
            {{"a", TiDB::TP::TypeLong}, {"b", TiDB::TP::TypeLong}},
            {toVec<Int32>("a", {1, 1, 3, 4}), toVec<Int32>("b", {1, 1, 4, 1})});

        context.addMockTable(
            join_table,
            "t2",
            {{"a", TiDB::TP::TypeLong}, {"b", TiDB::TP::TypeLong}},
            {toVec<Int32>("a", {1, 4, 2}), toVec<Int32>("b", {2, 6, 2})});

        /// For topn
        context.addMockTable(
            {db_name, topn_table},
            {{topn_col, TiDB::TP::TypeString}},
            {toNullableVec<String>(
                topn_col,
                ColumnWithString{"col0-0", "col0-1", "col0-2", {}, "col0-4", {}, "col0-6", "col0-7"})});

        /// For projection
        context.addMockTable(
            {db_name, proj_table},
            {{proj_col[0], TiDB::TP::TypeString}, {proj_col[1], TiDB::TP::TypeString}},
            {toNullableVec<String>(proj_col[0], ColumnWithString{"col0-0", "col0-1", "", "col0-2", {}, "col0-3", ""}),
             toNullableVec<String>(proj_col[1], ColumnWithString{"", "col1-1", "", "col1-0", {}, "col1-3", "col1-2"})});

        /// For limit
        context.addMockTable(
            {db_name, limit_table},
            {{limit_col, TiDB::TP::TypeString}},
            {toNullableVec<String>(
                limit_col,
                ColumnWithString{"col0-0", {}, "col0-2", "col0-3", {}, "col0-5", "col0-6", "col0-7"})});

        /// For ExchangeSender
        context.addExchangeReceiver(
            sender_name,
            {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}, {"s3", TiDB::TP::TypeString}});
    }

    void setAndCheck(
        const String & table_name,
        const String & col_name,
        Int32 collation,
        const ColumnsWithTypeAndName & expect)
    {
        context.setCollation(collation);
        auto request = context.scan(db_name, table_name)
                           .aggregation(MockAstVec{}, {col(col_name)})
                           .project({col_name})
                           .build(context);
        ASSERT_COLUMNS_EQ_UR(expect, executeStreams(request));
    }

    std::queue<tipb::ExecType> checkExecutorCollation(std::shared_ptr<tipb::DAGRequest> dag_request) const;
    void checkScalarFunctionCollation(std::shared_ptr<tipb::DAGRequest> dag_request) const;
    void addExpr(std::queue<const tipb::Expr *> & exprs, const tipb::Expr * const expr) const;

    /// Prepare some names
    const String db_name{"test_db"};
    const String table_name{"collation_table"};
    const String col_name{"col"};
    const ColumnWithNullableString
        col{"china", "china", "china  ", "CHINA", "cHiNa ", "usa", "usa", "usa  ", "USA", "USA "};

    const String chinese_table{"chinese"};
    const String chinese_col_name{"col"};
    const ColumnWithNullableString chinese_col{
        "北京",
        "北京  ",
        "北bei京",
        "北Bei京",
        "北bei京  ",
        "上海",
        "上海  ",
        "shanghai  ",
        "ShangHai",
        "ShangHai  "};

    const String join_table{"join_table"};
    const String topn_table{"topn_table"};
    const String topn_col{"topn_col"};
    const String proj_table{"proj_table"};
    const std::vector<String> proj_col{"proj_col0", "proj_col1"};
    const String limit_table{"limit_table"};
    const String limit_col{"limit_col"};
    const String sender_name{"sender"};

    /// scalar functions whose collation must be set(Some more scalar functions may be added in the future)
    std::set<int> scalar_func_need_collation{
        tipb::ScalarFuncSig::EQInt,
        tipb::ScalarFuncSig::NEInt,
        tipb::ScalarFuncSig::GTInt,
        tipb::ScalarFuncSig::LTInt};
};

/// Collect scalar functions
void ExecutorCollation::addExpr(std::queue<const tipb::Expr *> & exprs, const tipb::Expr * const expr) const
{
    if (expr->tp() == tipb::ExprType::ScalarFunc) /// only add scalar function
        exprs.push(expr);
    int children_size = expr->children_size();

    /// recursively add expression
    for (int i = 0; i < children_size; ++i)
        addExpr(exprs, &(expr->children(i)));
}

std::queue<tipb::ExecType> ExecutorCollation::checkExecutorCollation(
    std::shared_ptr<tipb::DAGRequest> dag_request) const
{
    std::queue<tipb::ExecType> exec_collation_absent;

    auto checkExecutor = [&](const tipb::Executor & executor) -> bool {
#define CHECK(probe_type, exec_type)               \
    do                                             \
    {                                              \
        if (probe_type.collate() == 0)             \
        {                                          \
            exec_collation_absent.push(exec_type); \
            return true;                           \
        }                                          \
    } while (0);
        tipb::ExecType type = executor.tp();

        switch (type)
        {
        case tipb::ExecType::TypeJoin: /// need collation
        {
            const tipb::Join & join = executor.join();
            int probe_type_size = join.probe_types_size();
            int build_type_size = join.build_types_size();

            for (int i = 0; i < probe_type_size; ++i)
                CHECK(join.probe_types(i), tipb::ExecType::TypeJoin);

            for (int i = 0; i < build_type_size; ++i)
                CHECK(join.build_types(i), tipb::ExecType::TypeJoin);

            break;
        }
        case tipb::ExecType::TypeExchangeReceiver: /// need collation
        {
            const tipb::ExchangeReceiver & exchange_receiver = executor.exchange_receiver();
            int field_types_size = exchange_receiver.field_types_size();

            for (int i = 0; i < field_types_size; ++i)
                CHECK(exchange_receiver.field_types(i), tipb::ExecType::TypeExchangeReceiver);

            break;
        }
        case tipb::ExecType::TypeExchangeSender: /// need collation
        {
            const tipb::ExchangeSender & exchange_sender = executor.exchange_sender();
            int types_size = exchange_sender.types_size();
            int all_field_types_size = exchange_sender.all_field_types_size();

            for (int i = 0; i < types_size; ++i)
                CHECK(exchange_sender.types(i), tipb::ExecType::TypeExchangeSender);

            for (int i = 0; i < all_field_types_size; ++i)
                CHECK(exchange_sender.all_field_types(i), tipb::ExecType::TypeExchangeSender);

            break;
        }
        case tipb::ExecType::TypeSelection:
            break; /// Do nothing
        case tipb::ExecType::TypeAggregation:
            break; /// Do nothing
        case tipb::ExecType::TypeTopN:
            break; /// Do nothing
        case tipb::ExecType::TypeLimit:
            break; /// Do nothing
        case tipb::ExecType::TypeProjection:
            break; /// Do nothing
        case tipb::ExecType::TypeWindow:
            break; /// Do nothing
        case tipb::ExecType::TypeTableScan:
            break; /// Do nothing
        default:
        {
            auto exception_str = fmt::format("Unhandled executor {}", fmt::underlying(type));
            throw Exception(exception_str);
        }
        }

        return true; /// Alawys traverse the executors
    };

    traverseExecutors(dag_request.get(), checkExecutor);
    return exec_collation_absent;
}

void ExecutorCollation::checkScalarFunctionCollation(std::shared_ptr<tipb::DAGRequest> dag_request) const
{
    std::queue<const tipb::Expr *> exprs;

    using MultiExprs = ::google::protobuf::RepeatedPtrField<tipb::Expr>;
    auto add_multi_exprs = [&](const MultiExprs & field, int size) {
        for (int i = 0; i < size; ++i)
            addExpr(exprs, &(field.Get(i)));
    };

    auto collectExprs = [&](const tipb::Executor & executor) -> bool {
        tipb::ExecType type = executor.tp();

        switch (type)
        {
        case tipb::ExecType::TypeJoin: /// need collation
        {
            const tipb::Join & join = executor.join();

            add_multi_exprs(join.left_join_keys(), join.left_join_keys_size());
            add_multi_exprs(join.right_join_keys(), join.right_join_keys_size());
            add_multi_exprs(join.left_conditions(), join.left_conditions_size());
            add_multi_exprs(join.right_conditions(), join.right_conditions_size());
            add_multi_exprs(join.other_conditions(), join.other_conditions_size());
            add_multi_exprs(join.other_eq_conditions_from_in(), join.other_eq_conditions_from_in_size());
            break;
        }
        case tipb::ExecType::TypeExchangeReceiver: /// need collation
            break; /// Do nothing
        case tipb::ExecType::TypeExchangeSender: /// need collation
        {
            const tipb::ExchangeSender & exchange_sender = executor.exchange_sender();
            add_multi_exprs(exchange_sender.partition_keys(), exchange_sender.partition_keys_size());
            break;
        }
        case tipb::ExecType::TypeSelection:
        {
            const tipb::Selection & selection = executor.selection();
            add_multi_exprs(selection.conditions(), selection.conditions_size());
            break;
        }
        case tipb::ExecType::TypeAggregation:
        {
            const tipb::Aggregation & aggregation = executor.aggregation();
            add_multi_exprs(aggregation.group_by(), aggregation.group_by_size());
            add_multi_exprs(aggregation.agg_func(), aggregation.agg_func_size());
            break;
        }
        case tipb::ExecType::TypeTopN:
            break; /// Do nothing
        case tipb::ExecType::TypeLimit:
            break; /// Do nothing
        case tipb::ExecType::TypeProjection:
        {
            const tipb::Projection & projection = executor.projection();
            add_multi_exprs(projection.exprs(), projection.exprs_size());
            break;
        }
        case tipb::ExecType::TypeWindow:
        {
            const tipb::Window & window = executor.window();
            add_multi_exprs(window.func_desc(), window.func_desc_size());
            break;
        }
        case tipb::ExecType::TypeTableScan:
            break; /// Do nothing
        default:
        {
            auto exception_str = fmt::format("Unhandled executor {}", fmt::underlying(type));
            throw Exception(exception_str);
        }
        }
        return true; /// Alawys traverse the executors
    };

    /// Firstly, collect scalar functions
    traverseExecutors(dag_request.get(), collectExprs);

    /// Secondly, check collation of scalar functions
    while (!exprs.empty())
    {
        const tipb::Expr * expr = exprs.back();
        exprs.pop();

        /// We only guarantee the collations of scalar functions that have been add into "scalar_func_need_collation" to be set
        auto iter = scalar_func_need_collation.find(expr->sig());
        if (iter == scalar_func_need_collation.end())
            continue; /// Ignore this scalar function

        /// Check
        ASSERT_NE(expr->field_type().collate(), 0);
    }
}

/// Guarantee that test framework has correctly supported the collation.
TEST_F(ExecutorCollation, Verification)
try
{
    /// Test utf8mb4_bin
    setAndCheck(
        table_name,
        col_name,
        TiDB::ITiDBCollator::UTF8MB4_BIN,
        ColumnsWithTypeAndName{
            toNullableVec<String>(col_name, ColumnWithNullableString{"usa", "CHINA", "USA", "china", "cHiNa "})});
    setAndCheck(
        chinese_table,
        chinese_col_name,
        TiDB::ITiDBCollator::UTF8MB4_BIN,
        ColumnsWithTypeAndName{toNullableVec<String>(
            chinese_col_name,
            ColumnWithNullableString{"ShangHai", "北京", "北Bei京", "shanghai  ", "北bei京", "上海"})});

    /// Test utf8mb4_general_ci
    setAndCheck(
        table_name,
        col_name,
        TiDB::ITiDBCollator::UTF8_GENERAL_CI,
        ColumnsWithTypeAndName{toNullableVec<String>(col_name, ColumnWithNullableString{"usa", "china"})});
    setAndCheck(
        chinese_table,
        chinese_col_name,
        TiDB::ITiDBCollator::UTF8_GENERAL_CI,
        ColumnsWithTypeAndName{toNullableVec<String>(
            chinese_col_name,
            ColumnWithNullableString{"北京", "shanghai  ", "北bei京", "上海"})});

    /// Test utf8_bin
    setAndCheck(
        table_name,
        col_name,
        TiDB::ITiDBCollator::UTF8_BIN,
        ColumnsWithTypeAndName{
            toNullableVec<String>(col_name, ColumnWithNullableString{"USA", "CHINA", "usa", "china", "cHiNa "})});

    /// Test utf8_unicode_CI
    setAndCheck(
        table_name,
        col_name,
        TiDB::ITiDBCollator::UTF8_UNICODE_CI,
        ColumnsWithTypeAndName{toNullableVec<String>(col_name, ColumnWithNullableString{"china", "usa"})});
}
CATCH

/// Guarantee the collations of executors or functions have been set
TEST_F(ExecutorCollation, CheckCollation)
try
{
    {
        /// Check collation for executors
        auto request = context.scan(join_table, "t1")
                           .join(context.scan(join_table, "t2"), tipb::JoinType::TypeInnerJoin, {col("a")})
                           .aggregation({Max(col("a")), Min(col("a")), Count(col("a"))}, {col("b")})
                           .build(context);
        ASSERT_EQ(checkExecutorCollation(request).size(), 0);

        request = context.scan(db_name, topn_table).topN(topn_col, true, 100).build(context);
        ASSERT_EQ(checkExecutorCollation(request).size(), 0);

        request = context.scan(db_name, proj_table).project(MockAstVec{col(proj_col[0])}).build(context);
        ASSERT_EQ(checkExecutorCollation(request).size(), 0);

        request = context.scan(db_name, limit_table).limit(100).build(context);
        ASSERT_EQ(checkExecutorCollation(request).size(), 0);

        request
            = context.receive(sender_name).project({"s1", "s2", "s3"}).exchangeSender(tipb::Broadcast).build(context);
        ASSERT_EQ(checkExecutorCollation(request).size(), 0);

        /// TODO some collation fields may not be set for some executors, set them and test it!
    }

    {
        /// Check collation for expressions
        auto request
            = context.scan(db_name, proj_table)
                  .project(MockAstVec{eq(col(proj_col[0]), col(proj_col[0])), gt(col(proj_col[0]), col(proj_col[1]))})
                  .build(context);
        checkScalarFunctionCollation(request);

        /// TODO test more scalar functions...
    }
}
CATCH

} // namespace tests
} // namespace DB
