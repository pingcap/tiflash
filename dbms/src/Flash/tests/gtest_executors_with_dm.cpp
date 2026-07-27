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

#include <Common/MyDuration.h>
#include <Common/MyTime.h>
#include <Debug/MockExecutor/AstToPB.h>
#include <Debug/MockStorage.h>
#include <Flash/Statistics/ExecutorStatisticsCollector.h>
#include <Interpreters/Context.h>
#include <Operators/OperatorProfileInfo.h>
#include <Storages/KVStore/Types.h>
#include <Storages/MutableSupport.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/InputStreamTestUtils.h>
#include <TestUtils/mockExecutor.h>

namespace DB
{
namespace tests
{
class ExecutorsWithDMTestRunner : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
        context.mockStorage()->setUseDeltaMerge(true);
        context.context->getSettingsRef().dt_enable_read_thread = true;
        context.context->getSettingsRef().dt_segment_stable_pack_rows = 1;
        context.context->getSettingsRef().dt_segment_limit_rows = 1;
        context.context->getSettingsRef().dt_segment_delta_cache_limit_rows = 1;
        // note that
        // 1. the first column is pk.
        // 2. The decimal type is not supported.
        context.addMockDeltaMerge(
            {"test_db", "t0"},
            {{"col0", TiDB::TP::TypeLongLong}},
            {{toVec<Int64>("col0", {0, 1, 2, 3, 4, 5, 6, 7})}});

        context.addMockDeltaMerge(
            {"test_db", "t1"},
            {{"col0", TiDB::TP::TypeLongLong}, {"col1", TiDB::TP::TypeString}},
            {{toVec<Int64>("col0", {0, 1, 2, 3, 4, 5, 6, 7})},
             {toNullableVec<String>("col1", {"col1-0", "col1-1", "col1-2", {}, "col1-4", {}, "col1-6", "col1-7"})}});

        context.addMockDeltaMerge(
            {"test_db", "t2"},
            {{"col0", TiDB::TP::TypeLongLong},
             {"col1", TiDB::TP::TypeTiny},
             {"col2", TiDB::TP::TypeShort},
             {"col3", TiDB::TP::TypeLong},
             {"col4", TiDB::TP::TypeLongLong},
             {"col5", TiDB::TP::TypeFloat},
             {"col6", TiDB::TP::TypeDouble},
             {"col7", TiDB::TP::TypeDate},
             {"col8", TiDB::TP::TypeDatetime},
             {"col9", TiDB::TP::TypeString}},
            {toVec<Int64>("col0", col_id),
             toNullableVec<Int8>("col1", col_tinyint),
             toNullableVec<Int16>("col2", col_smallint),
             toNullableVec<Int32>("col3", col_int),
             toNullableVec<Int64>("col4", col_bigint),
             toNullableVec<Float32>("col5", col_float),
             toNullableVec<Float64>("col6", col_double),
             toNullableVec<MyDate>("col7", col_mydate),
             toNullableVec<MyDateTime>("col8", col_mydatetime),
             toNullableVec<String>("col9", col_string)});

        // with 200 rows.
        std::vector<TypeTraits<Int64>::FieldType> key(200);
        std::vector<std::optional<String>> value(200);
        for (size_t i = 0; i < 200; ++i)
        {
            key[i] = i % 15;
            value[i] = {fmt::format("val_{}", i)};
        }
        context.addMockDeltaMerge(
            {"test_db", "big_table"},
            {{"key", TiDB::TP::TypeLongLong}, {"value", TiDB::TP::TypeString}},
            {toVec<Int64>("key", key), toNullableVec<String>("value", value)});

        context.addMockDeltaMerge(
            {"test_db", "empty_table"},
            {{"col0", TiDB::TP::TypeLongLong}},
            {toVec<Int32>("col0", {})});

        MockColumnInfoVec multi_stage_lm_columns;
        ColumnsWithTypeAndName multi_stage_lm_data;
        constexpr size_t multi_stage_lm_rows = 32;
        for (size_t col_id = 0; col_id < 15; ++col_id)
        {
            const auto name = fmt::format("c{}", col_id);
            multi_stage_lm_columns.push_back({name, TiDB::TP::TypeLongLong});

            std::vector<Int64> values;
            values.reserve(multi_stage_lm_rows);
            for (size_t row = 0; row < multi_stage_lm_rows; ++row)
            {
                if (col_id == 0)
                    values.push_back(row);
                else if (col_id == 1)
                    values.push_back(row % 8);
                else
                    values.push_back(static_cast<Int64>(col_id * 1000 + row));
            }
            multi_stage_lm_data.emplace_back(toVec<Int64>(name, values));
        }
        context.addMockDeltaMerge(
            {"test_db", "multi_stage_lm"},
            multi_stage_lm_columns,
            multi_stage_lm_data,
            /*concurrency_hint=*/4);
    }

    ColumnWithInt64 col_id{1, 2, 3, 4, 5, 6, 7, 8, 9};
    ColumnWithNullableInt8 col_tinyint{1, 2, 3, {}, {}, 0, 0, -1, -2};
    ColumnWithNullableInt16 col_smallint{2, 3, {}, {}, 0, -1, -2, 4, 0};
    ColumnWithNullableInt32 col_int{4, {}, {}, 0, 123, -1, -1, 123, 4};
    ColumnWithNullableInt64 col_bigint{2, 2, {}, 0, -1, {}, -1, 0, 123};
    ColumnWithNullableFloat32 col_float{3.3, {}, 0, 4.0, 3.3, 5.6, -0.1, -0.1, {}};
    ColumnWithNullableFloat64 col_double{0.1, 0, 1.1, 1.1, 1.2, {}, {}, -1.2, -1.2};
    ColumnWithNullableMyDate col_mydate{1000000, 2000000, {}, 300000, 1000000, {}, 0, 2000000, {}};
    ColumnWithNullableMyDateTime col_mydatetime{2000000, 0, {}, 3000000, 1000000, {}, 0, 2000000, 1000000};
    ColumnWithNullableString col_string{{}, "pingcap", "PingCAP", {}, "PINGCAP", "PingCAP", {}, "Shanghai", "Shanghai"};
};

#define WRAP_FOR_DM_TEST_BEGIN                     \
    std::vector<bool> pipeline_bools{false, true}; \
    for (auto enable_pipeline : pipeline_bools)    \
    {                                              \
        enablePipeline(enable_pipeline);

#define WRAP_FOR_DM_TEST_END }

namespace
{
tipb::TableScan * findMutableTableScan(tipb::Executor * executor)
{
    switch (executor->tp())
    {
    case tipb::ExecType::TypeTableScan:
        return executor->mutable_tbl_scan();
    case tipb::ExecType::TypeSelection:
        return findMutableTableScan(executor->mutable_selection()->mutable_child());
    case tipb::ExecType::TypeProjection:
        return findMutableTableScan(executor->mutable_projection()->mutable_child());
    default:
        RUNTIME_CHECK_MSG(false, "Unexpected executor type {}", tipb::ExecType_Name(executor->tp()));
    }
}

tipb::Selection * findMutableSelection(tipb::Executor * executor)
{
    switch (executor->tp())
    {
    case tipb::ExecType::TypeSelection:
        return executor->mutable_selection();
    case tipb::ExecType::TypeProjection:
        return findMutableSelection(executor->mutable_projection()->mutable_child());
    default:
        RUNTIME_CHECK_MSG(false, "Unexpected executor type {}", tipb::ExecType_Name(executor->tp()));
    }
}

MockAstVec buildPkSpecialAndPayloadColumnProjection(const String & special_column_name)
{
    MockAstVec projections;
    projections.reserve(16);
    projections.push_back(col("pk"));
    projections.push_back(col(special_column_name));
    for (size_t col_id = 1; col_id <= 14; ++col_id)
        projections.push_back(col(fmt::format("c{}", col_id)));
    return projections;
}

template <typename Predicate>
std::vector<Int64> selectRows(size_t rows, Predicate predicate)
{
    std::vector<Int64> selected_rows;
    selected_rows.reserve(rows);
    for (size_t row = 0; row < rows; ++row)
    {
        if (predicate(row))
            selected_rows.push_back(static_cast<Int64>(row));
    }
    return selected_rows;
}

template <typename ValueGetter>
std::vector<std::optional<Int64>> buildNullableInt64Values(
    const std::vector<Int64> & selected_rows,
    ValueGetter value_getter)
{
    std::vector<std::optional<Int64>> values;
    values.reserve(selected_rows.size());
    for (auto row : selected_rows)
        values.push_back(value_getter(row));
    return values;
}

template <typename ValueGetter>
std::vector<Int64> buildInt64Values(const std::vector<Int64> & selected_rows, ValueGetter value_getter)
{
    std::vector<Int64> values;
    values.reserve(selected_rows.size());
    for (auto row : selected_rows)
        values.push_back(value_getter(row));
    return values;
}

std::vector<std::optional<Int64>> buildNullablePayloadValues(const std::vector<Int64> & selected_rows, size_t col_id)
{
    return buildNullableInt64Values(selected_rows, [col_id](Int64 row) {
        if (col_id == 0)
            return row;
        if (col_id == 1)
            return row % 8;
        return static_cast<Int64>(col_id * 1000 + row);
    });
}

std::vector<Int64> buildPayloadValues(const std::vector<Int64> & selected_rows, size_t col_id)
{
    return buildInt64Values(selected_rows, [col_id](Int64 row) {
        if (col_id == 0)
            return row;
        if (col_id == 1)
            return row % 8;
        return static_cast<Int64>(col_id * 1000 + row);
    });
}

template <typename ValueGetter>
std::vector<std::optional<typename TypeTraits<MyDateTime>::FieldType>> buildNullableMyDateTimeValues(
    const std::vector<Int64> & selected_rows,
    ValueGetter value_getter)
{
    std::vector<std::optional<typename TypeTraits<MyDateTime>::FieldType>> values;
    values.reserve(selected_rows.size());
    for (auto row : selected_rows)
        values.push_back(value_getter(row));
    return values;
}

ColumnsWithTypeAndName buildExpectedPayloadColumns(const std::vector<Int64> & selected_rows)
{
    ColumnsWithTypeAndName expected;
    expected.reserve(15);
    for (size_t col_id = 0; col_id < 15; ++col_id)
        expected.emplace_back(
            toNullableVec<Int64>(fmt::format("c{}", col_id), buildNullablePayloadValues(selected_rows, col_id)));
    return expected;
}

ColumnsWithTypeAndName buildExpectedPkSpecialPayloadColumnsForDAG(
    const std::vector<Int64> & selected_rows,
    ColumnWithTypeAndName special_column,
    const String & column_name_prefix = "")
{
    ColumnsWithTypeAndName expected;
    expected.reserve(16);
    expected.emplace_back(
        toVec<Int64>(column_name_prefix + "pk", buildInt64Values(selected_rows, [](Int64 row) { return row; })));
    expected.emplace_back(std::move(special_column));
    for (size_t col_id = 1; col_id <= 14; ++col_id)
        expected.emplace_back(
            toVec<Int64>(column_name_prefix + fmt::format("c{}", col_id), buildPayloadValues(selected_rows, col_id)));
    return expected;
}

ColumnWithTypeAndName buildExpectedTimeColumnForDAG(const std::vector<Int64> & selected_rows, const String & name)
{
    return toVec<MyDuration>(name, buildInt64Values(selected_rows, [](Int64 row) {
                                 return MyDuration(1, static_cast<Int32>(row % 8), 0, 0, 0, 0).nanoSecond();
                             }));
}

void assertColumnsEqual(const ColumnsWithTypeAndName & expected, const ColumnsWithTypeAndName & actual)
{
    ASSERT_TRUE(columnsEqual(expected, actual, /*_restrict=*/false))
        << "\n  expect_block: \n"
        << getColumnsContent(expected) << "\n actual_block: \n"
        << getColumnsContent(actual);
}

void rewriteTableScanColumnAsCommitTS(tipb::DAGRequest * request, Int32 column_index)
{
    auto * table_scan = findMutableTableScan(request->mutable_root_executor());
    RUNTIME_CHECK(column_index >= 0 && column_index < table_scan->columns_size());
    auto * column = table_scan->mutable_columns(column_index);
    column->set_column_id(ExtraCommitTSColumnID);
    column->set_tp(TiDB::TypeLongLong);
    column->set_flag(0);
    column->set_columnlen(20);
    column->set_decimal(0);
}

void rewriteTableScanColumnAsExtraTableID(tipb::DAGRequest * request, Int32 column_index)
{
    auto * table_scan = findMutableTableScan(request->mutable_root_executor());
    RUNTIME_CHECK(column_index >= 0 && column_index < table_scan->columns_size());
    auto * column = table_scan->mutable_columns(column_index);
    column->set_column_id(ExtraTableIDColumnID);
    column->set_tp(TiDB::TypeLongLong);
    column->set_flag(0);
    column->set_columnlen(20);
    column->set_decimal(0);
}

UInt64 getExecutionSummaryRows(const tipb::SelectResponse & response, const String & executor_id)
{
    for (const auto & summary : response.execution_summaries())
    {
        if (summary.executor_id() == executor_id)
            return summary.num_produced_rows();
    }
    RUNTIME_CHECK_MSG(false, "Execution summary for {} not found", executor_id);
    return 0;
}

std::shared_ptr<tipb::DAGRequest> buildDAGRequestWithPushedDownFilter(
    MockDAGRequestContext & context,
    const String & table_name,
    const ASTPtr & pushed_down_filter,
    const ASTPtr & residual_filter)
{
    auto pushed_down_filter_request = context.scan("test_db", table_name).filter(pushed_down_filter).build(context);
    RUNTIME_CHECK(pushed_down_filter_request->root_executor().tp() == tipb::ExecType::TypeSelection);
    RUNTIME_CHECK(pushed_down_filter_request->root_executor().selection().conditions_size() == 1);
    const auto & pushed_down_filter_expr = pushed_down_filter_request->root_executor().selection().conditions(0);

    auto request = context.scan("test_db", table_name).filter(residual_filter).build(context);
    *findMutableTableScan(request->mutable_root_executor())->add_pushed_down_filter_conditions()
        = pushed_down_filter_expr;
    return request;
}

std::shared_ptr<tipb::DAGRequest> buildDAGRequestWithPushedDownFilterAndProjection(
    MockDAGRequestContext & context,
    const String & table_name,
    const ASTPtr & pushed_down_filter,
    const ASTPtr & residual_filter,
    const MockAstVec & projections)
{
    auto pushed_down_filter_request = context.scan("test_db", table_name).filter(pushed_down_filter).build(context);
    RUNTIME_CHECK(pushed_down_filter_request->root_executor().tp() == tipb::ExecType::TypeSelection);
    RUNTIME_CHECK(pushed_down_filter_request->root_executor().selection().conditions_size() == 1);
    const auto & pushed_down_filter_expr = pushed_down_filter_request->root_executor().selection().conditions(0);

    auto request = context.scan("test_db", table_name).filter(residual_filter).project(projections).build(context);
    *findMutableTableScan(request->mutable_root_executor())->add_pushed_down_filter_conditions()
        = pushed_down_filter_expr;
    return request;
}

void rewriteComparisonRightLiteral(
    tipb::Expr * condition,
    tipb::ScalarFuncSig sig,
    const TiDB::ColumnInfo & literal_type,
    const Field & value,
    Int32 collator_id)
{
    RUNTIME_CHECK(condition->tp() == tipb::ExprType::ScalarFunc);
    RUNTIME_CHECK(condition->children_size() == 2);
    condition->set_sig(sig);
    literalFieldToTiPBExpr(literal_type, value, condition->mutable_children(1), collator_id);
}

void rewriteOverlappedExtraCastFilterLiterals(
    tipb::DAGRequest * request,
    const TiDB::ColumnInfo & literal_type,
    const Field & pushed_down_value,
    const Field & residual_value,
    tipb::ScalarFuncSig pushed_down_sig,
    tipb::ScalarFuncSig residual_sig,
    Int32 collator_id)
{
    auto * table_scan = findMutableTableScan(request->mutable_root_executor());
    RUNTIME_CHECK(table_scan->pushed_down_filter_conditions_size() == 1);
    rewriteComparisonRightLiteral(
        table_scan->mutable_pushed_down_filter_conditions(0),
        pushed_down_sig,
        literal_type,
        pushed_down_value,
        collator_id);

    auto * selection = findMutableSelection(request->mutable_root_executor());
    RUNTIME_CHECK(selection->conditions_size() == 1);
    rewriteComparisonRightLiteral(
        selection->mutable_conditions(0),
        residual_sig,
        literal_type,
        residual_value,
        collator_id);
}

void assertMultiStageRowsOverride(DAGContext & dag_context, UInt64 expected_selection_rows)
{
    auto table_scan_rows = dag_context.getExecutorRowsOverride("table_scan_0");
    auto selection_rows = dag_context.getExecutorRowsOverride("selection_1");
    ASSERT_NE(table_scan_rows, nullptr);
    ASSERT_NE(selection_rows, nullptr);
    ASSERT_EQ(selection_rows->load(), expected_selection_rows);
    ASSERT_GE(table_scan_rows->load(), selection_rows->load());
}
} // namespace

TEST_F(ExecutorsWithDMTestRunner, Basic)
try
{
    std::vector<bool> keep_order_opt{false, true};

    WRAP_FOR_DM_TEST_BEGIN
    for (auto keep_order : keep_order_opt)
    {
        auto request = context.scan("test_db", "t0", keep_order).build(context);
        executeAndAssertColumnsEqual(request, {{toNullableVec<Int64>("col0", {0, 1, 2, 3, 4, 5, 6, 7})}});

        request = context.scan("test_db", "t1", keep_order).build(context);
        executeAndAssertColumnsEqual(
            request,
            {{toNullableVec<Int64>("col0", {0, 1, 2, 3, 4, 5, 6, 7})},
             {toNullableVec<String>("col1", {"col1-0", "col1-1", "col1-2", {}, "col1-4", {}, "col1-6", "col1-7"})}});

        request = context.scan("test_db", "t2", keep_order).build(context);

        executeAndAssertColumnsEqual(
            request,
            {toNullableVec<Int64>({1, 2, 3, 4, 5, 6, 7, 8, 9}),
             toNullableVec<Int8>(col_tinyint),
             toNullableVec<Int16>(col_smallint),
             toNullableVec<Int32>(col_int),
             toNullableVec<Int64>(col_bigint),
             toNullableVec<Float32>(col_float),
             toNullableVec<Float64>(col_double),
             toNullableVec<MyDate>(col_mydate),
             toNullableVec<MyDateTime>(col_mydatetime),
             toNullableVec<String>(col_string)});

        request = context.scan("test_db", "big_table", keep_order).build(context);
        auto expect = executeStreams(request, 1);

        executeAndAssertColumnsEqual(request, expect);

        request = context.scan("test_db", "empty_table", keep_order).build(context);
        executeAndAssertColumnsEqual(request, {});

        // projection
        request = context.scan("test_db", "t1", keep_order).project({col("col0")}).build(context);
        executeAndAssertColumnsEqual(request, {{toNullableVec<Int64>("col0", {0, 1, 2, 3, 4, 5, 6, 7})}});

        request = context.scan("test_db", "t1", keep_order).project({col("col1")}).build(context);
        executeAndAssertColumnsEqual(
            request,
            {{toNullableVec<String>("col1", {"col1-0", "col1-1", "col1-2", {}, "col1-4", {}, "col1-6", "col1-7"})}});

        // filter
        request = context.scan("test_db", "t0", keep_order)
                      .filter(lt(col("col0"), lit(Field(static_cast<Int64>(4)))))
                      .build(context);
        executeAndAssertColumnsEqual(request, {{toNullableVec<Int64>("col0", {0, 1, 2, 3})}});

        request = context.scan("test_db", "t1", keep_order)
                      .filter(lt(col("col0"), lit(Field(static_cast<Int64>(4)))))
                      .build(context);
        executeAndAssertColumnsEqual(
            request,
            {{toNullableVec<Int64>("col0", {0, 1, 2, 3})},
             {toNullableVec<String>("col1", {"col1-0", "col1-1", "col1-2", {}})}});
    }
    WRAP_FOR_DM_TEST_END
}
CATCH

TEST_F(ExecutorsWithDMTestRunner, MultiStageLateMaterializationStrongResidualFilter)
try
{
    enablePipeline(true);
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(64)));

    auto request = buildDAGRequestWithPushedDownFilter(
        context,
        "multi_stage_lm",
        lt(col("c0"), lit(Field(static_cast<Int64>(16)))),
        eq(makeASTFunction("bitand", col("c1"), lit(Field(static_cast<Int64>(3)))), lit(Field(static_cast<Int64>(0)))));

    context.context->getSettingsRef().dt_enable_multi_stage_late_materialization = false;
    DAGContext disabled_dag_context(*request, dag_context_ptr->log->identifier(), /*concurrency=*/4);
    auto expected = executeStreams(&disabled_dag_context);
    ASSERT_EQ(disabled_dag_context.getExecutorRowsOverride("table_scan_0"), nullptr);
    ASSERT_EQ(disabled_dag_context.getExecutorRowsOverride("selection_1"), nullptr);

    context.context->getSettingsRef().dt_enable_multi_stage_late_materialization = true;
    DAGContext enabled_dag_context(*request, dag_context_ptr->log->identifier(), /*concurrency=*/4);
    auto actual = executeStreams(&enabled_dag_context);
    ASSERT_TRUE(columnsEqual(expected, actual, /*_restrict=*/false))
        << "\n  expect_block: \n"
        << getColumnsContent(expected) << "\n actual_block: \n"
        << getColumnsContent(actual);

    assertMultiStageRowsOverride(enabled_dag_context, 4);
}
CATCH

TEST_F(ExecutorsWithDMTestRunner, MultiStageLateMaterializationWeakResidualFilter)
try
{
    enablePipeline(true);
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(64)));

    auto request = buildDAGRequestWithPushedDownFilter(
        context,
        "multi_stage_lm",
        lt(col("c0"), lit(Field(static_cast<Int64>(16)))),
        eq(makeASTFunction("bitand", col("c1"), lit(Field(static_cast<Int64>(8)))), lit(Field(static_cast<Int64>(0)))));

    context.context->getSettingsRef().dt_enable_multi_stage_late_materialization = false;
    DAGContext disabled_dag_context(*request, dag_context_ptr->log->identifier(), /*concurrency=*/4);
    auto expected = executeStreams(&disabled_dag_context);
    ASSERT_EQ(disabled_dag_context.getExecutorRowsOverride("table_scan_0"), nullptr);
    ASSERT_EQ(disabled_dag_context.getExecutorRowsOverride("selection_1"), nullptr);

    context.context->getSettingsRef().dt_enable_multi_stage_late_materialization = true;
    DAGContext enabled_dag_context(*request, dag_context_ptr->log->identifier(), /*concurrency=*/4);
    auto actual = executeStreams(&enabled_dag_context);
    ASSERT_TRUE(columnsEqual(expected, actual, /*_restrict=*/false))
        << "\n  expect_block: \n"
        << getColumnsContent(expected) << "\n actual_block: \n"
        << getColumnsContent(actual);

    assertMultiStageRowsOverride(enabled_dag_context, 16);
}
CATCH

TEST_F(ExecutorsWithDMTestRunner, MultiStageLateMaterializationDisabledWhenStage0CoversAllColumns)
try
{
    enablePipeline(true);
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(64)));

    ASTPtr stage0_filter = gt(col("c0"), lit(Field(static_cast<Int64>(-1))));
    for (size_t col_id = 1; col_id < 15; ++col_id)
    {
        stage0_filter = And(stage0_filter, gt(col(fmt::format("c{}", col_id)), lit(Field(static_cast<Int64>(-1)))));
    }

    auto pushed_down_filter_request = context.scan("test_db", "multi_stage_lm").filter(stage0_filter).build(context);
    RUNTIME_CHECK(pushed_down_filter_request->root_executor().tp() == tipb::ExecType::TypeSelection);

    auto request = context.scan("test_db", "multi_stage_lm")
                       .filter(lt(col("c1"), lit(Field(static_cast<Int64>(4)))))
                       .build(context);
    auto * table_scan = findMutableTableScan(request->mutable_root_executor());
    for (const auto & condition : pushed_down_filter_request->root_executor().selection().conditions())
        *table_scan->add_pushed_down_filter_conditions() = condition;

    context.context->getSettingsRef().dt_enable_multi_stage_late_materialization = false;
    DAGContext disabled_dag_context(*request, dag_context_ptr->log->identifier(), /*concurrency=*/4);
    auto expected = executeStreams(&disabled_dag_context);

    context.context->getSettingsRef().dt_enable_multi_stage_late_materialization = true;
    DAGContext enabled_dag_context(*request, dag_context_ptr->log->identifier(), /*concurrency=*/4);
    auto actual = executeStreams(&enabled_dag_context);

    ASSERT_TRUE(columnsEqual(expected, actual, /*_restrict=*/false))
        << "\n  expect_block: \n"
        << getColumnsContent(expected) << "\n actual_block: \n"
        << getColumnsContent(actual);

    ASSERT_EQ(enabled_dag_context.getExecutorRowsOverride("table_scan_0"), nullptr);
    ASSERT_EQ(enabled_dag_context.getExecutorRowsOverride("selection_1"), nullptr);
}
CATCH

TEST_F(ExecutorsWithDMTestRunner, MultiStageLateMaterializationSharedTimeColumnCast)
try
{
    enablePipeline(true);
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(64)));

    MockColumnInfoVec columns;
    columns.push_back({"pk", TiDB::TP::TypeLongLong});
    columns.push_back({"time_col", TiDB::TP::TypeTime});
    for (size_t col_id = 1; col_id <= 14; ++col_id)
        columns.push_back({fmt::format("c{}", col_id), TiDB::TP::TypeLongLong});

    constexpr size_t rows = 32;
    std::vector<Int64> pk_values;
    std::vector<Int64> time_values;
    pk_values.reserve(rows);
    time_values.reserve(rows);
    for (size_t row = 0; row < rows; ++row)
    {
        pk_values.push_back(row);
        time_values.push_back(MyDuration(1, static_cast<Int32>(row % 8), 0, 0, 0, 6).nanoSecond());
    }

    ColumnsWithTypeAndName data;
    data.emplace_back(toVec<Int64>("pk", pk_values));
    data.emplace_back(toVec<Int64>("time_col", time_values));
    for (size_t col_id = 1; col_id <= 14; ++col_id)
    {
        std::vector<Int64> values;
        values.reserve(rows);
        for (size_t row = 0; row < rows; ++row)
        {
            if (col_id == 1)
                values.push_back(row % 8);
            else
                values.push_back(static_cast<Int64>(col_id * 1000 + row));
        }
        data.emplace_back(toVec<Int64>(fmt::format("c{}", col_id), values));
    }
    context.addMockDeltaMerge(
        {"test_db", "multi_stage_lm_time"},
        columns,
        data,
        /*concurrency_hint=*/4);

    auto request = context.scan("test_db", "multi_stage_lm_time")
                       .filter(NOT(makeASTFunction("isnull", col("time_col"))))
                       .project(buildPkSpecialAndPayloadColumnProjection("time_col"))
                       .build(context);
    auto pushed_down_filter_request = context.scan("test_db", "multi_stage_lm_time")
                                          .filter(NOT(makeASTFunction("isnull", col("time_col"))))
                                          .build(context);
    const auto & pushed_down_filter_expr = pushed_down_filter_request->root_executor().selection().conditions(0);
    *findMutableTableScan(request->mutable_root_executor())->add_pushed_down_filter_conditions()
        = pushed_down_filter_expr;

    context.context->getSettingsRef().dt_enable_multi_stage_late_materialization = false;
    DAGContext disabled_dag_context(*request, dag_context_ptr->log->identifier(), /*concurrency=*/4);
    const auto selected_rows = selectRows(rows, [](size_t) { return true; });
    auto expected = buildExpectedPkSpecialPayloadColumnsForDAG(
        selected_rows,
        buildExpectedTimeColumnForDAG(selected_rows, "project_2_time_col"),
        "project_2_");
    assertColumnsEqual(expected, executeStreams(&disabled_dag_context));

    context.context->getSettingsRef().dt_enable_multi_stage_late_materialization = true;
    DAGContext enabled_dag_context(*request, dag_context_ptr->log->identifier(), /*concurrency=*/4);
    auto actual = executeStreams(&enabled_dag_context);

    assertColumnsEqual(expected, actual);
}
CATCH

TEST_F(ExecutorsWithDMTestRunner, MultiStageLateMaterializationPushedDownTimestampColumnOutputCast)
try
{
    enablePipeline(true);
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(64)));

    MockColumnInfoVec columns;
    columns.push_back({"pk", TiDB::TP::TypeLongLong});
    columns.push_back({"ts_col", TiDB::TP::TypeTimestamp});
    for (size_t col_id = 1; col_id <= 14; ++col_id)
        columns.push_back({fmt::format("c{}", col_id), TiDB::TP::TypeLongLong});

    constexpr size_t rows = 32;
    std::vector<Int64> pk_values;
    ColumnWithNullableMyDateTime ts_values;
    pk_values.reserve(rows);
    ts_values.reserve(rows);
    for (size_t row = 0; row < rows; ++row)
    {
        pk_values.push_back(row);
        ts_values.push_back(MyDateTime(2020, 1, 1, static_cast<UInt32>(row % 2), 0, 0, 0).toPackedUInt());
    }

    ColumnsWithTypeAndName data;
    data.emplace_back(toVec<Int64>("pk", pk_values));
    data.emplace_back(toNullableVec<MyDateTime>("ts_col", ts_values));
    for (size_t col_id = 1; col_id <= 14; ++col_id)
    {
        std::vector<Int64> values;
        values.reserve(rows);
        for (size_t row = 0; row < rows; ++row)
        {
            if (col_id == 1)
                values.push_back(row % 8);
            else
                values.push_back(static_cast<Int64>(col_id * 1000 + row));
        }
        data.emplace_back(toVec<Int64>(fmt::format("c{}", col_id), values));
    }
    context.addMockDeltaMerge(
        {"test_db", "multi_stage_lm_timestamp"},
        columns,
        data,
        /*concurrency_hint=*/4);

    auto request = buildDAGRequestWithPushedDownFilter(
        context,
        "multi_stage_lm_timestamp",
        NOT(makeASTFunction("isnull", col("ts_col"))),
        eq(makeASTFunction("bitand", col("c1"), lit(Field(static_cast<Int64>(3)))), lit(Field(static_cast<Int64>(0)))));
    request->set_time_zone_name("Asia/Shanghai");

    const auto selected_rows = selectRows(rows, [](size_t row) { return ((row % 8) & 3) == 0; });
    // The column name comes from the root final projection cast that converts the result back to TiDB's
    // timestamp representation. This is different from the extra cast after TableScan, which projects the
    // casted column back to the original column name.
    auto expected_ts_col = toNullableVec<MyDateTime>(
        "table_scan_0_ConvertTimeZoneToUTC(ts_col, Asia/Shanghai_String)_collator_0 ",
        buildNullableMyDateTimeValues(selected_rows, [](Int64 row) {
            return MyDateTime(2020, 1, 1, static_cast<UInt32>(row % 2), 0, 0, 0).toPackedUInt();
        }));
    auto expected = buildExpectedPkSpecialPayloadColumnsForDAG(selected_rows, expected_ts_col, "table_scan_0_");

    context.context->getSettingsRef().dt_enable_multi_stage_late_materialization = true;
    DAGContext enabled_dag_context(*request, dag_context_ptr->log->identifier(), /*concurrency=*/4);
    auto actual = executeStreams(&enabled_dag_context);

    assertColumnsEqual(expected, actual);
}
CATCH

TEST_F(ExecutorsWithDMTestRunner, MultiStageLateMaterializationTypeTimeOverlappedExtraCast)
try
{
    enablePipeline(true);
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(64)));

    MockColumnInfoVec columns;
    columns.push_back({"pk", TiDB::TP::TypeLongLong});
    columns.push_back({"time_col", TiDB::TP::TypeTime});
    for (size_t col_id = 1; col_id <= 14; ++col_id)
        columns.push_back({fmt::format("c{}", col_id), TiDB::TP::TypeLongLong});

    constexpr size_t rows = 32;
    std::vector<Int64> pk_values;
    std::vector<Int64> time_values;
    pk_values.reserve(rows);
    time_values.reserve(rows);
    for (size_t row = 0; row < rows; ++row)
    {
        pk_values.push_back(row);
        time_values.push_back(MyDuration(1, static_cast<Int32>(row % 8), 0, 0, 0, 6).nanoSecond());
    }

    ColumnsWithTypeAndName data;
    data.emplace_back(toVec<Int64>("pk", pk_values));
    data.emplace_back(toVec<Int64>("time_col", time_values));
    for (size_t col_id = 1; col_id <= 14; ++col_id)
    {
        std::vector<Int64> values;
        values.reserve(rows);
        for (size_t row = 0; row < rows; ++row)
        {
            if (col_id == 1)
                values.push_back(row % 8);
            else
                values.push_back(static_cast<Int64>(col_id * 1000 + row));
        }
        data.emplace_back(toVec<Int64>(fmt::format("c{}", col_id), values));
    }
    context.addMockDeltaMerge(
        {"test_db", "multi_stage_lm_time_overlap"},
        columns,
        data,
        /*concurrency_hint=*/4);

    auto request = buildDAGRequestWithPushedDownFilterAndProjection(
        context,
        "multi_stage_lm_time_overlap",
        gt(col("time_col"), lit(Field(MyDuration(1, 2, 0, 0, 0, 0).nanoSecond()))),
        lt(col("time_col"), lit(Field(MyDuration(1, 6, 0, 0, 0, 0).nanoSecond()))),
        buildPkSpecialAndPayloadColumnProjection("time_col"));
    TiDB::ColumnInfo duration_literal_type;
    duration_literal_type.tp = TiDB::TypeTime;
    rewriteOverlappedExtraCastFilterLiterals(
        request.get(),
        duration_literal_type,
        Field(MyDuration(1, 2, 0, 0, 0, 0).nanoSecond()),
        Field(MyDuration(1, 6, 0, 0, 0, 0).nanoSecond()),
        tipb::ScalarFuncSig::GTDuration,
        tipb::ScalarFuncSig::LTDuration,
        context.getCollation());

    context.context->getSettingsRef().dt_enable_multi_stage_late_materialization = false;
    DAGContext disabled_dag_context(*request, dag_context_ptr->log->identifier(), /*concurrency=*/4);
    const auto selected_rows = selectRows(rows, [](size_t row) { return row % 8 > 2 && row % 8 < 6; });
    auto expected = buildExpectedPkSpecialPayloadColumnsForDAG(
        selected_rows,
        buildExpectedTimeColumnForDAG(selected_rows, "project_2_time_col"),
        "project_2_");
    assertColumnsEqual(expected, executeStreams(&disabled_dag_context));

    context.context->getSettingsRef().dt_enable_multi_stage_late_materialization = true;
    DAGContext enabled_dag_context(*request, dag_context_ptr->log->identifier(), /*concurrency=*/4);
    auto actual = executeStreams(&enabled_dag_context);

    assertColumnsEqual(expected, actual);

    assertMultiStageRowsOverride(enabled_dag_context, 12);
}
CATCH

TEST_F(ExecutorsWithDMTestRunner, MultiStageLateMaterializationTimestampOverlappedExtraCast)
try
{
    enablePipeline(true);
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(64)));

    MockColumnInfoVec columns;
    columns.push_back({"pk", TiDB::TP::TypeLongLong});
    columns.push_back({"ts_col", TiDB::TP::TypeTimestamp});
    for (size_t col_id = 1; col_id <= 14; ++col_id)
        columns.push_back({fmt::format("c{}", col_id), TiDB::TP::TypeLongLong});

    constexpr size_t rows = 32;
    std::vector<Int64> pk_values;
    ColumnWithNullableMyDateTime ts_values;
    pk_values.reserve(rows);
    ts_values.reserve(rows);
    for (size_t row = 0; row < rows; ++row)
    {
        pk_values.push_back(row);
        ts_values.push_back(MyDateTime(2020, 1, 1, static_cast<UInt32>(row % 8), 0, 0, 0).toPackedUInt());
    }

    ColumnsWithTypeAndName data;
    data.emplace_back(toVec<Int64>("pk", pk_values));
    data.emplace_back(toNullableVec<MyDateTime>("ts_col", ts_values));
    for (size_t col_id = 1; col_id <= 14; ++col_id)
    {
        std::vector<Int64> values;
        values.reserve(rows);
        for (size_t row = 0; row < rows; ++row)
        {
            if (col_id == 1)
                values.push_back(row % 8);
            else
                values.push_back(static_cast<Int64>(col_id * 1000 + row));
        }
        data.emplace_back(toVec<Int64>(fmt::format("c{}", col_id), values));
    }
    context.addMockDeltaMerge(
        {"test_db", "multi_stage_lm_timestamp_overlap"},
        columns,
        data,
        /*concurrency_hint=*/4);

    auto request = buildDAGRequestWithPushedDownFilterAndProjection(
        context,
        "multi_stage_lm_timestamp_overlap",
        gt(col("ts_col"), lit(Field(MyDateTime(2020, 1, 1, 2, 0, 0, 0).toPackedUInt()))),
        lt(col("ts_col"), lit(Field(MyDateTime(2020, 1, 1, 6, 0, 0, 0).toPackedUInt()))),
        buildPkSpecialAndPayloadColumnProjection("ts_col"));
    TiDB::ColumnInfo timestamp_literal_type;
    timestamp_literal_type.tp = TiDB::TypeTimestamp;
    rewriteOverlappedExtraCastFilterLiterals(
        request.get(),
        timestamp_literal_type,
        Field(MyDateTime(2020, 1, 1, 2, 0, 0, 0).toPackedUInt()),
        Field(MyDateTime(2020, 1, 1, 6, 0, 0, 0).toPackedUInt()),
        tipb::ScalarFuncSig::GTTime,
        tipb::ScalarFuncSig::LTTime,
        context.getCollation());
    request->set_time_zone_name("Asia/Shanghai");

    context.context->getSettingsRef().dt_enable_multi_stage_late_materialization = false;
    DAGContext disabled_dag_context(*request, dag_context_ptr->log->identifier(), /*concurrency=*/4);
    auto expected = executeStreams(&disabled_dag_context);

    context.context->getSettingsRef().dt_enable_multi_stage_late_materialization = true;
    DAGContext enabled_dag_context(*request, dag_context_ptr->log->identifier(), /*concurrency=*/4);
    auto actual = executeStreams(&enabled_dag_context);

    ASSERT_TRUE(columnsEqual(expected, actual, /*_restrict=*/false))
        << "\n  expect_block: \n"
        << getColumnsContent(expected) << "\n actual_block: \n"
        << getColumnsContent(actual);

    assertMultiStageRowsOverride(enabled_dag_context, 12);
}
CATCH

TEST_F(ExecutorsWithDMTestRunner, MultiStageLateMaterializationPlainColumnOverlapped)
try
{
    enablePipeline(true);
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(64)));

    auto request = buildDAGRequestWithPushedDownFilter(
        context,
        "multi_stage_lm",
        gt(col("c0"), lit(Field(static_cast<Int64>(16)))),
        lt(col("c0"), lit(Field(static_cast<Int64>(24)))));

    context.context->getSettingsRef().dt_enable_multi_stage_late_materialization = false;
    DAGContext disabled_dag_context(*request, dag_context_ptr->log->identifier(), /*concurrency=*/4);
    const auto selected_rows = selectRows(32, [](size_t row) { return row > 16 && row < 24; });
    auto expected = buildExpectedPayloadColumns(selected_rows);
    assertColumnsEqual(expected, executeStreams(&disabled_dag_context));

    context.context->getSettingsRef().dt_enable_multi_stage_late_materialization = true;
    DAGContext enabled_dag_context(*request, dag_context_ptr->log->identifier(), /*concurrency=*/4);
    auto actual = executeStreams(&enabled_dag_context);

    assertColumnsEqual(expected, actual);

    assertMultiStageRowsOverride(enabled_dag_context, 7);
}
CATCH

TEST_F(ExecutorsWithDMTestRunner, MultiStageLateMaterializationHiddenCommitTSColumn)
try
{
    enablePipeline(true);
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(64)));

    MockColumnInfoVec columns;
    columns.push_back({"pk", TiDB::TP::TypeLongLong});
    columns.push_back({"commit_ts", TiDB::TP::TypeLongLong});
    for (size_t col_id = 1; col_id <= 14; ++col_id)
        columns.push_back({fmt::format("c{}", col_id), TiDB::TP::TypeLongLong});

    constexpr size_t rows = 32;
    std::vector<Int64> pk_values;
    std::vector<Int64> commit_ts_dummy_values;
    pk_values.reserve(rows);
    commit_ts_dummy_values.reserve(rows);
    for (size_t row = 0; row < rows; ++row)
    {
        pk_values.push_back(row);
        commit_ts_dummy_values.push_back(-1);
    }

    ColumnsWithTypeAndName data;
    data.emplace_back(toVec<Int64>("pk", pk_values));
    data.emplace_back(toVec<Int64>("commit_ts", commit_ts_dummy_values));
    for (size_t col_id = 1; col_id <= 14; ++col_id)
    {
        std::vector<Int64> values;
        values.reserve(rows);
        for (size_t row = 0; row < rows; ++row)
        {
            if (col_id == 1)
                values.push_back(row % 8);
            else
                values.push_back(static_cast<Int64>(col_id * 1000 + row));
        }
        data.emplace_back(toVec<Int64>(fmt::format("c{}", col_id), values));
    }
    context.addMockDeltaMerge(
        {"test_db", "multi_stage_lm_commit_ts"},
        columns,
        data,
        /*concurrency_hint=*/4);

    auto request = buildDAGRequestWithPushedDownFilter(
        context,
        "multi_stage_lm_commit_ts",
        gt(col("commit_ts"), lit(Field(static_cast<Int64>(8)))),
        lt(col("c1"), lit(Field(static_cast<Int64>(4)))));
    rewriteTableScanColumnAsCommitTS(request.get(), /*column_index=*/1);

    const auto selected_rows = selectRows(rows, [](size_t row) { return row >= 8 && row % 8 < 4; });
    auto expected_commit_ts_col = toNullableVec<Int64>(
        MutableSupport::version_column_name,
        buildNullableInt64Values(selected_rows, [](Int64 row) { return row + 1; }));
    auto expected = buildExpectedPkSpecialPayloadColumnsForDAG(selected_rows, expected_commit_ts_col);

    context.context->getSettingsRef().dt_enable_multi_stage_late_materialization = false;
    DAGContext disabled_dag_context(*request, dag_context_ptr->log->identifier(), /*concurrency=*/4);
    assertColumnsEqual(expected, executeStreams(&disabled_dag_context));

    context.context->getSettingsRef().dt_enable_multi_stage_late_materialization = true;
    DAGContext enabled_dag_context(*request, dag_context_ptr->log->identifier(), /*concurrency=*/4);
    auto actual = executeStreams(&enabled_dag_context);
    assertColumnsEqual(expected, actual);

    auto table_scan_rows = enabled_dag_context.getExecutorRowsOverride("table_scan_0");
    auto selection_rows = enabled_dag_context.getExecutorRowsOverride("selection_1");
    ASSERT_NE(table_scan_rows, nullptr);
    ASSERT_NE(selection_rows, nullptr);
    ASSERT_EQ(selection_rows->load(), 12);
    ASSERT_GE(table_scan_rows->load(), selection_rows->load());
}
CATCH

TEST_F(ExecutorsWithDMTestRunner, MultiStageLateMaterializationDisabledForStage1ExtraTableIDColumn)
try
{
    enablePipeline(true);
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(64)));

    auto request = buildDAGRequestWithPushedDownFilter(
        context,
        "multi_stage_lm",
        lt(col("c0"), lit(Field(static_cast<Int64>(16)))),
        gt(col("c1"), lit(Field(static_cast<Int64>(-1)))));
    rewriteTableScanColumnAsExtraTableID(request.get(), /*column_index=*/1);

    context.context->getSettingsRef().dt_enable_multi_stage_late_materialization = true;
    DAGContext enabled_dag_context(*request, dag_context_ptr->log->identifier(), /*concurrency=*/4);
    auto actual = executeStreams(&enabled_dag_context);

    ASSERT_FALSE(actual.empty());
    ASSERT_EQ(actual.front().column->size(), 16);
    ASSERT_EQ(enabled_dag_context.getExecutorRowsOverride("table_scan_0"), nullptr);
    ASSERT_EQ(enabled_dag_context.getExecutorRowsOverride("selection_1"), nullptr);
}
CATCH

TEST_F(ExecutorsWithDMTestRunner, MultiStageLateMaterializationRowsOverrideMergesRemoteProfileRows)
try
{
    enablePipeline(true);
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(64)));

    auto request = buildDAGRequestWithPushedDownFilter(
        context,
        "multi_stage_lm",
        lt(col("c0"), lit(Field(static_cast<Int64>(16)))),
        eq(makeASTFunction("bitand", col("c1"), lit(Field(static_cast<Int64>(3)))), lit(Field(static_cast<Int64>(0)))));
    request->set_collect_execution_summaries(true);

    context.context->getSettingsRef().dt_enable_multi_stage_late_materialization = true;
    DAGContext dag_context(*request, dag_context_ptr->log->identifier(), /*concurrency=*/4);
    executeStreams(&dag_context);

    auto table_scan_rows = dag_context.getExecutorRowsOverride("table_scan_0");
    auto selection_rows = dag_context.getExecutorRowsOverride("selection_1");
    ASSERT_NE(table_scan_rows, nullptr);
    ASSERT_NE(selection_rows, nullptr);
    ASSERT_EQ(table_scan_rows->load(), 16);
    ASSERT_EQ(selection_rows->load(), 4);

    OperatorProfileInfos remote_table_scan_profiles;
    auto remote_table_scan_profile = std::make_shared<OperatorProfileInfo>();
    remote_table_scan_profiles.push_back(remote_table_scan_profile);
    dag_context.addProfileInfosToExecutorRowsOverride("table_scan_0", remote_table_scan_profiles);
    dag_context.addOperatorProfileInfos(
        "table_scan_0",
        OperatorProfileInfos(remote_table_scan_profiles),
        /*is_append=*/true);

    OperatorProfileInfos remote_selection_profiles;
    auto remote_selection_profile = std::make_shared<OperatorProfileInfo>();
    remote_selection_profiles.push_back(remote_selection_profile);
    dag_context.addProfileInfosToExecutorRowsOverride("selection_1", remote_selection_profiles);
    dag_context.addOperatorProfileInfos(
        "selection_1",
        OperatorProfileInfos(remote_selection_profiles),
        /*is_append=*/true);

    remote_table_scan_profile->rows = 6;
    remote_selection_profile->rows = 6;

    ExecutorStatisticsCollector statistics_collector(dag_context_ptr->log->identifier(), true);
    statistics_collector.initialize(&dag_context);
    statistics_collector.setLocalRUConsumption(
        RUConsumption{.cpu_ru = 0.0, .cpu_time_ns = 0, .read_ru = 0.0, .read_bytes = 0});
    auto response = statistics_collector.genExecutionSummaryResponse();
    ASSERT_EQ(getExecutionSummaryRows(response, "table_scan_0"), 22);
    ASSERT_EQ(getExecutionSummaryRows(response, "selection_1"), 10);
}
CATCH

#undef WRAP_FOR_DM_TEST_BEGIN
#undef WRAP_FOR_DM_TEST_END

} // namespace tests
} // namespace DB
