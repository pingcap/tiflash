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

#include <Common/TiFlashException.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/DAGStorageInterpreter.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/StorageDisaggregatedInterpreter.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/Plans/PhysicalTableScan.h>
#include <DataStreams/GeneratedColumnPlaceholderBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Operators/ExpressionTransformOp.h>

#include <string_view>

namespace DB
{
namespace
{
tipb::Expr buildFTSExpression(const TiDBTableScan & table_scan, Int32 result_type)
{
    const auto & query_info = table_scan.getFTSQueryInfo();
    const auto query_func = query_info.has_query_func() ? query_info.query_func() : tipb::ScalarFuncSig::FTSMatchWord;
    if (query_func == tipb::ScalarFuncSig::FTSMatchWord)
        RUNTIME_CHECK_MSG(query_info.columns_size() == 1, "FTS_MATCH_WORD currently supports exactly one column");
    else
        RUNTIME_CHECK_MSG(
            query_func == tipb::ScalarFuncSig::FTSMatchExpression && query_info.columns_size() > 0,
            "Unsupported full-text query function or empty MATCH column list");

    tipb::Expr expression;
    expression.set_tp(tipb::ExprType::ScalarFunc);
    expression.set_sig(query_func);
    expression.mutable_field_type()->set_tp(result_type);
    expression.mutable_field_type()->set_flag(TiDB::ColumnFlagNotNull);
    *expression.add_children() = constructStringLiteralTiExpr(query_info.query_text());

    bool fts_collation_initialized = false;
    bool fts_result_nullable = false;
    for (const auto & query_column : query_info.columns())
    {
        const auto column_id = query_column.column_id();
        const TiDB::ColumnInfo * column_info = nullptr;
        Int64 column_index = -1;
        for (size_t i = 0; i < table_scan.getColumns().size(); ++i)
        {
            const auto & column = table_scan.getColumns()[i];
            if (column.id == column_id)
            {
                column_info = &column;
                column_index = i;
                break;
            }
        }
        RUNTIME_CHECK_MSG(column_info != nullptr, "Full-text column is not present in table scan columns");

        tipb::Expr column_ref;
        column_ref.set_tp(tipb::ExprType::ColumnRef);
        WriteBufferFromOwnString ss;
        encodeDAGInt64(column_index, ss);
        column_ref.set_val(ss.releaseStr());
        *column_ref.mutable_field_type() = TiDB::columnInfoToFieldType(*column_info);
        fts_result_nullable = fts_result_nullable || !column_info->hasNotNullFlag();
        // FTS returns a numeric score, but its string matching semantics are
        // determined by the MATCH column collation. Carry the first column's
        // protocol collation on the scalar expression so the DAG analyzer
        // passes the same collator to fts_match_expression as it does for
        // LIKE and comparison functions.
        if (!fts_collation_initialized && column_ref.field_type().collate() != 0)
        {
            expression.mutable_field_type()->set_collate(column_ref.field_type().collate());
            fts_collation_initialized = true;
        }
        *expression.add_children() = std::move(column_ref);
    }

    // MATCH returns NULL when any input column is NULL. Keep the result type
    // nullable so the analyzer does not insert a cast from Nullable(Float64)
    // to Float64, which would fail at runtime for a NULL row.
    if (fts_result_nullable)
        expression.mutable_field_type()->set_flag(0);

    if (query_func == tipb::ScalarFuncSig::FTSMatchExpression && query_info.has_boolean_query())
        *expression.mutable_fts_boolean_query() = query_info.boolean_query();
    return expression;
}

tipb::Expr buildFTSFilter(const TiDBTableScan & table_scan)
{
    return buildFTSExpression(table_scan, TiDB::TypeDouble);
}

String getFTSScorePlaceholderName(const TiDBTableScan & table_scan)
{
    for (size_t i = 0; i < table_scan.getColumns().size(); ++i)
    {
        if (isTiDBFTSScoreColumn(table_scan.getColumns()[i].id))
            return GeneratedColumnPlaceholderBlockInputStream::getColumnName(i);
    }
    throw TiFlashException("FTS score column is missing from table scan", Errors::Coprocessor::BadRequest);
}

ExpressionActionsPtr buildFTSScoreActions(
    const Block & input_header,
    const TiDBTableScan & table_scan,
    const Context & context)
{
    auto actions = std::make_shared<ExpressionActions>(input_header.getColumnsWithTypeAndName());
    DAGExpressionAnalyzer analyzer(input_header, context);
    const auto score_expr = buildFTSExpression(table_scan, TiDB::TypeFloat);
    const auto score_expr_name = analyzer.getActions(score_expr, actions);
    const auto score_column_name = getFTSScorePlaceholderName(table_scan);

    RUNTIME_CHECK_MSG(
        actions->getSampleBlock().has(score_column_name),
        "FTS score placeholder is missing from table scan input");
    actions->add(ExpressionAction::removeColumn(score_column_name));
    actions->add(ExpressionAction::copyColumn(score_expr_name, score_column_name));

    NamesWithAliases project_columns;
    project_columns.reserve(input_header.columns());
    for (const auto & column : input_header)
        project_columns.emplace_back(column.name, column.name);
    actions->add(ExpressionAction::project(project_columns));
    actions->finalize(input_header.getNames());
    return actions;
}

NamesWithAliases buildTableScanProjectionCols(
    Int64 logical_table_id,
    const NamesAndTypes & schema,
    const Block & storage_header)
{
    if (unlikely(schema.size() != storage_header.columns()))
        throw TiFlashException(
            fmt::format(
                "The tidb table scan schema size {} is different from the tiflash storage schema size {}, table id is "
                "{}",
                schema.size(),
                storage_header.columns(),
                logical_table_id),
            Errors::Planner::BadRequest);
    NamesWithAliases schema_project_cols;
    for (size_t i = 0; i < schema.size(); ++i)
    {
        const auto & table_scan_col_name = schema[i].name;
        const auto & table_scan_col_type = schema[i].type;
        const auto & storage_col_name = storage_header.getColumnsWithTypeAndName()[i].name;
        const auto & storage_col_type = storage_header.getColumnsWithTypeAndName()[i].type;
        if (unlikely(!table_scan_col_type->equals(*storage_col_type)))
            throw TiFlashException(
                fmt::format(
                    R"(The data type {} from tidb table scan schema is different from the data type {} from tiflash storage schema, 
                    table id is {}, 
                    column index is {}, 
                    column name from tidb table scan is {}, 
                    column name from tiflash storage is {})",
                    table_scan_col_type->getName(),
                    storage_col_type->getName(),
                    logical_table_id,
                    i,
                    table_scan_col_name,
                    storage_col_name),
                Errors::Planner::BadRequest);
        schema_project_cols.emplace_back(storage_col_name, table_scan_col_name);
    }
    return schema_project_cols;
}
} // namespace

PhysicalTableScan::PhysicalTableScan(
    const String & executor_id_,
    const NamesAndTypes & schema_,
    const String & req_id,
    const TiDBTableScan & tidb_table_scan_,
    const Block & sample_block_)
    : PhysicalLeaf(executor_id_, PlanType::TableScan, schema_, FineGrainedShuffle{}, req_id)
    , tidb_table_scan(tidb_table_scan_)
    , sample_block(sample_block_)
{
    if (tidb_table_scan.getFTSQueryInfo().columns_size() > 0)
    {
        google::protobuf::RepeatedPtrField<tipb::Expr> conditions;
        *conditions.Add() = buildFTSFilter(tidb_table_scan);
        filter_conditions = FilterConditions(executor_id_, conditions);
    }
}

PhysicalPlanNodePtr PhysicalTableScan::build(
    const String & executor_id,
    const LoggerPtr & log,
    const TiDBTableScan & table_scan)
{
    auto schema = genNamesAndTypesForTableScan(table_scan);
    auto physical_table_scan
        = std::make_shared<PhysicalTableScan>(executor_id, schema, log->identifier(), table_scan, Block(schema));
    return physical_table_scan;
}

void PhysicalTableScan::buildBlockInputStreamImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    RUNTIME_CHECK(pipeline.streams.empty());

    if (context.getSharedContextDisagg()->isDisaggregatedComputeMode())
    {
        StorageDisaggregatedInterpreter disaggregated_tiflash_interpreter(
            context,
            tidb_table_scan,
            filter_conditions,
            max_streams);
        disaggregated_tiflash_interpreter.execute(pipeline);
    }
    else
    {
        DAGStorageInterpreter storage_interpreter(context, tidb_table_scan, filter_conditions, max_streams);
        storage_interpreter.execute(pipeline);
    }
    buildProjection(pipeline, context);
}

void PhysicalTableScan::buildPipeline(
    PipelineBuilder & builder,
    Context & context,
    PipelineExecutorContext & exec_context)
{
    // For building PipelineExec in compile time.
    if (context.getSharedContextDisagg()->isDisaggregatedComputeMode())
    {
        StorageDisaggregatedInterpreter disaggregated_tiflash_interpreter(
            context,
            tidb_table_scan,
            filter_conditions,
            context.getMaxStreams());
        disaggregated_tiflash_interpreter.execute(exec_context, pipeline_exec_builder);
    }
    else
    {
        DAGStorageInterpreter storage_interpreter(context, tidb_table_scan, filter_conditions, context.getMaxStreams());
        storage_interpreter.execute(exec_context, pipeline_exec_builder);
    }
    buildProjection(exec_context, pipeline_exec_builder, context);

    PhysicalPlanNode::buildPipeline(builder, context, exec_context);
}

void PhysicalTableScan::buildPipelineExecGroupImpl(
    PipelineExecutorContext & /*exec_status*/,
    PipelineExecGroupBuilder & group_builder,
    Context & /*context*/,
    size_t /*concurrency*/)
{
    assert(group_builder.empty());
    group_builder = std::move(pipeline_exec_builder);
}

void PhysicalTableScan::buildProjection(DAGPipeline & pipeline, Context & context)
{
    if (tidb_table_scan.getFTSQueryInfo().query_type() == tipb::FTSQueryType::FTSQueryTypeWithScore)
    {
        RUNTIME_CHECK_MSG(
            !pipeline.streams.empty(),
            "FTS score cannot be materialized without a table scan stream");
        auto score_actions = buildFTSScoreActions(pipeline.firstStream()->getHeader(), tidb_table_scan, context);
        executeExpression(pipeline, score_actions, log, "full-text score");
    }

    const auto & schema_project_cols = buildTableScanProjectionCols(
        tidb_table_scan.getLogicalTableID(),
        schema,
        pipeline.firstStream()->getHeader());
    /// In order to keep BlockInputStream's schema consistent with PhysicalPlan's schema.
    /// It is worth noting that the column uses the name as the unique identifier in the Block, so the column name must also be consistent.
    ExpressionActionsPtr schema_project = generateProjectExpressionActions(pipeline.firstStream(), schema_project_cols);
    executeExpression(pipeline, schema_project, log, "table scan schema projection");
}

void PhysicalTableScan::buildProjection(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    Context & context)
{
    if (tidb_table_scan.getFTSQueryInfo().query_type() == tipb::FTSQueryType::FTSQueryTypeWithScore)
    {
        auto score_actions = buildFTSScoreActions(group_builder.getCurrentHeader(), tidb_table_scan, context);
        executeExpression(exec_context, group_builder, score_actions, log);
    }

    auto header = group_builder.getCurrentHeader();
    const auto & schema_project_cols
        = buildTableScanProjectionCols(tidb_table_scan.getLogicalTableID(), schema, header);

    /// In order to keep TransformOp's schema consistent with PhysicalPlan's schema.
    /// It is worth noting that the column uses the name as the unique identifier in the Block, so the column name must also be consistent.
    ExpressionActionsPtr schema_actions = PhysicalPlanHelper::newActions(header);
    schema_actions->add(ExpressionAction::project(schema_project_cols));
    executeExpression(exec_context, group_builder, schema_actions, log);
}

void PhysicalTableScan::finalizeImpl(const Names & parent_require)
{
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
}

const Block & PhysicalTableScan::getSampleBlock() const
{
    return sample_block;
}

bool PhysicalTableScan::setFilterConditions(const String & filter_executor_id, const tipb::Selection & selection)
{
    if (!hasFilterConditions())
        filter_conditions = FilterConditions::filterConditionsFrom(filter_executor_id, selection);
    else
        for (const auto & condition : selection.conditions())
            *filter_conditions.conditions.Add() = condition;
    return true;
}

bool PhysicalTableScan::hasFilterConditions() const
{
    return filter_conditions.hasValue();
}

const String & PhysicalTableScan::getFilterConditionsId() const
{
    RUNTIME_CHECK(hasFilterConditions());
    return filter_conditions.executor_id;
}
} // namespace DB
