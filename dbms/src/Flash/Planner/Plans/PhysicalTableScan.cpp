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
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/DAGStorageInterpreter.h>
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/StorageDisaggregatedInterpreter.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/Plans/PhysicalTableScan.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Operators/ExpressionTransformOp.h>

namespace DB
{
namespace
{
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
{}

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
    buildProjection(pipeline);
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
    buildProjection(exec_context, pipeline_exec_builder);

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

void PhysicalTableScan::buildProjection(DAGPipeline & pipeline)
{
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
    PipelineExecGroupBuilder & group_builder)
{
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
    /// Since there is at most one selection on the table scan, setFilterConditions() will only be called at most once.
    /// So in this case hasFilterConditions() is always false.
    if (unlikely(hasFilterConditions()))
    {
        return false;
    }

    filter_conditions = FilterConditions::filterConditionsFrom(filter_executor_id, selection);
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
