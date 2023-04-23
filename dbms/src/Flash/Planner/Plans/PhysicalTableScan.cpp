// Copyright 2023 PingCAP, Ltd.
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
    auto physical_table_scan = std::make_shared<PhysicalTableScan>(
        executor_id,
        schema,
        log->identifier(),
        table_scan,
        Block(schema));
    return physical_table_scan;
}

void PhysicalTableScan::buildBlockInputStreamImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    RUNTIME_CHECK(pipeline.streams.empty());

    if (context.getSharedContextDisagg()->isDisaggregatedComputeMode())
    {
        StorageDisaggregatedInterpreter disaggregated_tiflash_interpreter(context, tidb_table_scan, filter_conditions, max_streams);
        disaggregated_tiflash_interpreter.execute(pipeline);
        buildProjection(pipeline, disaggregated_tiflash_interpreter.analyzer->getCurrentInputColumns());
    }
    else
    {
        DAGStorageInterpreter storage_interpreter(context, tidb_table_scan, filter_conditions, max_streams);
        storage_interpreter.execute(pipeline);
        buildProjection(pipeline, storage_interpreter.analyzer->getCurrentInputColumns());
    }
}

void PhysicalTableScan::buildPipeline(
    PipelineBuilder & builder,
    Context & context,
    PipelineExecutorStatus & exec_status)
{
    storage_interpreter = std::make_unique<DAGStorageInterpreter>(
        context,
        tidb_table_scan,
        filter_conditions,
        context.getMaxStreams());
    source_ops = storage_interpreter->execute(exec_status);
    PhysicalPlanNode::buildPipeline(builder, context, exec_status);
}

void PhysicalTableScan::buildPipelineExecGroup(
    PipelineExecutorStatus & exec_status,
    PipelineExecGroupBuilder & group_builder,
    Context &,
    size_t)
{
    group_builder.init(source_ops.size());
    size_t i = 0;
    group_builder.transform([&](auto & builder) {
        builder.setSourceOp(std::move(source_ops[i++]));
    });
    storage_interpreter->executeSuffix(exec_status, group_builder);
    buildProjection(exec_status, group_builder, storage_interpreter->analyzer->getCurrentInputColumns());
}

void PhysicalTableScan::buildProjection(DAGPipeline & pipeline, const NamesAndTypes & storage_schema)
{
    const auto & schema_project_cols = buildTableScanProjectionCols(schema, storage_schema);
    /// In order to keep BlockInputStream's schema consistent with PhysicalPlan's schema.
    /// It is worth noting that the column uses the name as the unique identifier in the Block, so the column name must also be consistent.
    ExpressionActionsPtr schema_project = generateProjectExpressionActions(pipeline.firstStream(), schema_project_cols);
    executeExpression(pipeline, schema_project, log, "table scan schema projection");
}

void PhysicalTableScan::buildProjection(
    PipelineExecutorStatus & exec_status,
    PipelineExecGroupBuilder & group_builder,
    const NamesAndTypes & storage_schema)
{
    const auto & schema_project_cols = buildTableScanProjectionCols(schema, storage_schema);

    /// In order to keep TransformOp's schema consistent with PhysicalPlan's schema.
    /// It is worth noting that the column uses the name as the unique identifier in the Block, so the column name must also be consistent.
    ExpressionActionsPtr schema_actions = PhysicalPlanHelper::newActions(group_builder.getCurrentHeader());
    schema_actions->add(ExpressionAction::project(schema_project_cols));
    executeExpression(exec_status, group_builder, schema_actions, log);
}

void PhysicalTableScan::finalize(const Names & parent_require)
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
