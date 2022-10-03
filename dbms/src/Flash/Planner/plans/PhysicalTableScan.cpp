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

#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/DAGStorageInterpreter.h>
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/MockSourceStream.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/plans/PhysicalTableScan.h>
#include <Interpreters/Context.h>

namespace DB
{
PhysicalTableScan::PhysicalTableScan(
    const String & executor_id_,
    const NamesAndTypes & schema_,
    const String & req_id,
    const TiDBTableScan & tidb_table_scan_,
    const Block & sample_block_)
    : PhysicalLeaf(executor_id_, PlanType::TableScan, schema_, req_id)
    , tidb_table_scan(tidb_table_scan_)
    , sample_block(sample_block_)
{}

PhysicalPlanNodePtr PhysicalTableScan::build(
    const String & executor_id,
    const LoggerPtr & log,
    const TiDBTableScan & table_scan)
{
    auto schema = genNamesAndTypes(table_scan, "table_scan");
    auto physical_table_scan = std::make_shared<PhysicalTableScan>(
        executor_id,
        schema,
        log->identifier(),
        table_scan,
        Block(schema));
    return physical_table_scan;
}

void PhysicalTableScan::transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    assert(pipeline.streams.empty() && pipeline.streams_with_non_joined_data.empty());

    DAGStorageInterpreter storage_interpreter(context, tidb_table_scan, push_down_filter, max_streams);
    storage_interpreter.execute(pipeline);

    const auto & storage_schema = storage_interpreter.analyzer->getCurrentInputColumns();
    RUNTIME_CHECK(
        storage_schema.size() == schema.size(),
        storage_schema.size(),
        schema.size());
    NamesWithAliases schema_project_cols;
    for (size_t i = 0; i < schema.size(); ++i)
    {
        RUNTIME_CHECK(
            schema[i].type->equals(*storage_schema[i].type),
            schema[i].name,
            schema[i].type->getName(),
            storage_schema[i].name,
            storage_schema[i].type->getName());
        assert(!storage_schema[i].name.empty() && !schema[i].name.empty());
        schema_project_cols.emplace_back(storage_schema[i].name, schema[i].name);
    }
    /// In order to keep BlockInputStream's schema consistent with PhysicalPlan's schema.
    /// It is worth noting that the column uses the name as the unique identifier in the Block, so the column name must also be consistent.
    ExpressionActionsPtr schema_project = generateProjectExpressionActions(pipeline.firstStream(), context, schema_project_cols);
    executeExpression(pipeline, schema_project, log, "table scan schema projection");
}

void PhysicalTableScan::finalize(const Names & parent_require)
{
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
}

const Block & PhysicalTableScan::getSampleBlock() const
{
    return sample_block;
}

bool PhysicalTableScan::pushDownFilter(const String & filter_executor_id, const tipb::Selection & selection)
{
    /// Since there is at most one selection on the table scan, pushDownFilter will only be called at most once.
    /// So in this case hasPushDownFilter() is always false.
    if (unlikely(hasPushDownFilter()))
    {
        return false;
    }

    push_down_filter = PushDownFilter::pushDownFilterFrom(filter_executor_id, selection);
    return true;
}

bool PhysicalTableScan::hasPushDownFilter() const
{
    return push_down_filter.hasValue();
}

const String & PhysicalTableScan::getPushDownFilterId() const
{
    assert(hasPushDownFilter());
    return push_down_filter.executor_id;
}
} // namespace DB
