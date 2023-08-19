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

#include <DataStreams/MockTableScanBlockInputStream.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Flash/Coprocessor/MockSourceStream.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/plans/PhysicalMockTableScan.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace
{
std::pair<NamesAndTypes, BlockInputStreams> mockSchemaAndStreams(
    Context & context,
    const String & executor_id,
    const LoggerPtr & log,
    const TiDBTableScan & table_scan)
{
    NamesAndTypes schema;
    BlockInputStreams mock_streams;

    auto & dag_context = *context.getDAGContext();
    size_t max_streams = dag_context.initialize_concurrency;
    assert(max_streams > 0);

    if (!context.mockStorage().tableExists(table_scan.getLogicalTableID()))
    {
        /// build with default blocks.
        schema = genNamesAndTypes(table_scan, "mock_table_scan");
        auto columns_with_type_and_name = getColumnWithTypeAndName(schema);
        for (size_t i = 0; i < max_streams; ++i)
            mock_streams.emplace_back(std::make_shared<MockTableScanBlockInputStream>(columns_with_type_and_name, context.getSettingsRef().max_block_size));
    }
    else
    {
        /// build from user input blocks.
        auto [names_and_types, mock_table_scan_streams] = mockSourceStream<MockTableScanBlockInputStream>(context, max_streams, log, executor_id, table_scan.getLogicalTableID());
        schema = std::move(names_and_types);
        mock_streams.insert(mock_streams.end(), mock_table_scan_streams.begin(), mock_table_scan_streams.end());
    }

    assert(!schema.empty());
    assert(!mock_streams.empty());

    return {std::move(schema), std::move(mock_streams)};
}
} // namespace

PhysicalMockTableScan::PhysicalMockTableScan(
    const String & executor_id_,
    const NamesAndTypes & schema_,
    const String & req_id,
    const Block & sample_block_,
    const BlockInputStreams & mock_streams_)
    : PhysicalLeaf(executor_id_, PlanType::MockTableScan, schema_, req_id)
    , sample_block(sample_block_)
    , mock_streams(mock_streams_)
{}

PhysicalPlanNodePtr PhysicalMockTableScan::build(
    Context & context,
    const String & executor_id,
    const LoggerPtr & log,
    const TiDBTableScan & table_scan)
{
    assert(context.isTest());

    auto [schema, mock_streams] = mockSchemaAndStreams(context, executor_id, log, table_scan);

    auto physical_mock_table_scan = std::make_shared<PhysicalMockTableScan>(
        executor_id,
        schema,
        log->identifier(),
        Block(schema),
        mock_streams);
    return physical_mock_table_scan;
}

void PhysicalMockTableScan::transformImpl(DAGPipeline & pipeline, Context & /*context*/, size_t /*max_streams*/)
{
    assert(pipeline.streams.empty() && pipeline.streams_with_non_joined_data.empty());
    pipeline.streams.insert(pipeline.streams.end(), mock_streams.begin(), mock_streams.end());
}

void PhysicalMockTableScan::finalize(const Names & parent_require)
{
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
}

const Block & PhysicalMockTableScan::getSampleBlock() const
{
    return sample_block;
}
} // namespace DB
