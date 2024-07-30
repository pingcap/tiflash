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
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/MockSourceStream.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/Plans/PhysicalMockTableScan.h>
#include <Interpreters/Context.h>
#include <Operators/BlockInputStreamSourceOp.h>

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
    size_t max_streams = getMockSourceStreamConcurrency(dag_context.initialize_concurrency, context.mockStorage()->getScanConcurrencyHint(table_scan.getLogicalTableID()));
    assert(max_streams > 0);

    if (context.mockStorage()->useDeltaMerge())
    {
        assert(context.mockStorage()->tableExistsForDeltaMerge(table_scan.getLogicalTableID()));
        schema = context.mockStorage()->getNameAndTypesForDeltaMerge(table_scan.getLogicalTableID());
        mock_streams.emplace_back(context.mockStorage()->getStreamFromDeltaMerge(context, table_scan.getLogicalTableID()));
    }
    else
    {
        /// build from user input blocks.
        assert(context.mockStorage()->tableExists(table_scan.getLogicalTableID()));
        NamesAndTypes names_and_types;
        std::vector<std::shared_ptr<DB::MockTableScanBlockInputStream>> mock_table_scan_streams;
        if (context.isMPPTest())
        {
            std::tie(names_and_types, mock_table_scan_streams) = mockSourceStreamForMpp(context, max_streams, log, table_scan);
        }
        else
        {
            std::tie(names_and_types, mock_table_scan_streams) = mockSourceStream<MockTableScanBlockInputStream>(context, max_streams, log, executor_id, table_scan.getLogicalTableID(), table_scan.getColumns());
        }
        schema = std::move(names_and_types);
        mock_streams.insert(mock_streams.end(), mock_table_scan_streams.begin(), mock_table_scan_streams.end());
    }

    assert(!schema.empty());
    assert(!mock_streams.empty());

    // Ignore handling GeneratedColumnPlaceholderBlockInputStream for now, because we don't support generated column in test framework.
    return {std::move(schema), std::move(mock_streams)};
}
} // namespace

PhysicalMockTableScan::PhysicalMockTableScan(
    const String & executor_id_,
    const NamesAndTypes & schema_,
    const String & req_id,
    const Block & sample_block_,
    const BlockInputStreams & mock_streams_,
    Int64 table_id_)
    : PhysicalLeaf(executor_id_, PlanType::MockTableScan, schema_, FineGrainedShuffle{}, req_id)
    , sample_block(sample_block_)
    , mock_streams(mock_streams_)
    , table_id(table_id_)
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
        mock_streams,
        table_scan.getLogicalTableID());
    return physical_mock_table_scan;
}

void PhysicalMockTableScan::buildBlockInputStreamImpl(DAGPipeline & pipeline, Context & /*context*/, size_t /*max_streams*/)
{
    assert(pipeline.streams.empty());
    pipeline.streams.insert(pipeline.streams.end(), mock_streams.begin(), mock_streams.end());
}

void PhysicalMockTableScan::buildPipelineExecGroup(
    PipelineExecutorStatus & exec_status,
    PipelineExecGroupBuilder & group_builder,
    Context & context,
    size_t concurrency)
{
    if (context.mockStorage()->useDeltaMerge())
    {
        auto source_ops = context.mockStorage()->getSourceOpsFromDeltaMerge(exec_status, context, table_id, concurrency);
        group_builder.init(source_ops.size());
        size_t i = 0;
        group_builder.transform([&](auto & builder) {
            builder.setSourceOp(std::move(source_ops[i++]));
        });
    }
    else
    {
        group_builder.init(mock_streams.size());
        size_t i = 0;
        group_builder.transform([&](auto & builder) {
            builder.setSourceOp(std::make_unique<BlockInputStreamSourceOp>(exec_status, log->identifier(), mock_streams[i++]));
        });
    }
}

void PhysicalMockTableScan::finalize(const Names & parent_require)
{
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
}

const Block & PhysicalMockTableScan::getSampleBlock() const
{
    return sample_block;
}

void PhysicalMockTableScan::updateStreams(Context & context)
{
    mock_streams.clear();
    assert(context.mockStorage()->tableExistsForDeltaMerge(table_id));
    mock_streams.emplace_back(context.mockStorage()->getStreamFromDeltaMerge(context, table_id, &filter_conditions));
}

bool PhysicalMockTableScan::setFilterConditions(Context & context, const String & filter_executor_id, const tipb::Selection & selection)
{
    if (unlikely(hasFilterConditions()))
    {
        return false;
    }
    filter_conditions = FilterConditions::filterConditionsFrom(filter_executor_id, selection);
    updateStreams(context);
    return true;
}

bool PhysicalMockTableScan::hasFilterConditions() const
{
    return filter_conditions.hasValue();
}

const String & PhysicalMockTableScan::getFilterConditionsId() const
{
    assert(hasFilterConditions());
    return filter_conditions.executor_id;
}

Int64 PhysicalMockTableScan::getLogicalTableID() const
{
    return table_id;
}

} // namespace DB
