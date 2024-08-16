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
#include <Operators/UnorderedSourceOp.h>
#include <Storages/DeltaMerge/ReadThread/UnorderedInputStream.h>

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
    size_t max_streams = getMockSourceStreamConcurrency(
        dag_context.initialize_concurrency,
        context.mockStorage()->getScanConcurrencyHint(table_scan.getLogicalTableID()));
    RUNTIME_CHECK(max_streams > 0);

    if (context.mockStorage()->useDeltaMerge())
    {
        RUNTIME_CHECK(context.mockStorage()->tableExistsForDeltaMerge(table_scan.getLogicalTableID()));
        schema = context.mockStorage()->getNameAndTypesForDeltaMerge(table_scan.getLogicalTableID());
        mock_streams.emplace_back(context.mockStorage()->getStreamFromDeltaMerge(
            context,
            table_scan.getLogicalTableID(),
            nullptr,
            false,
            table_scan.getRuntimeFilterIDs(),
            10000));
    }
    else
    {
        /// build from user input blocks.
        RUNTIME_CHECK(context.mockStorage()->tableExists(table_scan.getLogicalTableID()));
        NamesAndTypes names_and_types;
        std::vector<std::shared_ptr<DB::MockTableScanBlockInputStream>> mock_table_scan_streams;
        if (context.isMPPTest())
        {
            std::tie(names_and_types, mock_table_scan_streams)
                = mockSourceStreamForMpp(context, max_streams, log, table_scan);
        }
        else
        {
            std::tie(names_and_types, mock_table_scan_streams) = mockSourceStream<MockTableScanBlockInputStream>(
                context,
                max_streams,
                log,
                executor_id,
                table_scan.getLogicalTableID(),
                table_scan.getColumns());
        }
        schema = std::move(names_and_types);
        mock_streams.insert(mock_streams.end(), mock_table_scan_streams.begin(), mock_table_scan_streams.end());
    }

    RUNTIME_CHECK(!schema.empty());
    RUNTIME_CHECK(!mock_streams.empty());

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
    Int64 table_id_,
    bool keep_order_,
    const std::vector<Int32> & runtime_filter_ids)
    : PhysicalLeaf(executor_id_, PlanType::MockTableScan, schema_, FineGrainedShuffle{}, req_id)
    , sample_block(sample_block_)
    , mock_streams(mock_streams_)
    , table_id(table_id_)
    , keep_order(keep_order_)
    , runtime_filter_ids(runtime_filter_ids)
{}

PhysicalPlanNodePtr PhysicalMockTableScan::build(
    Context & context,
    const String & executor_id,
    const LoggerPtr & log,
    const TiDBTableScan & table_scan)
{
    RUNTIME_CHECK(context.isTest());
    auto [schema, mock_streams] = mockSchemaAndStreams(context, executor_id, log, table_scan);
    auto physical_mock_table_scan = std::make_shared<PhysicalMockTableScan>(
        executor_id,
        schema,
        log->identifier(),
        Block(schema),
        mock_streams,
        table_scan.getLogicalTableID(),
        table_scan.keepOrder(),
        table_scan.getRuntimeFilterIDs());
    return physical_mock_table_scan;
}

void PhysicalMockTableScan::buildBlockInputStreamImpl(
    DAGPipeline & pipeline,
    Context & context /*context*/,
    size_t /*max_streams*/)
{
    RUNTIME_CHECK(pipeline.streams.empty());
    buildRuntimeFilterInLocalStream(context);
    pipeline.streams.insert(pipeline.streams.end(), mock_streams.begin(), mock_streams.end());
}

void PhysicalMockTableScan::buildPipelineExecGroupImpl(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    Context & context,
    size_t)
{
    if (context.mockStorage()->useDeltaMerge())
    {
        context.mockStorage()->buildExecFromDeltaMerge(
            exec_context,
            group_builder,
            context,
            table_id,
            context.getMaxStreams(),
            keep_order,
            &filter_conditions,
            runtime_filter_ids,
            rf_max_wait_time_ms);
        for (size_t i = 0; i < group_builder.concurrency(); ++i)
        {
            if (auto * source_op = dynamic_cast<UnorderedSourceOp *>(group_builder.getCurBuilder(i).source_op.get()))
            {
                auto runtime_filter_list = getRuntimeFilterList(context);
                // todo config max wait time
                source_op->setRuntimeFilterInfo(runtime_filter_list, rf_max_wait_time_ms);
            }
        }
    }
    else
    {
        for (const auto & stream : mock_streams)
        {
            group_builder.addConcurrency(
                std::make_unique<BlockInputStreamSourceOp>(exec_context, log->identifier(), stream));
        }
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

bool PhysicalMockTableScan::setFilterConditions(
    Context & context,
    const String & filter_executor_id,
    const tipb::Selection & selection)
{
    if (unlikely(hasFilterConditions()))
    {
        return false;
    }
    if (context.mockStorage()->useDeltaMerge() && context.mockStorage()->tableExistsForDeltaMerge(table_id))
    {
        filter_conditions = FilterConditions::filterConditionsFrom(filter_executor_id, selection);

        // For the pipeline model, because pipeline_exec has not been generated yet, there is no need to update it at this time.
        // The update here is only for the stream model.
        mock_streams.clear();
        RUNTIME_CHECK(context.mockStorage()->tableExistsForDeltaMerge(table_id));
        mock_streams.emplace_back(
            context.mockStorage()
                ->getStreamFromDeltaMerge(context, table_id, &filter_conditions, false, runtime_filter_ids, 10000));

        return true;
    }
    else
    {
        return false;
    }
}

bool PhysicalMockTableScan::hasFilterConditions() const
{
    return filter_conditions.hasValue();
}

const String & PhysicalMockTableScan::getFilterConditionsId() const
{
    RUNTIME_CHECK(hasFilterConditions());
    return filter_conditions.executor_id;
}

void PhysicalMockTableScan::buildRuntimeFilterInLocalStream(Context & context)
{
    for (const auto & local_stream : mock_streams)
    {
        if (auto * p_stream = dynamic_cast<DM::UnorderedInputStream *>(local_stream.get()))
        {
            auto runtime_filter_list = getRuntimeFilterList(context);
            // todo config max wait time
            p_stream->setRuntimeFilterInfo(runtime_filter_list, rf_max_wait_time_ms);
        }
    }
}

RuntimeFilteList PhysicalMockTableScan::getRuntimeFilterList(Context & context)
{
    auto mock_column_infos = context.mockStorage()->getTableSchemaForDeltaMerge(table_id);
    auto column_infos = mockColumnInfosToTiDBColumnInfos(mock_column_infos);
    auto column_defines = context.mockStorage()->getStoreColumnDefines(table_id);
    auto rfs = context.getDAGContext()->runtime_filter_mgr.getLocalRuntimeFilterByIds(runtime_filter_ids);
    for (auto & rf : rfs)
    {
        rf->setTargetAttr(column_infos, column_defines);
    }
    return rfs;
}
} // namespace DB
