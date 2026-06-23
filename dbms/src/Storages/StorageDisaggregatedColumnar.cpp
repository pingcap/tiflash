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

#include <Common/config.h> // for ENABLE_NEXT_GEN_COLUMNAR
#if ENABLE_NEXT_GEN_COLUMNAR
#include <Common/Exception.h>
#include <Common/MyTime.h>
#include <Common/RedactHelpers.h>
#include <Common/Stopwatch.h>
#include <Core/NamesAndTypes.h>
#include <DataStreams/AddExtraTableIDColumnTransformAction.h>
#include <DataStreams/IBlockInputStream.h>
#include <Flash/Coprocessor/CodecUtils.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/FilterConditions.h>
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/RequestUtils.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/Columnar/ColumnarReader.h>
#include <Storages/Columnar/ColumnarScanContext.h>
#include <Storages/Columnar/ColumnarSourceOp.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageDisaggregated.h>
#include <Storages/StorageDisaggregatedHelpers.h>
#include <TiDB/Decode/TypeMapping.h>
#include <TiDB/Schema/TiDB.h>
#include <common/DateLUT.h>

#include <ext/scope_guard.h>

namespace DB
{

namespace
{
std::vector<std::tuple<UInt64, String, DataTypePtr>> genGeneratedColumnInfosForDisaggregatedRead(
    const TiDBTableScan & table_scan)
{
    std::vector<std::tuple<UInt64, String, DataTypePtr>> generated_column_infos;
    generated_column_infos.reserve(table_scan.getColumnSize());
    for (Int32 i = 0; i < table_scan.getColumnSize(); ++i)
    {
        const auto & ci = table_scan.getColumns()[i];
        if (!ci.hasGeneratedColumnFlag())
            continue;
        // Disaggregated read behaves like ExchangeReceiver output.
        generated_column_infos.emplace_back(
            static_cast<UInt64>(i),
            genNameForExchangeReceiver(i),
            getDataTypeByColumnInfoForComputingLayer(ci));
    }
    return generated_column_infos;
}
} // namespace

void StorageDisaggregated::filterConditionsWithPushedDownFilters(
    DAGExpressionAnalyzer & analyzer,
    DAGPipeline & pipeline)
{
    // Columnar reader uses late-materialization filters only to reduce packs loaded from disk.
    // It does not guarantee that all rows failing those filters are removed, so merge them into
    // FilterConditions and re-apply them in the TiFlash pipeline for correctness.
    FilterConditions conditions(filter_conditions.executor_id, filter_conditions.conditions);
    conditions.conditions.MergeFrom(table_scan.getPushedDownFilters());
    if (conditions.hasValue())
    {
        ::DB::executePushedDownFilter(conditions, analyzer, log, pipeline);
        auto & profile_streams = context.getDAGContext()->getProfileStreamsMap()[conditions.executor_id];
        pipeline.transform([&profile_streams](auto & stream) { profile_streams.push_back(stream); });
    }
}

void StorageDisaggregated::filterConditionsWithPushedDownFilters(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    DAGExpressionAnalyzer & analyzer)
{
    // Columnar reader uses late-materialization filters only to reduce packs loaded from disk.
    // It does not guarantee that all rows failing those filters are removed, so merge them into
    // FilterConditions and re-apply them in the TiFlash pipeline for correctness.
    FilterConditions conditions(filter_conditions.executor_id, filter_conditions.conditions);
    conditions.conditions.MergeFrom(table_scan.getPushedDownFilters());
    if (conditions.hasValue())
    {
        ::DB::executePushedDownFilter(exec_context, group_builder, conditions, analyzer, log);
        context.getDAGContext()->addOperatorProfileInfos(conditions.executor_id, group_builder.getCurProfileInfos());
    }
}

BlockInputStreams StorageDisaggregated::readThroughColumnar(const Context & context, unsigned num_streams)
{
    DAGPipeline pipeline;
    const UInt64 start_ts = sender_target_mpp_task_id.gather_id.query_id.start_ts;
    auto [remote_table_ranges, region_num] = buildRemoteTableRanges();
    const auto generated_column_infos = genGeneratedColumnInfosForDisaggregatedRead(table_scan);
    auto columnar_task_pools = ColumnarReadTaskPool::buildWithBackoff(
        log,
        context,
        start_ts,
        table_scan,
        filter_conditions,
        remote_table_ranges,
        num_streams);
    for (auto & task_pool : columnar_task_pools)
    {
        auto streams = task_pool->getInputStreams();
        pipeline.streams.insert(pipeline.streams.end(), streams.begin(), streams.end());
    }
    // Avoid reading generated columns from columnar, generate placeholders locally.
    executeGeneratedColumnPlaceholder(generated_column_infos, log, pipeline);
    NamesAndTypes source_columns;
    source_columns.reserve(table_scan.getColumnSize());
    const auto & stream_header = pipeline.firstStream()->getHeader();
    for (const auto & col : stream_header)
    {
        source_columns.emplace_back(col.name, col.type);
    }
    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(source_columns), context);

    // Handle duration/timestamp cast for columnar path.
    // We still execute pushed-down filters on RN side, so timestamp columns in those filters
    // must also be converted from UTC to session timezone.
    extraCast(*analyzer, pipeline, /*include_pushed_down_filter_columns=*/true);
    // Handle filter
    filterConditionsWithPushedDownFilters(*analyzer, pipeline);
    return pipeline.streams;
}


void StorageDisaggregated::readThroughColumnar(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    const Context & context,
    unsigned num_streams)
{
    const UInt64 start_ts = sender_target_mpp_task_id.gather_id.query_id.start_ts;
    auto [remote_table_ranges, region_num] = buildRemoteTableRanges();
    auto columnar_task_pools = ColumnarReadTaskPool::buildWithBackoff(
        log,
        context,
        start_ts,
        table_scan,
        filter_conditions,
        remote_table_ranges,
        num_streams);
    const auto generated_column_infos = genGeneratedColumnInfosForDisaggregatedRead(table_scan);
    if (!columnar_task_pools.empty())
    {
        auto & task_pool = columnar_task_pools.front();
        const size_t source_num = task_pool->getSourceNum();
        LOG_INFO(
            log,
            "use shared columnar reader task pool, reader_num={}, source_num={}",
            task_pool->getReaderCount(),
            source_num);
        for (size_t i = 0; i < source_num; ++i)
        {
            group_builder.addConcurrency(ColumnarSourceOp::create({
                .exec_context = exec_context,
                .task = task_pool,
            }));
        }
    }

    executeGeneratedColumnPlaceholder(exec_context, group_builder, generated_column_infos, log);

    NamesAndTypes source_columns;
    auto header = group_builder.getCurrentHeader();
    source_columns.reserve(header.columns());
    for (const auto & col : header)
        source_columns.emplace_back(col.name, col.type);
    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(source_columns), context);

    // Handle duration/timestamp cast for columnar path.
    extraCast(exec_context, group_builder, *analyzer, /*include_pushed_down_filter_columns=*/true);
    // Handle filter
    filterConditionsWithPushedDownFilters(exec_context, group_builder, *analyzer);
}

} // namespace DB
#endif
