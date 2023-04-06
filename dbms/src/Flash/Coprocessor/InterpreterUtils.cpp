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

#include <Common/ThresholdUtils.h>
#include <DataStreams/CreatingSetsBlockInputStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <DataStreams/GeneratedColumnPlaceholderBlockInputStream.h>
#include <DataStreams/LimitTransformAction.h>
#include <DataStreams/MergeSortingBlockInputStream.h>
#include <DataStreams/PartialSortingBlockInputStream.h>
#include <DataStreams/SharedQueryBlockInputStream.h>
#include <DataStreams/SortHelper.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Interpreters/Context.h>
#include <Operators/ExpressionTransformOp.h>
#include <Operators/LimitTransformOp.h>
#include <Operators/LocalSortTransformOp.h>

namespace DB
{
namespace
{
using UnionWithBlock = UnionBlockInputStream<>;
using UnionWithoutBlock = UnionBlockInputStream<StreamUnionMode::Basic, /*ignore_block=*/true>;
} // namespace

void restoreConcurrency(
    DAGPipeline & pipeline,
    size_t concurrency,
    const LoggerPtr & log)
{
    if (concurrency > 1 && pipeline.streams.size() == 1)
    {
        BlockInputStreamPtr shared_query_block_input_stream
            = std::make_shared<SharedQueryBlockInputStream>(concurrency * 5, pipeline.firstStream(), log->identifier());
        shared_query_block_input_stream->setExtraInfo("restore concurrency");
        pipeline.streams.assign(concurrency, shared_query_block_input_stream);
    }
}

void executeUnion(
    DAGPipeline & pipeline,
    size_t max_streams,
    const LoggerPtr & log,
    bool ignore_block,
    const String & extra_info)
{
    if (pipeline.streams.size() > 1)
    {
        BlockInputStreamPtr stream;
        if (ignore_block)
            stream = std::make_shared<UnionWithoutBlock>(pipeline.streams, BlockInputStreams{}, max_streams, log->identifier());
        else
            stream = std::make_shared<UnionWithBlock>(pipeline.streams, BlockInputStreams{}, max_streams, log->identifier());
        stream->setExtraInfo(extra_info);

        pipeline.streams.resize(1);
        pipeline.firstStream() = std::move(stream);
    }
}

ExpressionActionsPtr generateProjectExpressionActions(
    const BlockInputStreamPtr & stream,
    const NamesWithAliases & project_cols)
{
    NamesAndTypesList input_column;
    for (const auto & column : stream->getHeader())
        input_column.emplace_back(column.name, column.type);
    ExpressionActionsPtr project = std::make_shared<ExpressionActions>(input_column);
    project->add(ExpressionAction::project(project_cols));
    return project;
}

void executeExpression(
    DAGPipeline & pipeline,
    const ExpressionActionsPtr & expr_actions,
    const LoggerPtr & log,
    const String & extra_info)
{
    if (expr_actions && !expr_actions->getActions().empty())
    {
        pipeline.transform([&](auto & stream) {
            stream = std::make_shared<ExpressionBlockInputStream>(stream, expr_actions, log->identifier());
            stream->setExtraInfo(extra_info);
        });
    }
}

void executeExpression(
    PipelineExecutorStatus & exec_status,
    PipelineExecGroupBuilder & group_builder,
    const ExpressionActionsPtr & expr_actions,
    const LoggerPtr & log)
{
    if (expr_actions && !expr_actions->getActions().empty())
    {
        group_builder.transform([&](auto & builder) {
            builder.appendTransformOp(std::make_unique<ExpressionTransformOp>(exec_status, log->identifier(), expr_actions));
        });
    }
}

void orderStreams(
    DAGPipeline & pipeline,
    size_t max_streams,
    const SortDescription & order_descr,
    Int64 limit,
    bool enable_fine_grained_shuffle,
    const Context & context,
    const LoggerPtr & log)
{
    const Settings & settings = context.getSettingsRef();
    String extra_info;
    if (enable_fine_grained_shuffle)
        extra_info = enableFineGrainedShuffleExtraInfo;

    pipeline.transform([&](auto & stream) {
        stream = std::make_shared<PartialSortingBlockInputStream>(stream, order_descr, log->identifier(), limit);
        stream->setExtraInfo(extra_info);
    });

    if (enable_fine_grained_shuffle)
    {
        pipeline.transform([&](auto & stream) {
            stream = std::make_shared<MergeSortingBlockInputStream>(
                stream,
                order_descr,
                settings.max_block_size,
                limit,
                getAverageThreshold(settings.max_bytes_before_external_sort, pipeline.streams.size()),
                SpillConfig(context.getTemporaryPath(), fmt::format("{}_sort", log->identifier()), settings.max_cached_data_bytes_in_spiller, settings.max_spilled_rows_per_file, settings.max_spilled_bytes_per_file, context.getFileProvider()),
                log->identifier());
            stream->setExtraInfo(String(enableFineGrainedShuffleExtraInfo));
        });
    }
    else
    {
        /// If there are several streams, we merge them into one
        executeUnion(pipeline, max_streams, log, false, "for partial order");

        /// Merge the sorted blocks.
        pipeline.firstStream() = std::make_shared<MergeSortingBlockInputStream>(
            pipeline.firstStream(),
            order_descr,
            settings.max_block_size,
            limit,
            settings.max_bytes_before_external_sort,
            // todo use identifier_executor_id as the spill id
            SpillConfig(context.getTemporaryPath(), fmt::format("{}_sort", log->identifier()), settings.max_cached_data_bytes_in_spiller, settings.max_spilled_rows_per_file, settings.max_spilled_bytes_per_file, context.getFileProvider()),
            log->identifier());
    }
}

void executeLocalSort(
    PipelineExecutorStatus & exec_status,
    PipelineExecGroupBuilder & group_builder,
    const SortDescription & order_descr,
    std::optional<size_t> limit,
    const Context & context,
    const LoggerPtr & log)
{
    auto input_header = group_builder.getCurrentHeader();
    if (SortHelper::isSortByConstants(input_header, order_descr))
    {
        // For order by const col and has limit, we will generate LimitOperator directly.
        if (limit)
        {
            group_builder.transform([&](auto & builder) {
                auto local_limit = std::make_shared<LocalLimitTransformAction>(input_header, *limit);
                builder.appendTransformOp(std::make_unique<LimitTransformOp<LocalLimitPtr>>(exec_status, log->identifier(), local_limit));
            });
        }
        // For order by const and doesn't has limit, do nothing here.
    }
    else
    {
        const Settings & settings = context.getSettingsRef();
        size_t max_bytes_before_external_sort = getAverageThreshold(settings.max_bytes_before_external_sort, group_builder.concurrency);
        SpillConfig spill_config{
            context.getTemporaryPath(),
            fmt::format("{}_sort", log->identifier()),
            settings.max_cached_data_bytes_in_spiller,
            settings.max_spilled_rows_per_file,
            settings.max_spilled_bytes_per_file,
            context.getFileProvider()};
        group_builder.transform([&](auto & builder) {
            builder.appendTransformOp(std::make_unique<LocalSortTransformOp>(
                exec_status,
                log->identifier(),
                order_descr,
                limit.value_or(0), // 0 means that no limit in LocalSortTransformOp.
                settings.max_block_size,
                max_bytes_before_external_sort,
                spill_config));
        });
    }
}

void executeCreatingSets(
    DAGPipeline & pipeline,
    const Context & context,
    size_t max_streams,
    const LoggerPtr & log)
{
    DAGContext & dag_context = *context.getDAGContext();
    /// add union to run in parallel if needed
    if (unlikely(context.isExecutorTest() || context.isInterpreterTest()))
        executeUnion(pipeline, max_streams, log, /*ignore_block=*/false, "for test");
    else if (context.isMPPTest())
        executeUnion(pipeline, max_streams, log, /*ignore_block=*/true, "for mpp test");
    else if (dag_context.isMPPTask())
        /// MPPTask do not need the returned blocks.
        executeUnion(pipeline, max_streams, log, /*ignore_block=*/true, "for mpp");
    else
        executeUnion(pipeline, max_streams, log, /*ignore_block=*/false, "for non mpp");
    if (dag_context.hasSubquery())
    {
        const Settings & settings = context.getSettingsRef();
        pipeline.firstStream() = std::make_shared<CreatingSetsBlockInputStream>(
            pipeline.firstStream(),
            std::move(dag_context.moveSubqueries()),
            SizeLimits(settings.max_rows_to_transfer, settings.max_bytes_to_transfer, settings.transfer_overflow_mode),
            log->identifier());
    }
}

std::tuple<ExpressionActionsPtr, String, ExpressionActionsPtr> buildPushDownFilter(
    const google::protobuf::RepeatedPtrField<tipb::Expr> & conditions,
    DAGExpressionAnalyzer & analyzer)
{
    assert(!conditions.empty());

    ExpressionActionsChain chain;
    analyzer.initChain(chain);
    String filter_column_name = analyzer.appendWhere(chain, conditions);
    ExpressionActionsPtr before_where = chain.getLastActions();
    chain.addStep();

    // remove useless tmp column and keep the schema of local streams and remote streams the same.
    for (const auto & col : analyzer.getCurrentInputColumns())
    {
        chain.getLastStep().required_output.push_back(col.name);
    }
    ExpressionActionsPtr project_after_where = chain.getLastActions();
    chain.finalize();
    chain.clear();

    RUNTIME_CHECK(!project_after_where->getActions().empty());
    return {before_where, filter_column_name, project_after_where};
}

void executePushedDownFilter(
    size_t remote_read_streams_start_index,
    const FilterConditions & filter_conditions,
    DAGExpressionAnalyzer & analyzer,
    LoggerPtr log,
    DAGPipeline & pipeline)
{
    auto [before_where, filter_column_name, project_after_where] = ::DB::buildPushDownFilter(filter_conditions.conditions, analyzer);

    assert(remote_read_streams_start_index <= pipeline.streams.size());
    // for remote read, filter had been pushed down, don't need to execute again.
    for (size_t i = 0; i < remote_read_streams_start_index; ++i)
    {
        auto & stream = pipeline.streams[i];
        stream = std::make_shared<FilterBlockInputStream>(stream, before_where, filter_column_name, log->identifier());
        stream->setExtraInfo("push down filter");
        // after filter, do project action to keep the schema of local streams and remote streams the same.
        stream = std::make_shared<ExpressionBlockInputStream>(stream, project_after_where, log->identifier());
        stream->setExtraInfo("projection after push down filter");
    }
}

void executeGeneratedColumnPlaceholder(
    size_t remote_read_streams_start_index,
    const std::vector<std::tuple<UInt64, String, DataTypePtr>> & generated_column_infos,
    LoggerPtr log,
    DAGPipeline & pipeline)
{
    if (generated_column_infos.empty())
        return;
    assert(remote_read_streams_start_index <= pipeline.streams.size());
    for (size_t i = 0; i < remote_read_streams_start_index; ++i)
    {
        auto & stream = pipeline.streams[i];
        stream = std::make_shared<GeneratedColumnPlaceholderBlockInputStream>(stream, generated_column_infos, log->identifier());
        stream->setExtraInfo("generated column placeholder above table scan");
    }
}

} // namespace DB
