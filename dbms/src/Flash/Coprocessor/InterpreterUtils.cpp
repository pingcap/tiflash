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

<<<<<<< HEAD
=======
#include <Common/ThresholdUtils.h>
#include <DataStreams/CreatingSetsBlockInputStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <DataStreams/GeneratedColumnPlaceholderBlockInputStream.h>
#include <DataStreams/MergeSortingBlockInputStream.h>
#include <DataStreams/PartialSortingBlockInputStream.h>
>>>>>>> e84ed489e6 (add GeneratedColumnPlaceholderInputStream (#6796))
#include <DataStreams/SharedQueryBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Interpreters/Context.h>

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
    if (concurrency > 1 && pipeline.streams.size() == 1 && pipeline.streams_with_non_joined_data.empty())
    {
        BlockInputStreamPtr shared_query_block_input_stream
            = std::make_shared<SharedQueryBlockInputStream>(concurrency * 5, pipeline.firstStream(), log->identifier());
        pipeline.streams.assign(concurrency, shared_query_block_input_stream);
    }
}

BlockInputStreamPtr combinedNonJoinedDataStream(
    DAGPipeline & pipeline,
    size_t max_threads,
    const LoggerPtr & log,
    bool ignore_block)
{
    BlockInputStreamPtr ret = nullptr;
    if (pipeline.streams_with_non_joined_data.size() == 1)
        ret = pipeline.streams_with_non_joined_data.at(0);
    else if (pipeline.streams_with_non_joined_data.size() > 1)
    {
        if (ignore_block)
            ret = std::make_shared<UnionWithoutBlock>(pipeline.streams_with_non_joined_data, nullptr, max_threads, log->identifier());
        else
            ret = std::make_shared<UnionWithBlock>(pipeline.streams_with_non_joined_data, nullptr, max_threads, log->identifier());
    }
    pipeline.streams_with_non_joined_data.clear();
    return ret;
}

void executeUnion(
    DAGPipeline & pipeline,
    size_t max_streams,
    const LoggerPtr & log,
    bool ignore_block)
{
    if (pipeline.streams.size() == 1 && pipeline.streams_with_non_joined_data.empty())
        return;
    auto non_joined_data_stream = combinedNonJoinedDataStream(pipeline, max_streams, log, ignore_block);
    if (!pipeline.streams.empty())
    {
        if (ignore_block)
            pipeline.firstStream() = std::make_shared<UnionWithoutBlock>(pipeline.streams, non_joined_data_stream, max_streams, log->identifier());
        else
            pipeline.firstStream() = std::make_shared<UnionWithBlock>(pipeline.streams, non_joined_data_stream, max_streams, log->identifier());
        pipeline.streams.resize(1);
    }
    else if (non_joined_data_stream != nullptr)
    {
        pipeline.streams.push_back(non_joined_data_stream);
    }
}

ExpressionActionsPtr generateProjectExpressionActions(
    const BlockInputStreamPtr & stream,
    const Context & context,
    const NamesWithAliases & project_cols)
{
    NamesAndTypesList input_column;
    for (const auto & column : stream->getHeader())
        input_column.emplace_back(column.name, column.type);
    ExpressionActionsPtr project = std::make_shared<ExpressionActions>(input_column, context.getSettingsRef());
    project->add(ExpressionAction::project(project_cols));
    return project;
}
<<<<<<< HEAD
=======

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
                SpillConfig(context.getTemporaryPath(), fmt::format("{}_sort", log->identifier()), settings.max_spilled_size_per_spill, context.getFileProvider()),
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
            SpillConfig(context.getTemporaryPath(), fmt::format("{}_sort", log->identifier()), settings.max_spilled_size_per_spill, context.getFileProvider()),
            log->identifier());
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
    const FilterConditions & filter_conditions,
    DAGExpressionAnalyzer & analyzer)
{
    assert(filter_conditions.hasValue());

    ExpressionActionsChain chain;
    analyzer.initChain(chain);
    String filter_column_name = analyzer.appendWhere(chain, filter_conditions.conditions);
    ExpressionActionsPtr before_where = chain.getLastActions();
    chain.addStep();

    // remove useless tmp column and keep the schema of local streams and remote streams the same.
    NamesWithAliases project_cols;
    for (const auto & col : analyzer.getCurrentInputColumns())
    {
        chain.getLastStep().required_output.push_back(col.name);
        project_cols.emplace_back(col.name, col.name);
    }
    chain.getLastActions()->add(ExpressionAction::project(project_cols));
    ExpressionActionsPtr project_after_where = chain.getLastActions();
    chain.finalize();
    chain.clear();

    return {before_where, filter_column_name, project_after_where};
}

void executePushedDownFilter(
    size_t remote_read_streams_start_index,
    const FilterConditions & filter_conditions,
    DAGExpressionAnalyzer & analyzer,
    LoggerPtr log,
    DAGPipeline & pipeline)
{
    auto [before_where, filter_column_name, project_after_where] = ::DB::buildPushDownFilter(filter_conditions, analyzer);

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
>>>>>>> e84ed489e6 (add GeneratedColumnPlaceholderInputStream (#6796))
} // namespace DB
