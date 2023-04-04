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

#include <DataStreams/CreatingSetsBlockInputStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/GeneratedColumnPlaceholderBlockInputStream.h>
#include <DataStreams/MergeSortingBlockInputStream.h>
#include <DataStreams/PartialSortingBlockInputStream.h>
#include <DataStreams/SharedQueryBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
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
    switch (pipeline.streams.size() + pipeline.streams_with_non_joined_data.size())
    {
    case 0:
        break;
    case 1:
    {
        if (pipeline.streams.size() == 1)
            break;
        // streams_with_non_joined_data's size is 1.
        pipeline.streams.push_back(pipeline.streams_with_non_joined_data.at(0));
        pipeline.streams_with_non_joined_data.clear();
        break;
    }
    default:
    {
        BlockInputStreamPtr stream;
        if (ignore_block)
            stream = std::make_shared<UnionWithoutBlock>(pipeline.streams, pipeline.streams_with_non_joined_data, max_streams, log->identifier());
        else
            stream = std::make_shared<UnionWithBlock>(pipeline.streams, pipeline.streams_with_non_joined_data, max_streams, log->identifier());
        stream->setExtraInfo(extra_info);

        pipeline.streams.resize(1);
        pipeline.streams_with_non_joined_data.clear();
        pipeline.firstStream() = std::move(stream);
        break;
    }
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
        auto sorting_stream = std::make_shared<PartialSortingBlockInputStream>(stream, order_descr, log->identifier(), limit);

        /// Limits on sorting
        IProfilingBlockInputStream::LocalLimits limits;
        limits.mode = IProfilingBlockInputStream::LIMITS_TOTAL;
        limits.size_limits = SizeLimits(settings.max_rows_to_sort, settings.max_bytes_to_sort, settings.sort_overflow_mode);
        sorting_stream->setLimits(limits);

        stream = sorting_stream;
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
                settings.max_bytes_before_external_sort,
                context.getTemporaryPath(),
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
            context.getTemporaryPath(),
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
    if (unlikely(context.isExecutorTest()))
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
