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

#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/MergeSortingBlockInputStream.h>
#include <DataStreams/PartialSortingBlockInputStream.h>
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
        shared_query_block_input_stream->setExtraInfo("restore concurrency");
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
        {
            ret = std::make_shared<UnionWithoutBlock>(pipeline.streams_with_non_joined_data, nullptr, max_threads, log->identifier());
            ret->setExtraInfo("combine non joined(ignore block)");
        }
        else
        {
            ret = std::make_shared<UnionWithBlock>(pipeline.streams_with_non_joined_data, nullptr, max_threads, log->identifier());
            ret->setExtraInfo("combine non joined");
        }
    }
    pipeline.streams_with_non_joined_data.clear();
    return ret;
}

void executeUnion(
    DAGPipeline & pipeline,
    size_t max_streams,
    const LoggerPtr & log,
    bool ignore_block,
    const String & extra_info)
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
        pipeline.firstStream()->setExtraInfo(extra_info);
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
    SortDescription order_descr,
    Int64 limit,
    const Context & context,
    const LoggerPtr & log)
{
    const Settings & settings = context.getSettingsRef();

    pipeline.transform([&](auto & stream) {
        auto sorting_stream = std::make_shared<PartialSortingBlockInputStream>(stream, order_descr, log->identifier(), limit);

        /// Limits on sorting
        IProfilingBlockInputStream::LocalLimits limits;
        limits.mode = IProfilingBlockInputStream::LIMITS_TOTAL;
        limits.size_limits = SizeLimits(settings.max_rows_to_sort, settings.max_bytes_to_sort, settings.sort_overflow_mode);
        sorting_stream->setLimits(limits);

        stream = sorting_stream;
    });

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
} // namespace DB
