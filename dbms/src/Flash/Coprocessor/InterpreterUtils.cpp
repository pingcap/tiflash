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

#include <DataStreams/ParallelWritingBlockInputStream.h>
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

// return ParallelWritingBlockInputStream or origin stream(writer is not executed).
BlockInputStreamPtr executeParallelForNonJoined(
    DAGPipeline & pipeline,
    const ParallelWriterPtr & parallel_writer,
    size_t max_threads,
    const LoggerPtr & log)
{
    assert(!pipeline.streams.empty());
    BlockInputStreamPtr ret = nullptr;
    if (pipeline.streams_with_non_joined_data.size() == 1)
    {
        ret = pipeline.streams_with_non_joined_data.at(0);
    }
    else if (pipeline.streams_with_non_joined_data.size() > 1)
    {
        ret = std::make_shared<ParallelWritingBlockInputStream>(
            pipeline.streams_with_non_joined_data,
            nullptr,
            parallel_writer,
            max_threads,
            log->identifier());
    }
    else // pipeline.streams_with_non_joined_data.empty(), just else here.
    {
    }
    pipeline.streams_with_non_joined_data.clear();
    return ret;
}
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

void executeParallel(
    DAGPipeline & pipeline,
    const ParallelWriterPtr & parallel_writer,
    size_t max_streams,
    const LoggerPtr & log)
{
    assert(!pipeline.streams.empty() && !pipeline.streams_with_non_joined_data.empty());
    if (pipeline.streams.empty()) // !pipeline.streams_with_non_joined_data.empty()
    {
        pipeline.streams = std::move(pipeline.streams_with_non_joined_data);
        pipeline.streams_with_non_joined_data = {};
        executeParallel(pipeline, parallel_writer, max_streams, log);
    }
    else
    {
        auto non_joined_data_stream = executeParallelForNonJoined(pipeline, parallel_writer, max_streams, log);
        if (pipeline.streams.size() > 1)
        {
            pipeline.firstStream() = std::make_shared<ParallelWritingBlockInputStream>(
                pipeline.streams,
                non_joined_data_stream,
                parallel_writer,
                max_streams,
                log->identifier());
            pipeline.streams.resize(1);
        }
        else // pipeline.streams.size() == 1)
        {
            pipeline.firstStream() = std::make_shared<SerialWritingBlockInputStream>(
                pipeline.firstStream(),
                non_joined_data_stream,
                parallel_writer,
                log->identifier());
        }
    }
    assert(pipeline.streams.size() == 1 && pipeline.streams_with_non_joined_data.empty());
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
} // namespace DB
