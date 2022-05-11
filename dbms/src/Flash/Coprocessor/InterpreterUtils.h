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

#pragma once

#include <Common/Logger.h>
#include <DataStreams/ParallelBlockInputStream.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{
class Context;

void restoreConcurrency(
    DAGPipeline & pipeline,
    size_t concurrency,
    const LoggerPtr & log);

BlockInputStreamPtr combinedNonJoinedDataStream(
    DAGPipeline & pipeline,
    size_t max_threads,
    const LoggerPtr & log,
    bool ignore_block = false);

void executeUnion(
    DAGPipeline & pipeline,
    size_t max_streams,
    const LoggerPtr & log,
    bool ignore_block = false);

template <typename StreamHandler>
void executeParallel(
    DAGPipeline & pipeline,
    size_t max_streams,
    const StreamHandler & stream_handler,
    const LoggerPtr & log)
{
    if (pipeline.streams.size() == 1 && pipeline.streams_with_non_joined_data.empty())
        return;
    auto non_joined_data_stream = combinedNonJoinedDataStream(pipeline, max_streams, log, false);
    if (!pipeline.streams.empty())
    {
        pipeline.firstStream() = std::make_shared<ParallelBlockInputStream>(
            pipeline.streams,
            non_joined_data_stream,
            max_streams,
            stream_handler,
            log->identifier());
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
    const NamesWithAliases & project_cols);
} // namespace DB
