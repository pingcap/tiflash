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
} // namespace DB
