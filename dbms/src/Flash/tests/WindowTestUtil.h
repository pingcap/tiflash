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

#pragma once

#include <Flash/Coprocessor/DAGQueryBlockInterpreter.h>

namespace DB
{
namespace tests
{

inline std::shared_ptr<DB::DAGQueryBlockInterpreter> mockInterpreter(
    Context & context,
    const std::vector<DB::NameAndTypePair> & source_columns,
    int concurrency)
{
    std::vector<BlockInputStreams> mock_input_streams_vec = {};
    DAGQueryBlock mock_query_block(0, static_cast<const google::protobuf::RepeatedPtrField<tipb::Executor>>(nullptr));
    std::vector<SubqueriesForSets> mock_subqueries_for_sets = {};
    std::shared_ptr<DAGQueryBlockInterpreter> mock_interpreter
        = std::make_shared<DAGQueryBlockInterpreter>(context, mock_input_streams_vec, mock_query_block, concurrency);
    mock_interpreter->analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(source_columns), context);
    return mock_interpreter;
}

inline void mockExecuteProject(
    std::shared_ptr<DAGQueryBlockInterpreter> & mock_interpreter,
    DAGPipeline & pipeline,
    NamesWithAliases & final_project)
{
    mock_interpreter->executeProject(pipeline, final_project);
}

inline void mockExecuteWindowOrder(
    std::shared_ptr<DAGQueryBlockInterpreter> & mock_interpreter,
    DAGPipeline & pipeline,
    const tipb::Sort & sort,
    uint64_t fine_grained_shuffle_stream_count)
{
    mock_interpreter->handleWindowOrder(
        pipeline,
        sort,
        ::DB::enableFineGrainedShuffle(fine_grained_shuffle_stream_count));
    mock_interpreter->input_streams_vec[0] = pipeline.streams;
    NamesWithAliases final_project;
    for (const auto & column : (*mock_interpreter->analyzer).source_columns)
    {
        final_project.push_back({column.name, ""});
    }
    mockExecuteProject(mock_interpreter, pipeline, final_project);
}

inline void mockExecuteWindow(
    std::shared_ptr<DAGQueryBlockInterpreter> & mock_interpreter,
    DAGPipeline & pipeline,
    const tipb::Window & window,
    uint64_t fine_grained_shuffle_stream_count)
{
    mock_interpreter->handleWindow(pipeline, window, ::DB::enableFineGrainedShuffle(fine_grained_shuffle_stream_count));
    mock_interpreter->input_streams_vec[0] = pipeline.streams;
    NamesWithAliases final_project;
    for (const auto & column : (*mock_interpreter->analyzer).source_columns)
    {
        final_project.push_back({column.name, ""});
    }
    mockExecuteProject(mock_interpreter, pipeline, final_project);
}

} // namespace tests
} // namespace DB
