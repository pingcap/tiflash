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

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Planner/PhysicalPlanBuilder.h>
#include <Flash/Planner/Planner.h>
#include <Flash/Planner/optimize.h>
#include <Interpreters/Context.h>

namespace DB
{
Planner::Planner(
    Context & context_,
    const std::vector<BlockInputStreams> & input_streams_vec_,
    const DAGQueryBlock & query_block_,
    size_t max_streams_)
    : context(context_)
    , input_streams_vec(input_streams_vec_)
    , query_block(query_block_)
    , max_streams(max_streams_)
    , log(Logger::get("Planner", dagContext().log ? dagContext().log->identifier() : ""))
{}

BlockInputStreams Planner::execute()
{
    DAGPipeline pipeline;
    executeImpl(pipeline);
    if (!pipeline.streams_with_non_joined_data.empty())
    {
        executeUnion(pipeline, max_streams, log);
        restorePipelineConcurrency(pipeline);
    }
    return pipeline.streams;
}

bool Planner::isSupported(const DAGQueryBlock &)
{
    return false;
}

DAGContext & Planner::dagContext() const
{
    return *context.getDAGContext();
}

void Planner::restorePipelineConcurrency(DAGPipeline & pipeline)
{
    if (query_block.can_restore_pipeline_concurrency)
        restoreConcurrency(pipeline, dagContext().final_concurrency, log);
}

void Planner::executeImpl(DAGPipeline & pipeline)
{
    PhysicalPlanBuilder builder{context, log->identifier()};
    for (const auto & input_streams : input_streams_vec)
    {
        assert(!input_streams.empty());
        builder.buildSource(input_streams.back()->getHeader());
    }

    auto physical_plan = builder.getResult();
    optimize(context, physical_plan);
    physical_plan->transform(pipeline, context, max_streams);
}
} // namespace DB
