// Copyright 2023 PingCAP, Ltd.
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
#include <Flash/Executor/PipelineExecutor.h>
#include <Flash/Pipeline/Pipeline.h>
#include <Flash/Pipeline/Schedule/Events/Event.h>
#include <Flash/Planner/PhysicalPlan.h>
#include <Interpreters/Context.h>

namespace DB
{
PipelineExecutor::PipelineExecutor(
    const MemoryTrackerPtr & memory_tracker_,
    Context & context_,
    const String & req_id)
    : QueryExecutor(memory_tracker_, context_, req_id)
    , status(req_id)
{
    PhysicalPlan physical_plan{context, log->identifier()};
    physical_plan.build(context.getDAGContext()->dag_request());
    physical_plan.outputAndOptimize();
    root_pipeline = physical_plan.toPipeline(status, context);
}

void PipelineExecutor::scheduleEvents()
{
    assert(root_pipeline);
    auto events = root_pipeline->toEvents(status, context, context.getMaxStreams());
    Events sources;
    for (const auto & event : events)
    {
        if (event->prepare())
            sources.push_back(event);
    }
    for (const auto & event : sources)
        event->schedule();
}

void PipelineExecutor::wait()
{
    if (unlikely(context.isTest()))
    {
        // In test mode, a single query should take no more than 5 minutes to execute.
        static std::chrono::minutes timeout(5);
        status.waitFor(timeout);
    }
    else
    {
        status.wait();
    }
}

void PipelineExecutor::consume(ResultHandler & result_handler)
{
    assert(result_handler);
    if (unlikely(context.isTest()))
    {
        // In test mode, a single query should take no more than 5 minutes to execute.
        static std::chrono::minutes timeout(5);
        status.consumeFor(result_handler, timeout);
    }
    else
    {
        status.consume(result_handler);
    }
}

ExecutionResult PipelineExecutor::execute(ResultHandler && result_handler)
{
    if (result_handler)
    {
        ///                                 ┌──get_result_sink
        /// result_handler◄──result_queue◄──┼──get_result_sink
        ///                                 └──get_result_sink

        // The queue size is same as UnionBlockInputStream = concurrency * 5.
        assert(root_pipeline);
        root_pipeline->addGetResultSink(status.toConsumeMode(/*queue_size=*/context.getMaxStreams() * 5));
        scheduleEvents();
        consume(result_handler);
    }
    else
    {
        scheduleEvents();
        wait();
    }
    return status.toExecutionResult();
}

void PipelineExecutor::cancel()
{
    status.cancel();
}

String PipelineExecutor::toString() const
{
    assert(root_pipeline);
    FmtBuffer buffer;
    root_pipeline->toTreeString(buffer);
    return buffer.toString();
}

int PipelineExecutor::estimateNewThreadCount()
{
    return 0;
}

RU PipelineExecutor::collectRequestUnit()
{
    // TODO support collectRequestUnit
    return 0;
}

Block PipelineExecutor::getSampleBlock() const
{
    return root_pipeline->getSampleBlock();
}

BaseRuntimeStatistics PipelineExecutor::getRuntimeStatistics() const
{
    // TODO support getRuntimeStatistics
    BaseRuntimeStatistics runtime_statistics;
    return runtime_statistics;
}
} // namespace DB
