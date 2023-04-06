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

#include <Flash/Executor/PipelineExecutor.h>
#include <Flash/Pipeline/Pipeline.h>
#include <Flash/Pipeline/Schedule/Events/Event.h>
#include <Interpreters/Context.h>

namespace DB
{
PipelineExecutor::PipelineExecutor(
    const MemoryTrackerPtr & memory_tracker_,
    Context & context_,
    const String & req_id,
    const PipelinePtr & root_pipeline_)
    : QueryExecutor(memory_tracker_, context_, req_id)
    , root_pipeline(root_pipeline_)
    , status(req_id)
{
    assert(root_pipeline);
}

ExecutionResult PipelineExecutor::execute(ResultHandler && result_handler)
{
    assert(root_pipeline);
    // for !result_handler.isIgnored(), the sink plan of root_pipeline must be nullptr.
    // TODO Now the result handler for batch cop introduces io blocking, we should find a better implementation of get result sink.
    if (unlikely(!result_handler.isIgnored()))
        root_pipeline->addGetResultSink(std::move(result_handler));

    {
        auto events = root_pipeline->toEvents(status, context, context.getMaxStreams());
        Events without_input_events;
        for (const auto & event : events)
        {
            if (event->withoutInput())
                without_input_events.push_back(event);
        }
        for (const auto & event : without_input_events)
            event->schedule();
    }

    if (unlikely(context.isTest()))
    {
<<<<<<< HEAD
        // In test mode, a single query should take no more than 15 seconds to execute.
        std::chrono::seconds timeout(15);
=======
        // In test mode, a single query should take no more than 5 minutes to execute.
        static std::chrono::minutes timeout(5);
>>>>>>> c35877f6d0 (fix potential deadlock in `CreatingSetBlockInputStream` (#7223))
        status.waitFor(timeout);
    }
    else
    {
        status.wait();
    }
<<<<<<< HEAD
=======
}

void PipelineExecutor::consume(const ResultQueuePtr & result_queue, ResultHandler && result_handler)
{
    Block ret;
    if (unlikely(context.isTest()))
    {
        // In test mode, a single query should take no more than 5 minutes to execute.
        static std::chrono::minutes timeout(5);
        while (result_queue->popTimeout(ret, timeout) == MPMCQueueResult::OK)
            result_handler(ret);
    }
    else
    {
        while (result_queue->pop(ret) == MPMCQueueResult::OK)
            result_handler(ret);
    }
}

ExecutionResult PipelineExecutor::execute(ResultHandler && result_handler)
{
    if (result_handler.isIgnored())
    {
        scheduleEvents();
        wait();
    }
    else
    {
        ///                                 ┌──get_result_sink
        /// result_handler◄──result_queue◄──┼──get_result_sink
        ///                                 └──get_result_sink

        // The queue size is same as UnionBlockInputStream = concurrency * 5.
        auto result_queue = status.registerResultQueue(/*queue_size=*/context.getMaxStreams() * 5);
        assert(root_pipeline);
        root_pipeline->addGetResultSink(result_queue);
        scheduleEvents();
        consume(result_queue, std::move(result_handler));
    }
>>>>>>> c35877f6d0 (fix potential deadlock in `CreatingSetBlockInputStream` (#7223))
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
