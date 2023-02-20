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
    const ProcessListEntryPtr & process_list_entry_,
    Context & context_,
    const PipelinePtr & root_pipeline_)
    : QueryExecutor(process_list_entry_, context_)
    , root_pipeline(root_pipeline_)
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
        // In test mode, a single query should take no more than 15 seconds to execute.
        std::chrono::seconds timeout(15);
        status.waitFor(timeout);
    }
    else
    {
        status.wait();
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
