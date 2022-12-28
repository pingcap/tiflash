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

#include <Flash/Pipeline/Event.h>
#include <Flash/Pipeline/PipelineExecutor.h>
#include <Interpreters/Context.h>

namespace DB
{
ExecutionResult PipelineExecutor::execute(ResultHandler result_handler)
{
    assert(root_pipeline);
    // for result_handler.isIgnored(), the sink plan of root_pipeline must be nullptr.
    if (!result_handler.isIgnored())
        root_pipeline->addGetResultSink(result_handler);

    {
        auto events = root_pipeline->toEvents(status, context, context.getMaxStreams());
        Events non_dependent_events;
        for (const auto & event : events)
        {
            if (event->isNonDependent())
                non_dependent_events.push_back(event);
        }
        for (const auto & event : non_dependent_events)
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

    auto err_msg = status.getErrMsg();
    return err_msg.empty()
        ? ExecutionResult::success()
        : ExecutionResult::fail(err_msg);
}

void PipelineExecutor::cancel()
{
    status.cancel();
}

String PipelineExecutor::toString() const
{
    assert(root_pipeline);
    FmtBuffer buffer;
    // just call the root pipeline.
    root_pipeline->toTreeString(buffer);
    return buffer.toString();
}

int PipelineExecutor::estimateNewThreadCount()
{
    return 0;
}
} // namespace DB
