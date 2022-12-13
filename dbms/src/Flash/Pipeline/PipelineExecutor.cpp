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
    // for result_handler.isIgnored(), the sink plan of root_pipeline must be nullptr.
    if (!result_handler.isIgnored())
        root_pipeline->addGetResultSink(result_handler);

    {
        auto events = root_pipeline->toEvents(status, context, context.getMaxStreams());
        for (const auto & event : events)
        {
            if (event->isNonDependent())
                event->schedule();
        }
    }
    status.wait();

    auto err_msg = status.getErrMsg();
    return err_msg.empty()
        ? ExecutionResult::success()
        : ExecutionResult::fail(err_msg);
}

void PipelineExecutor::cancel(bool /*is_kill*/)
{
    status.cancel();
}

String PipelineExecutor::dump() const
{
    FmtBuffer buffer;
    assert(root_pipeline);
    // just call the root pipeline.
    root_pipeline->toTreeString(buffer);
    return buffer.toString();
}

int PipelineExecutor::estimateNewThreadCount()
{
    return 0;
}
} // namespace DB
