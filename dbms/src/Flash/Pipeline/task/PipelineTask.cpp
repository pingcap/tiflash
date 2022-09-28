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

#include <Common/Exception.h>
#include <Common/MemoryTrackerSetter.h>
#include <Flash/Pipeline/task/PipelineTask.h>

namespace DB
{
PipelineTaskResult PipelineTask::execute()
{
    try
    {
        MemoryTrackerSetter setter(true, getMemTracker());
        switch (status)
        {
        case PipelineTaskStatus::running:
        {
            if (!transforms->execute())
                status = PipelineTaskStatus::finish;
            return running();
        }
        case PipelineTaskStatus::prepare:
        {
            transforms->prepare();
            status = PipelineTaskStatus::running;
            return running();
        }
        case PipelineTaskStatus::finish:
        {
            return transforms->finish() ? finish() : running();
        }
        default:
            return fail("unknown status");
        }
    }
    catch (...)
    {
        return fail(getCurrentExceptionMessage(true));
    }
}

PipelineTaskResult PipelineTask::finish()
{
    return PipelineTaskResult{PipelineTaskResultType::finished, ""};
}
PipelineTaskResult PipelineTask::fail(const String & err_msg)
{
    return PipelineTaskResult{PipelineTaskResultType::error, err_msg};
}
PipelineTaskResult PipelineTask::running()
{
    return PipelineTaskResult{PipelineTaskResultType::running, ""};
}
} // namespace DB
