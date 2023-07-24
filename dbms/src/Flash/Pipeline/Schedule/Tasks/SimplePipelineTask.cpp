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

#include <Flash/Pipeline/Exec/PipelineExecMappingTask.h>
#include <Flash/Pipeline/Schedule/Tasks/SimplePipelineTask.h>

namespace DB
{
SimplePipelineTask::SimplePipelineTask(
    PipelineExecutorContext & exec_context_,
    const String & req_id,
    PipelineExecPtr && pipeline_exec_)
    : Task(exec_context_, req_id, ExecTaskStatus::RUNNING)
    , pipeline_exec_holder(std::move(pipeline_exec_))
    , pipeline_exec(pipeline_exec_holder.get())
{
    RUNTIME_CHECK(pipeline_exec);
    pipeline_exec->executePrefix();
}

ExecTaskStatus SimplePipelineTask::executeImpl()
{
    MAPPING_TASK_EXECUTE(pipeline_exec);
}

ExecTaskStatus SimplePipelineTask::executeIOImpl()
{
    MAPPING_TASK_EXECUTE_IO(pipeline_exec);
}

ExecTaskStatus SimplePipelineTask::awaitImpl()
{
    MAPPING_TASK_AWAIT(pipeline_exec);
}

void SimplePipelineTask::finalizeImpl()
{
    assert(pipeline_exec);
    pipeline_exec->executeSuffix();
    pipeline_exec->finalizeProfileInfo(profile_info.getCPUPendingTimeNs() + profile_info.getIOPendingTimeNs());
    pipeline_exec = nullptr;
    pipeline_exec_holder.reset();
}

} // namespace DB
