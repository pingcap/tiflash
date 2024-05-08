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

#include <Flash/Pipeline/Exec/PipelineExec.h>
#include <Flash/Pipeline/Schedule/Tasks/PipelineTaskBase.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>

namespace DB
{
class SimplePipelineTask
    : public Task
    , public PipelineTaskBase
{
public:
    SimplePipelineTask(
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        PipelineExecPtr && pipeline_exec_)
        : Task(exec_context_, req_id, ExecTaskStatus::RUNNING)
        , PipelineTaskBase(std::move(pipeline_exec_))
    {}

protected:
    ExecTaskStatus executeImpl() override { return runExecute(); }

    ExecTaskStatus executeIOImpl() override { return runExecuteIO(); }

    ExecTaskStatus awaitImpl() override { return runAwait(); }

    void notifyImpl() override { runNotify(); }

    void finalizeImpl() override
    {
        runFinalize(profile_info.getCPUPendingTimeNs() + profile_info.getIOPendingTimeNs(), 0);
    }
};
} // namespace DB
