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
#include <Flash/Pipeline/Schedule/Tasks/EventTask.h>
#include <Flash/Pipeline/Schedule/Tasks/PipelineTaskBase.h>

namespace DB
{
class PipelineTask
    : public EventTask
    , public PipelineTaskBase
{
public:
    PipelineTask(
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        const EventPtr & event_,
        PipelineExecPtr && pipeline_exec_)
        : EventTask(exec_context_, req_id, event_, ExecTaskStatus::RUNNING)
        , PipelineTaskBase(std::move(pipeline_exec_))
    {}

protected:
    ExecTaskStatus executeImpl() override { return runExecute(); }

    ExecTaskStatus executeIOImpl() override { return runExecuteIO(); }

    ExecTaskStatus awaitImpl() override { return runAwait(); }

    void doFinalizeImpl() override
    {
        runFinalize(profile_info.getCPUPendingTimeNs() + profile_info.getIOPendingTimeNs() + getScheduleDuration());
    }
};
} // namespace DB
