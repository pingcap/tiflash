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
#include <Flash/Pipeline/Schedule/Events/Event.h>

namespace DB
{
class FineGrainedPipelineEvent : public Event
{
public:
    FineGrainedPipelineEvent(
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        PipelineExecPtr && pipeline_exec_)
        : Event(exec_context_, req_id)
        , pipeline_exec(std::move(pipeline_exec_))
    {
        RUNTIME_CHECK(pipeline_exec);
    }

protected:
    void scheduleImpl() override;

private:
    // The pipeline exec for executing the specific fine-grained partition.
    PipelineExecPtr pipeline_exec;
};
} // namespace DB
