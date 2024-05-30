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

#include <Flash/Pipeline/Schedule/Events/Event.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>

namespace DB
{
// The base class of event related task.
class EventTask : public Task
{
public:
    // Only used for unit test.
    EventTask(PipelineExecutorContext & exec_context_, const EventPtr & event_);

    EventTask(
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        const EventPtr & event_,
        ExecTaskStatus init_status = ExecTaskStatus::RUNNING);

protected:
    void finalizeImpl() final;
    virtual void doFinalizeImpl(){};

    UInt64 getScheduleDuration() const;

private:
    EventPtr event;
};

} // namespace DB
