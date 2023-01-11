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

#pragma once

#include <Flash/Pipeline/Schedule/Event/Event.h>

namespace DB
{
class Pipeline;
using PipelinePtr = std::shared_ptr<Pipeline>;

// The base class of pipeline related event.
class PipelineEvent : public Event
{
public:
    PipelineEvent(
        PipelineExecutorStatus & exec_status_,
        MemoryTrackerPtr mem_tracker_,
        Context & context_,
        const PipelinePtr & pipeline_)
        : Event(exec_status_, std::move(mem_tracker_))
        , context(context_)
        , pipeline(pipeline_)
    {}

    void finishImpl() override
    {
        // Plan nodes in pipeline hold resources like hash table for join, when destruction they will operate memory tracker in MPP task. But MPP task may get destructed once `exec_status.onEventFinish()` is called.
        // So pipeline needs to be released before `exec_status.onEventFinish()` is called.
        pipeline.reset();
    }

protected:
    Context & context;
    PipelinePtr pipeline;
};
} // namespace DB
