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

#pragma once

#include <Flash/Pipeline/Event.h>

namespace DB
{
class PipelineCompleteEvent : public Event
{
public:
    PipelineCompleteEvent(PipelineExecStatus & exec_status_, MemoryTrackerPtr mem_tracker_)
        : Event(exec_status_, std::move(mem_tracker_))
    {}

protected:
    bool scheduleImpl() override
    {
        return !isCancelled();
    }

    void finalizeFinish() override
    {
        // In order to ensure that `exec_status.wait()` doesn't finish when there is an active event,
        // we have to call `exec_status.completePipeline()` at finalizeFinish,
        // since `exec_status.addActivePipeline()` will have been called by the next events.
        // The call order will be `eventA++ ───► eventB++ ───► eventA-- ───► eventB-- ───► exec_status.await finished`.
        exec_status.completePipeline();
    }
};
} // namespace DB
