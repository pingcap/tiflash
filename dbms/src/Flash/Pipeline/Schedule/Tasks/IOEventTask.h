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

#include <Flash/Pipeline/Schedule/Tasks/EventTask.h>

namespace DB
{
template <bool is_input>
class IOEventTask : public EventTask
{
public:
    IOEventTask(
        MemoryTrackerPtr mem_tracker_,
        const String & req_id,
        PipelineExecutorStatus & exec_status_,
        const EventPtr & event_)
        : EventTask(std::move(mem_tracker_), req_id, exec_status_, event_)
    {
    }

private:
    ExecTaskStatus doExecuteImpl() override
    {
        if constexpr (is_input)
            return ExecTaskStatus::IO_IN;
        else
            return ExecTaskStatus::IO_OUT;
    }

    ExecTaskStatus doAwaitImpl() override
    {
        if constexpr (is_input)
            return ExecTaskStatus::IO_IN;
        else
            return ExecTaskStatus::IO_OUT;
    }
};

using InputIOEventTask = IOEventTask<true>;
using OutputIOEventTask = IOEventTask<false>;

} // namespace DB
