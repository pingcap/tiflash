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

#include <Flash/Pipeline/Schedule/Tasks/EventTask.h>

namespace DB
{
template <bool is_input>
class IOEventTask : public EventTask
{
public:
    IOEventTask(PipelineExecutorContext & exec_context_, const String & req_id, const EventPtr & event_)
        : EventTask(exec_context_, req_id, event_, is_input ? ExecTaskStatus::IO_IN : ExecTaskStatus::IO_OUT)
    {}

private:
    ReturnStatus executeImpl() final
    {
        if constexpr (is_input)
            return ExecTaskStatus::IO_IN;
        else
            return ExecTaskStatus::IO_OUT;
    }

    ReturnStatus awaitImpl() final
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
