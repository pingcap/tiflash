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

#include <Flash/Pipeline/Task.h>
#include <Operators/OperatorExecutor.h>

namespace DB
{
class Event;
using EventPtr = std::shared_ptr<Event>;

class PipelineTask : public Task
{
public:
    PipelineTask(
        MemoryTrackerPtr mem_tracker_,
        const EventPtr & event_,
        OperatorExecutorPtr && op_executor_)
        : Task(mem_tracker_)
        , event(event_)
        , op_executor(std::move(op_executor_))
    {}

    ~PipelineTask();

protected:
    ExecTaskStatus executeImpl() override;

    ExecTaskStatus awaitImpl() override;

    ExecTaskStatus spillImpl() override;

private:
    EventPtr event;
    OperatorExecutorPtr op_executor;
};
} // namespace DB
