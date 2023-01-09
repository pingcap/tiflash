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

#include <Common/MemoryTracker.h>
#include <Flash/Executor/PipelineExecutorStatus.h>
#include <Flash/Pipeline/Schedule/Task.h>

#include <atomic>
#include <memory>
#include <vector>

namespace DB
{
enum class EventStatus
{
    INIT,
    SCHEDULED,
    FINISHED,
};

class Event;
using EventPtr = std::shared_ptr<Event>;
using Events = std::vector<EventPtr>;

class Event : public std::enable_shared_from_this<Event>
{
public:
    Event(PipelineExecutorStatus & exec_status_, MemoryTrackerPtr mem_tracker_)
        : exec_status(exec_status_)
        , mem_tracker(std::move(mem_tracker_))
    {}
    virtual ~Event() = default;

    void addDependency(const EventPtr & dependency);

    // schedule, finishTask and finish maybe called directly in TaskScheduler,
    // so these functions must be noexcept.
    void schedule() noexcept;

    void finishTask() noexcept;

    bool isNonDependent();

    bool isCancelled()
    {
        return exec_status.isCancelled();
    }

    void toError(std::string && err_msg);

    PipelineExecutorStatus & getExecStatus() { return exec_status; }

protected:
    // Returns true meaning no task is scheduled.
    virtual bool scheduleImpl() { return true; }

    virtual void finishImpl() {}

    virtual void finalizeFinish() {}

    void scheduleTask(std::vector<TaskPtr> & tasks);

private:
    void finish() noexcept;

    void addNext(const EventPtr & next);

    void completeDependency();

    void switchStatus(EventStatus from, EventStatus to);

protected:
    PipelineExecutorStatus & exec_status;

    MemoryTrackerPtr mem_tracker;

private:
    std::vector<EventPtr> next_events;

    std::atomic_int32_t unfinished_dependencies{0};

    std::atomic_int32_t unfinished_tasks{0};

    std::atomic<EventStatus> status{EventStatus::INIT};
};
} // namespace DB
