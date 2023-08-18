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

#include <Common/Logger.h>
#include <Common/MemoryTracker.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>

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

class PipelineExecutorStatus;

class Event : public std::enable_shared_from_this<Event>
{
public:
    Event(
        PipelineExecutorStatus & exec_status_,
        MemoryTrackerPtr mem_tracker_,
        const String & req_id = "")
        : exec_status(exec_status_)
        , mem_tracker(std::move(mem_tracker_))
        , log(Logger::get(req_id))
    {}
    virtual ~Event() = default;

    void addInput(const EventPtr & input);

    // schedule, onTaskFinish and finish maybe called directly in TaskScheduler,
    // so these functions must be noexcept.
    void schedule() noexcept;

    void onTaskFinish() noexcept;

    // return true for source event.
    bool prepareForSource();

protected:
    // Returns the tasks ready to be scheduled.
    virtual std::vector<TaskPtr> scheduleImpl() { return {}; }

    // So far the ownership and the life-cycle of the resources are not very well-defined so we still rely on things like "A must be released before B".
    // And this is the explicit place to release all the resources that need to be cleaned up before event destruction, so that we can satisfy the above constraints.
    virtual void finishImpl() {}

    /// This method can only be called in finishImpl and is used to dynamically adjust the topology of events.
    void insertEvent(const EventPtr & insert_event) noexcept;

private:
    void scheduleTasks(std::vector<TaskPtr> & tasks) noexcept;

    void finish() noexcept;

    void addOutput(const EventPtr & output);

    void onInputFinish() noexcept;

    void switchStatus(EventStatus from, EventStatus to) noexcept;

protected:
    PipelineExecutorStatus & exec_status;

    MemoryTrackerPtr mem_tracker;

    LoggerPtr log;

private:
    Events outputs;

    std::atomic_int32_t unfinished_inputs{0};

    std::atomic_int32_t unfinished_tasks{0};

    std::atomic<EventStatus> status{EventStatus::INIT};

    // is_source is true if and only if there is no input.
    bool is_source = true;
};
} // namespace DB
