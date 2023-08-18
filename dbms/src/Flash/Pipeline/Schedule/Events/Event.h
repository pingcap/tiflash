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
#include <Common/Stopwatch.h>
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

class PipelineExecutorContext;

class Event : public std::enable_shared_from_this<Event>
{
public:
    explicit Event(PipelineExecutorContext & exec_context_, const String & req_id = "");

    virtual ~Event() = default;

    void addInput(const EventPtr & input);

    void schedule();

    void onTaskFinish(const TaskProfileInfo & task_profile_info);

    // return true for source event.
    bool prepare();

    UInt64 getScheduleDuration() const;

    UInt64 getFinishDuration() const;

protected:
    // add task ready to be scheduled.
    void addTask(TaskPtr && task);

    // Generate the tasks ready to be scheduled and use `addTask` to add the tasks.
    virtual void scheduleImpl() {}

    // So far the ownership and the life-cycle of the resources are not very well-defined so we still rely on things like "A must be released before B".
    // And this is the explicit place to release all the resources that need to be cleaned up before event destruction, so that we can satisfy the above constraints.
    virtual void finishImpl() {}

    /// This method can only be called in finishImpl and is used to dynamically adjust the topology of events.
    void insertEvent(const EventPtr & insert_event);

private:
    void scheduleTasks();

    void finish();

    void addOutput(const EventPtr & output);

    void onInputFinish();

    void switchStatus(EventStatus from, EventStatus to);

    void assertStatus(EventStatus expect) const;

protected:
    PipelineExecutorContext & exec_context;

    MemoryTrackerPtr mem_tracker;

    LoggerPtr log;

private:
    Events outputs;

    std::atomic_int32_t unfinished_inputs{0};

    // hold the tasks that ready to be scheduled.
    std::vector<TaskPtr> tasks;

    std::atomic_int32_t unfinished_tasks{0};

    std::atomic<EventStatus> status{EventStatus::INIT};

    // is_source is true if and only if there is no input.
    bool is_source = true;

    Stopwatch stopwatch{CLOCK_MONOTONIC_COARSE};
    UInt64 schedule_duration = 0;
    UInt64 finish_duration = 0;
};
} // namespace DB
