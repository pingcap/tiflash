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

#include <Common/Logger.h>
#include <Common/MemoryTracker.h>
#include <Flash/Pipeline/Schedule/Tasks/TaskProfileInfo.h>
#include <memory.h>

namespace DB
{
/**
 *           CANCELLED/ERROR/FINISHED
 *                      ▲
 *                      │
 *         ┌────────────────────────┐
 *         │     ┌──►RUNNING◄──┐    │
 * INIT───►│     │             │    │
 *         │     ▼             ▼    │
 *         │ WATITING◄────────►IO   │
 *         └────────────────────────┘
 */
enum class ExecTaskStatus
{
    INIT,
    WAITING,
    RUNNING,
    IO,
    FINISHED,
    ERROR,
    CANCELLED,
};

class Task
{
public:
    Task();

    Task(MemoryTrackerPtr mem_tracker_, const String & req_id);

    virtual ~Task();

    ExecTaskStatus execute();

    ExecTaskStatus executeIO();

    ExecTaskStatus await();

    // `finalize` must be called before destructuring.
    // `TaskHelper::FINALIZE_TASK` can help this.
    void finalize();

    ALWAYS_INLINE void startTraceMemory()
    {
        assert(nullptr == current_memory_tracker);
        assert(0 == CurrentMemoryTracker::getLocalDeltaMemory());
        current_memory_tracker = mem_tracker_ptr;
    }
    ALWAYS_INLINE static void endTraceMemory()
    {
        CurrentMemoryTracker::submitLocalDeltaMemory();
        current_memory_tracker = nullptr;
    }

public:
    LoggerPtr log;

protected:
    virtual ExecTaskStatus executeImpl() = 0;
    virtual ExecTaskStatus executeIOImpl() { return ExecTaskStatus::RUNNING; }
    // Avoid allocating memory in `await` if possible.
    virtual ExecTaskStatus awaitImpl() { return ExecTaskStatus::RUNNING; }

    // Used to release held resources, just like `Event::finishImpl`.
    virtual void finalizeImpl() {}

private:
    inline void switchStatus(ExecTaskStatus to);

public:
    TaskProfileInfo profile_info;

    // level of multi-level feedback queue.
    size_t mlfq_level{0};

protected:
    // To ensure that the memory tracker will not be destructed prematurely and prevent crashes due to accessing invalid memory tracker pointers.
    MemoryTrackerPtr mem_tracker_holder;
    // To reduce the overheads of `mem_tracker_holder.get()`
    MemoryTracker * mem_tracker_ptr;

    ExecTaskStatus task_status{ExecTaskStatus::INIT};

    bool is_finalized = false;
};
using TaskPtr = std::unique_ptr<Task>;

} // namespace DB
