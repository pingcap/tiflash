// Copyright 2024 PingCAP, Inc.
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

#include <Flash/Pipeline/Schedule/Tasks/TaskProfileInfo.h>
#include <common/types.h>

namespace DB
{
static constexpr int64_t YIELD_MAX_TIME_SPENT_NS = 100'000'000L;

struct TaskTimer
{
    TaskProfileInfo & profile_info;
    UInt64 executing_time = 0;
    UInt64 cpu_executing_time = 0;
    UInt64 cpu_last_time = 0;
    /// Wall-clock time this task has spent in the current handleTask() round.
    ///
    /// Samples TaskProfileInfo's CLOCK_MONOTONIC_COARSE stopwatch, so blocking
    /// IO, sleep, lock wait, and being descheduled all count. Used for yield
    /// (`YIELD_MAX_TIME_SPENT_NS`), MLFQ / IO-priority stats, and LAC RU.
    /// Returns the cumulative `executing_time` for this round, not a delta.
    ///
    /// Do not confuse with `updateCPUExecutingTime()`: that uses thread CPU
    /// time (`CLOCK_THREAD_CPUTIME_ID`) for the keyspace CPU quota. A blocked
    /// IO task can accrue a large `executing_time` while CPU time stays near 0.
    UInt64 updateExecutingTime();

    /// Thread CPU time consumed since the previous sample in this round.
    ///
    /// Uses CLOCK_THREAD_CPUTIME_ID, so blocking IO does not count while CPU
    /// spent by columnar IO (decode, decompress, etc.) does. The keyspace CPU
    /// limiter charges this value. Returns the delta since the last sample
    /// (see `cpu_last_time`); `cpu_executing_time` is the round accumulator.
    ///
    /// Independent of `updateExecutingTime()`: different clock, different
    /// consumers, and this returns a delta rather than a cumulative total.
    UInt64 updateCPUExecutingTime();
    void startCPUTime();
};

extern thread_local TaskTimer * current_task_timer;
} // namespace DB
