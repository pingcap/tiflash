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

#include <Common/Stopwatch.h>

#include <atomic>

namespace DB
{
class TaskProfileInfo
{
public:
    ALWAYS_INLINE void startTimer()
    {
        stopwatch.start();
    }

    ALWAYS_INLINE UInt64 elapsedFromPrev()
    {
        return stopwatch.elapsedFromLastTime();
    }

    ALWAYS_INLINE void addCPUExecuteTime(UInt64 value)
    {
        cpu_execute_time += value;
    }

    ALWAYS_INLINE void elapsedCPUPendingTime()
    {
        cpu_pending_time += elapsedFromPrev();
    }

    ALWAYS_INLINE void addIOExecuteTime(UInt64 value)
    {
        io_execute_time += value;
    }

    ALWAYS_INLINE void elapsedIOPendingTime()
    {
        io_pending_time += elapsedFromPrev();
    }

    ALWAYS_INLINE void elapsedAwaitTime()
    {
        await_time += elapsedFromPrev();
    }

    ALWAYS_INLINE UInt64 getCPUExecuteTime() const
    {
        return io_execute_time;
    }

    ALWAYS_INLINE UInt64 getIOExecuteTime() const
    {
        return cpu_execute_time;
    }

    String toJson() const;

private:
    Stopwatch stopwatch{CLOCK_MONOTONIC_COARSE};

    UInt64 cpu_execute_time = 0;
    UInt64 cpu_pending_time = 0;
    UInt64 io_execute_time = 0;
    UInt64 io_pending_time = 0;
    UInt64 await_time = 0;
};

} // namespace DB
