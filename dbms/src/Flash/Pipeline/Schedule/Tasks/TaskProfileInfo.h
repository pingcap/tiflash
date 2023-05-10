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
    void startTimer() noexcept;

    UInt64 elapsedFromPrev() noexcept;

    void addCPUExecuteTime(UInt64 value) noexcept;

    void elapsedCPUPendingTime() noexcept;

    void addIOExecuteTime(UInt64 value) noexcept;

    void elapsedIOPendingTime() noexcept;

    void elapsedAwaitTime() noexcept;

    String toJson() const;

    UInt64 getCPUExecuteTime() const;
    UInt64 getIOExecuteTime() const;

private:
    Stopwatch stopwatch{CLOCK_MONOTONIC_COARSE};

    UInt64 cpu_execute_time = 0;
    UInt64 cpu_pending_time = 0;
    UInt64 io_execute_time = 0;
    UInt64 io_pending_time = 0;
    UInt64 await_time = 0;
};

} // namespace DB
