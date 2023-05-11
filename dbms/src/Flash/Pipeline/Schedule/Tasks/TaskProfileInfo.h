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
#include <fmt/format.h>

#include <atomic>

namespace DB
{
template <typename UnitType>
class ProfileInfo
{
public:
    UInt64 getCPUExecuteTimeNs() const { return cpu_execute_time_ns; }
    UInt64 getCPUPendingTimeNs() const { return cpu_pending_time_ns; }
    UInt64 getIOExecuteTimeNs() const { return io_execute_time_ns; }
    UInt64 getIOPendingTimeNs() const { return io_pending_time_ns; }
    UInt64 getAwaitTimeNs() const { return await_time_ns; }

    String toJson() const
    {
        return fmt::format(
            R"({{"cpu_execute_time_ns":{},"cpu_pending_time_ns":{},"io_execute_time_ns":{},"io_pending_time_ns":{},"await_time_ns":{}}})",
            cpu_execute_time_ns,
            cpu_pending_time_ns,
            io_execute_time_ns,
            io_pending_time_ns,
            await_time_ns);
    }

protected:
    UnitType cpu_execute_time_ns = 0;
    UnitType cpu_pending_time_ns = 0;
    UnitType io_execute_time_ns = 0;
    UnitType io_pending_time_ns = 0;
    UnitType await_time_ns = 0;
};

class TaskProfileInfo : public ProfileInfo<UInt64>
{
public:
    void startTimer() noexcept;

    UInt64 elapsedFromPrev() noexcept;

    void addCPUExecuteTime(UInt64 value) noexcept;

    void elapsedCPUPendingTime() noexcept;

    void addIOExecuteTime(UInt64 value) noexcept;

    void elapsedIOPendingTime() noexcept;

    void elapsedAwaitTime() noexcept;

private:
    Stopwatch stopwatch{CLOCK_MONOTONIC_COARSE};
};

class QueryProfileInfo : public ProfileInfo<std::atomic_uint64_t>
{
public:
    void merge(const TaskProfileInfo & local_one);
};
} // namespace DB
