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

#include <Common/FmtUtils.h>
#include <Flash/Pipeline/Schedule/Tasks/TaskProfileInfo.h>

namespace DB
{
void TaskProfileInfo::startTimer() noexcept
{
    stopwatch.start();
}

UInt64 TaskProfileInfo::elapsedFromPrev() noexcept
{
    return stopwatch.elapsedFromLastTime();
}

void TaskProfileInfo::addCPUExecuteTime(UInt64 value) noexcept
{
    cpu_execute_time += value;
}

void TaskProfileInfo::elapsedCPUPendingTime() noexcept
{
    cpu_pending_time += elapsedFromPrev();
}

void TaskProfileInfo::addIOExecuteTime(UInt64 value) noexcept
{
    io_execute_time += value;
}

void TaskProfileInfo::elapsedIOPendingTime() noexcept
{
    io_pending_time += elapsedFromPrev();
}

void TaskProfileInfo::elapsedAwaitTime() noexcept
{
    await_time += elapsedFromPrev();
}

String TaskProfileInfo::toJson() const
{
    return fmt::format(
        R"({{"cpu_execute_time_ns":{},"cpu_pending_time_ns":{},"io_execute_time_ns":{},"io_pending_time_ns":{},"await_time_ns":{}}})",
        cpu_execute_time,
        cpu_pending_time,
        io_execute_time,
        io_pending_time,
        await_time);
}

UInt64 TaskProfileInfo::getCPUExecuteTime() const
{
    return cpu_execute_time;
}

UInt64 TaskProfileInfo::getIOExecuteTime() const
{
    return io_execute_time;
}
} // namespace DB
