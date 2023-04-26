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
namespace
{
template <typename ProfileInfo>
String profileInfoToJson(const ProfileInfo & profile_info)
{
    return fmt::format(
        R"({{"cpu_execute_time_ns":{},"cpu_pending_time_ns":{},"io_execute_time_ns":{},"io_pending_time_ns":{},"await_time_ns":{}}})",
        profile_info.cpu_execute_time,
        profile_info.cpu_pending_time,
        profile_info.io_execute_time,
        profile_info.io_pending_time,
        profile_info.await_time);
}
} // namespace

void LocalTaskProfileInfo::startTimer() noexcept
{
    stopwatch.start();
}

UInt64 LocalTaskProfileInfo::elapsedFromPrev() noexcept
{
    return stopwatch.elapsedFromLastTime();
}

void LocalTaskProfileInfo::addCPUExecuteTime(UInt64 value) noexcept
{
    cpu_execute_time += value;
}

void LocalTaskProfileInfo::elapsedCPUPendingTime() noexcept
{
    cpu_pending_time += elapsedFromPrev();
}

void LocalTaskProfileInfo::addIOExecuteTime(UInt64 value) noexcept
{
    io_execute_time += value;
}

void LocalTaskProfileInfo::elapsedIOPendingTime() noexcept
{
    io_pending_time += elapsedFromPrev();
}

void LocalTaskProfileInfo::elapsedAwaitTime() noexcept
{
    await_time += elapsedFromPrev();
}

String LocalTaskProfileInfo::toJson() const
{
    return profileInfoToJson(*this);
}
} // namespace DB
