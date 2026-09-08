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

#include <Flash/Pipeline/Schedule/Tasks/TaskTimer.h>
#include <time.h>

namespace DB
{
thread_local TaskTimer * current_task_timer = nullptr;

UInt64 TaskTimer::updateCPUExecutingTime()
{
#if defined(CLOCK_THREAD_CPUTIME_ID)
    timespec ts{};
    if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) != 0)
        return 0;
    const auto current_cpu_time = static_cast<UInt64>(ts.tv_sec) * 1'000'000'000ULL + ts.tv_nsec;
    if (cpu_last_time == 0)
        cpu_last_time = current_cpu_time;
    const auto delta = current_cpu_time >= cpu_last_time ? current_cpu_time - cpu_last_time : 0;
    cpu_last_time = current_cpu_time;
    cpu_executing_time += delta;
    return delta;
#else
    return 0;
#endif
}

void TaskTimer::startCPUTime()
{
#if defined(CLOCK_THREAD_CPUTIME_ID)
    timespec ts{};
    if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) == 0)
        cpu_last_time = static_cast<UInt64>(ts.tv_sec) * 1'000'000'000ULL + ts.tv_nsec;
#endif
}
} // namespace DB
