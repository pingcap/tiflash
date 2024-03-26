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

#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <fmt/format.h>

#include <atomic>

namespace DB
{
template <typename UnitType>
class ProfileInfo
{
public:
    ALWAYS_INLINE UInt64 getCPUExecuteTimeNs() const { return cpu_execute_time_ns; }
    ALWAYS_INLINE UInt64 getCPUPendingTimeNs() const { return cpu_pending_time_ns; }
    ALWAYS_INLINE UInt64 getIOExecuteTimeNs() const { return io_execute_time_ns; }
    ALWAYS_INLINE UInt64 getIOPendingTimeNs() const { return io_pending_time_ns; }
    ALWAYS_INLINE UInt64 getAwaitTimeNs() const { return await_time_ns; }
    ALWAYS_INLINE UInt64 getWaitForNotifyTimeNs() const { return wait_for_notify_time_ns; }

    ALWAYS_INLINE String toJson() const
    {
        return fmt::format(
            R"({{"cpu_execute_time_ns":{},"cpu_pending_time_ns":{},"io_execute_time_ns":{},"io_pending_time_ns":{},"await_time_ns":{},"wait_for_notify_time_ns":{}}})",
            cpu_execute_time_ns,
            cpu_pending_time_ns,
            io_execute_time_ns,
            io_pending_time_ns,
            await_time_ns,
            wait_for_notify_time_ns);
    }

protected:
    UnitType cpu_execute_time_ns = 0;
    UnitType cpu_pending_time_ns = 0;
    UnitType io_execute_time_ns = 0;
    UnitType io_pending_time_ns = 0;
    UnitType await_time_ns = 0;
    UnitType wait_for_notify_time_ns = 0;
};

class TaskProfileInfo : public ProfileInfo<UInt64>
{
public:
    ALWAYS_INLINE void startTimer() { stopwatch.start(); }

    ALWAYS_INLINE UInt64 elapsedFromPrev() { return stopwatch.elapsedFromLastTime(); }

    ALWAYS_INLINE void addCPUExecuteTime(UInt64 value)
    {
        cpu_execute_time_ns += value;
        if (value > cpu_execute_max_time_ns_per_round)
            cpu_execute_max_time_ns_per_round = value;
    }

    ALWAYS_INLINE void elapsedCPUPendingTime() { cpu_pending_time_ns += elapsedFromPrev(); }

    ALWAYS_INLINE void addIOExecuteTime(UInt64 value)
    {
        io_execute_time_ns += value;
        if (value > io_execute_max_time_ns_per_round)
            io_execute_max_time_ns_per_round = value;
    }

    ALWAYS_INLINE void elapsedIOPendingTime() { io_pending_time_ns += elapsedFromPrev(); }

    ALWAYS_INLINE void elapsedAwaitTime() { await_time_ns += elapsedFromPrev(); }

    ALWAYS_INLINE void elapsedWaitForNotifyTime() { wait_for_notify_time_ns += elapsedFromPrev(); }

    ALWAYS_INLINE void reportMetrics() const
    {
#ifdef __APPLE__
#define REPORT_DURATION_METRICS(type, value_ns)                                          \
    if (auto value_seconds = (value_ns) / 1'000'000'000.0; value_seconds > 0)            \
    {                                                                                    \
        GET_METRIC(tiflash_pipeline_task_duration_seconds, type).Observe(value_seconds); \
    }
#define REPORT_ROUND_METRICS(type, value_ns)                                                               \
    if (auto value_seconds = (value_ns) / 1'000'000'000.0; value_seconds > 0)                              \
    {                                                                                                      \
        GET_METRIC(tiflash_pipeline_task_execute_max_time_seconds_per_round, type).Observe(value_seconds); \
    }
#else
#define REPORT_DURATION_METRICS(type, value_ns)                                                \
    if (auto value_seconds = (value_ns) / 1'000'000'000.0; value_seconds > 0)                  \
    {                                                                                          \
        thread_local auto & metric = GET_METRIC(tiflash_pipeline_task_duration_seconds, type); \
        metric.Observe(value_seconds);                                                         \
    }
#define REPORT_ROUND_METRICS(type, value_ns)                                                                     \
    if (auto value_seconds = (value_ns) / 1'000'000'000.0; value_seconds > 0)                                    \
    {                                                                                                            \
        thread_local auto & metric = GET_METRIC(tiflash_pipeline_task_execute_max_time_seconds_per_round, type); \
        metric.Observe(value_seconds);                                                                           \
    }
#endif

        REPORT_DURATION_METRICS(type_cpu_execute, cpu_execute_time_ns);
        REPORT_DURATION_METRICS(type_cpu_queue, cpu_pending_time_ns);
        REPORT_DURATION_METRICS(type_io_execute, io_execute_time_ns);
        REPORT_DURATION_METRICS(type_io_queue, io_pending_time_ns);
        REPORT_DURATION_METRICS(type_await, await_time_ns);
        REPORT_DURATION_METRICS(type_wait_for_notify, wait_for_notify_time_ns);

        REPORT_ROUND_METRICS(type_cpu, cpu_execute_max_time_ns_per_round);
        REPORT_ROUND_METRICS(type_io, io_execute_max_time_ns_per_round);

#undef REPORT_METRICS
#undef REPORT_ROUND_METRICS
    }

private:
    Stopwatch stopwatch{CLOCK_MONOTONIC_COARSE};

    UInt64 cpu_execute_max_time_ns_per_round = 0;
    UInt64 io_execute_max_time_ns_per_round = 0;
};

class QueryProfileInfo : public ProfileInfo<std::atomic_uint64_t>
{
public:
    ALWAYS_INLINE void merge(const TaskProfileInfo & task_profile_info)
    {
        cpu_execute_time_ns += task_profile_info.getCPUExecuteTimeNs();
        cpu_pending_time_ns += task_profile_info.getCPUPendingTimeNs();
        io_execute_time_ns += task_profile_info.getIOExecuteTimeNs();
        io_pending_time_ns += task_profile_info.getIOPendingTimeNs();
        await_time_ns += task_profile_info.getAwaitTimeNs();
        wait_for_notify_time_ns += task_profile_info.getWaitForNotifyTimeNs();
    }
};
} // namespace DB
