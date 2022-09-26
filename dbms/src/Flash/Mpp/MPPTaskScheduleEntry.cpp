// Copyright 2022 PingCAP, Ltd.
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

#include <Common/CPUAffinityManager.h>
#include <Common/FailPoint.h>
#include <Common/ThreadFactory.h>
#include <Common/ThreadManager.h>
#include <Common/TiFlashMetrics.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/SquashingBlockOutputStream.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Mpp/GRPCReceiverContext.h>
#include <Flash/Mpp/MPPTask.h>
#include <Flash/Mpp/MPPTaskScheduleEntry.h>
#include <Flash/Mpp/MPPTunnelSet.h>
#include <Flash/Mpp/Utils.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <Flash/executeQuery.h>
#include <Interpreters/ProcessList.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/TMTContext.h>
#include <fmt/core.h>

#include <chrono>
#include <ext/scope_guard.h>
#include <magic_enum.hpp>
#include <map>

namespace DB
{
MPPTaskScheduleEntry::MPPTaskScheduleEntry(MPPTaskManager * manager_, const MPPTaskId & id_)
    : manager(manager_)
    , id(id_)
    , needed_threads(0)
    , schedule_state(ScheduleState::WAITING)
    , log(Logger::get("MPPTaskScheduleEntry", id.toString()))
{}

MPPTaskScheduleEntry::~MPPTaskScheduleEntry()
{
    if (schedule_state == ScheduleState::SCHEDULED)
    {
        manager->releaseThreadsFromScheduler(needed_threads);
        schedule_state = ScheduleState::COMPLETED;
    }
}

bool MPPTaskScheduleEntry::schedule(ScheduleState state)
{
    std::unique_lock lock(schedule_mu);
    if (schedule_state == ScheduleState::WAITING)
    {
        LOG_FMT_INFO(log, "task is {}.", state == ScheduleState::SCHEDULED ? "scheduled" : " failed to schedule");
        schedule_state = state;
        schedule_cv.notify_one();
        return true;
    }
    return false;
}

void MPPTaskScheduleEntry::waitForSchedule()
{
    LOG_FMT_INFO(log, "task waits for schedule");
    Stopwatch stopwatch;
    double time_cost = 0;
    {
        std::unique_lock lock(schedule_mu);
        schedule_cv.wait(lock, [&] { return schedule_state != ScheduleState::WAITING; });
        time_cost = stopwatch.elapsedSeconds();
        GET_METRIC(tiflash_task_scheduler_waiting_duration_seconds).Observe(time_cost);

        if (schedule_state == ScheduleState::EXCEEDED)
        {
            throw Exception(fmt::format("{} is failed to schedule because of exceeding the thread hard limit in min-tso scheduler after waiting for {}s.", id.toString(), time_cost));
        }
        else if (schedule_state == ScheduleState::FAILED)
        {
            throw Exception(fmt::format("{} is failed to schedule because of being cancelled in min-tso scheduler after waiting for {}s.", id.toString(), time_cost));
        }
    }
    LOG_FMT_INFO(log, "task waits for {} s to schedule and starts to run in parallel.", time_cost);
}

const MPPTaskId & MPPTaskScheduleEntry::getMPPTaskId() const
{
    return id;
}

int MPPTaskScheduleEntry::getNeededThreads() const
{
    if (needed_threads == 0)
    {
        throw Exception(" the needed_threads of task " + id.toString() + " is not initialized!");
    }
    return needed_threads;
}

void MPPTaskScheduleEntry::setNeededThreads(int needed_threads_)
{
    needed_threads = needed_threads_;
}

} // namespace DB