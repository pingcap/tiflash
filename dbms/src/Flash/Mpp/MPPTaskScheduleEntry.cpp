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

#include <Common/TiFlashMetrics.h>
#include <Flash/Mpp/MPPTaskManager.h>
#include <Flash/Mpp/MPPTaskScheduleEntry.h>
#include <fmt/core.h>

namespace DB
{
MPPTaskScheduleEntry::MPPTaskScheduleEntry(MPPTaskManager * manager_, const MPPTaskId & id_)
    : manager(manager_)
    , id(id_)
    , needed_threads(0)
    , schedule_state(ScheduleState::WAITING)
    , log(Logger::get(id.toString()))
{}

MPPTaskScheduleEntry::~MPPTaskScheduleEntry()
{
    if (schedule_state == ScheduleState::SCHEDULED)
    {
        manager->releaseThreadsFromScheduler(getResourceGroupName(), needed_threads);
        schedule_state = ScheduleState::COMPLETED;
    }
}

bool MPPTaskScheduleEntry::schedule(ScheduleState state)
{
    std::unique_lock lock(schedule_mu);
    if (schedule_state == ScheduleState::WAITING)
    {
        auto log_level = state == ScheduleState::SCHEDULED ? Poco::Message::PRIO_DEBUG : Poco::Message::PRIO_WARNING;
        LOG_IMPL(log, log_level, "task is {}.", state == ScheduleState::SCHEDULED ? "scheduled" : "failed to schedule");
        schedule_state = state;
        schedule_cv.notify_one();
        return true;
    }
    return false;
}

void MPPTaskScheduleEntry::waitForSchedule()
{
    LOG_DEBUG(log, "task waits for schedule");
    Stopwatch stopwatch;
    double time_cost = 0;
    {
        std::unique_lock lock(schedule_mu);
        schedule_cv.wait(lock, [&] { return schedule_state != ScheduleState::WAITING; });
        time_cost = stopwatch.elapsedSeconds();
        GET_RESOURCE_GROUP_METRIC(tiflash_task_scheduler_waiting_duration_seconds, getResourceGroupName())
            .Observe(time_cost);

        if (schedule_state == ScheduleState::EXCEEDED)
        {
            throw Exception(fmt::format(
                "{} is failed to schedule because of exceeding the thread hard limit in min-tso scheduler after "
                "waiting for {}s.",
                id.toString(),
                time_cost));
        }
        else if (schedule_state == ScheduleState::FAILED)
        {
            throw Exception(fmt::format(
                "{} is failed to schedule because of being cancelled in min-tso scheduler after waiting for {}s.",
                id.toString(),
                time_cost));
        }
    }
    LOG_DEBUG(log, "task waits for {} s to schedule and starts to run in parallel.", time_cost);
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
