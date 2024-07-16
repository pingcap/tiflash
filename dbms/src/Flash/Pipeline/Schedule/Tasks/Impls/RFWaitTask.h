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

#include <Flash/Coprocessor/RuntimeFilterMgr.h>
#include <Flash/Executor/PipelineExecutorContext.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReadTaskScheduler.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

namespace DB
{
/// Polling in the wait reactor to check whether the runtime filters are ready.
/// Once the maximum check time is reached or all runtime filters are ready,
/// the segment pool will be submitted to the segment read task scheduler for execution.
class RFWaitTask : public Task
{
public:
    RFWaitTask(
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        const DM::SegmentReadTaskPoolPtr & task_pool_,
        int max_wait_time_ms,
        RuntimeFilteList && waiting_rf_list_,
        RuntimeFilteList && ready_rf_list_)
        : Task(exec_context_, req_id, ExecTaskStatus::WAITING)
        , task_pool(task_pool_)
        , max_wait_time_ns(max_wait_time_ms < 0 ? 0 : 1000000UL * max_wait_time_ms)
        , waiting_rf_list(std::move(waiting_rf_list_))
        , ready_rf_list(std::move(ready_rf_list_))
    {}

    static void filterAndMoveReadyRfs(RuntimeFilteList & waiting_rf_list, RuntimeFilteList & ready_rf_list)
    {
        for (auto it = waiting_rf_list.begin(); it != waiting_rf_list.end();)
        {
            if ((*it)->isReady())
            {
                ready_rf_list.push_back(std::move((*it)));
                it = waiting_rf_list.erase(it);
            }
            else if ((*it)->isFailed())
            {
                it = waiting_rf_list.erase(it);
            }
            else
            {
                ++it;
            }
        }
    }

    static void submitReadyRfsAndSegmentTaskPool(
        const RuntimeFilteList & ready_rf_list,
        const DM::SegmentReadTaskPoolPtr & task_pool)
    {
        for (const RuntimeFilterPtr & rf : ready_rf_list)
        {
            auto rs_operator = rf->parseToRSOperator();
            task_pool->appendRSOperator(rs_operator);
        }
        DM::SegmentReadTaskScheduler::instance().add(task_pool);
    }

private:
    ExecTaskStatus executeImpl() override { throw Exception("unreachable"); }

    ExecTaskStatus awaitImpl() override
    {
        filterAndMoveReadyRfs(waiting_rf_list, ready_rf_list);
        if (waiting_rf_list.empty() || stopwatch.elapsed() >= max_wait_time_ns)
        {
            submitReadyRfsAndSegmentTaskPool(ready_rf_list, task_pool);
            return ExecTaskStatus::FINISHED;
        }
        return ExecTaskStatus::WAITING;
    }

private:
    DM::SegmentReadTaskPoolPtr task_pool;

    UInt64 max_wait_time_ns;
    RuntimeFilteList waiting_rf_list;
    RuntimeFilteList ready_rf_list;

    Stopwatch stopwatch{CLOCK_MONOTONIC_COARSE};
};
} // namespace DB
