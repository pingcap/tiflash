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

#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <Flash/Pipeline/Schedule/Tasks/RFWaitTask.h>
#include <Operators/UnorderedSourceOp.h>

#include <memory>

namespace DB
{
ReturnOpStatus UnorderedSourceOp::readImpl(Block & block)
{
    if unlikely (done)
        return OperatorStatus::HAS_OUTPUT;

    auto await_status = doFetchBlock();
    if (await_status.status == OperatorStatus::HAS_OUTPUT)
        std::swap(block, t_block);
    return await_status;
}

ReturnOpStatus UnorderedSourceOp::doFetchBlock()
{
    if (t_block)
        return OperatorStatus::HAS_OUTPUT;

    while (true)
    {
        if (!task_pool->tryPopBlock(t_block))
            return {notify_future};
        if (t_block)
        {
            if unlikely (t_block.rows() == 0)
            {
                t_block.clear();
                continue;
            }
            return OperatorStatus::HAS_OUTPUT;
        }
        else
        {
            done = true;
            return OperatorStatus::HAS_OUTPUT;
        }
    }
}

void UnorderedSourceOp::operatePrefixImpl()
{
    std::call_once(task_pool->addToSchedulerFlag(), [&]() {
        if (waiting_rf_list.empty())
        {
            DM::SegmentReadTaskScheduler::instance().add(task_pool);
        }
        else
        {
            // Check if the RuntimeFilters is ready immediately.
            RuntimeFilteList ready_rf_list;
            RFWaitTask::filterAndMoveReadyRfs(waiting_rf_list, ready_rf_list);

            if (max_wait_time_ms <= 0 || waiting_rf_list.empty())
            {
                RFWaitTask::submitReadyRfsAndSegmentTaskPool(ready_rf_list, task_pool);
            }
            else
            {
                // Poll and check if the RuntimeFilters is ready in the WaitReactor.
                TaskScheduler::instance->submitToWaitReactor(std::make_unique<RFWaitTask>(
                    exec_context,
                    log->identifier(),
                    task_pool,
                    max_wait_time_ms,
                    std::move(waiting_rf_list),
                    std::move(ready_rf_list)));
            }
        }
    });
}
} // namespace DB
