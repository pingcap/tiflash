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

#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <Flash/Pipeline/Schedule/Tasks/RFWaitTask.h>
#include <Operators/UnorderedSourceOp.h>

#include <memory>

namespace DB
{
OperatorStatus UnorderedSourceOp::readImpl(Block & block)
{
    auto await_status = awaitImpl();
    if (await_status == OperatorStatus::HAS_OUTPUT)
    {
        if (t_block.has_value())
        {
            std::swap(block, t_block.value());
            t_block.reset();
        }
    }
    return await_status;
}

OperatorStatus UnorderedSourceOp::awaitImpl()
{
    if (t_block.has_value())
        return OperatorStatus::HAS_OUTPUT;
    while (true)
    {
        Block res;
        if (!task_pool->tryPopBlock(res))
            return OperatorStatus::WAITING;
        if (res)
        {
            if (unlikely(res.rows() == 0))
                continue;
            t_block.emplace(std::move(res));
            return OperatorStatus::HAS_OUTPUT;
        }
        else
            return OperatorStatus::HAS_OUTPUT;
    }
}

void UnorderedSourceOp::operatePrefixImpl()
{
    std::call_once(task_pool->addToSchedulerFlag(), [&]() {
        if (runtime_filter_list.empty())
        {
            DM::SegmentReadTaskScheduler::instance().add(task_pool);
        }
        else
        {
            if (max_wait_time_ms <= 0)
            {
                // Check if the RuntimeFilters is ready immediately.
                RuntimeFilteList ready_rf_list;
                for (const RuntimeFilterPtr & rf : runtime_filter_list)
                {
                    if (rf->isReady())
                        ready_rf_list.push_back(rf);
                }
                for (const RuntimeFilterPtr & rf : ready_rf_list)
                {
                    auto rs_operator = rf->parseToRSOperator(task_pool->getColumnToRead());
                    task_pool->appendRSOperator(rs_operator);
                }
                DM::SegmentReadTaskScheduler::instance().add(task_pool);
            }
            else
            {
                // Poll and check if the RuntimeFilters is ready in the WaitReactor.
                TaskScheduler::instance->submitToWaitReactor(std::make_unique<RFWaitTask>(log->identifier(), exec_status, task_pool, max_wait_time_ms, std::move(runtime_filter_list)));
            }
        }
    });
}
} // namespace DB
