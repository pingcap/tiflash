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
#include <Flash/Pipeline/Schedule/Tasks/Impls/RFWaitTask.h>
#include <Operators/UnorderedSourceOp.h>

namespace DB
{
UnorderedSourceOp::UnorderedSourceOp(
    PipelineExecutorContext & exec_context_,
    const DM::SegmentReadTaskPoolPtr & task_pool_,
    const DM::ColumnDefines & columns_to_read_,
    int extra_table_id_index_,
    const String & req_id,
    const RuntimeFilteList & runtime_filter_list_,
    int max_wait_time_ms_)
    : SourceOp(exec_context_, req_id)
    , task_pool(task_pool_)
    , ref_no(0)
    , waiting_rf_list(runtime_filter_list_)
    , max_wait_time_ms(max_wait_time_ms_)
{
    setHeader(AddExtraTableIDColumnTransformAction::buildHeader(columns_to_read_, extra_table_id_index_));
    ref_no = task_pool->increaseUnorderedInputStreamRefCount();
}

OperatorStatus UnorderedSourceOp::readImpl(Block & block)
{
    if unlikely (done)
        return OperatorStatus::HAS_OUTPUT;

    while (true)
    {
        if (!task_pool->tryPopBlock(block))
        {
            setNotifyFuture(task_pool.get());
            return OperatorStatus::WAIT_FOR_NOTIFY;
        }

        if (block)
        {
            if unlikely (block.rows() == 0)
            {
                block.clear();
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
