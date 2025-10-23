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

#include <DataStreams/AddExtraTableIDColumnTransformAction.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <Flash/Pipeline/Schedule/Tasks/Impls/RFWaitTask.h>
#include <Operators/UnorderedSourceOp.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

namespace DB
{
UnorderedSourceOp::UnorderedSourceOp(
    PipelineExecutorContext & exec_context_,
    const DM::SegmentReadTaskPoolPtr & task_pool_,
    const DM::ColumnDefines & columns_to_read_,
    int extra_table_id_index_,
    const String & req_id,
    const RuntimeFilterList & runtime_filter_list_,
    int max_wait_time_ms_,
    bool is_disagg_)
    : SourceOp(exec_context_, req_id)
    , task_pool(task_pool_)
    , ref_no(0)
    , waiting_rf_list(runtime_filter_list_)
    , max_wait_time_ms(max_wait_time_ms_)
{
    setHeader(AddExtraTableIDColumnTransformAction::buildHeader(columns_to_read_, extra_table_id_index_));
    ref_no = task_pool->increaseUnorderedInputStreamRefCount();

    if (is_disagg_)
    {
        // One connection for inter zone, the other one for inner zone.
        const size_t connections = 2;
        io_profile_info = IOProfileInfo::createForRemote(profile_info_ptr, connections);
    }
    else
    {
        io_profile_info = IOProfileInfo::createForLocal(profile_info_ptr);
    }
}

UnorderedSourceOp::~UnorderedSourceOp()
{
    if (const auto rc_before_decr = task_pool->decreaseUnorderedInputStreamRefCount(); rc_before_decr == 1)
    {
        LOG_INFO(
            log,
            "All unordered input streams are finished, pool_id={} last_stream_ref_no={}",
            task_pool->pool_id,
            ref_no);
    }
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
            // return HAS_OUTPUT with empty block to indicate end of stream
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
            RuntimeFilterList ready_rf_list;
            RFWaitTask::filterAndMoveReadyRfs(waiting_rf_list, ready_rf_list);

            if (max_wait_time_ms <= 0 || waiting_rf_list.empty())
            {
                RFWaitTask::submitReadyRfsAndSegmentTaskPool(ready_rf_list, task_pool);
            }
            else
            {
                // Poll and check if the RuntimeFilters is ready in the WaitReactor.
                TaskScheduler::instance->submit(std::make_unique<RFWaitTask>(
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

void UnorderedSourceOp::operateSuffixImpl()
{
    // If io_profile_info is local, it means this table scan only read local data.
    // So no need to update connection info, because connection info indicate it's a remote table scan,
    // like CoprocessorReader or read delta data from WN.
    if (!io_profile_info->is_local)
    {
        std::call_once(task_pool->getRemoteConnectionInfoFlag(), [&]() {
            auto pool_connection_info_opt = task_pool->getRemoteConnectionInfo();
            RUNTIME_CHECK(
                pool_connection_info_opt.has_value() || (task_pool->getTotalReadTasks() == 0),
                pool_connection_info_opt.has_value(),
                task_pool->getTotalReadTasks());
            if (pool_connection_info_opt)
            {
                auto & connection_infos = io_profile_info->connection_profile_infos;
                RUNTIME_CHECK(connection_infos.size() == 2, connection_infos.size());

                connection_infos[0] = pool_connection_info_opt->first;
                connection_infos[1] = pool_connection_info_opt->second;
            }
        });
    }
}
} // namespace DB
