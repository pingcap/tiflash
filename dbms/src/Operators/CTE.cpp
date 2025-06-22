// Copyright 2025 PingCAP, Inc.
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

#include <Flash/Pipeline/Schedule/Tasks/Task.h>
#include <Operators/CTE.h>

#include <cassert>
#include <mutex>
#include <shared_mutex>

namespace DB
{
constexpr size_t PARTITION_NUM = 70; // TODO maybe need more tests to select a reasonable value

inline size_t getPartitionID(size_t id)
{
    return id % PARTITION_NUM;
}

void CTE::init()
{
    for (size_t i = 0; i < PARTITION_NUM; i++)
    {
        this->partitions.push_back(CTEPartition());
        this->partitions.back().mu = std::make_unique<std::mutex>();
        this->partitions.back().read_mu = std::make_unique<std::mutex>();
        this->partitions.back().write_mu = std::make_unique<std::mutex>();
        this->partitions.back().pipe_cv = std::make_unique<PipeConditionVariable>();
    }
}

CTEOpStatus CTE::tryGetBlockAt(size_t cte_reader_id, size_t source_id, Block & block)
{
    auto partition_id = getPartitionID(source_id);

    std::shared_lock<std::shared_mutex> rw_lock(this->rw_lock);
    std::lock_guard<std::mutex> read_lock(*this->partitions[partition_id].read_mu);
    std::lock_guard<std::mutex> lock(*this->partitions[partition_id].mu);
    auto status = this->checkBlockAvailableNoLock(cte_reader_id, partition_id);
    if (status != CTEOpStatus::Ok)
        return status;

    auto idx = this->partitions[partition_id].fetch_block_idxs[cte_reader_id].idx++;
    block = this->partitions[partition_id].blocks[idx];
    return status;
}

bool CTE::pushBlock(size_t sink_id, const Block & block)
{
    {
        std::shared_lock<std::shared_mutex> rw_lock(this->rw_lock);
        if unlikely (this->is_cancelled)
            return false;
    }

    if unlikely (block.rows() == 0)
        return true;

    auto partition_id = getPartitionID(sink_id);
    std::lock_guard<std::mutex> write_lock(*this->partitions[partition_id].write_mu);
    std::lock_guard<std::mutex> lock(*this->partitions[partition_id].mu);
    this->partitions[partition_id].memory_usages += block.bytes();
    this->partitions[partition_id].blocks.push_back(block);
    this->partitions[partition_id].pipe_cv->notifyOne();
    return true;
}

void CTE::registerTask(size_t partition_id, TaskPtr && task, NotifyType type)
{
    task->setNotifyType(type);
    this->partitions[partition_id].pipe_cv->registerTask(std::move(task));
}

void CTE::checkBlockAvailableAndRegisterTask(TaskPtr && task, size_t cte_reader_id, size_t source_id)
{
    auto partition_id = getPartitionID(source_id);
    CTEOpStatus status;

    std::shared_lock<std::shared_mutex> rw_lock(this->rw_lock);
    std::lock_guard<std::mutex> lock(*this->partitions[partition_id].mu);
    status = this->checkBlockAvailableNoLock(cte_reader_id, partition_id);

    if (status == CTEOpStatus::BlockNotAvailable)
    {
        this->registerTask(partition_id, std::move(task), NotifyType::WAIT_ON_CTE);
        return;
    }

    this->notifyTaskDirectly(partition_id, std::move(task));
}
} // namespace DB
