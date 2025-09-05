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

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Interpreters/Context.h>
#include <Operators/CTE.h>

#include <cassert>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <utility>

namespace DB
{
void CTE::initCTESpillContextAndPartitionConfig(
    const SpillConfig & spill_config,
    const Block & spill_block_schema,
    UInt64 operator_spill_threshold,
    Context & context)
{
    std::unique_lock<std::shared_mutex> lock(this->rw_lock);
    if (this->cte_spill_context)
        // Initialization has been executed before
        return;

    const auto & query_id_and_cte_id = context.getDAGContext()->getQueryIDAndCTEIDForSink();
    this->cte_spill_context
        = std::make_shared<CTESpillContext>(operator_spill_threshold, query_id_and_cte_id, this->partitions);

    this->partition_config = std::make_shared<CTEPartitionSharedConfig>(
        operator_spill_threshold == 0 ? 0 : operator_spill_threshold / this->partition_num,
        spill_config,
        spill_block_schema,
        query_id_and_cte_id,
        this->cte_spill_context->getLog(),
        this->partition_num);

    for (auto & partition : this->partitions)
        partition->setSharedConfig(this->partition_config);

    context.getDAGContext()->registerOperatorSpillContext(this->cte_spill_context);
}

CTEOpStatus CTE::tryGetBlockAt(size_t cte_reader_id, size_t partition_id, Block & block)
{
    std::shared_lock<std::shared_mutex> rw_lock(this->rw_lock);
    if unlikely (this->is_cancelled)
        return CTEOpStatus::CANCELLED;

    if unlikely (!this->areAllSinksRegistered<false>())
        return CTEOpStatus::SINK_NOT_REGISTERED;

    auto status = this->partitions[partition_id]->tryGetBlock(cte_reader_id, block);
    std::lock_guard<std::mutex> lock(this->mu_test);
    switch (status)
    {
    case CTEOpStatus::OK:
        // TODO delete ---------------------
        {
            auto [iter, _] = this->total_fetch_blocks.insert(std::make_pair(cte_reader_id, 0));
            iter->second.fetch_add(1);
        }
        {
            auto [iter, _] = this->total_fetch_rows.insert(std::make_pair(cte_reader_id, 0));
            iter->second.fetch_add(block.rows());
        }
        // ---------------------
        return status;
    case CTEOpStatus::BLOCK_NOT_AVAILABLE:
        return this->is_eof ? CTEOpStatus::END_OF_FILE : CTEOpStatus::BLOCK_NOT_AVAILABLE;
    default:
        return status;
    }
}
template <bool for_test>
CTEOpStatus CTE::pushBlock(size_t partition_id, const Block & block)
{
    std::shared_lock<std::shared_mutex> rw_lock(this->rw_lock);
    if unlikely (this->is_cancelled)
        return CTEOpStatus::CANCELLED;

    if unlikely (block.rows() == 0)
        return CTEOpStatus::OK;

    // TODO delete ------------------
    this->total_recv_blocks.fetch_add(1);
    this->total_recv_rows.fetch_add(block.rows());
    // ------------------

    return this->partitions[partition_id]->pushBlock<false>(block);
}

CTEOpStatus CTE::getBlockFromDisk(size_t cte_reader_id, size_t partition_id, Block & block)
{
    {
        std::shared_lock<std::shared_mutex> lock(this->rw_lock);
        if unlikely (this->is_cancelled)
            return CTEOpStatus::CANCELLED;
    }

    auto ret = this->partitions[partition_id]->getBlockFromDisk(cte_reader_id, block);
    // ---------------------
    // TODO delete it
    std::lock_guard<std::mutex> lock(this->mu_test);
    if (ret == CTEOpStatus::OK && block)
    {
        {
            auto [iter, _] = this->total_fetch_blocks_in_disk.insert(std::make_pair(cte_reader_id, 0));
            iter->second.fetch_add(1);
        }
        {
            auto [iter, _] = this->total_fetch_rows_in_disk.insert(std::make_pair(cte_reader_id, 0));
            iter->second.fetch_add(block.rows());
        }
    }
    // ---------------------
    return ret;
}

CTEOpStatus CTE::spillBlocks(size_t partition_id)
{
    {
        std::shared_lock<std::shared_mutex> lock(this->rw_lock);
        if unlikely (this->is_cancelled)
            return CTEOpStatus::CANCELLED;
    }

    return this->partitions[partition_id]->spillBlocks(this->total_spilled_blocks, this->total_spilled_rows);
}

void CTE::checkBlockAvailableAndRegisterTask(TaskPtr && task, size_t cte_reader_id, size_t partition_id)
{
    std::shared_lock<std::shared_mutex> rw_lock(this->rw_lock);
    if (this->is_cancelled)
    {
        this->notifyTaskDirectly(partition_id, std::move(task));
        return;
    }

    std::lock_guard<std::mutex> aux_lock(*(this->partitions[partition_id]->aux_lock));
    if (this->partitions[partition_id]->status == CTEPartitionStatus::IN_SPILLING)
    {
        this->notifyTaskDirectly(partition_id, std::move(task));
        return;
    }

    std::lock_guard<std::mutex> lock(*(this->partitions[partition_id]->mu));
    CTEOpStatus status = this->checkBlockAvailableNoLock(cte_reader_id, partition_id);
    if (status == CTEOpStatus::BLOCK_NOT_AVAILABLE)
    {
        this->registerTask(partition_id, std::move(task), NotifyType::WAIT_ON_CTE_READ);
        return;
    }

    this->notifyTaskDirectly(partition_id, std::move(task));
}

void CTE::checkInSpillingAndRegisterTask(TaskPtr && task, size_t partition_id)
{
    std::shared_lock<std::shared_mutex> rw_lock(this->rw_lock);
    if (this->is_cancelled)
    {
        this->notifyTaskDirectly(partition_id, std::move(task));
        return;
    }

    std::lock_guard<std::mutex> aux_lock(*(this->partitions[partition_id]->aux_lock));
    if (this->partitions[partition_id]->status == CTEPartitionStatus::IN_SPILLING)
        this->registerTask(partition_id, std::move(task), NotifyType::WAIT_ON_CTE_IO);
    else
        this->notifyTaskDirectly(partition_id, std::move(task));
}

CTEOpStatus CTE::checkBlockAvailableForTest(size_t cte_reader_id, size_t partition_id)
{
    std::shared_lock<std::shared_mutex> rw_lock(this->rw_lock);
    std::lock_guard<std::mutex> lock(*this->partitions[partition_id]->mu);
    return this->checkBlockAvailableNoLock(cte_reader_id, partition_id);
}

template CTEOpStatus CTE::pushBlock<false>(size_t, const Block &);
template CTEOpStatus CTE::pushBlock<true>(size_t, const Block &);
} // namespace DB
