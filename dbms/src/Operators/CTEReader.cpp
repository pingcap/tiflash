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

#include <Operators/CTE.h>
#include <Operators/CTEPartition.h>
#include <Operators/CTEReader.h>

#include <mutex>

namespace DB
{
CTEOpStatus CTEReader::fetchNextBlock(size_t source_id, Block & block)
{
    auto ret = this->cte->tryGetBlockAt(this->cte_reader_id, source_id, block);
    switch (ret)
    {
    case CTEOpStatus::END_OF_FILE:
    {
        std::lock_guard<std::mutex> lock(this->mu);
        if (this->resp.execution_summaries_size() == 0)
            this->cte->tryToGetResp(this->resp);
    }
    default:
        return ret;
    }
}

#ifndef NDEBUG
CTEOpStatus CTEReader::waitForBlockAvailableForTest(size_t partition_idx)
{
    auto & partition = this->cte->getPartitionForTest(partition_idx);
    auto & cte_rw_lock = this->cte->getRWLockForTest();
    std::shared_lock<std::shared_mutex> rw_lock(cte_rw_lock);
    std::unique_lock<std::mutex> lock(*(partition.mu));
    while (true)
    {
        auto status = this->cte->checkBlockAvailableNoLock(this->cte_reader_id, partition_idx);
        switch (status)
        {
        case CTEOpStatus::BLOCK_NOT_AVAILABLE:
            break;
        case CTEOpStatus::OK:
        case CTEOpStatus::CANCELLED:
        case CTEOpStatus::END_OF_FILE:
            return status;
        default:
            throw Exception("Should not reach here");
        }
        rw_lock.unlock();
        partition.cv_for_test->wait(lock);
        lock.unlock();

        // ensure rw_lock to get lock first, or we will encounter deadlock
        // For example: in `CTE::tryGetBlockAt`, we will lock rw_lock first then lock partition.mu.
        // If locking partition.mu first here, `CTE::tryGetBlockAt` may have locked rw_lock. Then
        // each of them needs to lock the other lock, but the other lock has been locked now.
        rw_lock.lock();
        lock.lock();
    }
}
#endif
} // namespace DB
