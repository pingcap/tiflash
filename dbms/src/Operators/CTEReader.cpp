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
    case CTEOpStatus::WAIT_SPILL:
    case CTEOpStatus::NEED_SPILL:
    case CTEOpStatus::IO_IN:
    case CTEOpStatus::SINK_NOT_REGISTERED:
    case CTEOpStatus::BLOCK_NOT_AVAILABLE:
    case CTEOpStatus::CANCELLED:
        // TODO merge
        return ret;
    case CTEOpStatus::OK:
        // TODO remove
        this->total_fetch_blocks.fetch_add(1);
        this->total_fetch_rows.fetch_add(block.rows());
        this->total_fetch_from_mem.fetch_add(1);
        return ret;
    }
    throw Exception("Should not reach here");
}

CTEOpStatus CTEReader::fetchBlockFromDisk(size_t source_id, Block & block)
{
    bool expected_bool = false;
    if (this->is_first_log.compare_exchange_weak(expected_bool, true))
        LOG_INFO(this->cte->getLog(), "Begin restore data from disk for cte.");
    auto ret = this->cte->getBlockFromDisk(this->cte_reader_id, source_id, block);
    if (block)
    {
        // TODO remove
        this->total_fetch_blocks.fetch_add(1);
        this->total_fetch_rows.fetch_add(block.rows());
        this->total_fetch_from_disk.fetch_add(1);
    }
    return ret;
}

CTEOpStatus CTEReader::waitForBlockAvailableForTest(size_t partition_idx)
{
    auto & partition = this->cte->getPartitionForTest(partition_idx);
    std::unique_lock<std::mutex> lock(*(partition->mu_for_test));
    while (true)
    {
        partition->cv_for_test->wait(lock);
        auto status = this->cte->checkBlockAvailable(this->cte_reader_id, partition_idx);
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
    }
}
} // namespace DB
