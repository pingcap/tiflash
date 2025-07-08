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
#include <Operators/CTEReader.h>

#include <mutex>

#include "Operators/CTEPartition.h"

namespace DB
{
CTEOpStatus CTEReader::fetchNextBlock(size_t partition_id, Block & block)
{
    auto ret = this->cte->tryGetBlockAt(this->cte_reader_id, partition_id, block);
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
    case CTEOpStatus::BLOCK_NOT_AVAILABLE:
    case CTEOpStatus::OK:
        return ret;
    case CTEOpStatus::CANCELLED:
        throw Exception(this->cte->getError());
    }
    throw Exception("Should not reach here");
}

CTEOpStatus CTEReader::fetchBlockFromDisk(size_t partition_id, Block & block)
{
    return this->cte->getBlockFromDisk(this->cte_reader_id, partition_id, block);
}
} // namespace DB
