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

namespace DB
{
std::pair<FetchStatus, Block> CTEReader::fetchNextBlock()
{
    Block block;
    std::lock_guard<std::mutex> lock(this->mu);
    auto ret = this->cte->tryGetBlockAt(this->block_fetch_idx, block);
    switch (ret)
    {
    case FetchStatus::Eof:
        if (this->resp.execution_summaries_size() == 0)
            this->cte->tryToGetResp(this->resp);
    case FetchStatus::Waiting:
    case FetchStatus::Cancelled:
        return {ret, Block()};
    case FetchStatus::Ok:
        this->block_fetch_idx++;
        return {ret, block};
    }
    throw Exception("Should not reach here");
}

FetchStatus CTEReader::checkAvailableBlock()
{
    Block block;
    std::lock_guard<std::mutex> lock(this->mu);
    auto ret = this->cte->tryGetBlockAt(this->block_fetch_idx, block);
    switch (ret)
    {
    case FetchStatus::Eof:
    case FetchStatus::Waiting:
    case FetchStatus::Cancelled:
        return ret;
    case FetchStatus::Ok:
        // Do not add block_fetch_idx here as we only check if there are available blocks
        return FetchStatus::Ok;
    }
    throw Exception("Should not reach here");
}
} // namespace DB
