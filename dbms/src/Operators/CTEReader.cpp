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
    std::lock_guard<std::mutex> lock(this->mu);
    if (!this->blocks.empty())
    {
        Block block = std::move(this->blocks.front());
        this->blocks.pop_front();
        return {FetchStatus::Ok, block};
    }

    auto ret = this->cte->tryGetBunchBlocks(this->block_fetch_idx, this->blocks);
    switch (ret)
    {
    case FetchStatus::Eof:
        if (this->resp.execution_summaries_size() == 0)
            this->resp = this->cte->getResp();
    case FetchStatus::Waiting:
    case FetchStatus::Cancelled:
        return {ret, Block()};
    case FetchStatus::Ok:
        this->block_fetch_idx += this->blocks.size();
        Block block = std::move(this->blocks.front());
        this->blocks.pop_front();
        return {ret, block};
    }
    throw Exception("Should not reach here");
}

FetchStatus CTEReader::checkAvailableBlock()
{
    std::lock_guard<std::mutex> lock(this->mu);
    if (!this->blocks.empty())
        return FetchStatus::Ok;

    auto ret = this->cte->tryGetBunchBlocks(this->block_fetch_idx, this->blocks);
    switch (ret)
    {
    case FetchStatus::Waiting:
    case FetchStatus::Cancelled:
    case FetchStatus::Eof:
        return ret;
    case FetchStatus::Ok:
        this->block_fetch_idx += this->blocks.size();
        return FetchStatus::Ok;
    }
    throw Exception("Should not reach here");
}
} // namespace DB
