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
    auto * log = &Poco::Logger::get("LRUCache");
    if (!this->blocks.empty())
    {
        Block block = std::move(this->blocks.front());
        this->blocks.pop_front();
        this->output_block_num++;
        this->output_row_num += block.rows();
        if (!block)
            LOG_INFO(log, "xzxdebug output empty block");
        return {FetchStatus::Ok, block};
    }

    auto ret = this->cte->tryGetBunchBlocks(this->block_fetch_idx, this->blocks);
    switch (ret)
    {
    case FetchStatus::Eof:
        if (!this->print_eof)
        {
            LOG_INFO(log, "xzxdebug block_fetch_idx: {}, block num in cte: {}", this->block_fetch_idx, this->cte->blockNumForTest());
            this->print_eof = true;
        }
        if (this->resp.execution_summaries_size() == 0)
            this->cte->tryToGetResp(this->resp);
    case FetchStatus::Waiting:
    case FetchStatus::Cancelled:
        return {ret, Block()};
    case FetchStatus::Ok:
        this->save_block_num += this->blocks.size();
        this->block_fetch_idx += this->blocks.size();
        Block block = std::move(this->blocks.front());
        this->blocks.pop_front();
        this->output_block_num++;
        this->output_row_num += block.rows();
        if (!block)
            LOG_INFO(log, "xzxdebug output empty block");
        return {ret, block};
    }
    throw Exception("Should not reach here");
}

FetchStatus CTEReader::checkAvailableBlock()
{
    std::lock_guard<std::mutex> lock(this->mu);
    if (!this->blocks.empty())
        return FetchStatus::Ok;

    auto * log = &Poco::Logger::get("LRUCache");
    auto ret = this->cte->tryGetBunchBlocks(this->block_fetch_idx, this->blocks);
    switch (ret)
    {
    case FetchStatus::Eof:
        if (!this->print_eof)
        {
            LOG_INFO(log, "xzxdebug block_fetch_idx: {}, block num in cte: {}", this->block_fetch_idx, this->cte->blockNumForTest());
            this->print_eof = true;
        }
    case FetchStatus::Waiting:
    case FetchStatus::Cancelled:
        return ret;
    case FetchStatus::Ok:
        this->save_block_num += this->blocks.size();
        this->block_fetch_idx += this->blocks.size();
        return FetchStatus::Ok;
    }
    throw Exception("Should not reach here");
}
} // namespace DB
