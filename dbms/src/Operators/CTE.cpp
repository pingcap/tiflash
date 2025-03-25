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

#include <mutex>
#include <shared_mutex>
#include <utility>

namespace DB
{
std::pair<bool, Block> CTE::tryGetBlockAt(size_t idx)
{
    std::shared_lock<std::shared_mutex> lock(this->rw_lock);
    auto block_num = this->blocks.size(); // TODO maybe blocks are in disk
    if (block_num <= idx)
        return {this->is_eof, Block()};
    // TODO maybe fetch block from disk
    return {false, this->blocks[idx]};
}

void CTE::pushBlock(const Block & block)
{
    // TODO track memory
    std::unique_lock<std::shared_mutex> lock(this->rw_lock);
    this->blocks.push_back(block); // TODO consider spill
}

void CTE::notifyEOF()
{
    std::unique_lock<std::shared_mutex> lock(this->rw_lock);
    this->is_eof = true;
}
} // namespace DB
