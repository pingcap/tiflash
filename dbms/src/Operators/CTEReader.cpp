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
std::pair<CTEOpStatus, Block> CTEReader::fetchNextBlock()
{
    Block block;
    std::lock_guard<std::mutex> lock(this->mu);
    auto ret = this->cte->tryGetBlockAt(this->block_fetch_idx, block);
    switch (ret)
    {
    case CTEOpStatus::Eof:
        if (this->resp.execution_summaries_size() == 0)
            this->cte->tryToGetResp(this->resp);
    case CTEOpStatus::BlockNotAvailable:
        this->notifier.setFetchBlockIdx(this->block_fetch_idx);
    case CTEOpStatus::Cancelled:
        return {ret, Block()};
    case CTEOpStatus::Ok:
        this->block_fetch_idx++;
        return {ret, block};
    case DB::CTEOpStatus::Error:
        throw Exception(this->cte->getError());
    }
    throw Exception("Should not reach here");
}
} // namespace DB
