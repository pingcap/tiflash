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
CTEOpStatus CTEReader::fetchNextBlock(Block & block)
{
    auto ret = this->cte->tryGetBlockAt(this->cte_reader_id, block);
    switch (ret)
    {
    case CTEOpStatus::Eof:
    {
        std::lock_guard<std::mutex> lock(this->mu);
        if (this->resp.execution_summaries_size() == 0)
            this->cte->tryToGetResp(this->resp);
    }
    case CTEOpStatus::BlockNotAvailable:
    case CTEOpStatus::Cancelled:
    case CTEOpStatus::Ok:
        return ret;
    case DB::CTEOpStatus::Error:
        throw Exception(this->cte->getError());
    }
    throw Exception("Should not reach here");
}
} // namespace DB
