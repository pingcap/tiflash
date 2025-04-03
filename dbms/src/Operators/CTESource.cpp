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
#include <Operators/CTESource.h>
#include <Operators/Operator.h>

namespace DB
{
void CTESourceOp::operateSuffixImpl()
{
    this->cte.reset();
    this->cte_manager->releaseCTE(this->query_id_and_cte_id);
    LOG_DEBUG(log, "finish read {} rows from cte source", total_rows);
}

// TODO in some cases, source needs to manually filter some data when cte saves all data
OperatorStatus CTESourceOp::readImpl(Block & block)
{
    // TODO when spill is triggered, this function may be blocked by spill. fix it
    auto res = this->cte->tryGetBlockAt(this->block_fetch_idx);
    switch (res.first)
    {
    case DB::FetchStatus::Eof:
    case DB::FetchStatus::Ok:
        block = res.second;
        ++(this->block_fetch_idx);
        return OperatorStatus::HAS_OUTPUT;
    case DB::FetchStatus::Waiting:
        if unlikely (this->block_fetch_idx == 0)
            // CTE has not begun to receive data yet when block_fetch_idx == 0
            // So we need to wait the notify from CTE
            return OperatorStatus::WAIT_FOR_NOTIFY;
        else
            return OperatorStatus::WAITING;
    case DB::FetchStatus::Cancelled:
        return OperatorStatus::CANCELLED;
    }
}

OperatorStatus CTESourceOp::awaitImpl()
{
    // TODO when spill is triggered, this function may be blocked by spill. fix it
    auto res = this->cte->checkAvailableBlockAt(this->block_fetch_idx);
    switch (res)
    {
    case DB::FetchStatus::Eof:
    case DB::FetchStatus::Ok:
        // Do not add block_fetch_idx here, as we just judge if there are available blocks
        return OperatorStatus::HAS_OUTPUT;
    case DB::FetchStatus::Waiting:
        return OperatorStatus::WAITING;
    case DB::FetchStatus::Cancelled:
        return OperatorStatus::CANCELLED;
    }
}
} // namespace DB
