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
    LOG_DEBUG(log, "finish read {} rows from cte source", total_rows);
}

OperatorStatus CTESourceOp::readImpl(Block & block)
{
    auto res = this->cte_reader->fetchNextBlock();
    switch (res.first)
    {
    case CTEOpStatus::Eof:
        this->cte_reader->getResp(this->resp);
        if (this->resp.execution_summaries_size() != 0)
            this->io_profile_info->remote_execution_summary.add(this->resp);
    case CTEOpStatus::Ok:
        block = res.second;
        this->total_rows += block.rows();
        return OperatorStatus::HAS_OUTPUT;
    case CTEOpStatus::Waiting:
        if likely (this->cte_reader->isBlockGenerated())
        {
            return OperatorStatus::WAITING;
        }
        else
        {
            // CTE has not begun to receive data yet
            // So we need to wait the notify from CTE
            this->cte_reader->setNotifyFuture();
            return OperatorStatus::WAIT_FOR_NOTIFY;
        }
    case CTEOpStatus::Cancelled:
        return OperatorStatus::CANCELLED;
    }
}

OperatorStatus CTESourceOp::awaitImpl()
{
    auto res = this->cte_reader->checkAvailableBlock();
    switch (res)
    {
    case CTEOpStatus::Eof:
    case CTEOpStatus::Ok:
        return OperatorStatus::HAS_OUTPUT;
    case CTEOpStatus::Waiting:
        return OperatorStatus::WAITING;
    case CTEOpStatus::Cancelled:
        return OperatorStatus::CANCELLED;
    }
}
} // namespace DB
