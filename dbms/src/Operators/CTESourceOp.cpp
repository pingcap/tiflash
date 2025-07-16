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

#include <Flash/Pipeline/Schedule/Tasks/NotifyFuture.h>
#include <Operators/CTE.h>
#include <Operators/CTESourceOp.h>
#include <Operators/Operator.h>

namespace DB
{
void CTESourceOp::operateSuffixImpl()
{
    LOG_DEBUG(log, "finish read {} rows from cte source", total_rows);
}

OperatorStatus CTESourceOp::readImpl(Block & block)
{
    auto ret = this->cte_reader->fetchNextBlock(this->id, block);
    switch (ret)
    {
    case CTEOpStatus::END_OF_FILE:
        this->cte_reader->getResp(this->resp);
        if (this->resp.execution_summaries_size() != 0)
            this->io_profile_info->remote_execution_summary.add(this->resp);
    case CTEOpStatus::OK:
        this->total_rows += block.rows();
        return OperatorStatus::HAS_OUTPUT;
    case CTEOpStatus::BLOCK_NOT_AVAILABLE:
        DB::setNotifyFuture(&(this->notifier));
        return OperatorStatus::WAIT_FOR_NOTIFY;
    case CTEOpStatus::SINK_NOT_REGISTERED:
        this->sw.start();
        return OperatorStatus::WAITING;
    default:
        throw Exception("Should not reach here");
    }
}
} // namespace DB
