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

#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Pipeline/Schedule/Tasks/NotifyFuture.h>
#include <Operators/CTE.h>
#include <Operators/CTEPartition.h>
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
        if (this->cte_reader->getResp(this->resp))
            this->io_profile_info->remote_execution_summary.add(this->resp);
    case CTEOpStatus::OK:
        // Blocks are produced by another MPP task and matched only by query_id_and_cte_id,
        // so their schema must be checked against the header declared by this CTE source.
        // Otherwise a mismatched producer block reaches downstream operators unchecked in
        // release builds. The remote exchange path validates decoded blocks the same way.
        if (block)
            assertBlockSchema(this->getHeader(), block, "CTESourceOp");
        this->total_rows += block.rows();
        return OperatorStatus::HAS_OUTPUT;
    case CTEOpStatus::BLOCK_NOT_AVAILABLE:
        DB::setNotifyFuture(&(this->notifier));
        return OperatorStatus::WAIT_FOR_NOTIFY;
    case CTEOpStatus::SINK_NOT_REGISTERED:
        this->sw.start();
        return OperatorStatus::WAITING;
    case CTEOpStatus::CANCELLED:
        throw Exception(this->cte_reader->getCTE()->getError());
    }
}
} // namespace DB
