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

#include <Operators/CTESinkOp.h>
#include <Operators/Operator.h>

namespace DB
{
void CTESinkOp::operateSuffixImpl()
{
    LOG_DEBUG(log, "finish write with {} rows", this->total_rows);
}

OperatorStatus CTESinkOp::writeImpl(Block && block)
{
    if (!block)
        return OperatorStatus::FINISHED;

    this->total_rows += block.rows();
    if (this->cte->pushBlock<false>(this->id, block))
        return OperatorStatus::NEED_INPUT;
    return OperatorStatus::CANCELLED;
}
} // namespace DB
