// Copyright 2023 PingCAP, Inc.
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

#include <Operators/AggregateContext.h>
#include <Operators/AggregateConvergentSourceOp.h>

namespace DB
{
AggregateConvergentSourceOp::AggregateConvergentSourceOp(
    PipelineExecutorContext & exec_context_,
    const AggregateContextPtr & agg_context_,
    size_t index_,
    const String & req_id)
    : SourceOp(exec_context_, req_id)
    , agg_context(agg_context_)
    , index(index_)
{
    setHeader(agg_context->getHeader());
}

OperatorStatus AggregateConvergentSourceOp::readImpl(Block & block)
{
    if unlikely (!agg_context->convertPendingDataToTwoLevel())
    {
        setNotifyFuture(agg_context);
        return OperatorStatus::WAIT_FOR_NOTIFY;
    }
    block = agg_context->readForConvergent(index);
    total_rows += block.rows();
    return OperatorStatus::HAS_OUTPUT;
}

void AggregateConvergentSourceOp::operateSuffixImpl()
{
    LOG_DEBUG(log, "finish read {} rows from aggregate context", total_rows);
}

} // namespace DB
