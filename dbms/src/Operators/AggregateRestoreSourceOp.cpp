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
#include <Operators/AggregateRestoreSourceOp.h>

namespace DB
{
AggregateRestoreSourceOp::AggregateRestoreSourceOp(
    PipelineExecutorContext & exec_context_,
    const AggregateContextPtr & agg_context_,
    SharedAggregateRestorerPtr && restorer_,
    const String & req_id)
    : SourceOp(exec_context_, req_id)
    , agg_context(agg_context_)
    , restorer(std::move(restorer_))
{
    assert(restorer);
    setHeader(agg_context->getHeader());
}

OperatorStatus AggregateRestoreSourceOp::readImpl(Block & block)
{
    return restorer->tryPop(block) ? OperatorStatus::HAS_OUTPUT : OperatorStatus::WAITING;
}

OperatorStatus AggregateRestoreSourceOp::awaitImpl()
{
    return restorer->tryLoadBucketData() == SharedLoadResult::RETRY ? OperatorStatus::WAITING
                                                                    : OperatorStatus::HAS_OUTPUT;
}

} // namespace DB
