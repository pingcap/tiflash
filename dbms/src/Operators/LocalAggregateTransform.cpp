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

#include <Flash/Executor/PipelineExecutorStatus.h>
#include <Operators/LocalAggregateTransform.h>

namespace DB
{
namespace
{
/// for local agg, the concurrency of build and convert must both be 1.
constexpr size_t local_concurrency = 1;

/// for local agg, the task_index of build and convert must both be 0.
constexpr size_t task_index = 0;
} // namespace

LocalAggregateTransform::LocalAggregateTransform(
    PipelineExecutorStatus & exec_status_,
    const String & req_id,
    const Aggregator::Params & params_)
    : TransformOp(exec_status_, req_id)
    , params(params_)
    , agg_context(req_id)
{
    agg_context.initBuild(params, local_concurrency, /*hook=*/[&]() { return exec_status.isCancelled(); });
}

OperatorStatus LocalAggregateTransform::transformImpl(Block & block)
{
    switch (status)
    {
    case LocalAggStatus::build:
        if (unlikely(!block))
        {
            // status from build to convert.
            status = LocalAggStatus::convert;
            agg_context.initConvergent();
            if likely (!agg_context.useNullSource())
            {
                RUNTIME_CHECK(agg_context.getConvergentConcurrency() == local_concurrency);
                block = agg_context.readForConvergent(task_index);
            }
            return OperatorStatus::HAS_OUTPUT;
        }
        agg_context.buildOnBlock(task_index, block);
        block.clear();
        return OperatorStatus::NEED_INPUT;
    case LocalAggStatus::convert:
        throw Exception("Unexpected status: convert");
    }
}

OperatorStatus LocalAggregateTransform::tryOutputImpl(Block & block)
{
    switch (status)
    {
    case LocalAggStatus::build:
        return OperatorStatus::NEED_INPUT;
    case LocalAggStatus::convert:
        if likely (!agg_context.useNullSource())
            block = agg_context.readForConvergent(task_index);
        return OperatorStatus::HAS_OUTPUT;
    }
}

void LocalAggregateTransform::transformHeaderImpl(Block & header_)
{
    header_ = agg_context.getHeader();
}
} // namespace DB
