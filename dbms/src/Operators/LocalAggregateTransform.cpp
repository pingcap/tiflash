// Copyright 2023 PingCAP, Ltd.
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

#include <magic_enum.hpp>

namespace DB
{
namespace
{
/// for local agg, the concurrency of build and convergent must both be 1.
constexpr size_t local_concurrency = 1;

/// for local agg, the task_index of build and convergent must both be 0.
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
        if unlikely (!block)
        {
            return agg_context.hasSpilledData()
                ? fromBuildToFinalSpillOrRestore()
                : fromBuildToConvergent(block);
        }
        agg_context.buildOnBlock(task_index, block);
        block.clear();
        return tryFromBuildToSpill();
    default:
        throw Exception(fmt::format("Unexpected status: {}", magic_enum::enum_name(status)));
    }
}

OperatorStatus LocalAggregateTransform::fromBuildToConvergent(Block & block)
{
    // status from build to convergent.
    assert(status == LocalAggStatus::build);
    status = LocalAggStatus::convergent;
    agg_context.initConvergent();
    RUNTIME_CHECK(agg_context.getConvergentConcurrency() == local_concurrency);
    block = agg_context.readForConvergent(task_index);
    return OperatorStatus::HAS_OUTPUT;
}

OperatorStatus LocalAggregateTransform::fromBuildToFinalSpillOrRestore()
{
    assert(status == LocalAggStatus::build);
    if (agg_context.needSpill(task_index, /*try_mark_need_spill=*/true))
    {
        status = LocalAggStatus::final_spill;
    }
    else
    {
        restorer = agg_context.buildLocalRestorer();
        status = LocalAggStatus::restore;
    }
    return OperatorStatus::IO;
}

OperatorStatus LocalAggregateTransform::tryFromBuildToSpill()
{
    assert(status == LocalAggStatus::build);
    if (agg_context.needSpill(task_index))
    {
        status = LocalAggStatus::spill;
        return OperatorStatus::IO;
    }
    return OperatorStatus::NEED_INPUT;
}

OperatorStatus LocalAggregateTransform::tryOutputImpl(Block & block)
{
    switch (status)
    {
    case LocalAggStatus::build:
        return OperatorStatus::NEED_INPUT;
    case LocalAggStatus::convergent:
        block = agg_context.readForConvergent(task_index);
        return OperatorStatus::HAS_OUTPUT;
    case LocalAggStatus::restore:
        return restorer->tryPop(block)
            ? OperatorStatus::HAS_OUTPUT
            : OperatorStatus::IO;
    default:
        throw Exception(fmt::format("Unexpected status: {}", magic_enum::enum_name(status)));
    }
}

OperatorStatus LocalAggregateTransform::executeIOImpl()
{
    switch (status)
    {
    case LocalAggStatus::spill:
    {
        agg_context.spillData(task_index);
        status = LocalAggStatus::build;
        return OperatorStatus::NEED_INPUT;
    }
    case LocalAggStatus::final_spill:
    {
        agg_context.spillData(task_index);
        restorer = agg_context.buildLocalRestorer();
        status = LocalAggStatus::restore;
        return OperatorStatus::IO;
    }
    case LocalAggStatus::restore:
    {
        assert(restorer);
        restorer->loadBucketData();
        return OperatorStatus::HAS_OUTPUT;
    }
    default:
        throw Exception(fmt::format("Unexpected status: {}", magic_enum::enum_name(status)));
    }
}

void LocalAggregateTransform::transformHeaderImpl(Block & header_)
{
    header_ = agg_context.getHeader();
}
} // namespace DB
