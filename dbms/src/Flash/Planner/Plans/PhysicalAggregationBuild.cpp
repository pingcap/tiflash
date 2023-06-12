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

#include <Flash/Coprocessor/AggregationInterpreterHelper.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Executor/PipelineExecutorStatus.h>
#include <Flash/Pipeline/Schedule/Events/AggregateFinalSpillEvent.h>
#include <Flash/Planner/Plans/PhysicalAggregationBuild.h>
#include <Interpreters/Context.h>
#include <Operators/AggregateBuildSinkOp.h>
#include <Operators/AggregateContext.h>

namespace DB
{
void PhysicalAggregationBuild::buildPipelineExecGroupImpl(
    PipelineExecutorStatus & exec_status,
    PipelineExecGroupBuilder & group_builder,
    Context & context,
    size_t /*concurrency*/)
{
    // For fine grained shuffle, PhysicalAggregation will not be broken into AggregateBuild and AggregateConvergent.
    // So only non fine grained shuffle is considered here.
    RUNTIME_CHECK(!fine_grained_shuffle.enable());

    executeExpression(exec_status, group_builder, before_agg_actions, log);

    Block before_agg_header = group_builder.getCurrentHeader();
    size_t concurrency = group_builder.concurrency();
    AggregationInterpreterHelper::fillArgColumnNumbers(aggregate_descriptions, before_agg_header);
    SpillConfig spill_config(
        context.getTemporaryPath(),
        fmt::format("{}_aggregation", log->identifier()),
        context.getSettingsRef().max_cached_data_bytes_in_spiller,
        context.getSettingsRef().max_spilled_rows_per_file,
        context.getSettingsRef().max_spilled_bytes_per_file,
        context.getFileProvider());
    auto params = AggregationInterpreterHelper::buildParams(
        context,
        before_agg_header,
        concurrency,
        1,
        aggregation_keys,
        aggregation_collators,
        aggregate_descriptions,
        is_final_agg,
        spill_config);
    aggregate_context->initBuild(params, concurrency, /*hook=*/[&]() { return exec_status.isCancelled(); });

    size_t build_index = 0;
    group_builder.transform([&](auto & builder) {
        builder.setSinkOp(std::make_unique<AggregateBuildSinkOp>(exec_status, build_index++, aggregate_context, log->identifier()));
    });

    // The profile info needs to be updated for the second stage's agg final spill.
    profile_infos = group_builder.getCurProfileInfos();
}

EventPtr PhysicalAggregationBuild::doSinkComplete(PipelineExecutorStatus & exec_status)
{
    if (!aggregate_context->hasSpilledData())
        return nullptr;

    /// Currently, the aggregation spill algorithm requires all bucket data to be spilled,
    /// so a new event is added here to execute the final spill.
    /// ...──►AggregateBuildSinkOp[local spill]──┐
    /// ...──►AggregateBuildSinkOp[local spill]──┤                                         ┌──►AggregateFinalSpillTask
    /// ...──►AggregateBuildSinkOp[local spill]──┼──►[final spill]AggregateFinalSpillEvent─┼──►AggregateFinalSpillTask
    /// ...──►AggregateBuildSinkOp[local spill]──┤                                         └──►AggregateFinalSpillTask
    /// ...──►AggregateBuildSinkOp[local spill]──┘
    std::vector<size_t> indexes;
    for (size_t index = 0; index < aggregate_context->getBuildConcurrency(); ++index)
    {
        if (aggregate_context->needSpill(index, /*try_mark_need_spill=*/true))
            indexes.push_back(index);
    }
    if (!indexes.empty())
    {
        auto mem_tracker = current_memory_tracker ? current_memory_tracker->shared_from_this() : nullptr;
        return std::make_shared<AggregateFinalSpillEvent>(exec_status, mem_tracker, log->identifier(), aggregate_context, std::move(indexes), std::move(profile_infos));
    }
    return nullptr;
}
} // namespace DB
