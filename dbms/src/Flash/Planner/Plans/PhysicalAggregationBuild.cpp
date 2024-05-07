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

#include <Common/FailPoint.h>
#include <Flash/Coprocessor/AggregationInterpreterHelper.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Executor/PipelineExecutorContext.h>
#include <Flash/Pipeline/Schedule/Events/AggregateFinalConvertEvent.h>
#include <Flash/Pipeline/Schedule/Events/AggregateFinalSpillEvent.h>
#include <Flash/Planner/Plans/PhysicalAggregationBuild.h>
#include <Interpreters/Context.h>
#include <Operators/AggregateBuildSinkOp.h>
#include <Operators/AggregateContext.h>

namespace DB
{
namespace FailPoints
{
extern const char force_agg_two_level_hash_table_before_merge[];
} // namespace FailPoints

void PhysicalAggregationBuild::buildPipelineExecGroupImpl(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    Context & context,
    size_t /*concurrency*/)
{
    // For fine grained shuffle, PhysicalAggregation will not be broken into AggregateBuild and AggregateConvergent.
    // So only non fine grained shuffle is considered here.
    RUNTIME_CHECK(!fine_grained_shuffle.enable());

    executeExpression(exec_context, group_builder, before_agg_actions, log);

    Block before_agg_header = group_builder.getCurrentHeader();
    size_t concurrency = group_builder.concurrency();
    AggregationInterpreterHelper::fillArgColumnNumbers(aggregate_descriptions, before_agg_header);
    SpillConfig spill_config(
        context.getTemporaryPath(),
        log->identifier(),
        context.getSettingsRef().max_cached_data_bytes_in_spiller,
        context.getSettingsRef().max_spilled_rows_per_file,
        context.getSettingsRef().max_spilled_bytes_per_file,
        context.getFileProvider(),
        context.getSettingsRef().max_threads,
        context.getSettingsRef().max_block_size);
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
    assert(aggregate_context);
    aggregate_context->initBuild(
        params,
        concurrency,
        /*hook=*/[&]() { return exec_context.isCancelled(); },
        exec_context.getRegisterOperatorSpillContext());

    size_t build_index = 0;
    group_builder.transform([&](auto & builder) {
        builder.setSinkOp(
            std::make_unique<AggregateBuildSinkOp>(exec_context, build_index++, aggregate_context, log->identifier()));
    });

    // The profile info needs to be updated for the second stage's agg final spill.
    profile_infos = group_builder.getCurProfileInfos();
}

EventPtr PhysicalAggregationBuild::doSinkComplete(PipelineExecutorContext & exec_context)
{
    assert(aggregate_context);

    SCOPE_EXIT({ aggregate_context.reset(); });

    aggregate_context->getAggSpillContext()->finishSpillableStage();
    bool need_final_spill = aggregate_context->hasSpilledData();
    if (!need_final_spill)
    {
        for (size_t i = 0; i < aggregate_context->getBuildConcurrency(); ++i)
        {
            if (aggregate_context->getAggSpillContext()->isThreadMarkedForAutoSpill(i))
            {
                need_final_spill = true;
                break;
            }
        }
    }

    if (aggregate_context->isConvertibleToTwoLevel())
    {
        bool need_convert_to_two_level = aggregate_context->hasAtLeastOneTwoLevel();
        fiu_do_on(FailPoints::force_agg_two_level_hash_table_before_merge, { need_convert_to_two_level = true; });
        if (need_convert_to_two_level)
        {
            std::vector<size_t> indexes;
            for (size_t index = 0; index < aggregate_context->getBuildConcurrency(); ++index)
            {
                if (!aggregate_context->isTwoLevelOrEmpty(index))
                    indexes.push_back(index);
            }
            if (!indexes.empty())
            {
                auto final_convert_event = std::make_shared<AggregateFinalConvertEvent>(
                    exec_context,
                    log->identifier(),
                    aggregate_context,
                    std::move(indexes),
                    need_final_spill,
                    std::move(profile_infos));
                return final_convert_event;
            }
        }
    }

    if (need_final_spill)
    {
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
            auto final_spill_event = std::make_shared<AggregateFinalSpillEvent>(
                exec_context,
                log->identifier(),
                aggregate_context,
                std::move(indexes),
                std::move(profile_infos));
            return final_spill_event;
        }
    }

    return nullptr;
}
} // namespace DB
