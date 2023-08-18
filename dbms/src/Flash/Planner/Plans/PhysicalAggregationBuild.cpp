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

#include <Flash/Coprocessor/AggregationInterpreterHelper.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Executor/PipelineExecutorStatus.h>
#include <Flash/Planner/Plans/PhysicalAggregationBuild.h>
#include <Interpreters/Context.h>
#include <Operators/AggregateBuildSinkOp.h>

namespace DB
{
void PhysicalAggregationBuild::buildPipelineExecGroup(
    PipelineExecutorStatus & exec_status,
    PipelineExecGroupBuilder & group_builder,
    Context & context,
    size_t /*concurrency*/)
{
    // For fine grained shuffle, PhysicalAggregation will not be broken into AggregateBuild and AggregateConvergent.
    // So only non fine grained shuffle is considered here.
    assert(!fine_grained_shuffle.enable());

    executeExpression(exec_status, group_builder, before_agg_actions, log);

    Block before_agg_header = group_builder.getCurrentHeader();
    size_t concurrency = group_builder.concurrency;
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
}
} // namespace DB
