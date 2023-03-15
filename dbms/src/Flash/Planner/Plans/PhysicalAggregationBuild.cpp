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
#include <Flash/Planner/Plans/PhysicalAggregationBuild.h>
#include <Interpreters/Context.h>
#include <Operators/AggregateSinkOp.h>
#include <Operators/ExpressionTransformOp.h>

namespace DB
{
void PhysicalAggregationBuild::buildPipelineExec(PipelineExecGroupBuilder & group_builder, Context & context, size_t /*concurrency*/)
{
    if (!before_agg_actions->getActions().empty())
    {
        group_builder.transform([&](auto & builder) {
            builder.appendTransformOp(std::make_unique<ExpressionTransformOp>(group_builder.exec_status, log->identifier(), before_agg_actions));
        });
    }

    size_t build_index = 0;
    group_builder.transform([&](auto & builder) {
        builder.setSinkOp(std::make_unique<AggregateSinkOp>(group_builder.exec_status, build_index++, aggregate_context, log->identifier()));
    });

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
        concurrency,
        aggregation_keys,
        aggregation_collators,
        aggregate_descriptions,
        is_final_agg,
        spill_config);

    aggregate_context->initBuild(params, concurrency);
}
} // namespace DB
