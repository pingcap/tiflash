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

#pragma once

#include <Flash/Coprocessor/AggregationInterpreterHelper.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Planner/Plans/PhysicalUnary.h>
#include <Flash/Planner/Plans/PipelineBreakerHelper.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Operators/AggregateContext.h>
#include <Operators/AggregateSinkOp.h>
#include <Operators/ExpressionTransformOp.h>

namespace DB
{
class PhysicalPreAggregation : public PhysicalUnary
{
public:
    PhysicalPreAggregation(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const PhysicalPlanNodePtr & child_,
        const ExpressionActionsPtr & before_agg_actions_,
        const Names & aggregation_keys_,
        const TiDB::TiDBCollators & aggregation_collators_,
        bool is_final_agg_,
        const AggregateDescriptions & aggregate_descriptions_,
        const ExpressionActionsPtr & expr_after_agg_,
        const AggregateContextPtr & agg_context_)
        : PhysicalUnary(executor_id_, PlanType::PreAggregation, schema_, req_id, child_)
        , before_agg_actions(before_agg_actions_)
        , aggregation_keys(aggregation_keys_)
        , aggregation_collators(aggregation_collators_)
        , is_final_agg(is_final_agg_)
        , aggregate_descriptions(aggregate_descriptions_)
        , expr_after_agg(expr_after_agg_)
        , agg_context(agg_context_)
    {}

    void buildPipelineExec(PipelineExecGroupBuilder & group_builder, Context & context, size_t /*concurrency*/) override
    {
        if (!before_agg_actions->getActions().empty())
        {
            group_builder.transform([&](auto & builder) {
                builder.appendTransformOp(std::make_unique<ExpressionTransformOp>(group_builder.exec_status, before_agg_actions, log->identifier()));
            });
        }

        size_t build_index = 0;
        group_builder.transform([&](auto & builder) {
            builder.setSinkOp(std::make_unique<AggregateSinkOp>(group_builder.exec_status, build_index++, agg_context));
        });

        // todo init agg_context
        Block before_agg_header = group_builder.getCurrentHeader();
        size_t concurrency = group_builder.concurrency;
        AggregationInterpreterHelper::fillArgColumnNumbers(aggregate_descriptions, before_agg_header);
        SpillConfig spill_config(context.getTemporaryPath(), fmt::format("{}_aggregation", log->identifier()), context.getSettingsRef().max_spilled_size_per_spill, context.getFileProvider());
        // ywq todo without spill
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

        agg_context->init(params, concurrency);
    }

private:
    DISABLE_USELESS_FUNCTION_FOR_BREAKER

private:
    ExpressionActionsPtr before_agg_actions;
    Names aggregation_keys;
    TiDB::TiDBCollators aggregation_collators;
    bool is_final_agg [[maybe_unused]];
    AggregateDescriptions aggregate_descriptions;
    ExpressionActionsPtr expr_after_agg;
    AggregateContextPtr agg_context;
};
} // namespace DB
