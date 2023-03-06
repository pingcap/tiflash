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
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/Plans/PhysicalAggregationConvergent.h>
#include <Operators/AggregateConvergentSourceOp.h>
#include <Operators/ExpressionTransformOp.h>
#include <Operators/NullSourceOp.h>
#include <Operators/OperatorProfileInfo.h>

namespace DB
{

void PhysicalAggregationConvergent::buildPipelineExec(PipelineExecGroupBuilder & group_builder, Context & context, size_t /*concurrency*/)
{
    aggregate_context->initConvergent();

    if (unlikely(aggregate_context->useNullSource()))
    {
        group_builder.init(1);
        OperatorProfileInfoGroup profile_group;
        profile_group.reserve(group_builder.concurrency);
        group_builder.transform([&](auto & builder) {
            auto source = std::make_unique<NullSourceOp>(
                group_builder.exec_status,
                aggregate_context->getHeader(),
                log->identifier());
            auto profile = std::make_shared<OperatorProfileInfo>();
            source->setProfileInfo(profile);
            profile_group.emplace_back(profile);
            builder.setSourceOp(std::move(source));
        });
        context.getDAGContext()->pipeline_profiles[executor_id].emplace_back(profile_group);
    }
    else
    {
        group_builder.init(aggregate_context->getConvergentConcurrency());
        OperatorProfileInfoGroup profile_group;
        profile_group.reserve(group_builder.concurrency);
        size_t index = 0;
        group_builder.transform([&](auto & builder) {
            auto source = std::make_unique<AggregateConvergentSourceOp>(
                group_builder.exec_status,
                aggregate_context,
                index++,
                log->identifier());
            auto profile = std::make_shared<OperatorProfileInfo>();
            source->setProfileInfo(profile);
            profile_group.emplace_back(profile);
            builder.setSourceOp(std::move(source));
        });
        context.getDAGContext()->pipeline_profiles[executor_id].emplace_back(profile_group);
    }

    if (!expr_after_agg->getActions().empty())
    {
        OperatorProfileInfoGroup profile_group;
        profile_group.reserve(group_builder.concurrency);
        group_builder.transform([&](auto & builder) {
            builder.appendTransformOp(std::make_unique<ExpressionTransformOp>(group_builder.exec_status, log->identifier(), expr_after_agg));
            PhysicalPlanHelper::registerProfileInfo(builder, profile_group);
        });
        context.getDAGContext()->pipeline_profiles[executor_id].emplace_back(profile_group);
    }
}
} // namespace DB
