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
#include <Flash/Planner/Plans/PhysicalAggregationConvergent.h>
#include <Operators/AggregateConvergentSourceOp.h>
#include <Operators/ExpressionTransformOp.h>
#include <Operators/NullSourceOp.h>

namespace DB
{

void PhysicalAggregationConvergent::buildPipelineExec(PipelineExecGroupBuilder & group_builder, Context & /*context*/, size_t /*concurrency*/)
{
    aggregate_context->initConvergent();

    if (unlikely(aggregate_context->useNullSource()))
    {
        group_builder.init(1);
        group_builder.transform([&](auto & builder) {
            builder.setSourceOp(std::make_unique<NullSourceOp>(
                group_builder.exec_status,
                aggregate_context->getHeader(),
                log->identifier()));
        });
    }
    else
    {
        group_builder.init(aggregate_context->getConvergentConcurrency());
        size_t index = 0;
        group_builder.transform([&](auto & builder) {
            builder.setSourceOp(std::make_unique<AggregateConvergentSourceOp>(
                group_builder.exec_status,
                aggregate_context,
                index++,
                log->identifier()));
        });
    }

    if (!expr_after_agg->getActions().empty())
    {
        group_builder.transform([&](auto & builder) {
            builder.appendTransformOp(std::make_unique<ExpressionTransformOp>(group_builder.exec_status, expr_after_agg, log->identifier()));
        });
    }
}
} // namespace DB
