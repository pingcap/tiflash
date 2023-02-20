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
#include <Flash/Planner/Plans/PhysicalConvergentAggregation.h>

namespace DB
{

void PhysicalConvergentAggregation::buildPipelineExec(PipelineExecGroupBuilder & group_builder, Context & /*context*/, size_t concurrency)
{
    group_builder.init(concurrency);

    group_builder.transform([&](auto & builder) {
        builder.setSourceOp(std::make_unique<AggregateConvergentSourceOp>(
            group_builder.exec_status,
            aggregate_context,
            log->identifier()));
    });

    if (!expr_after_agg->getActions().empty())
    {
        group_builder.transform([&](auto & builder) {
            builder.appendTransformOp(std::make_unique<ExpressionTransformOp>(group_builder.exec_status, expr_after_agg, log->identifier()));
        });
    }
}
} // namespace DB
