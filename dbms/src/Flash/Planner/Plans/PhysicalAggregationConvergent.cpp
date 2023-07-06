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
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Planner/Plans/PhysicalAggregationConvergent.h>
#include <Operators/AggregateConvergentSourceOp.h>
#include <Operators/AggregateRestoreSourceOp.h>
#include <Operators/NullSourceOp.h>

namespace DB
{
void PhysicalAggregationConvergent::buildPipelineExecGroupImpl(
    PipelineExecutorStatus & exec_status,
    PipelineExecGroupBuilder & group_builder,
    Context & /*context*/,
    size_t /*concurrency*/)
{
    // For fine grained shuffle, PhysicalAggregation will not be broken into AggregateBuild and AggregateConvergent.
    // So only non fine grained shuffle is considered here.
    RUNTIME_CHECK(!fine_grained_shuffle.enable());

    if (aggregate_context->hasSpilledData())
    {
        auto restorers = aggregate_context->buildSharedRestorer(exec_status);
        for (auto & restorer : restorers)
        {
            group_builder.addConcurrency(
                std::make_unique<AggregateRestoreSourceOp>(
                    exec_status,
                    aggregate_context,
                    std::move(restorer),
                    log->identifier()));
        }
    }
    else
    {
        aggregate_context->initConvergent();
        for (size_t index = 0; index < aggregate_context->getConvergentConcurrency(); ++index)
        {
            group_builder.addConcurrency(
                std::make_unique<AggregateConvergentSourceOp>(
                    exec_status,
                    aggregate_context,
                    index,
                    log->identifier()));
        }
    }

    executeExpression(exec_status, group_builder, expr_after_agg, log);
}
} // namespace DB
