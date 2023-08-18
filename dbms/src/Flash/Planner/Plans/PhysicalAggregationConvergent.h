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

#pragma once

#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Planner/Plans/PhysicalLeaf.h>
#include <Flash/Planner/Plans/PipelineBreakerHelper.h>
#include <Interpreters/ExpressionActions.h>
#include <Operators/AggregateContext.h>

namespace DB
{
class PhysicalAggregationConvergent : public PhysicalLeaf
{
public:
    PhysicalAggregationConvergent(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const AggregateContextPtr & aggregate_context_,
        const ExpressionActionsPtr & expr_after_agg_)
        : PhysicalLeaf(executor_id_, PlanType::AggregationConvergent, schema_, FineGrainedShuffle{}, req_id)
        , expr_after_agg(expr_after_agg_)
        , aggregate_context(aggregate_context_)
    {}

private:
    void buildPipelineExecGroupImpl(
        PipelineExecutorContext & exec_context,
        PipelineExecGroupBuilder & group_builder,
        Context & /*context*/,
        size_t /*concurrency*/) override;

    DISABLE_USELESS_FUNCTION_FOR_BREAKER

private:
    ExpressionActionsPtr expr_after_agg;
    AggregateContextPtr aggregate_context;
};
} // namespace DB
