// Copyright 2022 PingCAP, Ltd.
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

#include <Flash/Planner/plans/PhysicalLeaf.h>
#include <Interpreters/AggregateStore.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{
class PhysicalFinalAggregation : public PhysicalLeaf
{
public:
    PhysicalFinalAggregation(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const AggregateStorePtr & aggregate_store_,
        const ExpressionActionsPtr & expr_after_agg_)
        : PhysicalLeaf(executor_id_, PlanType::FinalAggregation, schema_, req_id)
        , aggregate_store(aggregate_store_)
        , expr_after_agg(expr_after_agg_)
    {}

    void finalize(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

    PhysicalPlanNodePtr cloneOne() const override
    {
        auto clone_one = std::make_shared<PhysicalFinalAggregation>(*this);
        return clone_one;
    }

private:
    void transformImpl(DAGPipeline & pipeline, Context & /*context*/, size_t max_streams) override;

    AggregateStorePtr aggregate_store;
    ExpressionActionsPtr expr_after_agg;
};
} // namespace DB
