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

#include <Flash/Planner/plans/PhysicalUnary.h>
#include <Interpreters/AggregateStore.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{
class PhysicalPartialAggregation : public PhysicalUnary
{
public:
    PhysicalPartialAggregation(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const PhysicalPlanNodePtr & child_,
        const ExpressionActionsPtr & before_agg_actions_,
        const Names & aggregation_keys_,
        const TiDB::TiDBCollators & aggregation_collators_,
        bool is_final_agg_,
        const AggregateDescriptions & aggregate_descriptions_,
        const AggregateStorePtr & aggregate_store_)
        : PhysicalUnary(executor_id_, PlanType::PartialAggregation, schema_, req_id, child_)
        , before_agg_actions(before_agg_actions_)
        , aggregation_keys(aggregation_keys_)
        , aggregation_collators(aggregation_collators_)
        , is_final_agg(is_final_agg_)
        , aggregate_descriptions(aggregate_descriptions_)
        , aggregate_store(aggregate_store_)
    {}

    void finalize(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

    PhysicalPlanNodePtr cloneOne() const override
    {
        auto clone_one = std::make_shared<PhysicalPartialAggregation>(*this);
        return clone_one;
    }

private:
    void transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams) override;

    ExpressionActionsPtr before_agg_actions;
    Names aggregation_keys;
    TiDB::TiDBCollators aggregation_collators;
    bool is_final_agg;
    AggregateDescriptions aggregate_descriptions;
    AggregateStorePtr aggregate_store;
};
} // namespace DB
