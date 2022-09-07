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

#include <Core/SortDescription.h>
#include <Flash/Planner/plans/PhysicalUnary.h>
#include <Interpreters/ExpressionActions.h>
#include <Transforms/SortBreaker.h>

namespace DB
{
class PhysicalPartialTopN : public PhysicalUnary
{
public:
    PhysicalPartialTopN(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const PhysicalPlanNodePtr & child_,
        const SortDescription & order_descr_,
        const ExpressionActionsPtr & before_sort_actions_,
        size_t limit_,
        const SortBreakerPtr & sort_breaker_)
        : PhysicalUnary(executor_id_, PlanType::PartialTopN, schema_, req_id, child_)
        , order_descr(order_descr_)
        , before_sort_actions(before_sort_actions_)
        , limit(limit_)
        , sort_breaker(sort_breaker_)
    {}

    void finalize(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

    PhysicalPlanNodePtr cloneOne() const override
    {
        auto clone_one = std::make_shared<PhysicalPartialTopN>(*this);
        return clone_one;
    }

    void transform(TransformsPipeline & pipeline, Context & context) override;

private:
    void transformImpl(DAGPipeline &, Context &, size_t) override;

    SortDescription order_descr;
    ExpressionActionsPtr before_sort_actions;
    size_t limit;
    SortBreakerPtr sort_breaker;
};
} // namespace DB
