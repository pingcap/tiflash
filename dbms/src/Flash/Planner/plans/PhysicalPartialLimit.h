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
#include <Transforms/LimitBreaker.h>

namespace DB
{
class PhysicalPartialLimit : public PhysicalUnary
{
public:
    PhysicalPartialLimit(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const PhysicalPlanNodePtr & child_,
        const LimitBreakerPtr & limit_breaker_)
        : PhysicalUnary(executor_id_, PlanType::PartialLimit, schema_, req_id, child_)
        , limit_breaker(limit_breaker_)
    {}

    void finalize(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

    PhysicalPlanNodePtr cloneOne() const override
    {
        auto clone_one = std::make_shared<PhysicalPartialLimit>(*this);
        return clone_one;
    }

    void transform(TransformsPipeline & pipeline, Context & context, size_t concurrency) override;

private:
    void transformImpl(DAGPipeline &, Context &, size_t) override;

    LimitBreakerPtr limit_breaker;
};
} // namespace DB
