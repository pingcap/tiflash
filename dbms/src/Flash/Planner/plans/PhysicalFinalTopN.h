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
#include <Transforms/SortBreaker.h>

namespace DB
{
class PhysicalFinalTopN : public PhysicalLeaf
{
public:
    PhysicalFinalTopN(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const Block & sample_block_,
        const SortBreakerPtr & sort_breaker_)
        : PhysicalLeaf(executor_id_, PlanType::FinalTopN, schema_, req_id)
        , sample_block(sample_block_)
        , sort_breaker(sort_breaker_)
    {}

    void finalize(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

    PhysicalPlanNodePtr cloneOne() const override
    {
        auto clone_one = std::make_shared<PhysicalFinalTopN>(*this);
        return clone_one;
    }

    void transform(TransformsPipeline & pipeline, Context &, size_t concurrency) override;

private:
    void transformImpl(DAGPipeline &, Context &, size_t) override;

    Block sample_block;

    SortBreakerPtr sort_breaker;
};
} // namespace DB
