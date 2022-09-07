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

#include <Flash/Planner/plans/PhysicalBinary.h>

namespace DB
{
class PhysicalPipelineBreaker : public PhysicalBinary
{
public:
    PhysicalPipelineBreaker(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const PhysicalPlanNodePtr & before_,
        const PhysicalPlanNodePtr & after_)
        : PhysicalBinary(executor_id_, PlanType::PipelineBreaker, schema_, req_id, before_, after_)
    {}

    void finalize(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

    /// the right side is the after side.
    const PhysicalPlanNodePtr & before() const { return left; }
    const PhysicalPlanNodePtr & after() const { return right; }

    PhysicalPlanNodePtr cloneOne() const override
    {
        auto clone_one = std::make_shared<PhysicalPipelineBreaker>(*this);
        return clone_one;
    }

private:
    void transformImpl(DAGPipeline &, Context &, size_t) override;
};
} // namespace DB
