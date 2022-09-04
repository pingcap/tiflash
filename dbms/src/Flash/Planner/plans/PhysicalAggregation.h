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
#include <tipb/executor.pb.h>

namespace DB
{
class PhysicalAggregation : public PhysicalBinary
{
public:
    static PhysicalPlanNodePtr build(
        const Context & context,
        const String & executor_id,
        const LoggerPtr & log,
        const tipb::Aggregation & aggregation,
        const PhysicalPlanNodePtr & child);

    PhysicalAggregation(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const PhysicalPlanNodePtr & partial_,
        const PhysicalPlanNodePtr & final_)
        : PhysicalBinary(executor_id_, PlanType::Aggregation, schema_, req_id, partial_, final_)
    {}

    void finalize(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

    /// the right side is the final side.
    const PhysicalPlanNodePtr & partial() const { return left; }
    const PhysicalPlanNodePtr & final() const { return right; }

    PhysicalPlanNodePtr cloneOne() const override
    {
        auto clone_one = std::make_shared<PhysicalAggregation>(*this);
        return clone_one;
    }

private:
    void transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams) override;
};
} // namespace DB
