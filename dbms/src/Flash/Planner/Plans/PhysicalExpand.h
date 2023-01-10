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
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Join.h>
#include <tipb/executor.pb.h>

namespace DB
{
class PhysicalExpand : public PhysicalUnary
{
public:
    static PhysicalPlanNodePtr build(
        const Context & context,
        const String & executor_id,
        const LoggerPtr & log,
        const tipb::Expand & expand,
        const PhysicalPlanNodePtr & child);

    PhysicalExpand(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const PhysicalPlanNodePtr & child_,
        const std::shared_ptr<Expand> & shared_expand,
        const Block & sample_block_)
        : PhysicalUnary(executor_id_, PlanType::Expand, schema_, req_id, child_)
        , shared_expand(shared_expand), sample_block(sample_block_){}

    void finalize(const Names & parent_require) override;

    void expandTransform(DAGPipeline & child_pipeline, Context & context);

    const Block & getSampleBlock() const override;

private:
    void transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams) override;
    std::shared_ptr<Expand> shared_expand;
    Block sample_block;
};
}  // namespace DB


