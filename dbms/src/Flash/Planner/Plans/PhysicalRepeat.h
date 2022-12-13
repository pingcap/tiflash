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
class PhysicalRepeat : public PhysicalUnary
{
public:
    static PhysicalPlanNodePtr build(
        const Context & context,
        const String & executor_id,
        const LoggerPtr & log,
        const tipb::RepeatSource & repeat,
        const PhysicalPlanNodePtr & child);

    PhysicalRepeat(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const PhysicalPlanNodePtr & child_,
        const std::shared_ptr<Repeat> & shared_repeat,
        const Block & sample_block_)
        : PhysicalUnary(executor_id_, PlanType::Repeat, schema_, req_id, child_)
        , shared_repeat(shared_repeat), sample_block(sample_block_){}

    void finalize(const Names & parent_require) override;

    void repeatTransform(DAGPipeline & child_pipeline, Context & context);

    const Block & getSampleBlock() const override;

private:
    void transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams) override;
    std::shared_ptr<Repeat> shared_repeat;
    Block sample_block;
};
}  // namespace DB


