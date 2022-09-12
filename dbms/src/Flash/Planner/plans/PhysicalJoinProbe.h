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

#include <optional>

namespace DB
{
class PhysicalJoinProbe : public PhysicalUnary
{
public:
    PhysicalJoinProbe(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const PhysicalPlanNodePtr & child_,
        const JoinPtr & join_ptr_,
        const NamesAndTypesList & columns_added_by_join_,
        const ExpressionActionsPtr & probe_side_prepare_actions_,
        bool has_non_joined_,
        const Block & sample_block_)
        : PhysicalUnary(executor_id_, PlanType::JoinProbe, schema_, req_id, child_)
        , join_ptr(join_ptr_)
        , columns_added_by_join(columns_added_by_join_)
        , probe_side_prepare_actions(probe_side_prepare_actions_)
        , has_non_joined(has_non_joined_)
        , sample_block(sample_block_)
    {}

    void finalize(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

    std::optional<PhysicalPlanNodePtr> splitNonJoinedPlanNode();

    PhysicalPlanNodePtr cloneOne() const override
    {
        auto clone_one = std::make_shared<PhysicalJoinProbe>(*this);
        return clone_one;
    }

    void transform(TransformsPipeline & pipeline, Context & context, size_t concurrency) override;

private:
    void probeSideTransform(DAGPipeline & probe_pipeline, Context & context, size_t max_streams);

    void doSchemaProject(DAGPipeline & pipeline, Context & context);

    void transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams) override;

private:
    JoinPtr join_ptr;

    NamesAndTypesList columns_added_by_join;

    ExpressionActionsPtr probe_side_prepare_actions;

    bool has_non_joined;

    Block sample_block;
};
} // namespace DB
