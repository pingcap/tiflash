// Copyright 2023 PingCAP, Inc.
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
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Join.h>
#include <tipb/executor.pb.h>

namespace DB
{
class PhysicalJoin : public PhysicalBinary
{
public:
    static PhysicalPlanNodePtr build(
        const Context & context,
        const String & executor_id,
        const LoggerPtr & log,
        const tipb::Join & join,
        const FineGrainedShuffle & fine_grained_shuffle,
        const PhysicalPlanNodePtr & left,
        const PhysicalPlanNodePtr & right);

    PhysicalJoin(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const PhysicalPlanNodePtr & probe_,
        const PhysicalPlanNodePtr & build_,
        const JoinPtr & join_ptr_,
        const NamesAndTypesList & columns_added_by_join_,
        const ExpressionActionsPtr & probe_side_prepare_actions_,
        const ExpressionActionsPtr & build_side_prepare_actions_,
        bool has_non_joined_,
        const Block & sample_block_,
        const FineGrainedShuffle & fine_grained_shuffle_)
        : PhysicalBinary(executor_id_, PlanType::Join, schema_, req_id, probe_, build_)
        , join_ptr(join_ptr_)
        , columns_added_by_join(columns_added_by_join_)
        , probe_side_prepare_actions(probe_side_prepare_actions_)
        , build_side_prepare_actions(build_side_prepare_actions_)
        , has_non_joined(has_non_joined_)
        , sample_block(sample_block_)
        , fine_grained_shuffle(fine_grained_shuffle_)
    {}

    void finalize(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

private:
    void probeSideTransform(DAGPipeline & probe_pipeline, Context & context, size_t max_streams);

    void buildSideTransform(DAGPipeline & build_pipeline, Context & context, size_t max_streams);

    void doSchemaProject(DAGPipeline & pipeline, Context & context);

    void transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams) override;

    /// the right side is the build side.
    const PhysicalPlanNodePtr & probe() const { return left; }
    const PhysicalPlanNodePtr & build() const { return right; }

private:
    JoinPtr join_ptr;

    NamesAndTypesList columns_added_by_join;

    ExpressionActionsPtr probe_side_prepare_actions;
    ExpressionActionsPtr build_side_prepare_actions;

    bool has_non_joined;

    Block sample_block;
    FineGrainedShuffle fine_grained_shuffle;
};
} // namespace DB
