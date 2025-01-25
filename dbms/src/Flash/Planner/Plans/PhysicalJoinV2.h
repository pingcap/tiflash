// Copyright 2024 PingCAP, Inc.
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

#include <Flash/Planner/Plans/PhysicalBinary.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/JoinV2/HashJoin.h>
#include <tipb/executor.pb.h>

namespace DB
{
class PhysicalJoinV2 : public PhysicalBinary
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

    PhysicalJoinV2(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const FineGrainedShuffle & fine_grained_shuffle_,
        const String & req_id,
        const PhysicalPlanNodePtr & probe_,
        const PhysicalPlanNodePtr & build_,
        const HashJoinPtr & join_ptr_,
        const ExpressionActionsPtr & probe_side_prepare_actions_,
        const ExpressionActionsPtr & build_side_prepare_actions_)
        : PhysicalBinary(executor_id_, PlanType::Join, schema_, fine_grained_shuffle_, req_id, probe_, build_)
        , join_ptr(join_ptr_)
        , probe_side_prepare_actions(probe_side_prepare_actions_)
        , build_side_prepare_actions(build_side_prepare_actions_)
    {}

    void buildPipeline(PipelineBuilder & builder, Context & context, PipelineExecutorContext & exec_context) override;

    void finalizeImpl(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

    static bool isSupported(const tipb::Join & join);

private:
    void buildBlockInputStreamImpl(DAGPipeline &, Context &, size_t) override { throw Exception("Unsupported"); }

    /// the right side is the build side.
    const PhysicalPlanNodePtr & probe() const { return left; }
    const PhysicalPlanNodePtr & build() const { return right; }

private:
    HashJoinPtr join_ptr;

    ExpressionActionsPtr probe_side_prepare_actions;
    ExpressionActionsPtr build_side_prepare_actions;
};
} // namespace DB
