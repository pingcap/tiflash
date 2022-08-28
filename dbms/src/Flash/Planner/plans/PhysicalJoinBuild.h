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
class PhysicalJoinBuild : public PhysicalUnary
{
public:
    PhysicalJoinBuild(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const PhysicalPlanNodePtr & child_,
        const JoinPtr & join_ptr_,
        const ExpressionActionsPtr & build_side_prepare_actions_)
        : PhysicalUnary(executor_id_, PlanType::JoinBuild, schema_, req_id, child_)
        , join_ptr(join_ptr_)
        , build_side_prepare_actions(build_side_prepare_actions_)
    {}

    void finalize(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

private:
    void buildSideTransform(DAGPipeline & build_pipeline, Context & context, size_t max_streams);

    void transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams) override;

private:
    JoinPtr join_ptr;

    ExpressionActionsPtr build_side_prepare_actions;
};
} // namespace DB
