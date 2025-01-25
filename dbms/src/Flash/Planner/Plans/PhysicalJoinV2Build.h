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

#include <Flash/Planner/Plans/PhysicalUnary.h>
#include <Flash/Planner/Plans/PipelineBreakerHelper.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/JoinV2/HashJoin.h>

namespace DB
{
class PhysicalJoinV2Build : public PhysicalUnary
{
public:
    PhysicalJoinV2Build(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const FineGrainedShuffle & fine_grained_shuffle_,
        const String & req_id,
        const PhysicalPlanNodePtr & child_,
        const HashJoinPtr & join_ptr_,
        const ExpressionActionsPtr & prepare_actions_)
        : PhysicalUnary(executor_id_, PlanType::JoinBuild, schema_, fine_grained_shuffle_, req_id, child_)
        , join_ptr(join_ptr_)
        , prepare_actions(prepare_actions_)
    {
        // The profile info of Join is collected by PhysicalJoinProbe,
        // so calling notTiDBoPerator for PhysicalJoinBuild to skip collecting profile info.
        notTiDBOperator();
    }

private:
    void buildPipelineExecGroupImpl(
        PipelineExecutorContext & exec_context,
        PipelineExecGroupBuilder & group_builder,
        Context & context,
        size_t /*concurrency*/) override;

    EventPtr doSinkComplete(PipelineExecutorContext & exec_context) override;

    DISABLE_USELESS_FUNCTION_FOR_BREAKER

private:
    HashJoinPtr join_ptr;
    ExpressionActionsPtr prepare_actions;
};
} // namespace DB
