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

#include <Flash/Planner/Plans/PhysicalUnary.h>
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
        const FineGrainedShuffle & fine_grained_shuffle_,
        const String & req_id,
        const PhysicalPlanNodePtr & child_,
        const std::shared_ptr<const Expand> & shared_expand,
        const ExpressionActionsPtr & expand_actions)
        : PhysicalUnary(executor_id_, PlanType::Expand, schema_, fine_grained_shuffle_, req_id, child_)
        , shared_expand(shared_expand)
        , expand_actions(expand_actions)
    {}

    void finalizeImpl(const Names & parent_require) override;

    void expandTransform(DAGPipeline & child_pipeline);

    const Block & getSampleBlock() const override;

private:
    void buildBlockInputStreamImpl(DAGPipeline & pipeline, Context & context, size_t max_streams) override;

    void buildPipelineExecGroupImpl(
        PipelineExecutorContext & exec_context,
        PipelineExecGroupBuilder & group_builder,
        Context & /*context*/,
        size_t /*concurrency*/) override;

private:
    std::shared_ptr<const Expand> shared_expand;
    ExpressionActionsPtr expand_actions;
};
} // namespace DB
