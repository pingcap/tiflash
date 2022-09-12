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

#include <Flash/Planner/plans/PhysicalLeaf.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Join.h>

namespace DB
{
class PhysicalNonJoinProbe : public PhysicalLeaf
{
public:
    PhysicalNonJoinProbe(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const JoinPtr & join_ptr_,
        const Block & probe_side_prepare_header_,
        const Block & sample_block_)
        : PhysicalLeaf(executor_id_, PlanType::NonJoinProbe, schema_, req_id)
        , join_ptr(join_ptr_)
        , probe_side_prepare_header(probe_side_prepare_header_)
        , sample_block(sample_block_)
    {}

    void finalize(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

    PhysicalPlanNodePtr cloneOne() const override
    {
        auto clone_one = std::make_shared<PhysicalNonJoinProbe>(*this);
        return clone_one;
    }

    void transform(TransformsPipeline & pipeline, Context & context, size_t) override;

private:
    void probeSideTransform(DAGPipeline & probe_pipeline, Context & context);

    void doSchemaProject(DAGPipeline & pipeline, Context & context);

    void transformImpl(DAGPipeline & pipeline, Context & context, size_t) override;

private:
    JoinPtr join_ptr;

    Block probe_side_prepare_header;

    Block sample_block;
};
} // namespace DB
