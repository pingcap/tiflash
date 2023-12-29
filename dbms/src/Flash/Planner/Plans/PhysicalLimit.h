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
#include <tipb/executor.pb.h>

namespace DB
{
class PhysicalLimit : public PhysicalUnary
{
public:
    static PhysicalPlanNodePtr build(
        const String & executor_id,
        const LoggerPtr & log,
        const tipb::Limit & limit,
        const PhysicalPlanNodePtr & child);

    PhysicalLimit(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const FineGrainedShuffle & fine_grained_shuffle_,
        const String & req_id,
        const PhysicalPlanNodePtr & child_,
        size_t limit_)
        : PhysicalUnary(executor_id_, PlanType::Limit, schema_, fine_grained_shuffle_, req_id, child_)
        , limit(limit_)
    {}

    void finalizeImpl(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

private:
    void buildBlockInputStreamImpl(DAGPipeline & pipeline, Context & context, size_t max_streams) override;

    void buildPipelineExecGroupImpl(
        PipelineExecutorContext & exec_context,
        PipelineExecGroupBuilder & group_builder,
        Context & /*context*/,
        size_t /*concurrency*/) override;

private:
    size_t limit;
};
} // namespace DB
