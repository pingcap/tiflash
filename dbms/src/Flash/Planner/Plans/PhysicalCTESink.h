// Copyright 2025 PingCAP, Inc.
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

#include <Flash/Coprocessor/FineGrainedShuffle.h>
#include <Flash/Planner/Plans/PhysicalUnary.h>
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>

namespace DB
{
class PhysicalCTESink : public PhysicalUnary
{
public:
    static PhysicalPlanNodePtr build(
        const String & executor_id,
        const LoggerPtr & log,
        const FineGrainedShuffle & fine_grained_shuffle,
        const PhysicalPlanNodePtr & child);

    PhysicalCTESink(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const FineGrainedShuffle & fine_grained_shuffle_,
        const String & req_id,
        const PhysicalPlanNodePtr & child_)
        : PhysicalUnary(executor_id_, PlanType::CTESink, schema_, fine_grained_shuffle_, req_id, child_)
    {}

    void finalizeImpl(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

private:
    void buildPipelineExecGroupImpl(
        PipelineExecutorContext & exec_context,
        PipelineExecGroupBuilder & group_builder,
        Context & context,
        size_t /*concurrency*/) override;
};
} // namespace DB
