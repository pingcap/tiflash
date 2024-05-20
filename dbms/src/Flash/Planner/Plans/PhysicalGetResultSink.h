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

#include <Flash/Executor/ResultQueue_fwd.h>
#include <Flash/Planner/Plans/PhysicalUnary.h>

namespace DB
{
class PhysicalGetResultSink : public PhysicalUnary
{
public:
    static PhysicalPlanNodePtr build(
        const ResultQueuePtr & result_queue,
        const LoggerPtr & log,
        const PhysicalPlanNodePtr & child);

    PhysicalGetResultSink(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const FineGrainedShuffle & fine_grained_shuffle_,
        const String & req_id,
        const PhysicalPlanNodePtr & child_,
        const ResultQueuePtr & result_queue_)
        : PhysicalUnary(executor_id_, PlanType::GetResult, schema_, fine_grained_shuffle_, req_id, child_)
        , result_queue(result_queue_)
    {
        assert(result_queue);
    }

    void finalizeImpl(const Names &) override { throw Exception("Unsupport"); }

    const Block & getSampleBlock() const override { throw Exception("Unsupport"); }

private:
    void buildBlockInputStreamImpl(DAGPipeline &, Context &, size_t) override { throw Exception("Unsupport"); }

    void buildPipelineExecGroupImpl(
        PipelineExecutorContext & exec_context,
        PipelineExecGroupBuilder & group_builder,
        Context & /*context*/,
        size_t /*concurrency*/) override;

private:
    ResultQueuePtr result_queue;
};
} // namespace DB
