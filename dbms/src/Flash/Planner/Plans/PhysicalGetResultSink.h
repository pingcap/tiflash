// Copyright 2023 PingCAP, Ltd.
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

#include <Flash/Executor/ResultHandler.h>
#include <Flash/Planner/Plans/PhysicalUnary.h>

namespace DB
{
class GetResultSinkOp;
class SharedQueue;
using SharedQueuePtr = std::shared_ptr<SharedQueue>;

class PhysicalGetResultSink : public PhysicalUnary
{
public:
    static PhysicalPlanNodePtr build(
        const SharedQueuePtr & shared_queue,
        const PhysicalPlanNodePtr & child);

    static PhysicalPlanNodePtr build(
        ResultHandler && result_handler,
        const PhysicalPlanNodePtr & child);

    PhysicalGetResultSink(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const PhysicalPlanNodePtr & child_,
        ResultHandler && result_handler_,
        const SharedQueuePtr & shared_queue_)
        : PhysicalUnary(executor_id_, PlanType::GetResult, schema_, req_id, child_)
        , result_handler(std::move(result_handler_))
        , shared_queue(shared_queue_)
    {
        assert((!result_handler.isIgnored()) != (shared_queue != nullptr));
    }

    void finalize(const Names &) override
    {
        throw Exception("Unsupport");
    }

    const Block & getSampleBlock() const override
    {
        throw Exception("Unsupport");
    }

    void buildPipelineExec(PipelineExecGroupBuilder & group_builder, Context & /*context*/, size_t /*concurrency*/) override;

private:
    friend class GetResultSinkOp;

    // for sync
    std::mutex mu;
    ResultHandler result_handler;

    // for async
    SharedQueuePtr shared_queue;

private:
    void buildBlockInputStreamImpl(DAGPipeline &, Context &, size_t) override
    {
        throw Exception("Unsupport");
    }
};
} // namespace DB
