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

#include <Flash/Executor/ResultHandler.h>
#include <Flash/Planner/plans/PhysicalUnary.h>
#include <Operators/Operator.h>

namespace DB
{
class PhysicalGetResultSink;

class GetResultSink : public Sink
{
public:
    explicit GetResultSink(PhysicalGetResultSink & physical_sink_)
        : physical_sink(physical_sink_)
    {
    }

    OperatorStatus write(Block && block) override;

private:
    PhysicalGetResultSink & physical_sink;
};

class PhysicalGetResultSink : public PhysicalUnary
{
public:
    static PhysicalPlanNodePtr build(
        ResultHandler result_handler,
        const PhysicalPlanNodePtr & child);

    PhysicalGetResultSink(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const PhysicalPlanNodePtr & child_,
        ResultHandler result_handler_)
        : PhysicalUnary(executor_id_, PlanType::GetResult, schema_, req_id, child_)
        , result_handler(result_handler_)
    {
        assert(!result_handler.isIgnored());
    }

    void finalize(const Names &) override
    {
        throw Exception("Unsupport");
    }

    const Block & getSampleBlock() const override
    {
        throw Exception("Unsupport");
    }

    void transform(OperatorGroupBuilder & op_builder, Context & /*context*/, size_t /*concurrency*/) override;

public:
    std::mutex mu;
    ResultHandler result_handler;

private:
    void transformImpl(DAGPipeline &, Context &, size_t) override
    {
        throw Exception("Unsupport");
    }
};
} // namespace DB
