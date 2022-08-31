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

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Executor/ResultHandler.h>
#include <Flash/Planner/plans/PhysicalUnary.h>

namespace DB
{
class ResultHandlerBlockInputStream : public IProfilingBlockInputStream
{
private:
    static constexpr auto NAME = "ResultHandler";

public:
    ResultHandlerBlockInputStream(
        const BlockInputStreamPtr & input,
        ResultHandler result_handler_)
        : result_handler(result_handler_)
    {
        children.push_back(input);
    }

    String getName() const override { return NAME; }
    Block getTotals() override
    {
        return children.back()->getHeader();
    }
    Block getHeader() const override
    {
        return children.back()->read();
    }

protected:
    Block readImpl() override
    {
        Block block = children.back()->read();
        if (block)
            result_handler(block);
        return block;
    }

private:
    ResultHandler result_handler;
};

class PhysicalResultHandler : public PhysicalUnary
{
public:
    static PhysicalPlanNodePtr build(
        ResultHandler result_handler,
        const String & req_id,
        const PhysicalPlanNodePtr & child)
    {
        return std::make_shared<PhysicalResultHandler>(
            "ResultHandler",
            child->getSchema(),
            req_id,
            child,
            result_handler);
    }

    PhysicalResultHandler(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const PhysicalPlanNodePtr & child_,
        ResultHandler result_handler_)
        : PhysicalUnary(executor_id_, PlanType::ResultHandler, schema_, req_id, child_)
        , result_handler(result_handler_)
    {}

    void finalize(const Names & parent_require) override
    {
        return child->finalize(parent_require);
    }

    const Block & getSampleBlock() const override
    {
        return child->getSampleBlock();
    }

private:
    void transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams) override
    {
        child->transform(pipeline, context, max_streams);
        executeUnion(pipeline, max_streams, log, /*ignore_block=*/false, "for result handler");
        pipeline.firstStream() = std::make_shared<ResultHandlerBlockInputStream>(pipeline.firstStream(), result_handler);
    }

private:
    ResultHandler result_handler;
};
}
