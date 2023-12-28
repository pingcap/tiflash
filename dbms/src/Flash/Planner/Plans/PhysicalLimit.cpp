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

#include <Common/Logger.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <DataStreams/LimitTransformAction.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Planner/Plans/PhysicalLimit.h>
#include <Interpreters/Context.h>
#include <Operators/LimitTransformOp.h>

namespace DB
{
PhysicalPlanNodePtr PhysicalLimit::build(
    const String & executor_id,
    const LoggerPtr & log,
    const tipb::Limit & limit,
    const PhysicalPlanNodePtr & child)
{
    RUNTIME_CHECK(child);
    auto physical_limit = std::make_shared<PhysicalLimit>(
        executor_id,
        child->getSchema(),
        child->getFineGrainedShuffle(),
        log->identifier(),
        child,
        limit.limit());
    return physical_limit;
}

void PhysicalLimit::buildBlockInputStreamImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    child->buildBlockInputStream(pipeline, context, max_streams);

    pipeline.transform([&](auto & stream) {
        stream = std::make_shared<LimitBlockInputStream>(stream, limit, /*offset*/ 0, log->identifier());
    });
    if (pipeline.hasMoreThanOneStream())
    {
        executeUnion(
            pipeline,
            max_streams,
            context.getSettingsRef().max_buffered_bytes_in_executor,
            log,
            false,
            "for partial limit");
        pipeline.transform([&](auto & stream) {
            stream = std::make_shared<LimitBlockInputStream>(stream, limit, /*offset*/ 0, log->identifier());
        });
    }
}

void PhysicalLimit::buildPipelineExecGroupImpl(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    Context & /*context*/,
    size_t /*concurrency*/)
{
    auto input_header = group_builder.getCurrentHeader();
    auto global_limit = std::make_shared<GlobalLimitTransformAction>(input_header, limit);
    group_builder.transform([&](auto & builder) {
        builder.appendTransformOp(
            std::make_unique<LimitTransformOp<GlobalLimitPtr>>(exec_context, log->identifier(), global_limit));
    });
}

void PhysicalLimit::finalizeImpl(const Names & parent_require)
{
    child->finalize(parent_require);
}

const Block & PhysicalLimit::getSampleBlock() const
{
    return child->getSampleBlock();
}
} // namespace DB
