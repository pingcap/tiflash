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

#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Planner/Plans/PhysicalGetResultSink.h>
#include <Operators/GetResultSinkOp.h>

namespace DB
{
PhysicalPlanNodePtr PhysicalGetResultSink::build(
    const ResultQueuePtr & result_queue,
    const LoggerPtr & log,
    const PhysicalPlanNodePtr & child)
{
    return std::make_shared<PhysicalGetResultSink>(
        "get_result_sink",
        child->getSchema(),
        child->getFineGrainedShuffle(),
        log->identifier(),
        child,
        result_queue);
}

void PhysicalGetResultSink::buildPipelineExecGroupImpl(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    Context & /*context*/,
    size_t /*concurrency*/)
{
    group_builder.transform([&](auto & builder) {
        builder.setSinkOp(std::make_unique<GetResultSinkOp>(exec_context, log->identifier(), result_queue));
    });
}
} // namespace DB
