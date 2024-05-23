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

#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Executor/PipelineExecutorContext.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Planner/Plans/PhysicalJoinProbe.h>
#include <Interpreters/Context.h>
#include <Operators/HashJoinProbeTransformOp.h>

namespace DB
{
void PhysicalJoinProbe::buildPipelineExecGroupImpl(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    Context & context,
    size_t concurrency)
{
    // Currently join probe does not support fine grained shuffle.
    RUNTIME_CHECK(!fine_grained_shuffle.enable());
    if (join_ptr->isSpilled() && group_builder.concurrency() == 1)
    {
        // When the join build operator spilled, the probe operator requires at least two or more threads to restore spilled hash partitions.
        auto restore_concurrency = std::max(2, concurrency);
        restoreConcurrency(
            exec_context,
            group_builder,
            restore_concurrency,
            context.getSettingsRef().max_buffered_bytes_in_executor,
            log);
    }

    executeExpression(exec_context, group_builder, prepare_actions, log);

    auto input_header = group_builder.getCurrentHeader();
    assert(join_ptr);
    join_ptr->initProbe(input_header, group_builder.concurrency());
    size_t probe_index = 0;
    const auto & max_block_size = context.getSettingsRef().max_block_size;
    group_builder.transform([&](auto & builder) {
        builder.appendTransformOp(std::make_unique<HashJoinProbeTransformOp>(
            exec_context,
            log->identifier(),
            join_ptr,
            probe_index++,
            max_block_size,
            input_header));
    });
    // The `join_ptr->wait_build_finished_future` does not need to be added to exec_context here;
    // it is only necessary to add it during the "restore build stage."
    // The order of build/probe here is ensured by the event.
    exec_context.addOneTimeFuture(join_ptr->wait_probe_finished_future);
    join_ptr.reset();
}
} // namespace DB
