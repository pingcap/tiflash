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

#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Planner/Plans/PhysicalJoinProbe.h>
#include <Interpreters/Context.h>
#include <Operators/HashJoinProbeTransformOp.h>

namespace DB
{
void PhysicalJoinProbe::buildPipelineExecGroupImpl(
    PipelineExecutorStatus & exec_status,
    PipelineExecGroupBuilder & group_builder,
    Context & context,
    size_t /*concurrency*/)
{
    executeExpression(exec_status, group_builder, prepare_actions, log);

    auto input_header = group_builder.getCurrentHeader();
    join_ptr->initProbe(input_header, group_builder.concurrency());
    size_t probe_index = 0;
    const auto & max_block_size = context.getSettingsRef().max_block_size;
    group_builder.transform([&](auto & builder) {
        builder.appendTransformOp(std::make_unique<HashJoinProbeTransformOp>(
            exec_status,
            log->identifier(),
            join_ptr,
            probe_index++,
            max_block_size,
            input_header));
    });
}
} // namespace DB
