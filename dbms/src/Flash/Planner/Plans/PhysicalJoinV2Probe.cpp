// Copyright 2024 PingCAP, Inc.
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
#include <Flash/Planner/Plans/PhysicalJoinV2Probe.h>
#include <Interpreters/Context.h>
#include <Operators/HashJoinV2ProbeTransformOp.h>

namespace DB
{
void PhysicalJoinV2Probe::buildPipelineExecGroupImpl(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    Context & /*context*/,
    size_t /*concurrency*/)
{
    executeExpression(exec_context, group_builder, prepare_actions, log);

    auto input_header = group_builder.getCurrentHeader();
    assert(join_ptr);
    join_ptr->initProbe(input_header, group_builder.concurrency());
    size_t probe_index = 0;
    group_builder.transform([&](auto & builder) {
        builder.appendTransformOp(
            std::make_unique<HashJoinV2ProbeTransformOp>(exec_context, log->identifier(), join_ptr, probe_index++));
    });
    join_ptr.reset();
}
} // namespace DB
