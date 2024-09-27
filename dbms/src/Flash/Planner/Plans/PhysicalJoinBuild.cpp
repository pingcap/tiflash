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

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Planner/Plans/PhysicalJoinBuild.h>
#include <Interpreters/Context.h>
#include <Operators/HashJoinBuildSink.h>

namespace DB
{
void PhysicalJoinBuild::buildPipelineExecGroupImpl(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    Context & context,
    size_t /*concurrency*/)
{
    executeExpression(exec_context, group_builder, prepare_actions, log);

    size_t build_index = 0;
    assert(join_ptr);
    group_builder.transform([&](auto & builder) {
        builder.setSinkOp(
            std::make_unique<HashJoinBuildSink>(exec_context, log->identifier(), join_ptr, build_index++));
    });
    auto & join_execute_info = context.getDAGContext()->getJoinExecuteInfoMap()[execId()];
    join_execute_info.join_build_profile_infos = group_builder.getCurProfileInfos();
    join_ptr->initBuild(group_builder.getCurrentHeader(), group_builder.concurrency());
    join_ptr->setInitActiveBuildThreads();
    join_ptr.reset();
}
} // namespace DB
