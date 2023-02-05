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

#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Planner/Plans/PhysicalJoinBuild.h>
#include <Operators/ExpressionTransformOp.h>
#include <Operators/HashJoinBuildSink.h>

namespace DB
{
const Block & PhysicalJoinBuild::getSampleBlock() const
{
    return prepare_actions->getSampleBlock();
}

void PhysicalJoinBuild::buildPipelineExec(PipelineExecGroupBuilder & group_builder, Context & /*context*/, size_t /*concurrency*/)
{
    if (!prepare_actions->getActions().empty())
    {
        group_builder.transform([&](auto & builder) {
            builder.appendTransformOp(std::make_unique<ExpressionTransformOp>(group_builder.exec_status, prepare_actions, log->identifier()));
        });
    }

    // TODO support join_execute_info like IBlockInputStream.
    size_t build_index = 0;
    group_builder.transform([&](auto & builder) {
        builder.setSinkOp(std::make_unique<HashJoinBuildSink>(group_builder.exec_status, join_ptr, build_index++, log->identifier()));
    });
    join_ptr->init(group_builder.getCurrentHeader(), group_builder.concurrency);
    join_ptr->setInitActiveBuildConcurrency();
}
} // namespace DB
