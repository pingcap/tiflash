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

#include <Flash/Planner/Plans/PhysicalJoinProbe.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Operators/ExpressionTransformOp.h>
#include <Operators/HashJoinProbeTransformOp.h>
#include <Interpreters/Context.h>

namespace DB
{
const Block & PhysicalJoinProbe::getSampleBlock() const
{
    return sample_block;
}

void PhysicalJoinProbe::buildPipelineExec(PipelineExecGroupBuilder & group_builder, Context & /*context*/, size_t concurrency)
{
    if (prepare_actions && !prepare_actions->getActions().empty())
    {
        group_builder.transform([&](auto & builder) {
            builder.appendTransformOp(std::make_unique<ExpressionTransformOp>(group_builder.exec_status, prepare_actions, log->identifier()));
        });
    }

    size_t probe_index = 0;
    join_ptr->setProbeConcurrency(group_builder.concurrency);
    const auto & max_block_size = context.getSettingsRef().max_block_size;
    const auto & input_header = group_builder.getCurrentHeader();
    group_builder.transform([&](auto & builder) {
        builder.appendTransformOp(std::make_unique<HashJoinProbeTransformOp>(
            group_builder.exec_status, 
            join_ptr, 
            probe_index++, 
            max_block_size,
            input_header,
            log->identifier()));
    });
}
} // namespace DB
