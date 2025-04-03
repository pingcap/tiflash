// Copyright 2025 PingCAP, Inc.
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

#include <Flash/Planner/Plans/PhysicalCTESource.h>
#include <Flash/Planner/FinalizeHelper.h>

namespace DB
{
void PhysicalCTESource::buildPipelineExecGroupImpl(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    Context & context,
    size_t concurrency)
{
    if (fine_grained_shuffle.enabled())
        concurrency = std::min(concurrency, fine_grained_shuffle.stream_count);

    for (size_t partition_id = 0; partition_id < concurrency; ++partition_id)
    {
        // group_builder.addConcurrency(std::make_unique<ExchangeReceiverSourceOp>(
        //     exec_context,
        //     log->identifier(),
        //     mpp_exchange_receiver,
        //     /*stream_id=*/fine_grained_shuffle.enabled() ? partition_id : 0));
    }
    // context.getDAGContext()->addInboundIOProfileInfos(executor_id, group_builder.getCurIOProfileInfos());
}

void PhysicalCTESource::finalizeImpl(const Names & parent_require)
{
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
}

const Block & PhysicalCTESource::getSampleBlock() const
{
    return sample_block;
}
} // namespace DB
