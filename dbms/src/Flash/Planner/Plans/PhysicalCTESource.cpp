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

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/Plans/PhysicalCTESource.h>
#include <Interpreters/Context.h>
#include <Operators/CTESource.h>

namespace DB
{
PhysicalPlanNodePtr PhysicalCTESource::build(
    const Context & context,
    const String & executor_id,
    const LoggerPtr & log,
    const FineGrainedShuffle & fine_grained_shuffle,
    PipelineExecutorContextPtr exec_context_ptr
    /* TODO tipb::ExchangeReceiver */)
{
    // TODO tipb for cte: need output schema field in tipb for cte source
    // TODO we need to get meta data such as `partition_col_collators` for partitioning data
    NamesAndTypes schema; // TODO scchema info is from tipb
    auto physical_exchange_receiver = std::make_shared<PhysicalCTESource>(
        context,
        executor_id,
        schema,
        fine_grained_shuffle,
        log->identifier(),
        Block(schema),
        exec_context_ptr);
    return physical_exchange_receiver;
}

void PhysicalCTESource::buildPipelineExecGroupImpl(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    Context & context,
    size_t concurrency)
{
    if (fine_grained_shuffle.enabled())
        concurrency = std::min(concurrency, fine_grained_shuffle.stream_count);

    String query_id_and_cte_id_prefix = fmt::format("{}_{}", this->exec_context_ptr->getQueryIdForCTE(), this->cte_id);

    for (size_t partition_id = 0; partition_id < concurrency; ++partition_id)
    {
        group_builder.addConcurrency(
            std::make_unique<CTESourceOp>(exec_context, log->identifier(), this->exec_context_ptr->));
    }
    context.getDAGContext()->addInboundIOProfileInfos(this->executor_id, group_builder.getCurIOProfileInfos());
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
