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

#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/Plans/PhysicalCTESource.h>
#include <Interpreters/Context.h>
#include <Operators/CTEReader.h>
#include <Operators/CTESource.h>

#include <memory>
#include <string>

namespace DB
{
PhysicalPlanNodePtr PhysicalCTESource::build(
    const Context & /*context*/,
    const String & executor_id,
    const LoggerPtr & log,
    const FineGrainedShuffle & fine_grained_shuffle,
    const tipb::CTESource & cte_source)
{
    DAGSchema dag_schema;
    for (int i = 0; i < cte_source.field_types_size(); ++i)
    {
        String name = genNameForCTESource(i);
        TiDB::ColumnInfo info = TiDB::fieldTypeToColumnInfo(cte_source.field_types(i));
        dag_schema.emplace_back(std::move(name), std::move(info));
    }

    NamesAndTypes schema = toNamesAndTypes(dag_schema);
    return std::make_shared<PhysicalCTESource>(
        executor_id,
        schema,
        fine_grained_shuffle,
        log->identifier(),
        Block(schema),
        cte_source.cte_id());
}

void PhysicalCTESource::buildPipelineExecGroupImpl(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    Context & context,
    size_t concurrency)
{
    if (fine_grained_shuffle.enabled())
        concurrency = std::min(concurrency, fine_grained_shuffle.stream_count);

    String query_id_and_cte_id = fmt::format("{}_{}", exec_context.getQueryIdForCTE(), this->cte_id);

    if (fine_grained_shuffle.enabled())
    {
        for (size_t partition_id = 0; partition_id < concurrency; ++partition_id)
        {
            group_builder.addConcurrency(std::make_unique<CTESourceOp>(
                exec_context,
                log->identifier(),
                std::make_shared<CTEReader>(query_id_and_cte_id, std::to_string(partition_id), context.getCTEManager()),
                schema));
        }
    }
    else
    {
        auto cte_reader = std::make_shared<CTEReader>(query_id_and_cte_id, "", context.getCTEManager());
        for (size_t partition_id = 0; partition_id < concurrency; ++partition_id)
        {
            group_builder.addConcurrency(
                std::make_unique<CTESourceOp>(exec_context, log->identifier(), cte_reader, schema));
        }
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
