// Copyright 2022 PingCAP, Ltd.
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
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Planner/PhysicalPlanBuilder.h>
#include <Flash/Planner/Planner.h>
#include <Flash/Planner/optimize.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace
{
void analyzePhysicalPlan(PhysicalPlanBuilder & builder, const DAGQueryBlock & query_block)
{
    assert(query_block.source);
    builder.build(query_block.source_name, query_block.source);

    // selection on table scan had been executed in table scan.
    if (query_block.selection && !query_block.isTableScanSource())
    {
        builder.build(query_block.selection_name, query_block.selection);
    }

    if (query_block.aggregation)
    {
        builder.build(query_block.aggregation_name, query_block.aggregation);

        if (query_block.having)
        {
            builder.build(query_block.having_name, query_block.having);
        }
    }

    // TopN/Limit
    if (query_block.limit_or_topn)
    {
        builder.build(query_block.limit_or_topn_name, query_block.limit_or_topn);
    }

    builder.buildFinalProjection(query_block.qb_column_prefix, query_block.isRootQueryBlock());

    if (query_block.exchange_sender)
    {
        builder.build(query_block.exchange_sender_name, query_block.exchange_sender);
    }
}
} // namespace

Planner::Planner(
    Context & context_,
    const std::vector<BlockInputStreams> & input_streams_vec_,
    const DAGQueryBlock & query_block_,
    size_t max_streams_)
    : context(context_)
    , input_streams_vec(input_streams_vec_)
    , query_block(query_block_)
    , max_streams(max_streams_)
    , log(Logger::get("Planner", dagContext().log ? dagContext().log->identifier() : ""))
{}

BlockInputStreams Planner::execute()
{
    DAGPipeline pipeline;
    executeImpl(pipeline);
    if (!pipeline.streams_with_non_joined_data.empty())
    {
        executeUnion(pipeline, max_streams, log);
        restorePipelineConcurrency(pipeline);
    }
    return pipeline.streams;
}

bool Planner::isSupported(const DAGQueryBlock & query_block)
{
    return query_block.source
        && (query_block.source->tp() == tipb::ExecType::TypeProjection
            || query_block.source->tp() == tipb::ExecType::TypeExchangeReceiver);
}

DAGContext & Planner::dagContext() const
{
    return *context.getDAGContext();
}

void Planner::restorePipelineConcurrency(DAGPipeline & pipeline)
{
    if (query_block.can_restore_pipeline_concurrency)
        restoreConcurrency(pipeline, dagContext().final_concurrency, log);
}

void Planner::executeImpl(DAGPipeline & pipeline)
{
    PhysicalPlanBuilder builder{context, log->identifier()};
    for (const auto & input_streams : input_streams_vec)
    {
        RUNTIME_ASSERT(!input_streams.empty(), log, "input streams cannot be empty");
        builder.buildSource(input_streams.back()->getHeader());
    }

    analyzePhysicalPlan(builder, query_block);

    auto physical_plan = builder.getResult();
    physical_plan = optimize(context, physical_plan);
    physical_plan->transform(pipeline, context, max_streams);
}
} // namespace DB
