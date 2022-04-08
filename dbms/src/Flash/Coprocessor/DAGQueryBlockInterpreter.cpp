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

#include <Common/FailPoint.h>
#include <Common/TiFlashException.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/Coprocessor/DAGQueryBlockInterpreter.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Planner/PhysicalPlanBuilder.h>
#include <Flash/Planner/plans/PhysicalTableScan.h>
#include <Flash/Planner/traversePhysicalPlans.h>
#include <Interpreters/Join.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB
{
namespace FailPoints
{
extern const char pause_after_copr_streams_acquired[];
extern const char minimum_block_size_for_cross_join[];
} // namespace FailPoints

DAGQueryBlockInterpreter::DAGQueryBlockInterpreter(
    Context & context_,
    const std::vector<BlockInputStreams> & input_streams_vec_,
    const DAGQueryBlock & query_block_,
    size_t max_streams_,
    bool keep_session_timezone_info_)
    : context(context_)
    , input_streams_vec(input_streams_vec_)
    , query_block(query_block_)
    , keep_session_timezone_info(keep_session_timezone_info_)
    , max_streams(max_streams_)
    , log(Logger::get("DAGQueryBlockInterpreter", dagContext().log ? dagContext().log->identifier() : ""))
{}

namespace
{
void analysePhysicalPlan(
    PhysicalPlanBuilder & builder,
    const DAGQueryBlock & query_block,
    bool keep_session_timezone_info)
{
    // selection on table scan had been executed in table scan.
    if (query_block.selection && !query_block.isTableScanSource())
    {
        builder.build(query_block.selection_name, query_block.selection);
    }
    // There will be either Agg...
    if (query_block.aggregation)
    {
        builder.build(query_block.aggregation_name, query_block.aggregation);

        if (query_block.having != nullptr)
        {
            builder.build(query_block.having_name, query_block.having);
        }
    }

    // TopN/Limit
    if (query_block.limit_or_topn)
    {
        builder.build(query_block.limit_or_topn_name, query_block.limit_or_topn);
    }

    // Append final project results if needed.
    if (query_block.isRootQueryBlock())
    {
        builder.buildRootFinalProjection(
            query_block.output_field_types,
            query_block.output_offsets,
            query_block.qb_column_prefix,
            keep_session_timezone_info);
    }
    else
    {
        builder.buildNonRootFinalProjection(query_block.qb_column_prefix);
    }

    if (query_block.exchange_sender)
    {
        builder.build(query_block.exchange_sender_name, query_block.exchange_sender);
    }
}
} // namespace

// To execute a query block, you have to:
// 1. generate the date stream and push it to pipeline.
// 2. assign the analyzer
// 3. construct a final projection, even if it's not necessary. just construct it.
// Talking about projection, it has the following rules.
// 1. if the query block does not contain agg, then the final project is the same as the source Executor
// 2. if the query block contains agg, then the final project is the same as agg Executor
// 3. if the cop task may contains more then 1 query block, and the current query block is not the root
//    query block, then the project should add an alias for each column that needs to be projected, something
//    like final_project.emplace_back(col.name, query_block.qb_column_prefix + col.name);
void DAGQueryBlockInterpreter::executeImpl(DAGPipeline & pipeline)
{
    PhysicalPlanBuilder physical_plan_builder{context};
    if (query_block.source->tp() == tipb::ExecType::TypeJoin)
    {
        const auto & left_input_sample_block = input_streams_vec[0].back()->getHeader();
        NamesAndTypes left_input_columns;
        for (const auto & column : left_input_sample_block)
            left_input_columns.emplace_back(column.name, column.type);
        physical_plan_builder.buildSource(
            query_block.source_name,
            left_input_columns,
            left_input_sample_block);
        const auto & right_input_sample_block = input_streams_vec[1].back()->getHeader();
        NamesAndTypes right_input_columns;
        for (const auto & column : right_input_sample_block)
            right_input_columns.emplace_back(column.name, column.type);
        physical_plan_builder.buildSource(
            query_block.source_name,
            right_input_columns,
            right_input_sample_block);

        physical_plan_builder.build(query_block.source_name, query_block.source);
    }
    else if (query_block.source->tp() == tipb::ExecType::TypeExchangeReceiver)
    {
        physical_plan_builder.build(query_block.source_name, query_block.source);
    }
    else if (query_block.source->tp() == tipb::ExecType::TypeProjection)
    {
        const auto & input_sample_block = input_streams_vec.back().back()->getHeader();
        NamesAndTypes input_columns;
        for (const auto & column : input_sample_block)
            input_columns.emplace_back(column.name, column.type);
        physical_plan_builder.buildSource(
            query_block.source_name,
            input_columns,
            input_sample_block);
        physical_plan_builder.build(query_block.source_name, query_block.source);
    }
    else if (query_block.isTableScanSource())
    {
        physical_plan_builder.build(query_block.source_name, query_block.source);
        if (query_block.selection)
        {
            auto physical_table_scan = std::dynamic_pointer_cast<PhysicalTableScan>(physical_plan_builder.getResult());
            assert(physical_table_scan);
            physical_table_scan->pushDownFilter(query_block.selection_name, query_block.selection->selection());
        }
    }
    else
    {
        throw TiFlashException(
            std::string(__PRETTY_FUNCTION__) + ": Unsupported source node: " + query_block.source_name,
            Errors::Coprocessor::BadRequest);
    }

    // this log measures the concurrent degree in this mpp task
    LOG_FMT_DEBUG(
        log,
        "execution stream size for query block(source) {} is {}",
        query_block.qb_column_prefix,
        pipeline.streams.size());
    dagContext().final_concurrency = std::min(std::max(dagContext().final_concurrency, pipeline.streams.size()), max_streams);

    assert(pipeline.hasMoreThanOneStream());
    analysePhysicalPlan(physical_plan_builder, query_block, keep_session_timezone_info);
    auto physical_plan = physical_plan_builder.getResult();
    LOG_FMT_DEBUG(log, "begin finalize physical plan");
    physical_plan->finalize();
    LOG_FMT_DEBUG(log, "finish finalize physical plan");

    auto physical_plan_to_string = [&physical_plan]() {
        FmtBuffer buffer;
        // now all physical_plan.childrenSize() == 0 or 1.
        String prefix;
        traverse(physical_plan, [&buffer, &prefix](const PhysicalPlanPtr & plan) {
            assert(plan);
            buffer.fmtAppend("{}{}\n", prefix, plan->toString());
            prefix += "  ";
        });
        return buffer.toString();
    };
    LOG_FMT_DEBUG(log, "physical plan tree: \n{}", physical_plan_to_string());

    physical_plan->transform(pipeline, context, max_streams);
}

void DAGQueryBlockInterpreter::restorePipelineConcurrency(DAGPipeline & pipeline)
{
    restoreConcurrency(pipeline, dagContext().final_concurrency, log);
}

BlockInputStreams DAGQueryBlockInterpreter::execute()
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
} // namespace DB
