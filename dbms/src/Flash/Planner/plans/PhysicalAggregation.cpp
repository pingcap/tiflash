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

#include <Common/Logger.h>
#include <Common/TiFlashException.h>
#include <DataStreams/AggregatingBlockInputStream.h>
#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/ParallelAggregatingBlockInputStream.h>
#include <Flash/Coprocessor/AggregationInterpreterHelper.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/plans/PhysicalAggregation.h>
#include <Interpreters/Context.h>

namespace DB
{
PhysicalPlanNodePtr PhysicalAggregation::build(
    const Context & context,
    const String & executor_id,
    const LoggerPtr & log,
    const tipb::Aggregation & aggregation,
    const PhysicalPlanNodePtr & child)
{
    assert(child);

    if (unlikely(aggregation.group_by_size() == 0 && aggregation.agg_func_size() == 0))
    {
        //should not reach here
        throw TiFlashException("Aggregation executor without group by/agg exprs", Errors::Planner::BadRequest);
    }

    DAGExpressionAnalyzer analyzer{child->getSchema(), context};
    ExpressionActionsPtr before_agg_actions = PhysicalPlanHelper::newActions(child->getSampleBlock(), context);
    NamesAndTypes aggregated_columns;
    AggregateDescriptions aggregate_descriptions;
    Names aggregation_keys;
    TiDB::TiDBCollators collators;
    {
        std::unordered_set<String> agg_key_set;
        analyzer.buildAggFuncs(aggregation, before_agg_actions, aggregate_descriptions, aggregated_columns);
        analyzer.buildAggGroupBy(
            aggregation.group_by(),
            before_agg_actions,
            aggregate_descriptions,
            aggregated_columns,
            aggregation_keys,
            agg_key_set,
            AggregationInterpreterHelper::isGroupByCollationSensitive(context),
            collators);
    }

    auto cast_after_agg_actions = PhysicalPlanHelper::newActions(aggregated_columns, context);
    analyzer.reset(aggregated_columns);
    analyzer.appendCastAfterAgg(cast_after_agg_actions, aggregation);
    /// project action after aggregation to remove useless columns.
    const NamesAndTypes & schema = analyzer.getCurrentInputColumns();
    cast_after_agg_actions->add(ExpressionAction::project(PhysicalPlanHelper::schemaToNames(schema)));

    auto physical_agg = std::make_shared<PhysicalAggregation>(
        executor_id,
        schema,
        log->identifier(),
        child,
        before_agg_actions,
        aggregation_keys,
        collators,
        AggregationInterpreterHelper::isFinalAgg(aggregation),
        aggregate_descriptions,
        cast_after_agg_actions);
    // For agg, `recordProfileStreams` has been called in `transformImpl`.
    physical_agg->disableRecordProfileStreams();
    return physical_agg;
}

void PhysicalAggregation::transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    child->transform(pipeline, context, max_streams);

    executeExpression(pipeline, before_agg_actions, log, "before aggregation");

    Block before_agg_header = pipeline.firstStream()->getHeader();
    AggregationInterpreterHelper::fillArgColumnNumbers(aggregate_descriptions, before_agg_header);
    auto params = AggregationInterpreterHelper::buildParams(
        context,
        before_agg_header,
        pipeline.streams.size(),
        aggregation_keys,
        aggregation_collators,
        aggregate_descriptions,
        is_final_agg);

    /// If there are several sources, then we perform parallel aggregation
    if (pipeline.streams.size() > 1 || pipeline.streams_with_non_joined_data.size() > 1)
    {
        const Settings & settings = context.getSettingsRef();
        BlockInputStreamPtr stream = std::make_shared<ParallelAggregatingBlockInputStream>(
            pipeline.streams,
            pipeline.streams_with_non_joined_data,
            params,
            context.getFileProvider(),
            true,
            max_streams,
            settings.aggregation_memory_efficient_merge_threads ? static_cast<size_t>(settings.aggregation_memory_efficient_merge_threads) : static_cast<size_t>(settings.max_threads),
            log->identifier());

        pipeline.streams.resize(1);
        pipeline.streams_with_non_joined_data.clear();
        pipeline.firstStream() = std::move(stream);

        // should record for agg before restore concurrency. See #3804.
        recordProfileStreams(pipeline, context);
        restoreConcurrency(pipeline, context.getDAGContext()->final_concurrency, log);
    }
    else
    {
        BlockInputStreams inputs;
        if (!pipeline.streams.empty())
            inputs.push_back(pipeline.firstStream());

        if (!pipeline.streams_with_non_joined_data.empty())
            inputs.push_back(pipeline.streams_with_non_joined_data.at(0));

        pipeline.streams.resize(1);
        pipeline.streams_with_non_joined_data.clear();

        pipeline.firstStream() = std::make_shared<AggregatingBlockInputStream>(
            std::make_shared<ConcatBlockInputStream>(inputs, log->identifier()),
            params,
            context.getFileProvider(),
            true,
            log->identifier());
        recordProfileStreams(pipeline, context);
    }

    executeExpression(pipeline, cast_after_agg, log, "cast after aggregation");
}

void PhysicalAggregation::finalize(const Names & parent_require)
{
    // schema.size() >= parent_require.size()
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
    cast_after_agg->finalize(PhysicalPlanHelper::schemaToNames(schema));

    Names before_agg_output;
    // set required output for agg funcs's arguments and group by keys.
    for (const auto & aggregate_description : aggregate_descriptions)
    {
        for (const auto & argument_name : aggregate_description.argument_names)
            before_agg_output.push_back(argument_name);
    }
    for (const auto & aggregation_key : aggregation_keys)
    {
        before_agg_output.push_back(aggregation_key);
    }

    before_agg_actions->finalize(before_agg_output);
    child->finalize(before_agg_actions->getRequiredColumns());
    FinalizeHelper::prependProjectInputIfNeed(before_agg_actions, child->getSampleBlock().columns());

    FinalizeHelper::checkSampleBlockContainsSchema(getSampleBlock(), schema);
}

const Block & PhysicalAggregation::getSampleBlock() const
{
    return cast_after_agg->getSampleBlock();
}
} // namespace DB
