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
PhysicalPlanPtr PhysicalAggregation::build(
    const Context & context,
    const String & executor_id,
    const tipb::Aggregation & aggregation,
    PhysicalPlanPtr child)
{
    assert(child);

    if (aggregation.group_by_size() == 0 && aggregation.agg_func_size() == 0)
    {
        //should not reach here
        throw TiFlashException("Aggregation executor without group by/agg exprs", Errors::Coprocessor::BadRequest);
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
        bool group_by_collation_sensitive = AggregationInterpreterHelper::isGroupByCollationSensitive(context);
        analyzer.buildAggGroupBy(aggregation.group_by(), before_agg_actions, aggregate_descriptions, aggregated_columns, aggregation_keys, agg_key_set, group_by_collation_sensitive, collators);
    }

    auto cast_after_agg_actions = PhysicalPlanHelper::newActions(aggregated_columns, context);
    analyzer.reset(aggregated_columns);
    analyzer.appendCastAfterAgg(cast_after_agg_actions, aggregation);
    cast_after_agg_actions->add(ExpressionAction::project(PhysicalPlanHelper::schemaToNames(analyzer.getCurrentInputColumns())));

    const NamesAndTypes & schema = analyzer.getCurrentInputColumns();
    auto physical_agg = std::make_shared<PhysicalAggregation>(
        executor_id,
        schema,
        before_agg_actions,
        aggregation_keys,
        collators,
        AggregationInterpreterHelper::isFinalAgg(aggregation),
        aggregate_descriptions,
        cast_after_agg_actions);
    physical_agg->appendChild(child);
    return physical_agg;
}

void PhysicalAggregation::transformImpl(DAGPipeline & pipeline, const Context & context, size_t max_streams)
{
    children(0)->transform(pipeline, context, max_streams);

    const auto & logger = context.getDAGContext()->log;

    pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, before_agg_actions, logger->identifier()); });

    Aggregator::Params params = AggregationInterpreterHelper::buildAggregatorParams(
        context,
        pipeline.firstStream()->getHeader(),
        pipeline.streams.size(),
        aggregation_keys,
        aggregation_collators,
        aggregate_descriptions,
        is_final_agg);

    const Settings & settings = context.getSettingsRef();
    /// If there are several sources, then we perform parallel aggregation
    if (pipeline.streams.size() > 1)
    {
        BlockInputStreamPtr stream_with_non_joined_data = combinedNonJoinedDataStream(pipeline, max_streams, logger);
        pipeline.firstStream() = std::make_shared<ParallelAggregatingBlockInputStream>(
            pipeline.streams,
            stream_with_non_joined_data,
            params,
            context.getFileProvider(),
            true,
            max_streams,
            settings.aggregation_memory_efficient_merge_threads ? static_cast<size_t>(settings.aggregation_memory_efficient_merge_threads) : static_cast<size_t>(settings.max_threads),
            logger->identifier());
        pipeline.streams.resize(1);
        restoreConcurrency(pipeline, context.getDAGContext()->final_concurrency, logger);
    }
    else
    {
        BlockInputStreamPtr stream_with_non_joined_data = combinedNonJoinedDataStream(pipeline, max_streams, logger);
        BlockInputStreams inputs;
        if (!pipeline.streams.empty())
            inputs.push_back(pipeline.firstStream());
        else
            pipeline.streams.resize(1);
        if (stream_with_non_joined_data)
            inputs.push_back(stream_with_non_joined_data);
        pipeline.firstStream() = std::make_shared<AggregatingBlockInputStream>(
            std::make_shared<ConcatBlockInputStream>(inputs, logger->identifier()),
            params,
            context.getFileProvider(),
            true,
            logger->identifier());
    }

    pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, cast_after_agg, logger->identifier()); });
}

void PhysicalAggregation::finalize(const Names & parent_require)
{
    // schema.size() >= parent_require.size()
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
    cast_after_agg->finalize(PhysicalPlanHelper::schemaToNames(schema));

    Names before_agg_output;
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
}

const Block & PhysicalAggregation::getSampleBlock() const
{
    return cast_after_agg->getSampleBlock();
}
} // namespace DB