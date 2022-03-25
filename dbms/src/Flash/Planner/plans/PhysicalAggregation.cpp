#include <Common/Logger.h>
#include <DataStreams/AggregatingBlockInputStream.h>
#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/ParallelAggregatingBlockInputStream.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/plans/PhysicalAggregation.h>
#include <Interpreters/Context.h>

namespace DB
{
void PhysicalAggregation::transformImpl(DAGPipeline & pipeline, const Context & context, size_t max_streams)
{
    children(0)->transform(pipeline, context, max_streams);

    const auto & logger = context.getDAGContext()->log;

    pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, before_agg_actions, logger->identifier()); });

    Block header = pipeline.firstStream()->getHeader();
    ColumnNumbers keys;
    for (const auto & name : aggregation_keys)
        keys.push_back(header.getPositionByName(name));
    for (auto & descr : aggregate_descriptions)
    {
        if (descr.arguments.empty())
        {
            for (const auto & name : descr.argument_names)
                descr.arguments.push_back(header.getPositionByName(name));
        }
    }

    const Settings & settings = context.getSettingsRef();

    /** Two-level aggregation is useful in two cases:
          * 1. Parallel aggregation is done, and the results should be merged in parallel.
          * 2. An aggregation is done with store of temporary data on the disk, and they need to be merged in a memory efficient way.
          */
    bool allow_to_use_two_level_group_by = pipeline.streams.size() > 1 || settings.max_bytes_before_external_group_by != 0;
    bool has_collator = std::any_of(begin(aggregation_collators), end(aggregation_collators), [](const auto & p) { return p != nullptr; });

    Aggregator::Params params(
        header,
        keys,
        aggregate_descriptions,
        false,
        settings.max_rows_to_group_by,
        settings.group_by_overflow_mode,
        allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold : SettingUInt64(0),
        allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold_bytes : SettingUInt64(0),
        settings.max_bytes_before_external_group_by,
        settings.empty_result_for_aggregation_by_empty_set,
        context.getTemporaryPath(),
        has_collator ? aggregation_collators : TiDB::dummy_collators);

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
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);

    cast_after_agg->finalize(FinalizeHelper::schemaToNames(schema));

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