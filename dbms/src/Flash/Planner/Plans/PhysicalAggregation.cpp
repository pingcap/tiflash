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

#include <Common/Logger.h>
#include <Common/TiFlashException.h>
#include <DataStreams/AggregatingBlockInputStream.h>
#include <DataStreams/ParallelAggregatingBlockInputStream.h>
#include <Flash/Coprocessor/AggregationInterpreterHelper.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Pipeline/PipelineBuilder.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/Plans/PhysicalAggregation.h>
#include <Flash/Planner/Plans/PhysicalAggregationBuild.h>
#include <Flash/Planner/Plans/PhysicalAggregationConvergent.h>
#include <Interpreters/Context.h>
#include <Operators/LocalAggregateTransform.h>

namespace DB
{
PhysicalPlanNodePtr PhysicalAggregation::build(
    const Context & context,
    const String & executor_id,
    const LoggerPtr & log,
    const tipb::Aggregation & aggregation,
    const FineGrainedShuffle & fine_grained_shuffle,
    const PhysicalPlanNodePtr & child)
{
    RUNTIME_CHECK(child);

    if (unlikely(aggregation.group_by_size() == 0 && aggregation.agg_func_size() == 0))
    {
        //should not reach here
        throw TiFlashException("Aggregation executor without group by/agg exprs", Errors::Planner::BadRequest);
    }

    DAGExpressionAnalyzer analyzer{child->getSchema(), context};
    ExpressionActionsPtr before_agg_actions = PhysicalPlanHelper::newActions(child->getSampleBlock());
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

    auto expr_after_agg_actions = PhysicalPlanHelper::newActions(aggregated_columns);
    analyzer.reset(aggregated_columns);
    analyzer.appendCastAfterAgg(expr_after_agg_actions, aggregation);
    /// project action after aggregation to remove useless columns.
    auto schema = PhysicalPlanHelper::addSchemaProjectAction(expr_after_agg_actions, analyzer.getCurrentInputColumns());

    auto physical_agg = std::make_shared<PhysicalAggregation>(
        executor_id,
        schema,
        fine_grained_shuffle,
        log->identifier(),
        child,
        before_agg_actions,
        aggregation_keys,
        collators,
        AggregationInterpreterHelper::isFinalAgg(aggregation),
        aggregate_descriptions,
        expr_after_agg_actions);
    return physical_agg;
}

void PhysicalAggregation::buildBlockInputStreamImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    child->buildBlockInputStream(pipeline, context, max_streams);

    executeExpression(pipeline, before_agg_actions, log, "before aggregation");

    Block before_agg_header = pipeline.firstStream()->getHeader();
    AggregationInterpreterHelper::fillArgColumnNumbers(aggregate_descriptions, before_agg_header);
    SpillConfig spill_config(
        context.getTemporaryPath(),
        fmt::format("{}_aggregation", log->identifier()),
        context.getSettingsRef().max_cached_data_bytes_in_spiller,
        context.getSettingsRef().max_spilled_rows_per_file,
        context.getSettingsRef().max_spilled_bytes_per_file,
        context.getFileProvider());
    auto params = AggregationInterpreterHelper::buildParams(
        context,
        before_agg_header,
        pipeline.streams.size(),
        fine_grained_shuffle.enable() ? pipeline.streams.size() : 1,
        aggregation_keys,
        aggregation_collators,
        aggregate_descriptions,
        is_final_agg,
        spill_config);

    if (fine_grained_shuffle.enable())
    {
        /// For fine_grained_shuffle, just do aggregation in streams independently
        pipeline.transform([&](auto & stream) {
            stream = std::make_shared<AggregatingBlockInputStream>(
                stream,
                params,
                true,
                log->identifier());
            stream->setExtraInfo(String(enableFineGrainedShuffleExtraInfo));
        });
    }
    else if (pipeline.streams.size() > 1)
    {
        /// If there are several sources, then we perform parallel aggregation
        const Settings & settings = context.getSettingsRef();
        BlockInputStreamPtr stream = std::make_shared<ParallelAggregatingBlockInputStream>(
            pipeline.streams,
            BlockInputStreams{},
            params,
            true,
            max_streams,
            settings.max_buffered_bytes_in_executor,
            settings.aggregation_memory_efficient_merge_threads ? static_cast<size_t>(settings.aggregation_memory_efficient_merge_threads) : static_cast<size_t>(settings.max_threads),
            log->identifier());

        pipeline.streams.resize(1);
        pipeline.firstStream() = std::move(stream);

        restoreConcurrency(pipeline, context.getDAGContext()->final_concurrency, context.getSettingsRef().max_buffered_bytes_in_executor, log);
    }
    else
    {
        RUNTIME_CHECK(pipeline.streams.size() == 1);
        pipeline.firstStream() = std::make_shared<AggregatingBlockInputStream>(
            pipeline.firstStream(),
            params,
            true,
            log->identifier());
    }

    // we can record for agg after restore concurrency.
    // Because the streams of expr_after_agg will provide the correct ProfileInfo.
    // See #3804.
    RUNTIME_CHECK(expr_after_agg && !expr_after_agg->getActions().empty());
    executeExpression(pipeline, expr_after_agg, log, "expr after aggregation");
}

void PhysicalAggregation::buildPipelineExecGroupImpl(
    PipelineExecutorStatus & exec_status,
    PipelineExecGroupBuilder & group_builder,
    Context & context,
    size_t /*concurrency*/)
{
    // For non fine grained shuffle, PhysicalAggregation will be broken into AggregateBuild and AggregateConvergent.
    // So only fine grained shuffle is considered here.
    RUNTIME_CHECK(fine_grained_shuffle.enable());

    executeExpression(exec_status, group_builder, before_agg_actions, log);

    Block before_agg_header = group_builder.getCurrentHeader();
    size_t concurrency = group_builder.concurrency();
    AggregationInterpreterHelper::fillArgColumnNumbers(aggregate_descriptions, before_agg_header);
    SpillConfig spill_config(
        context.getTemporaryPath(),
        fmt::format("{}_aggregation", log->identifier()),
        context.getSettingsRef().max_cached_data_bytes_in_spiller,
        context.getSettingsRef().max_spilled_rows_per_file,
        context.getSettingsRef().max_spilled_bytes_per_file,
        context.getFileProvider());
    auto params = AggregationInterpreterHelper::buildParams(
        context,
        before_agg_header,
        concurrency,
        concurrency,
        aggregation_keys,
        aggregation_collators,
        aggregate_descriptions,
        is_final_agg,
        spill_config);
    group_builder.transform([&](auto & builder) {
        builder.appendTransformOp(std::make_unique<LocalAggregateTransform>(exec_status, log->identifier(), params));
    });

    executeExpression(exec_status, group_builder, expr_after_agg, log);
}

void PhysicalAggregation::buildPipeline(
    PipelineBuilder & builder,
    Context & context,
    PipelineExecutorStatus & exec_status)
{
    auto aggregate_context = std::make_shared<AggregateContext>(log->identifier());
    if (fine_grained_shuffle.enable())
    {
        // For fine grained shuffle, Aggregate wouldn't be broken.
        child->buildPipeline(builder, context, exec_status);
        builder.addPlanNode(shared_from_this());
    }
    else
    {
        // For non fine grained shuffle, Aggregate would be broken into AggregateBuild and AggregateConvergent.
        auto agg_build = std::make_shared<PhysicalAggregationBuild>(
            executor_id,
            schema,
            log->identifier(),
            child,
            before_agg_actions,
            aggregation_keys,
            aggregation_collators,
            is_final_agg,
            aggregate_descriptions,
            aggregate_context);
        // Break the pipeline for agg_build.
        auto agg_build_builder = builder.breakPipeline(agg_build);
        // agg_build pipeline.
        child->buildPipeline(agg_build_builder, context, exec_status);
        agg_build_builder.build();
        // agg_convergent pipeline.
        auto agg_convergent = std::make_shared<PhysicalAggregationConvergent>(
            executor_id,
            schema,
            log->identifier(),
            aggregate_context,
            expr_after_agg);
        builder.addPlanNode(agg_convergent);
    }
}

void PhysicalAggregation::finalize(const Names & parent_require)
{
    // schema.size() >= parent_require.size()
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
    expr_after_agg->finalize(DB::toNames(schema));

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
    return expr_after_agg->getSampleBlock();
}
} // namespace DB
