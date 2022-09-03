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
#include <Flash/Coprocessor/AggregationInterpreterHelper.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/plans/PhysicalAggregation.h>
#include <Flash/Planner/plans/PhysicalFinalAggregation.h>
#include <Flash/Planner/plans/PhysicalPartialAggregation.h>
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

    auto expr_after_agg_actions = PhysicalPlanHelper::newActions(aggregated_columns, context);
    analyzer.reset(aggregated_columns);
    analyzer.appendCastAfterAgg(expr_after_agg_actions, aggregation);
    /// project action after aggregation to remove useless columns.
    auto schema = PhysicalPlanHelper::addSchemaProjectAction(expr_after_agg_actions, analyzer.getCurrentInputColumns());

    const auto & settings = context.getSettingsRef();
    AggregateStorePtr aggregate_store = std::make_shared<AggregateStore>(
        log->identifier(),
        context.getFileProvider(),
        true,
        settings.aggregation_memory_efficient_merge_threads ? static_cast<size_t>(settings.aggregation_memory_efficient_merge_threads) : static_cast<size_t>(settings.max_threads));

    auto physical_partial_agg = std::make_shared<PhysicalPartialAggregation>(
        executor_id,
        schema,
        log->identifier(),
        child,
        before_agg_actions,
        aggregation_keys,
        collators,
        AggregationInterpreterHelper::isFinalAgg(aggregation),
        aggregate_descriptions,
        aggregate_store);
    physical_partial_agg->notTiDBOperator();
    physical_partial_agg->disableRestoreConcurrency();

    auto physical_final_agg = std::make_shared<PhysicalFinalAggregation>(
        executor_id,
        schema,
        log->identifier(),
        aggregate_store,
        expr_after_agg_actions);

    auto physical_agg = std::make_shared<PhysicalAggregation>(
        executor_id,
        schema,
        log->identifier(),
        physical_partial_agg,
        physical_final_agg);
    physical_agg->notTiDBOperator();
    physical_agg->disableRestoreConcurrency();
    return physical_agg;
}

void PhysicalAggregation::transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    partial()->transform(pipeline, context, max_streams);
    executeUnion(pipeline, max_streams, log, /*ignore_block=*/true, "for pratial agg");

    {
        DAGPipeline final_pipeline;
        final()->transform(final_pipeline, context, max_streams);
        assert(final_pipeline.streams_with_non_joined_data.empty());
        pipeline.streams_with_non_joined_data = final_pipeline.streams;
    }
}

void PhysicalAggregation::finalize(const Names & parent_require)
{
    return final()->finalize(parent_require);
}

const Block & PhysicalAggregation::getSampleBlock() const
{
    return final()->getSampleBlock();
}
} // namespace DB
