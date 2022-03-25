#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Planner/plans/PhysicalAggregation.h>
#include <Flash/Planner/plans/PhysicalExchangeSender.h>
#include <Flash/Planner/plans/PhysicalFilter.h>
#include <Flash/Planner/plans/PhysicalLimit.h>
#include <Flash/Planner/plans/PhysicalProjection.h>
#include <Flash/Planner/plans/PhysicalSource.h>
#include <Flash/Planner/plans/PhysicalTopN.h>
#include <Flash/Planner/toPhysicalPlan.h>
#include <Storages/Transaction/TypeMapping.h>

namespace DB
{
namespace
{
bool isFinalAgg(const tipb::Expr & expr)
{
    if (!expr.has_aggfuncmode())
        /// set default value to true to make it compatible with old version of TiDB since before this
        /// change, all the aggregation in TiFlash is treated as final aggregation
        return true;
    return expr.aggfuncmode() == tipb::AggFunctionMode::FinalMode || expr.aggfuncmode() == tipb::AggFunctionMode::CompleteMode;
}

Names schemaToNames(const NamesAndTypes & schema)
{
    Names names;
    names.reserve(schema.size());
    for (const auto & column : schema)
        names.push_back(column.name);
    return names;
}
} // namespace

void PhysicalPlanBuilder::assignCurPlan(const PhysicalPlanPtr & new_cur_plan)
{
    new_cur_plan->appendChild(cur_plan);
    cur_plan = new_cur_plan;
}

ExpressionActionsPtr PhysicalPlanBuilder::newActionsForNewPlan()
{
    assert(!schema.empty());
    assert(cur_plan);

    ColumnsWithTypeAndName actions_input_column = cur_plan->getSampleBlock().getColumnsWithTypeAndName();
    return std::make_shared<ExpressionActions>(actions_input_column, context.getSettingsRef());
}

ExpressionActionsPtr PhysicalPlanBuilder::newActionsFromSchema()
{
    NamesAndTypesList actions_input_column;
    std::unordered_set<String> column_name_set;
    for (const auto & col : schema)
    {
        if (column_name_set.find(col.name) == column_name_set.end())
        {
            actions_input_column.emplace_back(col.name, col.type);
            column_name_set.emplace(col.name);
        }
    }
    return std::make_shared<ExpressionActions>(actions_input_column, context.getSettingsRef());
}

void PhysicalPlanBuilder::buildAggregation(const String & executor_id, const tipb::Aggregation & aggregation)
{
    assert(!schema.empty());
    assert(cur_plan);

    /// set default value to true to make it compatible with old version of TiDB since before this
    /// change, all the aggregation in TiFlash is treated as final aggregation
    bool is_final_agg = true;
    if (aggregation.agg_func_size() > 0 && !isFinalAgg(aggregation.agg_func(0)))
        is_final_agg = false;
    for (int i = 1; i < aggregation.agg_func_size(); ++i)
    {
        if (is_final_agg != isFinalAgg(aggregation.agg_func(i)))
            throw TiFlashException("Different aggregation mode detected", Errors::Coprocessor::BadRequest);
    }
    // todo now we can tell if the aggregation is final stage or partial stage, maybe we can do collation insensitive
    //  aggregation if the stage is partial
    bool group_by_collation_sensitive =
        /// collation sensitive group by is slower than normal group by, use normal group by by default
        context.getSettingsRef().group_by_collation_sensitive || context.getDAGContext()->isMPPTask();

    if (aggregation.group_by_size() == 0 && aggregation.agg_func_size() == 0)
    {
        //should not reach here
        throw TiFlashException("Aggregation executor without group by/agg exprs", Errors::Coprocessor::BadRequest);
    }

    DAGExpressionAnalyzer analyzer{schema, context};

    ExpressionActionsPtr before_agg_actions = newActionsForNewPlan();
    NamesAndTypes aggregated_columns;
    AggregateDescriptions aggregate_descriptions;
    Names aggregation_keys;
    TiDB::TiDBCollators collators;
    std::unordered_set<String> agg_key_set;
    analyzer.buildAggFuncs(aggregation, before_agg_actions, aggregate_descriptions, aggregated_columns);
    analyzer.buildAggGroupBy(aggregation.group_by(), before_agg_actions, aggregate_descriptions, aggregated_columns, aggregation_keys, agg_key_set, group_by_collation_sensitive, collators);

    schema = std::move(aggregated_columns);

    auto cast_after_agg_actions = newActionsFromSchema();
    analyzer.source_columns = schema;
    analyzer.appendCastAfterAgg(cast_after_agg_actions, aggregation);
    if (cast_after_agg_actions->getActions().empty())
        cast_after_agg_actions->add(ExpressionAction::project(schemaToNames(schema)));
    schema = analyzer.getCurrentInputColumns();

    assignCurPlan(std::make_shared<PhysicalAggregation>(executor_id, schema, before_agg_actions, aggregation_keys, collators, aggregate_descriptions, cast_after_agg_actions));
}

void PhysicalPlanBuilder::buildFilter(const String & executor_id, const tipb::Selection & selection)
{
    assert(!schema.empty());
    assert(cur_plan);

    ExpressionActionsPtr actions = newActionsForNewPlan();

    DAGExpressionAnalyzer analyzer{schema, context};

    std::vector<const tipb::Expr *> conditions;
    for (const auto & c : selection.conditions())
        conditions.push_back(&c);
    String filter_column_name = analyzer.buildFilterColumn(actions, conditions);

    assignCurPlan(std::make_shared<PhysicalFilter>(executor_id, schema, filter_column_name, actions));
}

void PhysicalPlanBuilder::buildLimit(const String & executor_id, const tipb::Limit & limit)
{
    assert(!schema.empty());
    assert(cur_plan);

    assignCurPlan(std::make_shared<PhysicalLimit>(executor_id, schema, limit.limit()));
}

void PhysicalPlanBuilder::buildTopN(const String & executor_id, const tipb::TopN & top_n)
{
    assert(!schema.empty());
    assert(cur_plan);

    if (top_n.order_by_size() == 0)
    {
        throw TiFlashException("TopN executor without order by exprs", Errors::Coprocessor::BadRequest);
    }

    auto actions = newActionsForNewPlan();
    DAGExpressionAnalyzer analyzer{schema, context};
    auto order_columns = analyzer.buildOrderColumns(actions, top_n.order_by());
    SortDescription order_descr = getSortDescription(order_columns, top_n.order_by());

    assignCurPlan(std::make_shared<PhysicalTopN>(executor_id, schema, order_descr, actions, top_n.limit()));
}

void PhysicalPlanBuilder::buildExchangeSender(const String & executor_id, const tipb::ExchangeSender & exchange_sender)
{
    assert(!schema.empty());
    assert(cur_plan);

    /// get partition column ids
    const auto & part_keys = exchange_sender.partition_keys();
    std::vector<Int64> partition_col_id;
    TiDB::TiDBCollators collators;
    /// in case TiDB is an old version, it has no collation info
    bool has_collator_info = exchange_sender.types_size() != 0;
    if (has_collator_info && part_keys.size() != exchange_sender.types_size())
    {
        throw TiFlashException(
            std::string(__PRETTY_FUNCTION__) + ": Invalid plan, in ExchangeSender, the length of partition_keys and types is not the same when TiDB new collation is enabled",
            Errors::Coprocessor::BadRequest);
    }
    for (int i = 0; i < part_keys.size(); ++i)
    {
        const auto & expr = part_keys[i];
        assert(isColumnExpr(expr));
        auto column_index = decodeDAGInt64(expr.val());
        partition_col_id.emplace_back(column_index);
        if (has_collator_info && removeNullable(getDataTypeByFieldTypeForComputingLayer(expr.field_type()))->isString())
        {
            collators.emplace_back(getCollatorFromFieldType(exchange_sender.types(i)));
        }
        else
        {
            collators.emplace_back(nullptr);
        }
    }

    assignCurPlan(std::make_shared<PhysicalExchangeSender>(executor_id, schema, partition_col_id, collators, exchange_sender.tp()));
}

void PhysicalPlanBuilder::buildSource(const String & executor_id, const NamesAndTypes & source_schema, const Block & source_sample_block)
{
    assert(!cur_plan);
    assert(schema.empty());
    schema = source_schema;
    assert(!schema.empty());
    cur_plan = std::make_shared<PhysicalSource>(executor_id, source_schema, source_sample_block);
}

void PhysicalPlanBuilder::buildNonRootFinalProjection(const String & column_prefix)
{
    assert(!schema.empty());
    assert(cur_plan);

    DAGExpressionAnalyzer analyzer{schema, context};
    auto final_project_aliases = analyzer.genNonRootFinalProjectAliases(column_prefix);
    auto actions = newActionsForNewPlan();
    actions->add(ExpressionAction::project(final_project_aliases));

    schema = analyzer.getCurrentInputColumns();

    assignCurPlan(std::make_shared<PhysicalProjection>("NonRootFinalProjection", schema, actions));
    cur_plan->disableRecordProfileStreams();
}

void PhysicalPlanBuilder::buildRootFinalProjection(
    const std::vector<tipb::FieldType> & require_schema,
    const std::vector<Int32> & output_offsets,
    const String & column_prefix,
    bool keep_session_timezone_info)
{
    assert(!schema.empty());
    assert(cur_plan);

    if (unlikely(output_offsets.empty()))
        throw Exception("Root Query block without output_offsets", ErrorCodes::LOGICAL_ERROR);

    auto actions = newActionsForNewPlan();
    DAGExpressionAnalyzer analyzer{schema, context};

    bool need_append_timezone_cast = !keep_session_timezone_info && !context.getTimezoneInfo().is_utc_timezone;
    auto [need_append_type_cast, need_append_type_cast_vec] = analyzer.checkIfCastIsRequired(require_schema, output_offsets);

    if (need_append_timezone_cast || need_append_type_cast)
    {
        analyzer.appendCastForRootFinalProjection(actions, require_schema, output_offsets, need_append_timezone_cast, need_append_type_cast_vec);
    }

    NamesWithAliases final_project_aliases = analyzer.genRootFinalProjectAliases(column_prefix, output_offsets);
    actions->add(ExpressionAction::project(final_project_aliases));

    schema = analyzer.getCurrentInputColumns();

    assignCurPlan(std::make_shared<PhysicalProjection>("RootFinalProjection", schema, actions));
    cur_plan->disableRecordProfileStreams();
}
} // namespace DB