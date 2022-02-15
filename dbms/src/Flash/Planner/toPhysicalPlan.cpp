#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Planner/toPhysicalPlan.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzerHelper.h>
#include <Storages/Transaction/TypeMapping.h>

namespace DB
{
namespace
{
tipb::Expr constructTZExpr(const TimezoneInfo & dag_timezone_info)
{
    if (dag_timezone_info.is_name_based)
        return constructStringLiteralTiExpr(dag_timezone_info.timezone_name);
    else
        return constructInt64LiteralTiExpr(dag_timezone_info.timezone_offset);
}
}

ExpressionActionsPtr ToPhysicalPlanBuilder::newActionsForNewPlan()
{
    if (!cur_plan)
    {
        NamesAndTypesList actions_input_column;
        for (const auto & column : schema)
            actions_input_column.emplace_back(column.name, column.type);
        return std::make_shared<ExpressionActions>(actions_input_column, context.getSettingsRef());
    }
    else
    {
        
    }
}

void ToPhysicalPlanBuilder::toPhysicalPlan(const String &, const tipb::Aggregation &)
{
    assert(!schema.empty());
    assert(cur_plan);
}

void ToPhysicalPlanBuilder::toPhysicalPlan(const String &, const tipb::Selection &)
{
    assert(!schema.empty());
    assert(cur_plan);

    initChain(chain, getCurrentInputColumns());
    ExpressionActionsChain::Step & last_step = chain.steps.back();

    String filter_column_name;
    if (conditions.size() == 1)
    {
        filter_column_name = getActions(*conditions[0], last_step.actions, true);
        if (isColumnExpr(*conditions[0])
        && (!exprHasValidFieldType(*conditions[0])
        /// if the column is not UInt8 type, we already add some convert function to convert it ot UInt8 type
        || isUInt8Type(getDataTypeByFieldTypeForComputingLayer(conditions[0]->field_type()))))
        {
            /// FilterBlockInputStream will CHANGE the filter column inplace, so
            /// filter column should never be a columnRef in DAG request, otherwise
            /// for queries like select c1 from t where c1 will got wrong result
            /// as after FilterBlockInputStream, c1 will become a const column of 1
            filter_column_name = convertToUInt8(last_step.actions, filter_column_name);
        }
    }
    else
    {
        Names arg_names;
        for (const auto * condition : conditions)
            arg_names.push_back(getActions(*condition, last_step.actions, true));
        // connect all the conditions by logical and
        filter_column_name = applyFunction("and", arg_names, last_step.actions, nullptr);
    }
    chain.steps.back().required_output.push_back(filter_column_name);
    return filter_column_name;
}

void ToPhysicalPlanBuilder::toPhysicalPlan(const String & executor_id, const tipb::Limit & limit)
{
    assert(!schema.empty());
    assert(cur_plan);
}

void ToPhysicalPlanBuilder::toPhysicalPlan(const String &, const tipb::Projection &)
{
    assert(!schema.empty());
    assert(cur_plan);
}

void ToPhysicalPlanBuilder::toPhysicalPlan(const String &, const tipb::TopN &)
{
    assert(!schema.empty());
    assert(cur_plan);
}

void ToPhysicalPlanBuilder::toPhysicalPlan(const String & executor_id, const NamesAndTypes & source_schema, const Block & source_sample_block)
{
    assert(!cur_plan);
    assert(schema.empty());
    cur_plan = std::make_shared<PhysicalSource>(executor_id, source_schema, source_sample_block);
    schema = source_schema;
    assert(!schema.empty());
}

void ToPhysicalPlanBuilder::appendNonRootFinalProjection(const String & column_prefix)
{
    assert(!schema.empty());
    assert(cur_plan);

    NamesWithAliases final_project_aliases;
    UniqueNameGenerator unique_name_generator;
    for (const auto & column : schema)
        final_project_aliases.emplace_back(column.name, unique_name_generator.toUniqueName(column_prefix + column.name));

    auto actions = newActions(schema, context);
    actions->add(ExpressionAction::project(final_project_aliases));

    cur_plan = std::shared_ptr<PhysicalProjection>("NonRootFinalProjection", schema, actions);
}

void ToPhysicalPlanBuilder::appendRootFinalProjection(
    const std::vector<tipb::FieldType> & require_schema,
    const std::vector<Int32> & output_offsets,
    const String & column_prefix,
    bool keep_session_timezone_info)
{
    assert(!schema.empty());

    if (unlikely(output_offsets.empty()))
        throw Exception("Root Query block without output_offsets", ErrorCodes::LOGICAL_ERROR);

    auto actions = newActions(schema, context);

    NamesWithAliases final_project_aliases;
    UniqueNameGenerator unique_name_generator;
    bool need_append_timezone_cast = !keep_session_timezone_info && !context.getTimezoneInfo().is_utc_timezone;
    /// TiDB can not guarantee that the field type in DAG request is accurate, so in order to make things work,
    /// TiFlash will append extra type cast if needed.
    bool need_append_type_cast = false;
    BoolVec need_append_type_cast_vec;
    /// we need to append type cast for root block if necessary
    for (UInt32 i : output_offsets)
    {
        const auto & actual_type = schema[i].type;
        auto expected_type = getDataTypeByFieldTypeForComputingLayer(require_schema[i]);
        if (actual_type->getName() != expected_type->getName())
        {
            need_append_type_cast = true;
            need_append_type_cast_vec.push_back(true);
        }
        else
        {
            need_append_type_cast_vec.push_back(false);
        }
    }

    if (!need_append_timezone_cast && !need_append_type_cast)
    {
        for (auto i : output_offsets)
        {
            final_project_aliases.emplace_back(
                schema[i].name,
                unique_name_generator.toUniqueName(column_prefix + schema[i].name));
        }
    }
    else
    {
        /// for all the columns that need to be returned, if the type is timestamp, then convert
        /// the timestamp column to UTC based, refer to appendTimeZoneCastsAfterTS for more details
        DAGExpressionAnalyzer analyzer{schema, context};

        tipb::Expr tz_expr = constructTZExpr(context.getTimezoneInfo());
        String tz_col;
        String tz_cast_func_name = context.getTimezoneInfo().is_name_based ? "ConvertTimeZoneToUTC" : "ConvertTimeZoneByOffsetToUTC";
        std::vector<Int32> casted(require_schema.size(), 0);
        std::unordered_map<String, String> casted_name_map;

        for (size_t index = 0; index < output_offsets.size(); index++)
        {
            UInt32 i = output_offsets[index];
            if ((need_append_timezone_cast && require_schema[i].tp() == TiDB::TypeTimestamp) || need_append_type_cast_vec[index])
            {
                const auto & it = casted_name_map.find(schema[i].name);
                if (it == casted_name_map.end())
                {
                    /// first add timestamp cast
                    String updated_name = schema[i].name;
                    if (need_append_timezone_cast && require_schema[i].tp() == TiDB::TypeTimestamp)
                    {
                        if (tz_col.length() == 0)
                            tz_col = analyzer.getActions(tz_expr, actions);
                        updated_name = analyzer.appendTimeZoneCast(tz_col, schema[i].name, tz_cast_func_name, actions);
                    }
                    /// then add type cast
                    if (need_append_type_cast_vec[index])
                    {
                        updated_name = analyzer.appendCast(getDataTypeByFieldTypeForComputingLayer(require_schema[i]), actions, updated_name);
                    }
                    final_project_aliases.emplace_back(updated_name, unique_name_generator.toUniqueName(column_prefix + updated_name));
                    casted_name_map[schema[i].name] = updated_name;
                }
                else
                {
                    final_project_aliases.emplace_back(it->second, unique_name_generator.toUniqueName(column_prefix + it->second));
                }
            }
            else
            {
                final_project_aliases.emplace_back(
                    schema[i].name,
                    unique_name_generator.toUniqueName(column_prefix + schema[i].name));
            }
        }
    }

    actions->add(ExpressionAction::project(final_project_aliases));

    cur_plan = std::shared_ptr<PhysicalProjection>("RootFinalProjection", schema, actions);
}
} // namespace DB