#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzerHelper.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Planner/toPhysicalPlan.h>
#include <Storages/Transaction/TypeMapping.h>

namespace DB
{
ExpressionActionsPtr ToPhysicalPlanBuilder::newActionsForNewPlan()
{
    assert(!schema.empty());
    assert(cur_plan);

    ColumnsWithTypeAndName actions_input_column = cur_plan->getSampleBlock().getColumnsWithTypeAndName();
    return std::make_shared<ExpressionActions>(actions_input_column, context.getSettingsRef());
}

void ToPhysicalPlanBuilder::toPhysicalPlan(const String & executor_id, const tipb::Aggregation & aggregation)
{
    assert(!schema.empty());
    assert(cur_plan);
}

void ToPhysicalPlanBuilder::toPhysicalPlan(const String & executor_id, const tipb::Selection & selection)
{
    assert(!schema.empty());
    assert(cur_plan);

    ExpressionActionsPtr actions = newActionsForNewPlan();

    DAGExpressionAnalyzer analyzer{schema, context};

    String filter_column_name;
    if (selection.conditions_size() == 1)
    {
        filter_column_name = analyzer.getActions(selection.conditions(0), actions, true);
        if (isColumnExpr(selection.conditions(0))
            && (!exprHasValidFieldType(selection.conditions(0))
                /// if the column is not UInt8 type, we already add some convert function to convert it ot UInt8 type
                || DAGExpressionAnalyzerHelper::isUInt8Type(getDataTypeByFieldTypeForComputingLayer(selection.conditions(0).field_type()))))
        {
            /// FilterBlockInputStream will CHANGE the filter column inplace, so
            /// filter column should never be a columnRef in DAG request, otherwise
            /// for queries like select c1 from t where c1 will got wrong result
            /// as after FilterBlockInputStream, c1 will become a const column of 1
            filter_column_name = analyzer.convertToUInt8(actions, filter_column_name);
        }
    }
    else
    {
        Names arg_names;
        for (const auto & condition : selection.conditions())
            arg_names.push_back(analyzer.getActions(condition, actions, true));
        // connect all the conditions by logical and
        filter_column_name = analyzer.applyFunction("and", arg_names, actions, nullptr);
    }

    auto filter_plan = std::make_shared<PhysicalFilter>(executor_id, schema, filter_column_name, actions);
    filter_plan->appendChild(cur_plan);
    cur_plan = filter_plan;
}

void ToPhysicalPlanBuilder::toPhysicalPlan(const String & executor_id, const tipb::Limit & limit)
{
    assert(!schema.empty());
    assert(cur_plan);

    auto limit_plan = std::make_shared<PhysicalLimit>(executor_id, schema, limit.limit());
    limit_plan->appendChild(cur_plan);
    cur_plan = limit_plan;
}

void ToPhysicalPlanBuilder::toPhysicalPlan(const String & executor_id, const tipb::TopN & top_n)
{
    assert(!schema.empty());
    assert(cur_plan);

    if (top_n.order_by_size() == 0)
    {
        throw TiFlashException("TopN executor without order by exprs", Errors::Coprocessor::BadRequest);
    }
    std::vector<NameAndTypePair> order_columns;
    order_columns.reserve(top_n.order_by_size());

    auto actions = newActionsForNewPlan();
    DAGExpressionAnalyzer analyzer{schema, context};

    for (const tipb::ByItem & by_item : top_n.order_by())
    {
        String name = analyzer.getActions(by_item.expr(), actions);
        auto type = actions->getSampleBlock().getByName(name).type;
        order_columns.emplace_back(name, type);
    }
    SortDescription order_descr = getSortDescription(order_columns, top_n.order_by());

    cur_plan = std::make_shared<PhysicalTopN>(executor_id, schema, order_descr, actions, top_n.limit());
}

void ToPhysicalPlanBuilder::toPhysicalPlan(const String & executor_id, const NamesAndTypes & source_schema, const Block & source_sample_block)
{
    assert(!cur_plan);
    assert(schema.empty());
    schema = source_schema;
    assert(!schema.empty());
    cur_plan = std::make_shared<PhysicalSource>(executor_id, source_schema, source_sample_block);
}

void ToPhysicalPlanBuilder::appendNonRootFinalProjection(const String & column_prefix)
{
    assert(!schema.empty());
    assert(cur_plan);

    NamesAndTypes new_schema;
    NamesWithAliases final_project_aliases;
    UniqueNameGenerator unique_name_generator;
    for (const auto & column : schema)
    {
        auto new_name = unique_name_generator.toUniqueName(column_prefix + column.name);
        final_project_aliases.emplace_back(column.name, new_name);
        new_schema.emplace_back(new_name, column.type);
    }

    auto actions = newActionsForNewPlan();
    actions->add(ExpressionAction::project(final_project_aliases));

    schema = std::move(new_schema);
    auto non_root_final_projection_plan = std::make_shared<PhysicalProjection>("NonRootFinalProjection", schema, actions);
    non_root_final_projection_plan->appendChild(cur_plan);
    cur_plan = non_root_final_projection_plan;
    cur_plan->disableRecordProfileStreams();
}

void ToPhysicalPlanBuilder::appendRootFinalProjection(
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

    NamesAndTypes new_schema;
    if (!need_append_timezone_cast && !need_append_type_cast)
    {
        for (auto i : output_offsets)
        {
            auto new_name = unique_name_generator.toUniqueName(column_prefix + schema[i].name);
            final_project_aliases.emplace_back(schema[i].name, new_name);
            new_schema.emplace_back(new_name, schema[i].type);
        }
    }
    else
    {
        /// for all the columns that need to be returned, if the type is timestamp, then convert
        /// the timestamp column to UTC based, refer to appendTimeZoneCastsAfterTS for more details
        DAGExpressionAnalyzer analyzer{schema, context};

        tipb::Expr tz_expr = DAGExpressionAnalyzerHelper::constructTZExpr(context.getTimezoneInfo());
        String tz_col;
        String tz_cast_func_name = context.getTimezoneInfo().is_name_based ? "ConvertTimeZoneToUTC" : "ConvertTimeZoneByOffsetToUTC";
        std::vector<Int32> casted(require_schema.size(), 0);
        std::unordered_map<String, size_t> casted_index_map;

        for (size_t index = 0; index < output_offsets.size(); ++index)
        {
            UInt32 i = output_offsets[index];
            if ((need_append_timezone_cast && require_schema[i].tp() == TiDB::TypeTimestamp) || need_append_type_cast_vec[index])
            {
                const auto & it = casted_index_map.find(schema[i].name);
                if (it == casted_index_map.end())
                {
                    /// first add timestamp cast
                    String updated_name = schema[i].name;
                    auto updated_data_type = schema[i].type;
                    if (need_append_timezone_cast && require_schema[i].tp() == TiDB::TypeTimestamp)
                    {
                        if (tz_col.length() == 0)
                            tz_col = analyzer.getActions(tz_expr, actions);
                        updated_name = analyzer.appendTimeZoneCast(tz_col, schema[i].name, tz_cast_func_name, actions);
                    }
                    /// then add type cast
                    if (need_append_type_cast_vec[index])
                    {
                        updated_data_type = getDataTypeByFieldTypeForComputingLayer(require_schema[i]);
                        updated_name = analyzer.appendCast(updated_data_type, actions, updated_name);
                    }
                    auto new_name = unique_name_generator.toUniqueName(column_prefix + updated_name);
                    final_project_aliases.emplace_back(updated_name, new_name);
                    casted_index_map[schema[i].name] = index;
                    new_schema.emplace_back(new_name, updated_data_type);
                }
                else
                {
                    auto has_casted_index = it->second;
                    auto new_name = unique_name_generator.toUniqueName(column_prefix + final_project_aliases[has_casted_index].first);
                    final_project_aliases.emplace_back(final_project_aliases[has_casted_index].first, new_name);
                    new_schema.emplace_back(new_name, new_schema[has_casted_index].type);
                }
            }
            else
            {
                auto new_name = unique_name_generator.toUniqueName(column_prefix + schema[i].name);
                final_project_aliases.emplace_back(schema[i].name, new_name);
                new_schema.emplace_back(new_name, schema[i].type);
            }
        }
    }

    actions->add(ExpressionAction::project(final_project_aliases));

    schema = std::move(new_schema);
    auto root_final_projection_plan = std::make_shared<PhysicalProjection>("RootFinalProjection", schema, actions);
    root_final_projection_plan->appendChild(cur_plan);
    cur_plan = root_final_projection_plan;
    cur_plan->disableRecordProfileStreams();
}
} // namespace DB