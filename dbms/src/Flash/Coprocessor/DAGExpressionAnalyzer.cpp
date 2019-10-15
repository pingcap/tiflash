#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnSet.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/FieldToDataType.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/Set.h>
#include <Interpreters/convertFieldToType.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/Codec.h>
#include <Storages/Transaction/TypeMapping.h>

namespace DB
{

namespace ErrorCodes
{
extern const int COP_BAD_DAG_REQUEST;
extern const int UNSUPPORTED_METHOD;
} // namespace ErrorCodes

static String genFuncString(const String & func_name, const Names & argument_names)
{
    std::stringstream ss;
    ss << func_name << "(";
    bool first = true;
    for (const String & argument_name : argument_names)
    {
        if (first)
        {
            first = false;
        }
        else
        {
            ss << ", ";
        }
        ss << argument_name;
    }
    ss << ") ";
    return ss.str();
}

DAGExpressionAnalyzer::DAGExpressionAnalyzer(const std::vector<NameAndTypePair> && source_columns_, const Context & context_)
    : source_columns(source_columns_),
      context(context_),
      after_agg(false),
      implicit_cast_count(0),
      log(&Logger::get("DAGExpressionAnalyzer"))
{
    settings = context.getSettings();
}

void DAGExpressionAnalyzer::appendAggregation(
    ExpressionActionsChain & chain, const tipb::Aggregation & agg, Names & aggregation_keys, AggregateDescriptions & aggregate_descriptions)
{
    if (agg.group_by_size() == 0 && agg.agg_func_size() == 0)
    {
        //should not reach here
        throw Exception("Aggregation executor without group by/agg exprs", ErrorCodes::COP_BAD_DAG_REQUEST);
    }
    initChain(chain, getCurrentInputColumns());
    ExpressionActionsChain::Step & step = chain.steps.back();

    Names agg_argument_names;
    for (const tipb::Expr & expr : agg.agg_func())
    {
        const String & agg_func_name = getAggFunctionName(expr);
        AggregateDescription aggregate;
        DataTypes types(expr.children_size());
        aggregate.argument_names.resize(expr.children_size());
        for (Int32 i = 0; i < expr.children_size(); i++)
        {
            String arg_name = getActions(expr.children(i), step.actions);
            agg_argument_names.push_back(arg_name);
            types[i] = step.actions->getSampleBlock().getByName(arg_name).type;
            aggregate.argument_names[i] = arg_name;
        }
        String func_string = genFuncString(agg_func_name, agg_argument_names);
        aggregate.column_name = func_string;
        //todo de-duplicate aggregation column
        aggregate.parameters = Array();
        aggregate.function = AggregateFunctionFactory::instance().get(agg_func_name, types);
        aggregate_descriptions.push_back(aggregate);
        DataTypePtr result_type = aggregate.function->getReturnType();
        // this is a temp result since implicit cast maybe added on these aggregated_columns
        aggregated_columns.emplace_back(func_string, result_type);
    }

    std::move(agg_argument_names.begin(), agg_argument_names.end(), std::back_inserter(step.required_output));

    for (const tipb::Expr & expr : agg.group_by())
    {
        String name = getActions(expr, step.actions);
        step.required_output.push_back(name);
        // this is a temp result since implicit cast maybe added on these aggregated_columns
        aggregated_columns.emplace_back(name, step.actions->getSampleBlock().getByName(name).type);
        aggregation_keys.push_back(name);
    }
    after_agg = true;
}

bool isUInt8Type(const DataTypePtr & type)
{
    auto non_nullable_type = type->isNullable() ? std::dynamic_pointer_cast<const DataTypeNullable>(type)->getNestedType() : type;
    return std::dynamic_pointer_cast<const DataTypeUInt8>(non_nullable_type) != nullptr;
}

String DAGExpressionAnalyzer::applyFunction(const String & func_name, Names & arg_names, ExpressionActionsPtr & actions)
{
    String result_name = genFuncString(func_name, arg_names);
    if (actions->getSampleBlock().has(result_name))
        return result_name;
    const FunctionBuilderPtr & function_builder = FunctionFactory::instance().get(func_name, context);
    const ExpressionAction & apply_function = ExpressionAction::applyFunction(function_builder, arg_names, result_name);
    actions->add(apply_function);
    return result_name;
}

void DAGExpressionAnalyzer::appendWhere(ExpressionActionsChain & chain, const tipb::Selection & sel, String & filter_column_name)
{
    if (sel.conditions_size() == 0)
    {
        throw Exception("Selection executor without condition exprs", ErrorCodes::COP_BAD_DAG_REQUEST);
    }

    initChain(chain, getCurrentInputColumns());
    ExpressionActionsChain::Step & last_step = chain.steps.back();
    Names arg_names;
    for (auto & condition : sel.conditions())
    {
        arg_names.push_back(getActions(condition, last_step.actions));
    }
    if (arg_names.size() == 1)
    {
        filter_column_name = arg_names[0];
    }
    else
    {
        // connect all the conditions by logical and
        filter_column_name = applyFunction("and", arg_names, last_step.actions);
    }

    auto & filter_column_type = chain.steps.back().actions->getSampleBlock().getByName(filter_column_name).type;
    if (!isUInt8Type(filter_column_type))
    {
        // find the original unit8 column
        auto & last_actions = last_step.actions->getActions();
        for (auto it = last_actions.rbegin(); it != last_actions.rend(); ++it)
        {
            if (it->type == ExpressionAction::Type::APPLY_FUNCTION && it->result_name == filter_column_name
                && it->function->getName() == "CAST")
            {
                // for cast function, the casted column is the first argument
                filter_column_name = it->argument_names[0];
                break;
            }
        }
    }
    chain.steps.back().required_output.push_back(filter_column_name);
}

void DAGExpressionAnalyzer::appendOrderBy(ExpressionActionsChain & chain, const tipb::TopN & topN, Strings & order_column_names)
{
    if (topN.order_by_size() == 0)
    {
        throw Exception("TopN executor without order by exprs", ErrorCodes::COP_BAD_DAG_REQUEST);
    }
    initChain(chain, getCurrentInputColumns());
    ExpressionActionsChain::Step & step = chain.steps.back();
    for (const tipb::ByItem & byItem : topN.order_by())
    {
        String name = getActions(byItem.expr(), step.actions);
        step.required_output.push_back(name);
        order_column_names.push_back(name);
    }
}

const std::vector<NameAndTypePair> & DAGExpressionAnalyzer::getCurrentInputColumns()
{
    return after_agg ? aggregated_columns : source_columns;
}

void DAGExpressionAnalyzer::appendFinalProject(ExpressionActionsChain & chain, const NamesWithAliases & final_project)
{
    initChain(chain, getCurrentInputColumns());
    for (const auto & name : final_project)
    {
        chain.steps.back().required_output.push_back(name.first);
    }
}

void constructTZExpr(tipb::Expr & tz_expr, const tipb::DAGRequest & rqst, bool from_utc)
{
    if (rqst.has_time_zone_name() && rqst.time_zone_name().length() > 0)
    {
        tz_expr.set_tp(tipb::ExprType::String);
        tz_expr.set_val(rqst.time_zone_name());
        auto * field_type = tz_expr.mutable_field_type();
        field_type->set_tp(TiDB::TypeString);
        field_type->set_flag(TiDB::ColumnFlagNotNull);
    }
    else
    {
        tz_expr.set_tp(tipb::ExprType::Int64);
        std::stringstream ss;
        encodeDAGInt64(from_utc ? rqst.time_zone_offset() : -rqst.time_zone_offset(), ss);
        tz_expr.set_val(ss.str());
        auto * field_type = tz_expr.mutable_field_type();
        field_type->set_tp(TiDB::TypeLongLong);
        field_type->set_flag(TiDB::ColumnFlagNotNull);
    }
}

bool hasMeaningfulTZInfo(const tipb::DAGRequest & rqst)
{
    if (rqst.has_time_zone_name() && rqst.time_zone_name().length() > 0)
        return rqst.time_zone_name() != "UTC";
    if (rqst.has_time_zone_offset())
        return rqst.has_time_zone_offset() != 0;
    return false;
}

String DAGExpressionAnalyzer::appendTimeZoneCast(
    const String & tz_col, const String & ts_col, const String & func_name, ExpressionActionsPtr & actions)
{
    Names cast_argument_names;
    cast_argument_names.push_back(ts_col);
    cast_argument_names.push_back(tz_col);
    String cast_expr_name = applyFunction(func_name, cast_argument_names, actions);
    return cast_expr_name;
}

// add timezone cast after table scan, this is used for session level timezone support
// the basic idea of supporting session level timezone is that:
// 1. for every timestamp column used in the dag request, after reading it from table scan, we add
//    cast function to convert its timezone to the timezone specified in DAG request
// 2. for every timestamp column that will be returned to TiDB, we add cast function to convert its
//    timezone to UTC
// for timestamp columns without any transformation or calculation(e.g. select ts_col from table),
// this will introduce two useless casts, in order to avoid these redundant cast, when cast the ts
// column to the columns with session-level timezone info, the original ts columns with UTC
// timezone are still kept
// for DAG request that does not contain agg, the final project will select the ts column with UTC
// timezone, which is exactly what TiDB want
// for DAG request that contains agg, any ts column after agg has session-level timezone info(since the ts
// column with UTC timezone will never be used in during agg), all the column with ts datatype will
// convert back to UTC timezone
bool DAGExpressionAnalyzer::appendTimeZoneCastsAfterTS(
    ExpressionActionsChain & chain, std::vector<bool> is_ts_column, const tipb::DAGRequest & rqst)
{
    if (!hasMeaningfulTZInfo(rqst))
        return false;

    bool ret = false;
    initChain(chain, getCurrentInputColumns());
    ExpressionActionsPtr actions = chain.getLastActions();
    tipb::Expr tz_expr;
    constructTZExpr(tz_expr, rqst, true);
    String tz_col;
    String func_name
        = rqst.has_time_zone_name() && rqst.time_zone_name().length() > 0 ? "ConvertTimeZoneFromUTC" : "ConvertTimeZoneByOffset";
    for (size_t i = 0; i < is_ts_column.size(); i++)
    {
        if (is_ts_column[i])
        {
            if (tz_col.length() == 0)
                tz_col = getActions(tz_expr, actions);
            String casted_name = appendTimeZoneCast(tz_col, source_columns[i].name, func_name, actions);
            source_columns.emplace_back(source_columns[i].name, source_columns[i].type);
            source_columns[i].name = casted_name;
            ret = true;
        }
    }
    return ret;
}

void DAGExpressionAnalyzer::appendAggSelect(
    ExpressionActionsChain & chain, const tipb::Aggregation & aggregation, const tipb::DAGRequest & rqst)
{
    initChain(chain, getCurrentInputColumns());
    bool need_update_aggregated_columns = false;
    NamesAndTypesList updated_aggregated_columns;
    ExpressionActionsChain::Step step = chain.steps.back();
    bool need_append_timezone_cast = hasMeaningfulTZInfo(rqst);
    tipb::Expr tz_expr;
    if (need_append_timezone_cast)
        constructTZExpr(tz_expr, rqst, false);
    String tz_col;
    String tz_cast_func_name
        = rqst.has_time_zone_name() && rqst.time_zone_name().length() > 0 ? "ConvertTimeZoneToUTC" : "ConvertTimeZoneByOffset";
    for (Int32 i = 0; i < aggregation.agg_func_size(); i++)
    {
        String & name = aggregated_columns[i].name;
        String updated_name = appendCastIfNeeded(aggregation.agg_func(i), step.actions, name);
        if (need_append_timezone_cast && aggregation.agg_func(i).field_type().tp() == TiDB::TypeTimestamp)
        {
            if (tz_col.length() == 0)
                tz_col = getActions(tz_expr, step.actions);
            updated_name = appendTimeZoneCast(tz_col, updated_name, tz_cast_func_name, step.actions);
        }
        if (name != updated_name)
        {
            need_update_aggregated_columns = true;
            DataTypePtr type = step.actions->getSampleBlock().getByName(updated_name).type;
            updated_aggregated_columns.emplace_back(updated_name, type);
            step.required_output.push_back(updated_name);
        }
        else
        {
            updated_aggregated_columns.emplace_back(name, aggregated_columns[i].type);
            step.required_output.push_back(name);
        }
    }
    for (Int32 i = 0; i < aggregation.group_by_size(); i++)
    {
        String & name = aggregated_columns[i + aggregation.agg_func_size()].name;
        String updated_name = appendCastIfNeeded(aggregation.group_by(i), step.actions, name);
        if (need_append_timezone_cast && aggregation.group_by(i).field_type().tp() == TiDB::TypeTimestamp)
        {
            if (tz_col.length() == 0)
                tz_col = getActions(tz_expr, step.actions);
            updated_name = appendTimeZoneCast(tz_col, updated_name, tz_cast_func_name, step.actions);
        }
        if (name != updated_name)
        {
            need_update_aggregated_columns = true;
            DataTypePtr type = step.actions->getSampleBlock().getByName(updated_name).type;
            updated_aggregated_columns.emplace_back(updated_name, type);
            step.required_output.push_back(updated_name);
        }
        else
        {
            updated_aggregated_columns.emplace_back(name, aggregated_columns[i].type);
            step.required_output.push_back(name);
        }
    }

    if (need_update_aggregated_columns)
    {
        auto updated_agg_col_names = updated_aggregated_columns.getNames();
        auto updated_agg_col_types = updated_aggregated_columns.getTypes();
        aggregated_columns.clear();
        for (size_t i = 0; i < updated_aggregated_columns.size(); i++)
        {
            aggregated_columns.emplace_back(updated_agg_col_names[i], updated_agg_col_types[i]);
        }
    }
}

String DAGExpressionAnalyzer::appendCastIfNeeded(const tipb::Expr & expr, ExpressionActionsPtr & actions, const String & expr_name)
{
    if (!expr.has_field_type() && context.getSettingsRef().dag_expr_field_type_strict_check)
    {
        throw Exception("Expression without field type", ErrorCodes::COP_BAD_DAG_REQUEST);
    }
    if (exprHasValidFieldType(expr) && isFunctionExpr(expr))
    {
        DataTypePtr expected_type = getDataTypeByFieldType(expr.field_type());
        DataTypePtr actual_type = actions->getSampleBlock().getByName(expr_name).type;
        //todo maybe use a more decent compare method
        // todo ignore nullable info??
        if (expected_type->getName() != actual_type->getName())
        {
            LOG_DEBUG(
                log, __PRETTY_FUNCTION__ << " Add implicit cast: from " << actual_type->getName() << " to " << expected_type->getName());
            implicit_cast_count++;
            // need to add cast function
            // first construct the second argument
            tipb::Expr type_expr;
            type_expr.set_tp(tipb::ExprType::String);
            type_expr.set_val(expected_type->getName());
            auto * type_field_type = type_expr.mutable_field_type();
            type_field_type->set_tp(TiDB::TypeString);
            type_field_type->set_flag(TiDB::ColumnFlagNotNull);
            auto type_expr_name = getActions(type_expr, actions);

            Names cast_argument_names;
            cast_argument_names.push_back(expr_name);
            cast_argument_names.push_back(type_expr_name);
            String cast_expr_name = applyFunction("CAST", cast_argument_names, actions);
            return cast_expr_name;
        }
        else
        {
            return expr_name;
        }
    }
    return expr_name;
}

void DAGExpressionAnalyzer::makeExplicitSetForIndex(const tipb::Expr & expr, const TMTStoragePtr & storage)
{
    for (auto & child : expr.children())
    {
        makeExplicitSetForIndex(child, storage);
    }
    if (expr.tp() != tipb::ExprType::ScalarFunc)
    {
        return;
    }
    const String & func_name = getFunctionName(expr);
    // only support col_name in (value_list)
    if (isInOrGlobalInOperator(func_name) && expr.children(0).tp() == tipb::ExprType::ColumnRef && !prepared_sets.count(&expr))
    {
        NamesAndTypesList column_list;
        for (const auto & col : getCurrentInputColumns())
        {
            column_list.emplace_back(col.name, col.type);
        }
        ExpressionActionsPtr temp_actions = std::make_shared<ExpressionActions>(column_list, settings);
        String name = getActions(expr.children(0), temp_actions);
        ASTPtr name_ast = std::make_shared<ASTIdentifier>(name);
        if (storage->mayBenefitFromIndexForIn(name_ast))
        {
            makeExplicitSet(expr, temp_actions->getSampleBlock(), true, name);
        }
    }
}

void DAGExpressionAnalyzer::makeExplicitSet(
    const tipb::Expr & expr, const Block & sample_block, bool create_ordered_set, const String & left_arg_name)
{
    if (prepared_sets.count(&expr))
    {
        return;
    }
    DataTypes set_element_types;
    // todo support tuple in, i.e. (a,b) in ((1,2), (3,4)), currently TiDB convert tuple in into a series of or/and/eq exprs
    //  which means tuple in is never be pushed to coprocessor, but it is quite in-efficient
    set_element_types.push_back(sample_block.getByName(left_arg_name).type);

    // todo if this is a single value in, then convert it to equal expr
    SetPtr set = std::make_shared<Set>(SizeLimits(settings.max_rows_in_set, settings.max_bytes_in_set, settings.set_overflow_mode));
    set->createFromDAGExpr(set_element_types, expr, create_ordered_set);
    prepared_sets[&expr] = std::move(set);
}

static String getUniqueName(const Block & block, const String & prefix)
{
    int i = 1;
    while (block.has(prefix + toString(i)))
        ++i;
    return prefix + toString(i);
}

String DAGExpressionAnalyzer::getActions(const tipb::Expr & expr, ExpressionActionsPtr & actions)
{
    if (isLiteralExpr(expr))
    {
        Field value = decodeLiteral(expr);
        DataTypePtr flash_type = applyVisitor(FieldToDataType(), value);
        DataTypePtr target_type = exprHasValidFieldType(expr) ? getDataTypeByFieldType(expr.field_type()) : flash_type;
        String name = exprToString(expr, getCurrentInputColumns()) + "_" + target_type->getName();
        if (actions->getSampleBlock().has(name))
            return name;

        ColumnWithTypeAndName column;
        column.column = target_type->createColumnConst(1, convertFieldToType(value, *target_type, flash_type.get()));
        column.name = name;
        column.type = target_type;

        actions->add(ExpressionAction::addColumn(column));
        return name;
    }
    else if (isColumnExpr(expr))
    {
        ColumnID column_id = getColumnID(expr);
        if (column_id < 0 || column_id >= (ColumnID)getCurrentInputColumns().size())
        {
            throw Exception("column id out of bound", ErrorCodes::COP_BAD_DAG_REQUEST);
        }
        //todo check if the column type need to be cast to field type
        return getCurrentInputColumns()[column_id].name;
    }
    else if (isFunctionExpr(expr))
    {
        if (isAggFunctionExpr(expr))
        {
            throw Exception("agg function is not supported yet", ErrorCodes::UNSUPPORTED_METHOD);
        }
        const String & func_name = getFunctionName(expr);
        Names argument_names;
        DataTypes argument_types;

        if (isInOrGlobalInOperator(func_name))
        {
            String name = getActions(expr.children(0), actions);
            argument_names.push_back(name);
            argument_types.push_back(actions->getSampleBlock().getByName(name).type);
            makeExplicitSet(expr, actions->getSampleBlock(), false, name);
            ColumnWithTypeAndName column;
            column.type = std::make_shared<DataTypeSet>();

            const SetPtr & set = prepared_sets[&expr];

            column.name = getUniqueName(actions->getSampleBlock(), "___set");
            column.column = ColumnSet::create(1, set);
            actions->add(ExpressionAction::addColumn(column));
            argument_names.push_back(column.name);
            argument_types.push_back(column.type);
        }
        else
        {
            for (auto & child : expr.children())
            {
                String name = getActions(child, actions);
                argument_names.push_back(name);
                argument_types.push_back(actions->getSampleBlock().getByName(name).type);
            }
        }

        String expr_name = applyFunction(func_name, argument_names, actions);
        // add cast if needed
        expr_name = appendCastIfNeeded(expr, actions, expr_name);
        return expr_name;
    }
    else
    {
        throw Exception("Unsupported expr type: " + getTypeName(expr), ErrorCodes::UNSUPPORTED_METHOD);
    }
}
} // namespace DB
