#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnSet.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/FieldToDataType.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/Set.h>
#include <Interpreters/convertFieldToType.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/DatumCodec.h>
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

DAGExpressionAnalyzer::DAGExpressionAnalyzer(std::vector<NameAndTypePair> && source_columns_, const Context & context_)
    : source_columns(std::move(source_columns_)), context(context_), after_agg(false), implicit_cast_count(0)
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

    for (const tipb::Expr & expr : agg.agg_func())
    {
        const String & agg_func_name = getAggFunctionName(expr);
        AggregateDescription aggregate;
        DataTypes types(expr.children_size());
        aggregate.argument_names.resize(expr.children_size());
        for (Int32 i = 0; i < expr.children_size(); i++)
        {
            String arg_name = getActions(expr.children(i), step.actions);
            types[i] = step.actions->getSampleBlock().getByName(arg_name).type;
            aggregate.argument_names[i] = arg_name;
            step.required_output.push_back(arg_name);
        }
        String func_string = genFuncString(agg_func_name, aggregate.argument_names);
        bool duplicate = false;
        for (const auto & pre_agg : aggregate_descriptions)
        {
            if (pre_agg.column_name == func_string)
            {
                aggregated_columns.emplace_back(func_string, pre_agg.function->getReturnType());
                duplicate = true;
                break;
            }
        }
        if (duplicate)
            continue;
        aggregate.column_name = func_string;
        aggregate.parameters = Array();
        aggregate.function = AggregateFunctionFactory::instance().get(agg_func_name, types, {}, 0, true);
        aggregate_descriptions.push_back(aggregate);
        DataTypePtr result_type = aggregate.function->getReturnType();
        // this is a temp result since implicit cast maybe added on these aggregated_columns
        aggregated_columns.emplace_back(func_string, result_type);
    }

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

String DAGExpressionAnalyzer::applyFunction(const String & func_name, const Names & arg_names, ExpressionActionsPtr & actions)
{
    String result_name = genFuncString(func_name, arg_names);
    if (actions->getSampleBlock().has(result_name))
        return result_name;
    const FunctionBuilderPtr & function_builder = FunctionFactory::instance().get(func_name, context);
    const ExpressionAction & apply_function = ExpressionAction::applyFunction(function_builder, arg_names, result_name);
    actions->add(apply_function);
    return result_name;
}

void DAGExpressionAnalyzer::appendWhere(
    ExpressionActionsChain & chain, const std::vector<const tipb::Expr *> & conditions, String & filter_column_name)
{
    initChain(chain, getCurrentInputColumns());
    ExpressionActionsChain::Step & last_step = chain.steps.back();
    Names arg_names;
    for (const auto * condition : conditions)
    {
        arg_names.push_back(getActions(*condition, last_step.actions));
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
        filter_column_name = convertToUInt8ForFilter(chain, filter_column_name);
    }
    chain.steps.back().required_output.push_back(filter_column_name);
}

String DAGExpressionAnalyzer::convertToUInt8ForFilter(ExpressionActionsChain & chain, const String & column_name)
{
    auto & last_actions = chain.steps.back().actions->getActions();
    // find the original unit8 column if possible
    for (auto it = last_actions.rbegin(); it != last_actions.rend(); ++it)
    {
        if (it->type == ExpressionAction::Type::APPLY_FUNCTION &&
            it->result_name == column_name &&
            it->function->getName() == "CAST" &&
            isUInt8Type(it->function->getArgumentTypes().at(0)))
        {
            // for cast function, the casted column is the first argument
            return it->argument_names[0];
        }
    }
    // couldn't find the original uint8 column, which means the top level expression of where
    // condition is not a comparision/logical expression. For example:
    // select * from table where c1 + c2
    // TiFlash does not support this, in order to make the dag request work, need to convert the
    // column type to UInt8
    // for where condition of non-comparision/logical expression, the basic rule is:
    // 1. if the column is numeric, compare it with 0
    // 2. if the column is string, convert it to numeric column, and compare with 0
    // 3. if the column is date/datetime, compare it with zeroDate
    // 4. if the column is other type, throw exception
    const auto & org_type = removeNullable(chain.steps.back().actions->getSampleBlock().getByName(column_name).type);
    auto & actions = chain.steps.back().actions;
    if (org_type->isNumber() || org_type->isDecimal())
    {
        tipb::Expr const_expr;
        constructInt64LiteralTiExpr(const_expr, 0);
        auto const_expr_name = getActions(const_expr, actions);
        return applyFunction("notEquals", {column_name, const_expr_name}, actions);
    }
    if (org_type->isStringOrFixedString())
    {
        String num_col_name = applyFunction("toInt64OrNull", {column_name}, actions);
        tipb::Expr const_expr;
        constructInt64LiteralTiExpr(const_expr, 0);
        auto const_expr_name = getActions(const_expr, actions);
        return applyFunction("notEquals", {num_col_name, const_expr_name}, actions);
    }
    if (org_type->isDateOrDateTime())
    {
        tipb::Expr const_expr;
        constructDateTimeLiteralTiExpr(const_expr, 0);
        auto const_expr_name = getActions(const_expr, actions);
        return applyFunction("notEquals", {column_name, const_expr_name}, actions);
    }
    throw Exception("Filter on " + org_type->getName() + " is not supported.", ErrorCodes::NOT_IMPLEMENTED);
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
        constructStringLiteralTiExpr(tz_expr, rqst.time_zone_name());
    else
        constructInt64LiteralTiExpr(tz_expr, from_utc ? rqst.time_zone_offset() : -rqst.time_zone_offset());
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
    String cast_expr_name = applyFunction(func_name, {ts_col, tz_col}, actions);
    return cast_expr_name;
}

// add timezone cast after table scan, this is used for session level timezone support
// the basic idea of supporting session level timezone is that:
// 1. for every timestamp column used in the dag request, after reading it from table scan,
//    we add cast function to convert its timezone to the timezone specified in DAG request
// 2. based on the dag encode type, the return column will be with session level timezone(Arrow encode)
//    or UTC timezone(Default encode), if UTC timezone is needed, another cast function is used to
//    convert the session level timezone to UTC timezone.
// In the worst case(e.g select ts_col from table with Default encode), this will introduce two
// useless casts to all the timestamp columns, in order to avoid redundant cast, when cast the ts
// column to the columns with session-level timezone info, the original ts columns with UTC timezone
// are still kept, and the InterpreterDAG will choose the correct column based on encode type
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
    ExpressionActionsChain & chain, const tipb::Aggregation & aggregation, const tipb::DAGRequest & rqst, bool keep_session_timezone_info)
{
    initChain(chain, getCurrentInputColumns());
    bool need_update_aggregated_columns = false;
    std::vector<NameAndTypePair> updated_aggregated_columns;
    ExpressionActionsChain::Step step = chain.steps.back();
    bool need_append_timezone_cast = !keep_session_timezone_info && hasMeaningfulTZInfo(rqst);
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
        aggregated_columns.clear();
        for (size_t i = 0; i < updated_aggregated_columns.size(); i++)
        {
            aggregated_columns.emplace_back(updated_aggregated_columns[i].name, updated_aggregated_columns[i].type);
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
            implicit_cast_count++;
            // need to add cast function
            // first construct the second argument
            tipb::Expr type_expr;
            constructStringLiteralTiExpr(type_expr, expected_type->getName());
            auto type_expr_name = getActions(type_expr, actions);
            String cast_expr_name = applyFunction("CAST", {expr_name, type_expr_name}, actions);
            return cast_expr_name;
        }
        else
        {
            return expr_name;
        }
    }
    return expr_name;
}

void DAGExpressionAnalyzer::makeExplicitSetForIndex(const tipb::Expr & expr, const ManageableStoragePtr & storage)
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
    if (functionIsInOrGlobalInOperator(func_name) && expr.children(0).tp() == tipb::ExprType::ColumnRef && !prepared_sets.count(&expr))
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
    auto remaining_exprs = set->createFromDAGExpr(set_element_types, expr, create_ordered_set);
    prepared_sets[&expr] = std::make_shared<DAGSet>(std::move(set), std::move(remaining_exprs));
}

static String getUniqueName(const Block & block, const String & prefix)
{
    int i = 1;
    while (block.has(prefix + toString(i)))
        ++i;
    return prefix + toString(i);
}

String DAGExpressionAnalyzer::getActionsForInOperator(const tipb::Expr & expr, ExpressionActionsPtr & actions)
{
    const String & func_name = getFunctionName(expr);
    Names argument_names;
    String key_name = getActions(expr.children(0), actions);
    argument_names.push_back(key_name);
    makeExplicitSet(expr, actions->getSampleBlock(), false, key_name);
    const DAGSetPtr & set = prepared_sets[&expr];

    ColumnWithTypeAndName column;
    column.type = std::make_shared<DataTypeSet>();

    column.name = getUniqueName(actions->getSampleBlock(), "___set");
    column.column = ColumnSet::create(1, set->constant_set);
    actions->add(ExpressionAction::addColumn(column));
    argument_names.push_back(column.name);

    String expr_name = applyFunction(func_name, argument_names, actions);
    // add cast if needed
    expr_name = appendCastIfNeeded(expr, actions, expr_name);
    if (set->remaining_exprs.empty())
    {
        return expr_name;
    }
    // if there are remaining non_constant_expr, then convert
    // key in (const1, const2, non_const1, non_const2) => or(key in (const1, const2), key eq non_const1, key eq non_const2)
    // key not in (const1, const2, non_const1, non_const2) => and(key not in (const1, const2), key not eq non_const1, key not eq non_const2)
    argument_names.clear();
    argument_names.push_back(expr_name);
    bool is_not_in = func_name == "notIn" || func_name == "globalNotIn" || func_name == "tidbNotIn";
    for (const tipb::Expr * non_constant_expr : set->remaining_exprs)
    {
        Names eq_arg_names;
        eq_arg_names.push_back(key_name);
        eq_arg_names.push_back(getActions(*non_constant_expr, actions));
        // do not need extra cast because TiDB will ensure type of key_name and right_expr_name is the same
        argument_names.push_back(applyFunction(is_not_in ? "notEquals" : "equals", eq_arg_names, actions));
    }
    return applyFunction(is_not_in ? "and" : "or", argument_names, actions);
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
        //todo check if the column type need to be cast to field type
        return getColumnNameForColumnExpr(expr, getCurrentInputColumns());
    }
    else if (isFunctionExpr(expr))
    {
        if (isAggFunctionExpr(expr))
        {
            throw Exception("agg function is not supported yet", ErrorCodes::UNSUPPORTED_METHOD);
        }
        const String & func_name = getFunctionName(expr);

        if (functionIsInOrGlobalInOperator(func_name))
        {
            return getActionsForInOperator(expr, actions);
        }
        Names argument_names;
        for (auto & child : expr.children())
        {
            String name = getActions(child, actions);
            argument_names.push_back(name);
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
