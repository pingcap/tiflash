#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupConcat.h>
#include <AggregateFunctions/AggregateFunctionNull.h>
#include <Columns/ColumnSet.h>
#include <Common/FmtUtils.h>
#include <Common/TiFlashException.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/FieldToDataType.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzerHelper.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsTiDBConversion.h>
#include <Interpreters/Context.h>
#include <Interpreters/Set.h>
#include <Interpreters/Settings.h>
#include <Interpreters/convertFieldToType.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/Transaction/TypeMapping.h>
#include <WindowFunctions/WindowFunctionFactory.h>
#include <google/protobuf/util/json_util.h>

namespace DB
{
namespace ErrorCodes
{
extern const int COP_BAD_DAG_REQUEST;
extern const int UNSUPPORTED_METHOD;
} // namespace ErrorCodes

DAGExpressionAnalyzer::DAGExpressionAnalyzer(std::vector<NameAndTypePair> source_columns_, const Context & context_)
    : source_columns(std::move(source_columns_))
    , context(context_)
    , settings(context.getSettingsRef())
{}

void DAGExpressionAnalyzer::buildGroupConcat(
    const tipb::Expr & expr,
    ExpressionActionsChain::Step & step,
    const String & agg_func_name,
    AggregateDescriptions & aggregate_descriptions,
    NamesAndTypes & aggregated_columns,
    bool result_is_nullable)
{
    AggregateDescription aggregate;
    /// the last parametric is the separator
    auto child_size = expr.children_size() - 1;
    NamesAndTypes all_columns_names_and_types;
    String delimiter;
    SortDescription sort_description;
    bool only_one_column = true;
    TiDB::TiDBCollators arg_collators;
    String arg_name;

    /// more than one args will be combined to one
    DataTypes types(1);
    aggregate.argument_names.resize(1);
    if (child_size == 1 && expr.order_by_size() == 0)
    {
        /// only one arg
        arg_name = getActions(expr.children(0), step.actions);
        types[0] = step.actions->getSampleBlock().getByName(arg_name).type;
        all_columns_names_and_types.emplace_back(arg_name, types[0]);
        if (removeNullable(types[0])->isString())
            arg_collators.push_back(getCollatorFromExpr(expr.children(0)));
        else
            arg_collators.push_back(nullptr);
    }
    else
    {
        /// args... -> tuple(args...)
        arg_name = buildTupleFunctionForGroupConcat(expr, sort_description, all_columns_names_and_types, arg_collators, step.actions);
        only_one_column = false;
        types[0] = step.actions->getSampleBlock().getByName(arg_name).type;
    }
    aggregate.argument_names[0] = arg_name;
    step.required_output.push_back(arg_name);

    /// the separator
    arg_name = getActions(expr.children(child_size), step.actions);
    if (expr.children(child_size).tp() == tipb::String)
    {
        const ColumnConst * col_delim
            = checkAndGetColumnConstStringOrFixedString(step.actions->getSampleBlock().getByName(arg_name).column.get());
        if (col_delim == nullptr)
        {
            throw Exception("the separator of group concat should not be invalid!");
        }
        delimiter = col_delim->getValue<String>();
    }

    /// return directly if the agg is duplicated
    String func_string = DAGExpressionAnalyzerHelper::genFuncString(agg_func_name, aggregate.argument_names, arg_collators);
    for (const auto & pre_agg : aggregate_descriptions)
    {
        if (pre_agg.column_name == func_string)
        {
            aggregated_columns.emplace_back(func_string, pre_agg.function->getReturnType());
            return;
        }
    }

    aggregate.column_name = func_string;
    aggregate.parameters = Array();
    /// if there is group by clause, there is no need to consider the empty input case
    aggregate.function = AggregateFunctionFactory::instance().get(agg_func_name, types, {}, 0, result_is_nullable);

    /// TODO(FZH) deliver these arguments through aggregate.parameters of Array() type to keep the same code fashion, the special arguments
    /// sort_description, all_columns_names_and_types can be set like the way of collators

    /// group_concat_max_length
    UInt64 max_len = decodeDAGUInt64(expr.val());

    int number_of_arguments = all_columns_names_and_types.size() - sort_description.size();
    for (int num = 0; num < number_of_arguments && !result_is_nullable; ++num)
    {
        if (all_columns_names_and_types[num].type->isNullable())
        {
            result_is_nullable = true;
        }
    }

#define NEW_GROUP_CONCAT_FUNC(result_is_nullable, only_one_column)                         \
    std::make_shared<AggregateFunctionGroupConcat<result_is_nullable, (only_one_column)>>( \
        aggregate.function,                                                                \
        types,                                                                             \
        delimiter,                                                                         \
        max_len,                                                                           \
        sort_description,                                                                  \
        all_columns_names_and_types,                                                       \
        arg_collators,                                                                     \
        expr.has_distinct())

    if (result_is_nullable)
    {
        if (only_one_column)
            aggregate.function = NEW_GROUP_CONCAT_FUNC(true, true);
        else
            aggregate.function = NEW_GROUP_CONCAT_FUNC(true, false);
    }
    else
    {
        if (only_one_column)
            aggregate.function = NEW_GROUP_CONCAT_FUNC(false, true);
        else
            aggregate.function = NEW_GROUP_CONCAT_FUNC(false, false);
    }
#undef NEW_GROUP_CONCAT_FUNC

    aggregate_descriptions.push_back(aggregate);
    DataTypePtr result_type = aggregate.function->getReturnType();
    // this is a temp result since implicit cast maybe added on these aggregated_columns
    aggregated_columns.emplace_back(func_string, result_type);
}

extern const String count_second_stage;

static String getAggFuncName(
    const tipb::Expr & expr,
    const tipb::Aggregation & agg,
    const Settings & settings)
{
    String agg_func_name = getAggFunctionName(expr);
    if (expr.has_distinct() && Poco::toLower(agg_func_name) == "countdistinct")
        return settings.count_distinct_implementation;
    if (agg.group_by_size() == 0 && agg_func_name == "sum" && expr.has_field_type()
        && !getDataTypeByFieldTypeForComputingLayer(expr.field_type())->isNullable())
    {
        /// this is a little hack: if the query does not have group by column, and the result of sum is not nullable, then the sum
        /// must be the second stage for count, in this case we should return 0 instead of null if the input is empty.
        return count_second_stage;
    }
    return agg_func_name;
}

void DAGExpressionAnalyzer::buildCommonAggFunc(
    const tipb::Expr & expr,
    ExpressionActionsChain::Step & step,
    const String & agg_func_name,
    AggregateDescriptions & aggregate_descriptions,
    NamesAndTypes & aggregated_columns,
    bool empty_input_as_null)
{
    AggregateDescription aggregate;
    auto child_size = expr.children_size();
    DataTypes types(child_size);
    TiDB::TiDBCollators arg_collators;
    aggregate.argument_names.resize(child_size);
    for (Int32 i = 0; i < child_size; i++)
    {
        String arg_name = getActions(expr.children(i), step.actions);
        types[i] = step.actions->getSampleBlock().getByName(arg_name).type;
        if (removeNullable(types[i])->isString())
            arg_collators.push_back(getCollatorFromExpr(expr.children(i)));
        else
            arg_collators.push_back(nullptr);
        aggregate.argument_names[i] = arg_name;
        step.required_output.push_back(arg_name);
    }
    String func_string = DAGExpressionAnalyzerHelper::genFuncString(agg_func_name, aggregate.argument_names, arg_collators);
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
        return;
    aggregate.column_name = func_string;
    aggregate.parameters = Array();
    aggregate.function = AggregateFunctionFactory::instance().get(agg_func_name, types, {}, 0, empty_input_as_null);
    aggregate.function->setCollators(arg_collators);
    aggregate_descriptions.push_back(aggregate);
    DataTypePtr result_type = aggregate.function->getReturnType();
    // this is a temp result since implicit cast maybe added on these aggregated_columns
    aggregated_columns.emplace_back(func_string, result_type);
}

std::tuple<Names, TiDB::TiDBCollators, AggregateDescriptions, ExpressionActionsPtr> DAGExpressionAnalyzer::appendAggregation(
    ExpressionActionsChain & chain,
    const tipb::Aggregation & agg,
    bool group_by_collation_sensitive)
{
    if (agg.group_by_size() == 0 && agg.agg_func_size() == 0)
    {
        //should not reach here
        throw TiFlashException("Aggregation executor without group by/agg exprs", Errors::Coprocessor::BadRequest);
    }

    Names aggregation_keys;
    TiDB::TiDBCollators collators;
    AggregateDescriptions aggregate_descriptions;
    NamesAndTypes aggregated_columns;

    initChain(chain, getCurrentInputColumns());
    ExpressionActionsChain::Step & step = chain.steps.back();
    std::unordered_set<String> agg_key_set;

    for (const tipb::Expr & expr : agg.agg_func())
    {
        if (expr.tp() == tipb::ExprType::GroupConcat)
        {
            buildGroupConcat(expr, step, getAggFuncName(expr, agg, settings), aggregate_descriptions, aggregated_columns, agg.group_by().empty());
        }
        else
        {
            /// if there is group by clause, there is no need to consider the empty input case
            bool empty_input_as_null = agg.group_by().empty();
            buildCommonAggFunc(expr, step, getAggFuncName(expr, agg, settings), aggregate_descriptions, aggregated_columns, empty_input_as_null);
        }
    }

    for (const tipb::Expr & expr : agg.group_by())
    {
        String name = getActions(expr, step.actions);
        step.required_output.push_back(name);
        bool duplicated_key = agg_key_set.find(name) != agg_key_set.end();
        if (!duplicated_key)
        {
            /// note this assume that column with the same name has the same collator
            /// need double check this assumption when we support agg with collation
            aggregation_keys.push_back(name);
            agg_key_set.emplace(name);
        }
        /// when group_by_collation_sensitive is true, TiFlash will do the aggregation with collation
        /// info, since the aggregation in TiFlash is actually the partial stage, and TiDB always do
        /// the final stage of the aggregation, even if TiFlash do the aggregation without collation
        /// info, the correctness of the query result is guaranteed by TiDB itself, so add a flag to
        /// let TiDB/TiFlash to decide whether aggregate the data with collation info or not
        if (group_by_collation_sensitive)
        {
            auto type = step.actions->getSampleBlock().getByName(name).type;
            TiDB::TiDBCollatorPtr collator = nullptr;
            if (removeNullable(type)->isString())
                collator = getCollatorFromExpr(expr);
            if (!duplicated_key)
                collators.push_back(collator);
            if (collator != nullptr)
            {
                /// if the column is a string with collation info, the `sort_key` of the column is used during
                /// aggregation, but we can not reconstruct the origin column by `sort_key`, so add an extra
                /// extra aggregation function any(group_by_column) here as the output of the group by column
                const String & agg_func_name = "any";
                AggregateDescription aggregate;
                TiDB::TiDBCollators arg_collators;
                arg_collators.push_back(collator);

                DataTypes types(1);
                aggregate.argument_names.resize(1);
                types[0] = type;
                aggregate.argument_names[0] = name;

                String func_string = DAGExpressionAnalyzerHelper::genFuncString(agg_func_name, aggregate.argument_names, arg_collators);
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
                aggregate.function = AggregateFunctionFactory::instance().get(agg_func_name, types, {}, 0, false);
                aggregate.function->setCollators(arg_collators);
                aggregate_descriptions.push_back(aggregate);
                DataTypePtr result_type = aggregate.function->getReturnType();
                // this is a temp result since implicit cast maybe added on these aggregated_columns
                aggregated_columns.emplace_back(func_string, result_type);
            }
            else
            {
                aggregated_columns.emplace_back(name, step.actions->getSampleBlock().getByName(name).type);
            }
        }
        else
        {
            // this is a temp result since implicit cast maybe added on these aggregated_columns
            aggregated_columns.emplace_back(name, step.actions->getSampleBlock().getByName(name).type);
        }
    }
    source_columns = aggregated_columns;

    auto before_agg = chain.getLastActions();
    chain.finalize();
    chain.clear();
    appendCastAfterAgg(chain, agg);
    return {aggregation_keys, collators, aggregate_descriptions, before_agg};
}

static bool checkWindowFunctionsInvalid(const tipb::Window & window)
{
    bool has_agg_func = false;
    bool has_window_func = false;
    for (const tipb::Expr & expr : window.func_desc())
    {
        has_agg_func = has_agg_func || isAggFunctionExpr(expr);
        has_window_func = has_window_func || isWindowFunctionExpr(expr);
    }

    return has_agg_func && has_window_func;
}

SortDescription DAGExpressionAnalyzer::getWindowSortDescription(const ::google::protobuf::RepeatedPtrField<tipb::ByItem> & byItems, ExpressionActionsChain::Step & step)
{
    std::vector<NameAndTypePair> byItem_columns;
    byItem_columns.reserve(byItems.size());
    for (auto byItem : byItems)
    {
        String name = getActions(byItem.expr(), step.actions);
        auto type = step.actions->getSampleBlock().getByName(name).type;
        byItem_columns.emplace_back(name, type);
    }
    return getSortDescription(byItem_columns, byItems);
}

WindowDescription DAGExpressionAnalyzer::appendWindow(
    ExpressionActionsChain & chain,
    const tipb::Window & window)
{
#ifdef DEBUG
    std::string s;
    google::protobuf::util::MessageToJsonString(window, &s);
    std::cout << "window_json : " << s << std::endl;
#endif

    WindowDescription window_description;
    initChain(chain, getCurrentInputColumns());
    ExpressionActionsChain::Step & step = chain.steps.back();

    if (window.partition_by_size() == 0 || window.func_desc_size() == 0)
    {
        //should not reach here
        throw TiFlashException("window executor without group by or agg exprs/window exprs", Errors::Coprocessor::BadRequest);
    }

    if (checkWindowFunctionsInvalid(window))
    {
        throw TiFlashException("can not have window and agg functions together in one window.", Errors::Coprocessor::BadRequest);
    }

    std::vector<NameAndTypePair> window_columns;

    if (window.has_frame())
    {
        window_description.setWindowFrame(window.frame());
    }

    for (const tipb::Expr & expr : window.func_desc())
    {
        if (isAggFunctionExpr(expr))
        {
            AggregateDescription aggregate_description;
            String agg_func_name = getAggFunctionName(expr);
            auto child_size = expr.children_size();
            DataTypes types(child_size);
            aggregate_description.argument_names.resize(child_size);
            TiDB::TiDBCollators arg_collators;
            for (Int32 i = 0; i < child_size; i++)
            {
                String arg_name = getActions(expr.children(i), step.actions);
                types[i] = step.actions->getSampleBlock().getByName(arg_name).type;
                aggregate_description.argument_names[i] = arg_name;
                if (removeNullable(types[i])->isString())
                    arg_collators.push_back(getCollatorFromExpr(expr.children(i)));
                else
                    arg_collators.push_back(nullptr);
                step.required_output.push_back(arg_name);
            }
            String func_string = DAGExpressionAnalyzerHelper::genFuncString(agg_func_name, aggregate_description.argument_names, arg_collators);
            aggregate_description.column_name = func_string;
            aggregate_description.function = AggregateFunctionFactory::instance().get(agg_func_name, types, {}, 0, window.partition_by_size() == 0);
            DataTypePtr result_type = aggregate_description.function->getReturnType();
            window_description.aggregate_descriptions.push_back(aggregate_description);
            window_columns.emplace_back(func_string, result_type);
        }
        else if (isWindowFunctionExpr(expr))
        {
            WindowFunctionDescription window_function_description;
            String window_func_name = getWindowFunctionName(expr);
            auto child_size = expr.children_size();
            DataTypes types(child_size);
            window_function_description.argument_names.resize(child_size);
            TiDB::TiDBCollators arg_collators;
            for (Int32 i = 0; i < child_size; i++)
            {
                String arg_name = getActions(expr.children(i), step.actions);
                types[i] = step.actions->getSampleBlock().getByName(arg_name).type;
                if (removeNullable(types[i])->isString())
                    arg_collators.push_back(getCollatorFromExpr(expr.children(i)));
                else
                    arg_collators.push_back({});
                if (i == 2 && isWindowLagOrLeadFunctionExpr(expr) && types[i] != types[0])
                {
                    arg_name = appendCast(types[0], step.actions, arg_name);
                    types[i] = types[0];
                }
                window_function_description.argument_names[i] = arg_name;
                step.required_output.push_back(arg_name);
            }
            if (0 == child_size)
                arg_collators.push_back({});

            String func_string = DAGExpressionAnalyzerHelper::genFuncString(window_func_name, window_function_description.argument_names, arg_collators);
            window_function_description.column_name = func_string;
            window_function_description.window_function = WindowFunctionFactory::instance().get(window_func_name, types, 0, window.partition_by_size() == 0);
            DataTypePtr result_type = window_function_description.window_function->getReturnType();
            window_description.window_functions_descriptions.push_back(window_function_description);
            window_columns.emplace_back(func_string, result_type);
            if (isWindowLagOrLeadFunctionExpr(expr))
            {
                window_description.frame.begin_type = WindowFrame::BoundaryType::Unbounded;
                window_description.frame.end_type = WindowFrame::BoundaryType::Unbounded;
            }
        }
        else
        {
            throw TiFlashException("unknow function expr.", Errors::Coprocessor::BadRequest);
        }
    }

    window_description.before_window = chain.getLastActions();
    window_description.partition_by = getWindowSortDescription(window.partition_by(), step);
    window_description.order_by = getWindowSortDescription(window.order_by(), step);
    chain.finalize(true);
    chain.clear();

    appendWindowSelect(chain, window, window_columns);
    window_description.before_window_select = chain.getLastActions();
    window_description.add_columns = window_columns;
    chain.finalize();
    chain.clear();

    return window_description;
}

bool isUInt8Type(const DataTypePtr & type)
{
    auto non_nullable_type = type->isNullable() ? std::dynamic_pointer_cast<const DataTypeNullable>(type)->getNestedType() : type;
    return std::dynamic_pointer_cast<const DataTypeUInt8>(non_nullable_type) != nullptr;
}

String DAGExpressionAnalyzer::applyFunction(
    const String & func_name,
    const Names & arg_names,
    ExpressionActionsPtr & actions,
    const TiDB::TiDBCollatorPtr & collator)
{
    String result_name = DAGExpressionAnalyzerHelper::genFuncString(func_name, arg_names, {collator});
    if (actions->getSampleBlock().has(result_name))
        return result_name;
    const FunctionBuilderPtr & function_builder = FunctionFactory::instance().get(func_name, context);
    const ExpressionAction & action = ExpressionAction::applyFunction(function_builder, arg_names, result_name, collator);
    actions->add(action);
    return result_name;
}

String DAGExpressionAnalyzer::buildFilterColumn(
    ExpressionActionsPtr & actions,
    const std::vector<const tipb::Expr *> & conditions)
{
    String filter_column_name;
    if (conditions.size() == 1)
    {
        filter_column_name = getActions(*conditions[0], actions, true);
        if (isColumnExpr(*conditions[0])
            && (!exprHasValidFieldType(*conditions[0])
                /// if the column is not UInt8 type, we already add some convert function to convert it ot UInt8 type
                || isUInt8Type(getDataTypeByFieldTypeForComputingLayer(conditions[0]->field_type()))))
        {
            /// FilterBlockInputStream will CHANGE the filter column inplace, so
            /// filter column should never be a columnRef in DAG request, otherwise
            /// for queries like select c1 from t where c1 will got wrong result
            /// as after FilterBlockInputStream, c1 will become a const column of 1
            filter_column_name = convertToUInt8(actions, filter_column_name);
        }
    }
    else
    {
        Names arg_names;
        for (const auto * condition : conditions)
            arg_names.push_back(getActions(*condition, actions, true));
        // connect all the conditions by logical and
        filter_column_name = applyFunction("and", arg_names, actions, nullptr);
    }
    return filter_column_name;
}

String DAGExpressionAnalyzer::appendWhere(
    ExpressionActionsChain & chain,
    const std::vector<const tipb::Expr *> & conditions)
{
    initChain(chain, getCurrentInputColumns());
    ExpressionActionsChain::Step & last_step = chain.steps.back();

    String filter_column_name = buildFilterColumn(last_step.actions, conditions);

    last_step.required_output.push_back(filter_column_name);
    return filter_column_name;
}

String DAGExpressionAnalyzer::convertToUInt8(ExpressionActionsPtr & actions, const String & column_name)
{
    // Some of the TiFlash operators(e.g. FilterBlockInputStream) only support UInt8 as its input, so need to convert the
    // column type to UInt8
    // the basic rule is:
    // 1. if the column is only null, just return it
    // 2. if the column is numeric, compare it with 0
    // 3. if the column is string, convert it to float-point column, and compare with 0
    // 4. if the column is date/datetime, compare it with zeroDate
    // 5. otherwise throw exception
    if (actions->getSampleBlock().getByName(column_name).type->onlyNull())
    {
        return column_name;
    }
    const auto & org_type = removeNullable(actions->getSampleBlock().getByName(column_name).type);
    if (org_type->isNumber() || org_type->isDecimal())
    {
        tipb::Expr const_expr = constructInt64LiteralTiExpr(0);
        auto const_expr_name = getActions(const_expr, actions);
        return applyFunction("notEquals", {column_name, const_expr_name}, actions, nullptr);
    }
    if (org_type->isStringOrFixedString())
    {
        /// use tidb_cast to make it compatible with TiDB
        tipb::FieldType field_type;
        // TODO: Use TypeDouble as return type, to be compatible with TiDB
        field_type.set_tp(TiDB::TypeDouble);
        field_type.set_flen(-1);
        tipb::Expr type_expr = constructStringLiteralTiExpr("Nullable(Double)");
        auto type_expr_name = getActions(type_expr, actions);
        String num_col_name = DAGExpressionAnalyzerHelper::buildCastFunctionInternal(
            this,
            {column_name, type_expr_name},
            false,
            field_type,
            actions);

        tipb::Expr const_expr = constructInt64LiteralTiExpr(0);
        auto const_expr_name = getActions(const_expr, actions);
        return applyFunction("notEquals", {num_col_name, const_expr_name}, actions, nullptr);
    }
    if (org_type->isDateOrDateTime())
    {
        tipb::Expr const_expr = constructDateTimeLiteralTiExpr(0);
        auto const_expr_name = getActions(const_expr, actions);
        return applyFunction("notEquals", {column_name, const_expr_name}, actions, nullptr);
    }
    throw TiFlashException(fmt::format("Filter on {} is not supported.", org_type->getName()), Errors::Coprocessor::Unimplemented);
}

std::vector<NameAndTypePair> DAGExpressionAnalyzer::appendWindowOrderBy(
    ExpressionActionsChain & chain,
    const tipb::Sort & window_sort)
{
    if (window_sort.byitems_size() == 0)
    {
        throw TiFlashException("window executor without order by exprs", Errors::Coprocessor::BadRequest);
    }

    std::string s;
    google::protobuf::util::MessageToJsonString(window_sort, &s);
    std::cout << "order_json : " << s << std::endl;

    std::vector<NameAndTypePair> order_columns;
    order_columns.reserve(window_sort.byitems_size());

    initChain(chain, getCurrentInputColumns());
    ExpressionActionsChain::Step & step = chain.steps.back();

    for (const tipb::ByItem & order_by : window_sort.byitems())
    {
        String name = getActions(order_by.expr(), step.actions);
        auto type = step.actions->getSampleBlock().getByName(name).type;
        order_columns.emplace_back(name, type);
        step.required_output.push_back(name);
    }
    return order_columns;
}

NamesAndTypes DAGExpressionAnalyzer::buildOrderColumns(
    ExpressionActionsPtr & actions,
    const ::google::protobuf::RepeatedPtrField<tipb::ByItem> & order_by)
{
    NamesAndTypes order_columns;
    order_columns.reserve(order_by.size());
    for (const tipb::ByItem & by_item : order_by)
    {
        String name = getActions(by_item.expr(), actions);
        auto type = actions->getSampleBlock().getByName(name).type;
        order_columns.emplace_back(name, type);
    }
    return order_columns;
}

std::vector<NameAndTypePair> DAGExpressionAnalyzer::appendOrderBy(
    ExpressionActionsChain & chain,
    const tipb::TopN & topN)
{
    if (topN.order_by_size() == 0)
    {
        throw TiFlashException("TopN executor without order by exprs", Errors::Coprocessor::BadRequest);
    }

    initChain(chain, getCurrentInputColumns());
    ExpressionActionsChain::Step & step = chain.steps.back();
    auto order_columns = buildOrderColumns(step.actions, topN.order_by());

    assert(static_cast<int>(order_columns.size()) == topN.order_by_size());
    for (const auto & order_column : order_columns)
        step.required_output.push_back(order_column.name);

    return order_columns;
}

const std::vector<NameAndTypePair> & DAGExpressionAnalyzer::getCurrentInputColumns() const
{
    return source_columns;
}

tipb::Expr constructTZExpr(const TimezoneInfo & dag_timezone_info)
{
    if (dag_timezone_info.is_name_based)
        return constructStringLiteralTiExpr(dag_timezone_info.timezone_name);
    else
        return constructInt64LiteralTiExpr(dag_timezone_info.timezone_offset);
}

String DAGExpressionAnalyzer::appendTimeZoneCast(
    const String & tz_col,
    const String & ts_col,
    const String & func_name,
    ExpressionActionsPtr & actions)
{
    String cast_expr_name = applyFunction(func_name, {ts_col, tz_col}, actions, nullptr);
    return cast_expr_name;
}

bool DAGExpressionAnalyzer::appendExtraCastsAfterTS(
    ExpressionActionsChain & chain,
    const std::vector<ExtraCastAfterTSMode> & need_cast_column,
    const tipb::TableScan & table_scan)
{
    bool ret = false;
    initChain(chain, getCurrentInputColumns());
    ExpressionActionsChain::Step & step = chain.getLastStep();
    // For TimeZone
    tipb::Expr tz_expr = constructTZExpr(context.getTimezoneInfo());
    String tz_col = getActions(tz_expr, step.actions);
    static const String convert_time_zone_form_utc = "ConvertTimeZoneFromUTC";
    static const String convert_time_zone_by_offset = "ConvertTimeZoneByOffsetFromUTC";
    const String & timezone_func_name = context.getTimezoneInfo().is_name_based ? convert_time_zone_form_utc : convert_time_zone_by_offset;

    // For Duration
    String fsp_col;
    static const String dur_func_name = "FunctionConvertDurationFromNanos";
    const auto & columns = table_scan.columns();
    for (size_t i = 0; i < need_cast_column.size(); ++i)
    {
        if (!context.getTimezoneInfo().is_utc_timezone && need_cast_column[i] == ExtraCastAfterTSMode::AppendTimeZoneCast)
        {
            String casted_name = appendTimeZoneCast(tz_col, source_columns[i].name, timezone_func_name, step.actions);
            source_columns[i].name = casted_name;
            ret = true;
        }

        if (need_cast_column[i] == ExtraCastAfterTSMode::AppendDurationCast)
        {
            if (columns[i].decimal() > 6)
                throw Exception("fsp must <= 6", ErrorCodes::LOGICAL_ERROR);
            auto fsp = columns[i].decimal() < 0 ? 0 : columns[i].decimal();
            tipb::Expr fsp_expr = constructInt64LiteralTiExpr(fsp);
            fsp_col = getActions(fsp_expr, step.actions);
            String casted_name = appendDurationCast(fsp_col, source_columns[i].name, dur_func_name, step.actions);
            source_columns[i].name = casted_name;
            source_columns[i].type = step.actions->getSampleBlock().getByName(casted_name).type;
            ret = true;
        }
    }
    NamesWithAliases project_cols;
    for (auto & col : source_columns)
    {
        step.required_output.push_back(col.name);
        project_cols.emplace_back(col.name, col.name);
    }
    step.actions->add(ExpressionAction::project(project_cols));
    return ret;
}

String DAGExpressionAnalyzer::appendDurationCast(
    const String & fsp_expr,
    const String & dur_expr,
    const String & func_name,
    ExpressionActionsPtr & actions)
{
    return applyFunction(func_name, {dur_expr, fsp_expr}, actions, nullptr);
}

void DAGExpressionAnalyzer::appendJoin(
    ExpressionActionsChain & chain,
    SubqueryForSet & join_query,
    const NamesAndTypesList & columns_added_by_join) const
{
    initChain(chain, getCurrentInputColumns());
    ExpressionActionsPtr actions = chain.getLastActions();
    actions->add(ExpressionAction::ordinaryJoin(join_query.join, columns_added_by_join));
}

bool DAGExpressionAnalyzer::appendJoinKeyAndJoinFilters(
    ExpressionActionsChain & chain,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & keys,
    const DataTypes & key_types,
    Names & key_names,
    bool left,
    bool is_right_out_join,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & filters,
    String & filter_column_name)
{
    bool ret = false;
    initChain(chain, getCurrentInputColumns());
    ExpressionActionsPtr actions = chain.getLastActions();
    UniqueNameGenerator unique_name_generator;

    for (int i = 0; i < keys.size(); i++)
    {
        const auto & key = keys.at(i);
        bool has_actions = key.tp() != tipb::ExprType::ColumnRef;

        String key_name = getActions(key, actions);
        DataTypePtr current_type = actions->getSampleBlock().getByName(key_name).type;
        if (!removeNullable(current_type)->equals(*removeNullable(key_types[i])))
        {
            /// need to convert to key type
            key_name = appendCast(key_types[i], actions, key_name);
            has_actions = true;
        }
        if (!has_actions && (!left || is_right_out_join))
        {
            /// if the join key is a columnRef, then add a new column as the join key if needed.
            /// In ClickHouse, the columns returned by join are: join_keys, left_columns and right_columns
            /// where left_columns and right_columns don't include the join keys if they are ColumnRef
            /// In TiDB, the columns returned by join are left_columns, right_columns, if the join keys
            /// are ColumnRef, they will be included in both left_columns and right_columns
            /// E.g, for table t1(id, value), t2(id, value) and query select * from t1 join t2 on t1.id = t2.id
            /// In ClickHouse, it returns id,t1_value,t2_value
            /// In TiDB, it returns t1_id,t1_value,t2_id,t2_value
            /// So in order to make the join compatible with TiDB, if the join key is a columnRef, for inner/left
            /// join, add a new key for right join key, for right join, add new key for both left and right join key
            String updated_key_name = unique_name_generator.toUniqueName((left ? "_l_k_" : "_r_k_") + key_name);
            /// duplicated key names, in Clickhouse join, it is assumed that here is no duplicated
            /// key names, so just copy a key with new name
            actions->add(ExpressionAction::copyColumn(key_name, updated_key_name));
            key_name = updated_key_name;
            has_actions = true;
        }
        else
        {
            String updated_key_name = unique_name_generator.toUniqueName(key_name);
            /// duplicated key names, in Clickhouse join, it is assumed that here is no duplicated
            /// key names, so just copy a key with new name
            if (key_name != updated_key_name)
            {
                actions->add(ExpressionAction::copyColumn(key_name, updated_key_name));
                key_name = updated_key_name;
                has_actions = true;
            }
        }
        key_names.push_back(key_name);
        ret |= has_actions;
    }

    if (!filters.empty())
    {
        ret = true;
        std::vector<const tipb::Expr *> filter_vector;
        for (const auto & c : filters)
        {
            filter_vector.push_back(&c);
        }
        filter_column_name = appendWhere(chain, filter_vector);
    }
    /// remove useless columns to avoid duplicate columns
    /// as when compiling the key/filter expression, the origin
    /// streams may be added some columns that have the
    /// same name on left streams and right streams, for
    /// example, if the join condition is something like:
    /// id + 1 = id + 1,
    /// the left streams and the right streams will have the
    /// same constant column for `1`
    /// Note that the origin left streams and right streams
    /// will never have duplicated columns because in
    /// DAGQueryBlockInterpreter we add qb_column_prefix in
    /// final project step, so if the join condition is not
    /// literal expression, the key names should never be
    /// duplicated. In the above example, the final key names should be
    /// something like `add(__qb_2_id, 1)` and `add(__qb_3_id, 1)`
    if (ret)
    {
        std::unordered_set<String> needed_columns;
        for (const auto & c : getCurrentInputColumns())
            needed_columns.insert(c.name);
        for (const auto & s : key_names)
            needed_columns.insert(s);
        if (!filter_column_name.empty())
            needed_columns.insert(filter_column_name);

        const auto & names = actions->getSampleBlock().getNames();
        for (const auto & name : names)
        {
            if (needed_columns.find(name) == needed_columns.end())
                actions->add(ExpressionAction::removeColumn(name));
        }
    }
    return ret;
}

void DAGExpressionAnalyzer::appendWindowSelect(
    ExpressionActionsChain & chain,
    const tipb::Window & window,
    const NamesAndTypes after_window_columns)
{
    initChain(chain, after_window_columns);
    bool need_update_source_columns = false;
    std::vector<NameAndTypePair> updated_after_window_columns;
    ExpressionActionsChain::Step & step = chain.steps.back();
    for (Int32 i = 0; i < window.func_desc_size(); i++)
    {
        const String & name = after_window_columns[i].name;
        String updated_name = appendCastIfNeeded(window.func_desc(i), step.actions, name);
        if (name != updated_name)
        {
            need_update_source_columns = true;
            DataTypePtr type = step.actions->getSampleBlock().getByName(updated_name).type;
            updated_after_window_columns.emplace_back(updated_name, type);
            step.required_output.push_back(updated_name);
        }
        else
        {
            updated_after_window_columns.emplace_back(name, after_window_columns[i].type);
            step.required_output.push_back(name);
        }
    }

    if (need_update_source_columns)
    {
        for (auto & col : updated_after_window_columns)
        {
            source_columns.emplace_back(col.name, col.type);
        }
    }
    else
    {
        for (auto & col : after_window_columns)
        {
            source_columns.emplace_back(col.name, col.type);
        }
    }
}

void DAGExpressionAnalyzer::appendCastAfterAgg(
    ExpressionActionsChain & chain,
    const tipb::Aggregation & aggregation)
{
    initChain(chain, getCurrentInputColumns());

    bool need_update_source_columns = false;
    std::vector<NameAndTypePair> updated_aggregated_columns;
    ExpressionActionsChain::Step & step = chain.steps.back();

    auto update_cast_column = [&](const tipb::Expr & expr, const NameAndTypePair & origin_column) {
        String updated_name = appendCastIfNeeded(expr, step.actions, origin_column.name);
        if (origin_column.name != updated_name)
        {
            DataTypePtr type = step.actions->getSampleBlock().getByName(updated_name).type;
            updated_aggregated_columns.emplace_back(updated_name, type);
            step.required_output.push_back(updated_name);
            need_update_source_columns = true;
        }
        else
        {
            updated_aggregated_columns.emplace_back(origin_column.name, origin_column.type);
            step.required_output.push_back(origin_column.name);
        }
    };

    for (Int32 i = 0; i < aggregation.agg_func_size(); ++i)
    {
        assert(static_cast<size_t>(i) < source_columns.size());
        update_cast_column(aggregation.agg_func(i), source_columns[i]);
    }
    for (Int32 i = 0; i < aggregation.group_by_size(); ++i)
    {
        size_t group_by_index = i + aggregation.agg_func_size();
        assert(group_by_index < source_columns.size());
        update_cast_column(aggregation.group_by(i), source_columns[group_by_index]);
    }

    if (need_update_source_columns)
    {
        std::swap(source_columns, updated_aggregated_columns);
    }
}

NamesWithAliases DAGExpressionAnalyzer::appendFinalProjectForNonRootQueryBlock(
    ExpressionActionsChain & chain,
    const String & column_prefix) const
{
    const auto & current_columns = getCurrentInputColumns();
    NamesWithAliases final_project;
    UniqueNameGenerator unique_name_generator;
    for (const auto & element : current_columns)
        final_project.emplace_back(element.name, unique_name_generator.toUniqueName(column_prefix + element.name));

    initChain(chain, current_columns);
    for (const auto & name : final_project)
        chain.steps.back().required_output.push_back(name.first);
    return final_project;
}

NamesWithAliases DAGExpressionAnalyzer::appendFinalProjectForRootQueryBlock(
    ExpressionActionsChain & chain,
    const std::vector<tipb::FieldType> & schema,
    const std::vector<Int32> & output_offsets,
    const String & column_prefix,
    bool keep_session_timezone_info)
{
    if (unlikely(output_offsets.empty()))
        throw Exception("Root Query block without output_offsets", ErrorCodes::LOGICAL_ERROR);

    NamesWithAliases final_project;
    const auto & current_columns = getCurrentInputColumns();
    UniqueNameGenerator unique_name_generator;
    bool need_append_timezone_cast = !keep_session_timezone_info && !context.getTimezoneInfo().is_utc_timezone;
    /// TiDB can not guarantee that the field type in DAG request is accurate, so in order to make things work,
    /// TiFlash will append extra type cast if needed.
    bool need_append_type_cast = false;
    BoolVec need_append_type_cast_vec;
    /// we need to append type cast for root block if necessary
    for (UInt32 i : output_offsets)
    {
        const auto & actual_type = current_columns[i].type;
        auto expected_type = getDataTypeByFieldTypeForComputingLayer(schema[i]);
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
            final_project.emplace_back(
                current_columns[i].name,
                unique_name_generator.toUniqueName(column_prefix + current_columns[i].name));
        }
    }
    else
    {
        /// for all the columns that need to be returned, if the type is timestamp, then convert
        /// the timestamp column to UTC based, refer to appendTimeZoneCastsAfterTS for more details
        initChain(chain, current_columns);
        ExpressionActionsChain::Step & step = chain.steps.back();

        tipb::Expr tz_expr = constructTZExpr(context.getTimezoneInfo());
        String tz_col;
        String tz_cast_func_name = context.getTimezoneInfo().is_name_based ? "ConvertTimeZoneToUTC" : "ConvertTimeZoneByOffsetToUTC";
        std::vector<Int32> casted(schema.size(), 0);
        std::unordered_map<String, String> casted_name_map;

        for (size_t index = 0; index < output_offsets.size(); index++)
        {
            UInt32 i = output_offsets[index];
            if ((need_append_timezone_cast && schema[i].tp() == TiDB::TypeTimestamp) || need_append_type_cast_vec[index])
            {
                const auto & it = casted_name_map.find(current_columns[i].name);
                if (it == casted_name_map.end())
                {
                    /// first add timestamp cast
                    String updated_name = current_columns[i].name;
                    if (need_append_timezone_cast && schema[i].tp() == TiDB::TypeTimestamp)
                    {
                        if (tz_col.length() == 0)
                            tz_col = getActions(tz_expr, step.actions);
                        updated_name = appendTimeZoneCast(tz_col, current_columns[i].name, tz_cast_func_name, step.actions);
                    }
                    /// then add type cast
                    if (need_append_type_cast_vec[index])
                    {
                        updated_name = appendCast(getDataTypeByFieldTypeForComputingLayer(schema[i]), step.actions, updated_name);
                    }
                    final_project.emplace_back(updated_name, unique_name_generator.toUniqueName(column_prefix + updated_name));
                    casted_name_map[current_columns[i].name] = updated_name;
                }
                else
                {
                    final_project.emplace_back(it->second, unique_name_generator.toUniqueName(column_prefix + it->second));
                }
            }
            else
            {
                final_project.emplace_back(
                    current_columns[i].name,
                    unique_name_generator.toUniqueName(column_prefix + current_columns[i].name));
            }
        }
    }

    initChain(chain, current_columns);
    for (const auto & name : final_project)
    {
        chain.steps.back().required_output.push_back(name.first);
    }
    return final_project;
}

String DAGExpressionAnalyzer::alignReturnType(
    const tipb::Expr & expr,
    ExpressionActionsPtr & actions,
    const String & expr_name,
    bool force_uint8)
{
    DataTypePtr orig_type = actions->getSampleBlock().getByName(expr_name).type;
    if (force_uint8 && isUInt8Type(orig_type))
        return expr_name;
    String updated_name = appendCastIfNeeded(expr, actions, expr_name);
    DataTypePtr updated_type = actions->getSampleBlock().getByName(updated_name).type;
    if (force_uint8 && !isUInt8Type(updated_type))
        updated_name = convertToUInt8(actions, updated_name);
    return updated_name;
}

void DAGExpressionAnalyzer::initChain(ExpressionActionsChain & chain, const std::vector<NameAndTypePair> & columns) const
{
    if (chain.steps.empty())
    {
        chain.settings = settings;
        NamesAndTypesList column_list;
        std::unordered_set<String> column_name_set;
        for (const auto & col : columns)
        {
            if (column_name_set.find(col.name) == column_name_set.end())
            {
                column_list.emplace_back(col.name, col.type);
                column_name_set.emplace(col.name);
            }
        }
        chain.steps.emplace_back(std::make_shared<ExpressionActions>(column_list, settings));
    }
}

String DAGExpressionAnalyzer::appendCast(const DataTypePtr & target_type, ExpressionActionsPtr & actions, const String & expr_name)
{
    // need to add cast function
    // first construct the second argument
    tipb::Expr type_expr = constructStringLiteralTiExpr(target_type->getName());
    auto type_expr_name = getActions(type_expr, actions);
    String cast_expr_name = applyFunction("CAST", {expr_name, type_expr_name}, actions, nullptr);
    return cast_expr_name;
}

String DAGExpressionAnalyzer::appendCastIfNeeded(
    const tipb::Expr & expr,
    ExpressionActionsPtr & actions,
    const String & expr_name)
{
    if (!isFunctionExpr(expr))
        return expr_name;

    if (!expr.has_field_type())
        throw TiFlashException("Function Expression without field type", Errors::Coprocessor::BadRequest);

    if (exprHasValidFieldType(expr))
    {
        DataTypePtr expected_type = getDataTypeByFieldTypeForComputingLayer(expr.field_type());
        DataTypePtr actual_type = actions->getSampleBlock().getByName(expr_name).type;
        if (expected_type->getName() != actual_type->getName())
            return appendCast(expected_type, actions, expr_name);
    }
    return expr_name;
}

void DAGExpressionAnalyzer::makeExplicitSet(
    const tipb::Expr & expr,
    const Block & sample_block,
    bool create_ordered_set,
    const String & left_arg_name)
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
    TiDB::TiDBCollators collators;
    collators.push_back(getCollatorFromExpr(expr));
    set->setCollators(collators);
    auto remaining_exprs = set->createFromDAGExpr(set_element_types, expr, create_ordered_set);
    prepared_sets[&expr] = std::make_shared<DAGSet>(std::move(set), std::move(remaining_exprs));
}

String DAGExpressionAnalyzer::getActions(const tipb::Expr & expr, ExpressionActionsPtr & actions, bool output_as_uint8_type)
{
    String ret;
    if (isLiteralExpr(expr))
    {
        Field value = decodeLiteral(expr);
        DataTypePtr flash_type = applyVisitor(FieldToDataType(), value);
        DataTypePtr target_type = inferDataType4Literal(expr);
        ret = exprToString(expr, getCurrentInputColumns()) + "_" + target_type->getName();
        if (!actions->getSampleBlock().has(ret))
        {
            ColumnWithTypeAndName column;
            column.column = target_type->createColumnConst(1, convertFieldToType(value, *target_type, flash_type.get()));
            column.name = ret;
            column.type = target_type;
            actions->add(ExpressionAction::addColumn(column));
        }
        if (expr.field_type().tp() == TiDB::TypeTimestamp && !context.getTimezoneInfo().is_utc_timezone)
        {
            /// append timezone cast for timestamp literal
            tipb::Expr tz_expr = constructTZExpr(context.getTimezoneInfo());
            String func_name = context.getTimezoneInfo().is_name_based ? "ConvertTimeZoneFromUTC" : "ConvertTimeZoneByOffsetFromUTC";
            String tz_col = getActions(tz_expr, actions);
            String casted_name = appendTimeZoneCast(tz_col, ret, func_name, actions);
            ret = casted_name;
        }
    }
    else if (isColumnExpr(expr))
    {
        ret = getColumnNameForColumnExpr(expr, getCurrentInputColumns());
    }
    else if (isScalarFunctionExpr(expr))
    {
        const String & func_name = getFunctionName(expr);
        if (DAGExpressionAnalyzerHelper::function_builder_map.count(func_name) != 0)
        {
            ret = DAGExpressionAnalyzerHelper::function_builder_map[func_name](this, expr, actions);
        }
        else
        {
            ret = buildFunction(expr, actions);
        }
    }
    else
    {
        throw TiFlashException(fmt::format("Unsupported expr type: {}", getTypeName(expr)), Errors::Coprocessor::Unimplemented);
    }

    ret = alignReturnType(expr, actions, ret, output_as_uint8_type);
    return ret;
}

String DAGExpressionAnalyzer::buildTupleFunctionForGroupConcat(
    const tipb::Expr & expr,
    SortDescription & sort_desc,
    NamesAndTypes & names_and_types,
    TiDB::TiDBCollators & collators,
    ExpressionActionsPtr & actions)
{
    const String & func_name = "tuple";
    Names argument_names;

    /// add the first N-1 expr into the tuple
    int child_size = expr.children_size() - 1;
    for (auto i = 0; i < child_size; ++i)
    {
        const auto & child = expr.children(i);
        String name = getActions(child, actions, false);
        argument_names.push_back(name);
        auto type = actions->getSampleBlock().getByName(name).type;
        names_and_types.emplace_back(name, type);
        if (removeNullable(type)->isString())
            collators.push_back(getCollatorFromExpr(expr.children(i)));
        else
            collators.push_back(nullptr);
    }

    std::vector<NameAndTypePair> order_columns;
    for (auto i = 0; i < expr.order_by_size(); ++i)
    {
        String name = getActions(expr.order_by(i).expr(), actions);
        argument_names.push_back(name);
        auto type = actions->getSampleBlock().getByName(name).type;
        order_columns.emplace_back(name, type);
        names_and_types.emplace_back(name, type);
        if (removeNullable(type)->isString())
            collators.push_back(getCollatorFromExpr(expr.order_by(i).expr()));
        else
            collators.push_back(nullptr);
    }
    sort_desc = getSortDescription(order_columns, expr.order_by());

    return applyFunction(func_name, argument_names, actions, nullptr);
}

String DAGExpressionAnalyzer::buildFunction(
    const tipb::Expr & expr,
    ExpressionActionsPtr & actions)
{
    const String & func_name = getFunctionName(expr);
    Names argument_names;
    for (const auto & child : expr.children())
    {
        String name = getActions(child, actions);
        argument_names.push_back(name);
    }
    return applyFunction(func_name, argument_names, actions, getCollatorFromExpr(expr));
}

} // namespace DB
