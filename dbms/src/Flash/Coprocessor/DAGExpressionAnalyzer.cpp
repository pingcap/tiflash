#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnSet.h>
#include <Common/TiFlashException.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/FieldToDataType.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsConditional.h>
#include <Functions/FunctionsTiDBConversion.h>
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

static String genFuncString(const String & func_name, const Names & argument_names, const TiDB::TiDBCollators & collators)
{
    assert(!collators.empty());
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
    ss << ")_collator";
    for (const auto & collator : collators)
    {
        if (collator == nullptr)
            ss << "_0";
        else
            ss << "_" << collator->getCollatorId();
    }
    ss << " ";
    return ss.str();
}

static String getUniqueName(const Block & block, const String & prefix)
{
    int i = 1;
    while (block.has(prefix + toString(i)))
        ++i;
    return prefix + toString(i);
}

static String buildMultiIfFunction(DAGExpressionAnalyzer * analyzer, const tipb::Expr & expr, ExpressionActionsPtr & actions)
{
    // multiIf is special because
    // 1. the type of odd argument(except the last one) must be UInt8
    // 2. if the total number of arguments is even, we need to add an extra NULL to multiIf
    const String & func_name = getFunctionName(expr);
    Names argument_names;
    for (int i = 0; i < expr.children_size(); i++)
    {
        String name = analyzer->getActions(expr.children(i), actions, i != expr.children_size() - 1 && i % 2 == 0);
        argument_names.push_back(name);
    }
    if (argument_names.size() % 2 == 0)
    {
        tipb::Expr null_expr;
        constructNULLLiteralTiExpr(null_expr);
        String name = analyzer->getActions(null_expr, actions);
        argument_names.push_back(name);
    }
    return analyzer->applyFunction(func_name, argument_names, actions, getCollatorFromExpr(expr));
}

static String buildIfNullFunction(DAGExpressionAnalyzer * analyzer, const tipb::Expr & expr, ExpressionActionsPtr & actions)
{
    // rewrite IFNULL function with multiIf
    // ifNull(arg1, arg2) -> if(isNull(arg1), arg2, arg1)
    const String & func_name = "multiIf";
    Names argument_names;
    if (expr.children_size() != 2)
    {
        throw TiFlashException("Invalid arguments of IFNULL function", Errors::Coprocessor::BadRequest);
    }

    String condition_arg_name = analyzer->getActions(expr.children(0), actions, false);
    String else_arg_name = analyzer->getActions(expr.children(1), actions, false);
    String is_null_result = analyzer->applyFunction("isNull", {condition_arg_name}, actions, getCollatorFromExpr(expr));

    argument_names.push_back(std::move(is_null_result));
    argument_names.push_back(std::move(else_arg_name));
    argument_names.push_back(std::move(condition_arg_name));

    return analyzer->applyFunction(func_name, argument_names, actions, getCollatorFromExpr(expr));
}

static String buildInFunction(DAGExpressionAnalyzer * analyzer, const tipb::Expr & expr, ExpressionActionsPtr & actions)
{
    const String & func_name = getFunctionName(expr);
    Names argument_names;
    String key_name = analyzer->getActions(expr.children(0), actions);
    // TiDB guarantees that arguments of IN function have same data type family but doesn't guarantees that their data types
    // are completely the same. For example, in an expression like `col_decimal_10_0 IN (1.1, 2.34)`, `1.1` and `2.34` are
    // both decimal type but `1.1`'s flen and decimal are 2 and 1 while that of `2.34` are 3 and 2.
    // We should convert them to a least super data type.
    DataTypes argument_types;
    const Block & sample_block = actions->getSampleBlock();
    argument_types.push_back(sample_block.getByName(key_name).type);
    for (int i = 1; i < expr.children_size(); ++i)
    {
        auto & child = expr.children(i);
        if (!isLiteralExpr(child))
        {
            // Non-literal expression will be rewritten with `OR`, for example:
            // `a IN (1, 2, b)` will be rewritten to `a IN (1, 2) OR a = b`
            continue;
        }
        DataTypePtr type = inferDataType4Literal(child);
        argument_types.push_back(type);
    }
    DataTypePtr resolved_type = getLeastSupertype(argument_types);
    if (!removeNullable(resolved_type)->equals(*removeNullable(argument_types[0])))
    {
        // Need cast left argument
        key_name = analyzer->appendCast(resolved_type, actions, key_name);
    }
    analyzer->makeExplicitSet(expr, sample_block, false, key_name);
    argument_names.push_back(key_name);
    const DAGSetPtr & set = analyzer->getPreparedSets()[&expr];

    ColumnWithTypeAndName column;
    column.type = std::make_shared<DataTypeSet>();

    column.name = getUniqueName(actions->getSampleBlock(), "___set");
    column.column = ColumnSet::create(1, set->constant_set);
    actions->add(ExpressionAction::addColumn(column));
    argument_names.push_back(column.name);

    auto collator = getCollatorFromExpr(expr);

    String expr_name = analyzer->applyFunction(func_name, argument_names, actions, collator);
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
        eq_arg_names.push_back(analyzer->getActions(*non_constant_expr, actions));
        // do not need extra cast because TiDB will ensure type of key_name and right_expr_name is the same
        argument_names.push_back(
            analyzer->applyFunction(is_not_in ? "notEquals" : "equals", eq_arg_names, actions, getCollatorFromExpr(expr)));
    }
    // logical op does not need collator
    return analyzer->applyFunction(is_not_in ? "and" : "or", argument_names, actions, nullptr);
}

static String buildLogicalFunction(DAGExpressionAnalyzer * analyzer, const tipb::Expr & expr, ExpressionActionsPtr & actions)
{
    const String & func_name = getFunctionName(expr);
    Names argument_names;
    for (auto & child : expr.children())
    {
        String name = analyzer->getActions(child, actions, true);
        argument_names.push_back(name);
    }
    return analyzer->applyFunction(func_name, argument_names, actions, getCollatorFromExpr(expr));
}

// left(str,len) = substrUTF8(str,1,len)
static String buildLeftUTF8Function(DAGExpressionAnalyzer * analyzer, const tipb::Expr & expr, ExpressionActionsPtr & actions)
{
    const String & func_name = "substringUTF8";
    Names argument_names;

    // the first parameter: str
    String str = analyzer->getActions(expr.children()[0], actions, false);
    argument_names.push_back(str);

    // the second parameter: const(1)
    auto const_one = tipb::Expr();
    constructInt64LiteralTiExpr(const_one, 1);
    auto col_const_one = analyzer->getActions(const_one, actions, false);
    argument_names.push_back(col_const_one);

    // the third parameter: len
    String name = analyzer->getActions(expr.children()[1], actions, false);
    argument_names.push_back(name);

    return analyzer->applyFunction(func_name, argument_names, actions, getCollatorFromExpr(expr));
}

static const String tidb_cast_name = "tidb_cast";

static String buildCastFunctionInternal(
    DAGExpressionAnalyzer * analyzer,
    const Names & argument_names,
    bool in_union,
    const tipb::FieldType & field_type,
    ExpressionActionsPtr & actions)
{
    String result_name = genFuncString(tidb_cast_name, argument_names, {nullptr});
    if (actions->getSampleBlock().has(result_name))
        return result_name;

    FunctionBuilderPtr function_builder = FunctionFactory::instance().get(tidb_cast_name, analyzer->getContext());
    FunctionBuilderTiDBCast * function_builder_tidb_cast = dynamic_cast<FunctionBuilderTiDBCast *>(function_builder.get());
    function_builder_tidb_cast->setInUnion(in_union);
    function_builder_tidb_cast->setTiDBFieldType(field_type);

    const ExpressionAction & apply_function = ExpressionAction::applyFunction(function_builder, argument_names, result_name, nullptr);
    actions->add(apply_function);
    return result_name;
}

/// buildCastFunction build tidb_cast function
static String buildCastFunction(DAGExpressionAnalyzer * analyzer, const tipb::Expr & expr, ExpressionActionsPtr & actions)
{
    if (expr.children_size() != 1)
        throw TiFlashException("Cast function only support one argument", Errors::Coprocessor::BadRequest);
    if (!exprHasValidFieldType(expr))
        throw TiFlashException("CAST function without valid field type", Errors::Coprocessor::BadRequest);

    String name = analyzer->getActions(expr.children(0), actions);
    DataTypePtr expected_type = getDataTypeByFieldType(expr.field_type());

    tipb::Expr type_expr;
    constructStringLiteralTiExpr(type_expr, expected_type->getName());
    auto type_expr_name = analyzer->getActions(type_expr, actions);

    // todo extract in_union from tipb::Expr
    return buildCastFunctionInternal(analyzer, {name, type_expr_name}, false, expr.field_type(), actions);
}

struct DateAdd
{
    static constexpr auto name = "date_add";
    static const std::unordered_map<String, String> unit_to_func_name_map;
};
const std::unordered_map<String, String> DateAdd::unit_to_func_name_map
    = {
        {"DAY", "addDays"},
        {"WEEK", "addWeeks"},
        {"MONTH", "addMonths"},
        {"YEAR", "addYears"},
        {"HOUR", "addHours"},
        {"MINUTE", "addMinutes"},
        {"SECOND", "addSeconds"}};
struct DateSub
{
    static constexpr auto name = "date_sub";
    static const std::unordered_map<String, String> unit_to_func_name_map;
};
const std::unordered_map<String, String> DateSub::unit_to_func_name_map
    = {
        {"DAY", "subtractDays"},
        {"WEEK", "subtractWeeks"},
        {"MONTH", "subtractMonths"},
        {"YEAR", "subtractYears"},
        {"HOUR", "subtractHours"},
        {"MINUTE", "subtractMinutes"},
        {"SECOND", "subtractSeconds"}};

template <typename Impl>
static String buildDateAddOrSubFunction(DAGExpressionAnalyzer * analyzer, const tipb::Expr & expr, ExpressionActionsPtr & actions)
{
    if (expr.children_size() != 3)
    {
        throw TiFlashException(std::string() + Impl::name + " function requires three arguments", Errors::Coprocessor::BadRequest);
    }
    String date_column = analyzer->getActions(expr.children(0), actions);
    String delta_column = analyzer->getActions(expr.children(1), actions);
    if (expr.children(2).tp() != tipb::ExprType::String)
    {
        throw TiFlashException(
            std::string() + "3rd argument of " + Impl::name + " function must be string literal",
            Errors::Coprocessor::BadRequest);
    }
    String unit = expr.children(2).val();
    if (Impl::unit_to_func_name_map.find(unit) == Impl::unit_to_func_name_map.end())
        throw TiFlashException(
            std::string() + Impl::name + " function does not support unit " + unit + " yet.",
            Errors::Coprocessor::Unimplemented);
    String func_name = Impl::unit_to_func_name_map.find(unit)->second;
    const auto & date_column_type = removeNullable(actions->getSampleBlock().getByName(date_column).type);
    if (!date_column_type->isDateOrDateTime())
    {
        // convert to datetime first
        Names arg_names;
        arg_names.push_back(date_column);
        date_column = analyzer->applyFunction("toMyDateTimeOrNull", arg_names, actions, nullptr);
    }
    const auto & delta_column_type = removeNullable(actions->getSampleBlock().getByName(delta_column).type);
    if (!delta_column_type->isNumber())
    {
        // convert to numeric first
        Names arg_names;
        arg_names.push_back(delta_column);
        delta_column = analyzer->applyFunction("toInt64OrNull", arg_names, actions, nullptr);
    }
    Names argument_names;
    argument_names.push_back(date_column);
    argument_names.push_back(delta_column);
    return analyzer->applyFunction(func_name, argument_names, actions, nullptr);
}

static String buildBitwiseFunction(DAGExpressionAnalyzer * analyzer, const tipb::Expr & expr, ExpressionActionsPtr & actions)
{
    const String & func_name = getFunctionName(expr);
    Names argument_names;
    // We should convert arguments to UInt64.
    // See https://github.com/pingcap/tics/issues/1756
    DataTypePtr uint64_type = std::make_shared<DataTypeUInt64>();
    const Block & sample_block = actions->getSampleBlock();
    for (auto & child : expr.children())
    {
        String name = analyzer->getActions(child, actions);
        DataTypePtr orig_type = sample_block.getByName(name).type;

        // Bump argument type
        if (!removeNullable(orig_type)->equals(*uint64_type))
        {
            if (orig_type->isNullable())
            {
                name = analyzer->appendCast(makeNullable(uint64_type), actions, name);
            }
            else
            {
                name = analyzer->appendCast(uint64_type, actions, name);
            }
        }
        argument_names.push_back(name);
    }
    return analyzer->applyFunction(func_name, argument_names, actions, nullptr);
}

static String buildFunction(DAGExpressionAnalyzer * analyzer, const tipb::Expr & expr, ExpressionActionsPtr & actions)
{
    const String & func_name = getFunctionName(expr);
    Names argument_names;
    for (auto & child : expr.children())
    {
        String name = analyzer->getActions(child, actions);
        argument_names.push_back(name);
    }
    return analyzer->applyFunction(func_name, argument_names, actions, getCollatorFromExpr(expr));
}

static std::unordered_map<String, std::function<String(DAGExpressionAnalyzer *, const tipb::Expr &, ExpressionActionsPtr &)>>
    function_builder_map({{"in", buildInFunction}, {"notIn", buildInFunction}, {"globalIn", buildInFunction},
        {"globalNotIn", buildInFunction}, {"tidbIn", buildInFunction}, {"tidbNotIn", buildInFunction}, {"ifNull", buildIfNullFunction},
        {"multiIf", buildMultiIfFunction}, {"tidb_cast", buildCastFunction}, {"and", buildLogicalFunction}, {"or", buildLogicalFunction},
        {"xor", buildLogicalFunction}, {"not", buildLogicalFunction}, {"bitAnd", buildBitwiseFunction}, {"bitOr", buildBitwiseFunction},
        {"bitXor", buildBitwiseFunction}, {"bitNot", buildBitwiseFunction}, {"leftUTF8", buildLeftUTF8Function},
        {"date_add", buildDateAddOrSubFunction<DateAdd>}, {"date_sub", buildDateAddOrSubFunction<DateSub>}});

DAGExpressionAnalyzer::DAGExpressionAnalyzer(std::vector<NameAndTypePair> && source_columns_, const Context & context_)
    : source_columns(std::move(source_columns_))
    , context(context_)
    , after_agg(false)
    , implicit_cast_count(0)
{
    settings = context.getSettings();
}

DAGExpressionAnalyzer::DAGExpressionAnalyzer(std::vector<NameAndTypePair> & source_columns_, const Context & context_)
    : source_columns(source_columns_)
    , context(context_)
    , after_agg(false)
    , implicit_cast_count(0)
{
    settings = context.getSettings();
}

extern const String CountSecondStage;

void DAGExpressionAnalyzer::appendAggregation(
    ExpressionActionsChain & chain,
    const tipb::Aggregation & agg,
    Names & aggregation_keys,
    TiDB::TiDBCollators & collators,
    AggregateDescriptions & aggregate_descriptions,
    bool group_by_collation_sensitive)
{
    if (agg.group_by_size() == 0 && agg.agg_func_size() == 0)
    {
        //should not reach here
        throw TiFlashException("Aggregation executor without group by/agg exprs", Errors::Coprocessor::BadRequest);
    }
    initChain(chain, getCurrentInputColumns());
    ExpressionActionsChain::Step & step = chain.steps.back();
    std::unordered_set<String> agg_key_set;

    for (const tipb::Expr & expr : agg.agg_func())
    {
        String agg_func_name = getAggFunctionName(expr);
        const String agg_func_name_lowercase = Poco::toLower(agg_func_name);
        if (expr.has_distinct() && agg_func_name_lowercase == "countdistinct")
        {
            agg_func_name = settings.count_distinct_implementation;
        }
        if (agg.group_by_size() == 0 && agg_func_name == "sum" && expr.has_field_type()
            && !getDataTypeByFieldType(expr.field_type())->isNullable())
        {
            /// this is a little hack: if the query does not have group by column, and the result of sum is not nullable, then the sum
            /// must be the second stage for count, in this case we should return 0 instead of null if the input is empty.
            agg_func_name = CountSecondStage;
        }

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
        auto function_collator = getCollatorFromExpr(expr);
        String func_string = genFuncString(agg_func_name, aggregate.argument_names, {function_collator});
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
        /// if there is group by clause, there is no need to consider the empty input case
        aggregate.function = AggregateFunctionFactory::instance().get(agg_func_name, types, {}, 0, agg.group_by_size() == 0);
        aggregate.function->setCollator(function_collator);
        aggregate_descriptions.push_back(aggregate);
        DataTypePtr result_type = aggregate.function->getReturnType();
        // this is a temp result since implicit cast maybe added on these aggregated_columns
        aggregated_columns.emplace_back(func_string, result_type);
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
            std::shared_ptr<TiDB::ITiDBCollator> collator = nullptr;
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

                DataTypes types(1);
                aggregate.argument_names.resize(1);
                types[0] = type;
                aggregate.argument_names[0] = name;

                auto function_collator = getCollatorFromExpr(expr);
                String func_string = genFuncString(agg_func_name, aggregate.argument_names, {function_collator});
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
                aggregate.function->setCollator(function_collator);
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
    after_agg = true;
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
    std::shared_ptr<TiDB::ITiDBCollator> collator)

{
    String result_name = genFuncString(func_name, arg_names, {collator});
    if (actions->getSampleBlock().has(result_name))
        return result_name;
    const FunctionBuilderPtr & function_builder = FunctionFactory::instance().get(func_name, context);
    const ExpressionAction & apply_function = ExpressionAction::applyFunction(function_builder, arg_names, result_name, collator);
    actions->add(apply_function);
    return result_name;
}

void DAGExpressionAnalyzer::appendWhere(
    ExpressionActionsChain & chain,
    const std::vector<const tipb::Expr *> & conditions,
    String & filter_column_name)
{
    initChain(chain, getCurrentInputColumns());
    ExpressionActionsChain::Step & last_step = chain.steps.back();
    Names arg_names;
    for (const auto * condition : conditions)
    {
        arg_names.push_back(getActions(*condition, last_step.actions, true));
    }
    if (arg_names.size() == 1)
    {
        filter_column_name = arg_names[0];
        if (isColumnExpr(*conditions[0]))
        {
            bool need_warp_column_expr = true;
            if (exprHasValidFieldType(*conditions[0]) && !isUInt8Type(getDataTypeByFieldType(conditions[0]->field_type())))
            {
                /// if the column is not UInt8 type, we already add some convert function to convert it ot UInt8 type
                need_warp_column_expr = false;
            }
            if (need_warp_column_expr)
            {
                /// FilterBlockInputStream will CHANGE the filter column inplace, so
                /// filter column should never be a columnRef in DAG request, otherwise
                /// for queries like select c1 from t where c1 will got wrong result
                /// as after FilterBlockInputStream, c1 will become a const column of 1
                filter_column_name = convertToUInt8(last_step.actions, filter_column_name);
            }
        }
    }
    else
    {
        // connect all the conditions by logical and
        filter_column_name = applyFunction("and", arg_names, last_step.actions, nullptr);
    }
    chain.steps.back().required_output.push_back(filter_column_name);
}

String DAGExpressionAnalyzer::convertToUInt8(ExpressionActionsPtr & actions, const String & column_name)
{
    // Some of the TiFlash operators(e.g. FilterBlockInputStream) only support uint8 as its input, so need to convert the
    // column type to UInt8
    // the basic rule is:
    // 1. if the column is only null, just return it is fine
    // 2. if the column is numeric, compare it with 0
    // 3. if the column is string, convert it to float-point column, and compare with 0
    // 4. if the column is date/datetime, compare it with zeroDate
    // 5. if the column is other type, throw exception
    if (actions->getSampleBlock().getByName(column_name).type->onlyNull())
    {
        return column_name;
    }
    const auto & org_type = removeNullable(actions->getSampleBlock().getByName(column_name).type);
    if (org_type->isNumber() || org_type->isDecimal())
    {
        tipb::Expr const_expr;
        constructInt64LiteralTiExpr(const_expr, 0);
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
        tipb::Expr type_expr;
        constructStringLiteralTiExpr(type_expr, "Nullable(Double)");
        auto type_expr_name = getActions(type_expr, actions);
        String num_col_name = buildCastFunctionInternal(this, {column_name, type_expr_name}, false, field_type, actions);

        tipb::Expr const_expr;
        constructInt64LiteralTiExpr(const_expr, 0);
        auto const_expr_name = getActions(const_expr, actions);
        return applyFunction("notEquals", {num_col_name, const_expr_name}, actions, nullptr);
    }
    if (org_type->isDateOrDateTime())
    {
        tipb::Expr const_expr;
        constructDateTimeLiteralTiExpr(const_expr, 0);
        auto const_expr_name = getActions(const_expr, actions);
        return applyFunction("notEquals", {column_name, const_expr_name}, actions, nullptr);
    }
    throw TiFlashException("Filter on " + org_type->getName() + " is not supported.", Errors::Coprocessor::Unimplemented);
}

void DAGExpressionAnalyzer::appendOrderBy(
    ExpressionActionsChain & chain,
    const tipb::TopN & topN,
    std::vector<NameAndTypePair> & order_columns)
{
    if (topN.order_by_size() == 0)
    {
        throw TiFlashException("TopN executor without order by exprs", Errors::Coprocessor::BadRequest);
    }
    initChain(chain, getCurrentInputColumns());
    ExpressionActionsChain::Step & step = chain.steps.back();
    for (const tipb::ByItem & byItem : topN.order_by())
    {
        String name = getActions(byItem.expr(), step.actions);
        auto type = step.actions->getSampleBlock().getByName(name).type;
        order_columns.emplace_back(name, type);
        step.required_output.push_back(name);
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

void constructTZExpr(tipb::Expr & tz_expr, const TimezoneInfo & dag_timezone_info, bool from_utc)
{
    if (dag_timezone_info.is_name_based)
        constructStringLiteralTiExpr(tz_expr, dag_timezone_info.timezone_name);
    else
        constructInt64LiteralTiExpr(tz_expr, from_utc ? dag_timezone_info.timezone_offset : -dag_timezone_info.timezone_offset);
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

// add timezone cast after table scan, this is used for session level timezone support
// the basic idea of supporting session level timezone is that:
// 1. for every timestamp column used in the dag request, after reading it from table scan,
//    we add cast function to convert its timezone to the timezone specified in DAG request
// 2. based on the dag encode type, the return column will be with session level timezone(Arrow encode)
//    or UTC timezone(Default encode), if UTC timezone is needed, another cast function is used to
//    convert the session level timezone to UTC timezone.
// Note in the worst case(e.g select ts_col from table with Default encode), this will introduce two
// useless casts to all the timestamp columns, however, since TiDB now use chunk encode as the default
// encoding scheme, the worst case should happen rarely
bool DAGExpressionAnalyzer::appendTimeZoneCastsAfterTS(ExpressionActionsChain & chain, std::vector<bool> is_ts_column)
{
    if (context.getTimezoneInfo().is_utc_timezone)
        return false;

    bool ret = false;
    initChain(chain, getCurrentInputColumns());
    ExpressionActionsPtr actions = chain.getLastActions();
    tipb::Expr tz_expr;
    constructTZExpr(tz_expr, context.getTimezoneInfo(), true);
    String tz_col;
    String func_name = context.getTimezoneInfo().is_name_based ? "ConvertTimeZoneFromUTC" : "ConvertTimeZoneByOffset";
    for (size_t i = 0; i < is_ts_column.size(); i++)
    {
        if (is_ts_column[i])
        {
            if (tz_col.length() == 0)
                tz_col = getActions(tz_expr, actions);
            String casted_name = appendTimeZoneCast(tz_col, source_columns[i].name, func_name, actions);
            source_columns[i].name = casted_name;
            ret = true;
        }
    }
    return ret;
}

void DAGExpressionAnalyzer::appendJoin(
    ExpressionActionsChain & chain,
    SubqueryForSet & join_query,
    const NamesAndTypesList & columns_added_by_join)
{
    initChain(chain, getCurrentInputColumns());
    ExpressionActionsPtr actions = chain.getLastActions();
    actions->add(ExpressionAction::ordinaryJoin(join_query.join, columns_added_by_join));
}
/// return true if some actions is needed
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
        appendWhere(chain, filter_vector, filter_column_name);
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

void DAGExpressionAnalyzer::appendAggSelect(ExpressionActionsChain & chain, const tipb::Aggregation & aggregation)
{
    initChain(chain, getCurrentInputColumns());
    bool need_update_aggregated_columns = false;
    std::vector<NameAndTypePair> updated_aggregated_columns;
    ExpressionActionsChain::Step step = chain.steps.back();
    for (Int32 i = 0; i < aggregation.agg_func_size(); i++)
    {
        String & name = aggregated_columns[i].name;
        String updated_name = appendCastIfNeeded(aggregation.agg_func(i), step.actions, name, false);
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
        Int32 output_column_index = i + aggregation.agg_func_size();
        String & name = aggregated_columns[output_column_index].name;
        String updated_name = appendCastIfNeeded(aggregation.group_by(i), step.actions, name, false);
        if (name != updated_name)
        {
            need_update_aggregated_columns = true;
            DataTypePtr type = step.actions->getSampleBlock().getByName(updated_name).type;
            updated_aggregated_columns.emplace_back(updated_name, type);
            step.required_output.push_back(updated_name);
        }
        else
        {
            updated_aggregated_columns.emplace_back(name, aggregated_columns[output_column_index].type);
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

void DAGExpressionAnalyzer::generateFinalProject(
    ExpressionActionsChain & chain,
    const std::vector<tipb::FieldType> & schema,
    const std::vector<Int32> & output_offsets,
    const String & column_prefix,
    bool keep_session_timezone_info,
    NamesWithAliases & final_project)
{
    if (unlikely(!keep_session_timezone_info && output_offsets.empty()))
        throw Exception("Root Query block without output_offsets", ErrorCodes::LOGICAL_ERROR);

    auto & current_columns = getCurrentInputColumns();
    UniqueNameGenerator unique_name_generator;
    bool need_append_timezone_cast = !keep_session_timezone_info && !context.getTimezoneInfo().is_utc_timezone;
    /// TiDB can not guarantee that the field type in DAG request is accurate, so in order to make things work,
    /// TiFlash will append extra type cast if needed.
    bool need_append_type_cast = false;
    std::vector<bool> need_append_type_cast_vec;
    if (!output_offsets.empty())
    {
        /// !output_offsets.empty() means root block, we need to append type cast for root block if necessary
        for (UInt32 i : output_offsets)
        {
            auto & actual_type = current_columns[i].type;
            auto expected_type = getDataTypeByFieldType(schema[i]);
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
    }
    if (!need_append_timezone_cast && !need_append_type_cast)
    {
        if (!output_offsets.empty())
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
            for (const auto & element : current_columns)
            {
                final_project.emplace_back(element.name, unique_name_generator.toUniqueName(column_prefix + element.name));
            }
        }
    }
    else
    {
        /// for all the columns that need to be returned, if the type is timestamp, then convert
        /// the timestamp column to UTC based, refer to appendTimeZoneCastsAfterTS for more details
        initChain(chain, getCurrentInputColumns());
        ExpressionActionsChain::Step step = chain.steps.back();

        tipb::Expr tz_expr;
        constructTZExpr(tz_expr, context.getTimezoneInfo(), false);
        String tz_col;
        String tz_cast_func_name = context.getTimezoneInfo().is_name_based ? "ConvertTimeZoneToUTC" : "ConvertTimeZoneByOffset";
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
                        updated_name = appendCast(getDataTypeByFieldType(schema[i]), step.actions, updated_name);
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
}

/**
 * when force_uint8 is false, alignReturnType align the data type in tiflash with the data type in dag request, otherwise
 * always convert the return type to uint8 or nullable(uint8)
 * @param expr
 * @param actions
 * @param expr_name
 * @param force_uint8
 * @return
 */
String DAGExpressionAnalyzer::alignReturnType(
    const tipb::Expr & expr,
    ExpressionActionsPtr & actions,
    const String & expr_name,
    bool force_uint8)
{
    DataTypePtr orig_type = actions->getSampleBlock().getByName(expr_name).type;
    if (force_uint8 && isUInt8Type(orig_type))
        return expr_name;
    String updated_name = appendCastIfNeeded(expr, actions, expr_name, false);
    DataTypePtr updated_type = actions->getSampleBlock().getByName(updated_name).type;
    if (force_uint8 && !isUInt8Type(updated_type))
        updated_name = convertToUInt8(actions, updated_name);
    return updated_name;
}

String DAGExpressionAnalyzer::appendCast(const DataTypePtr & target_type, ExpressionActionsPtr & actions, const String & expr_name)
{
    // need to add cast function
    // first construct the second argument
    tipb::Expr type_expr;
    constructStringLiteralTiExpr(type_expr, target_type->getName());
    auto type_expr_name = getActions(type_expr, actions);
    String cast_expr_name = applyFunction("CAST", {expr_name, type_expr_name}, actions, nullptr);
    return cast_expr_name;
}

String DAGExpressionAnalyzer::appendCastIfNeeded(
    const tipb::Expr & expr,
    ExpressionActionsPtr & actions,
    const String & expr_name,
    bool explicit_cast)
{
    if (!isFunctionExpr(expr))
        return expr_name;

    if (!expr.has_field_type())
    {
        throw TiFlashException("Function Expression without field type", Errors::Coprocessor::BadRequest);
    }
    if (exprHasValidFieldType(expr))
    {
        DataTypePtr expected_type = getDataTypeByFieldType(expr.field_type());
        DataTypePtr actual_type = actions->getSampleBlock().getByName(expr_name).type;
        if (expected_type->getName() != actual_type->getName())
        {
            implicit_cast_count += !explicit_cast;
            return appendCast(expected_type, actions, expr_name);
        }
        else
        {
            return expr_name;
        }
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
            tipb::Expr tz_expr;
            constructTZExpr(tz_expr, context.getTimezoneInfo(), true);
            String func_name = context.getTimezoneInfo().is_name_based ? "ConvertTimeZoneFromUTC" : "ConvertTimeZoneByOffset";
            String tz_col = getActions(tz_expr, actions);
            String casted_name = appendTimeZoneCast(tz_col, ret, func_name, actions);
            ret = casted_name;
        }
    }
    else if (isColumnExpr(expr))
    {
        ret = getColumnNameForColumnExpr(expr, getCurrentInputColumns());
    }
    else if (isFunctionExpr(expr))
    {
        if (isAggFunctionExpr(expr))
        {
            throw TiFlashException("agg function is not supported yet", Errors::Coprocessor::Unimplemented);
        }
        const String & func_name = getFunctionName(expr);
        if (function_builder_map.find(func_name) != function_builder_map.end())
        {
            ret = function_builder_map[func_name](this, expr, actions);
        }
        else
        {
            ret = buildFunction(this, expr, actions);
        }
    }
    else
    {
        throw TiFlashException("Unsupported expr type: " + getTypeName(expr), Errors::Coprocessor::Unimplemented);
    }

    ret = alignReturnType(expr, actions, ret, output_as_uint8_type);
    return ret;
}
} // namespace DB
