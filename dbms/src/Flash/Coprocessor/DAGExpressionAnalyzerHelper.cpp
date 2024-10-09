// Copyright 2023 PingCAP, Inc.
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

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnSet.h>
#include <Common/FmtUtils.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/getLeastSupertype.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzerHelper.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsGrouping.h>
#include <Functions/FunctionsJson.h>
#include <Functions/FunctionsTiDBConversion.h>
#include <TiDB/Decode/TypeMapping.h>

namespace DB
{
namespace
{
String getUniqueName(const Block & block, const String & prefix)
{
    for (int i = 1;; ++i)
    {
        auto name = fmt::format("{}{}", prefix, i);
        if (!block.has(name))
            return name;
    }
}

struct DateAdd
{
    static constexpr auto name = "date_add";
    static const std::unordered_map<String, String> unit_to_func_name_map;
};

const std::unordered_map<String, String> DateAdd::unit_to_func_name_map
    = {{"DAY", "addDays"},
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
    = {{"DAY", "subtractDays"},
       {"WEEK", "subtractWeeks"},
       {"MONTH", "subtractMonths"},
       {"YEAR", "subtractYears"},
       {"HOUR", "subtractHours"},
       {"MINUTE", "subtractMinutes"},
       {"SECOND", "subtractSeconds"}};
} // namespace

String DAGExpressionAnalyzerHelper::buildMultiIfFunction(
    DAGExpressionAnalyzer * analyzer,
    const tipb::Expr & expr,
    const ExpressionActionsPtr & actions)
{
    // multiIf is special because
    // 1. the type of odd argument(except the last one) must be UInt8
    // 2. if the total number of arguments is even, we need to add an extra NULL to multiIf
    const String & func_name = getFunctionName(expr);
    Names argument_names;
    for (int i = 0; i < expr.children_size(); i++)
    {
        bool output_as_uint8_type = (i + 1) != expr.children_size() && (i % 2 == 0);
        String name = analyzer->getActions(expr.children(i), actions, output_as_uint8_type);
        argument_names.push_back(name);
    }
    if (argument_names.size() % 2 == 0)
    {
        String name = analyzer->getActions(constructNULLLiteralTiExpr(), actions);
        argument_names.push_back(name);
    }
    return analyzer->applyFunction(func_name, argument_names, actions, getCollatorFromExpr(expr));
}

String DAGExpressionAnalyzerHelper::buildIfNullFunction(
    DAGExpressionAnalyzer * analyzer,
    const tipb::Expr & expr,
    const ExpressionActionsPtr & actions)
{
    // rewrite IFNULL function with multiIf
    // ifNull(arg1, arg2) -> multiIf(isNull(arg1), arg2, assumeNotNull(arg1))
    // todo if arg1 is not nullable, then just return arg1 is ok
    const String & func_name = "multiIf";
    Names argument_names;
    if (expr.children_size() != 2)
    {
        throw TiFlashException("Invalid arguments of IFNULL function", Errors::Coprocessor::BadRequest);
    }

    String condition_arg_name = analyzer->getActions(expr.children(0), actions, false);
    String else_arg_name = analyzer->getActions(expr.children(1), actions, false);
    String is_null_result = analyzer->applyFunction("isNull", {condition_arg_name}, actions, getCollatorFromExpr(expr));
    String not_null_condition_arg_name
        = analyzer->applyFunction("assumeNotNull", {condition_arg_name}, actions, nullptr);

    argument_names.push_back(std::move(is_null_result));
    argument_names.push_back(std::move(else_arg_name));
    argument_names.push_back(std::move(not_null_condition_arg_name));

    return analyzer->applyFunction(func_name, argument_names, actions, getCollatorFromExpr(expr));
}

String DAGExpressionAnalyzerHelper::buildInFunction(
    DAGExpressionAnalyzer * analyzer,
    const tipb::Expr & expr,
    const ExpressionActionsPtr & actions)
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
        const auto & child = expr.children(i);
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

    const auto * collator = getCollatorFromExpr(expr);

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
            analyzer
                ->applyFunction(is_not_in ? "notEquals" : "equals", eq_arg_names, actions, getCollatorFromExpr(expr)));
    }
    // logical op does not need collator
    return analyzer->applyFunction(is_not_in ? "and" : "or", argument_names, actions, nullptr);
}

String DAGExpressionAnalyzerHelper::buildLogicalFunction(
    DAGExpressionAnalyzer * analyzer,
    const tipb::Expr & expr,
    const ExpressionActionsPtr & actions)
{
    const String & func_name = getFunctionName(expr);
    Names argument_names;
    for (const auto & child : expr.children())
    {
        String name = analyzer->getActions(child, actions, true);
        argument_names.push_back(name);
    }
    return analyzer->applyFunction(func_name, argument_names, actions, getCollatorFromExpr(expr));
}

// left(str,len) = substrUTF8(str,1,len)
String DAGExpressionAnalyzerHelper::buildLeftUTF8Function(
    DAGExpressionAnalyzer * analyzer,
    const tipb::Expr & expr,
    const ExpressionActionsPtr & actions)
{
    const String & func_name = "substringUTF8";
    Names argument_names;

    // the first parameter: str
    String str = analyzer->getActions(expr.children()[0], actions, false);
    argument_names.push_back(str);

    // the second parameter: const(1)
    auto const_one = constructInt64LiteralTiExpr(1);
    auto col_const_one = analyzer->getActions(const_one, actions, false);
    argument_names.push_back(col_const_one);

    // the third parameter: len
    String name = analyzer->getActions(expr.children()[1], actions, false);
    argument_names.push_back(name);

    return analyzer->applyFunction(func_name, argument_names, actions, getCollatorFromExpr(expr));
}

String DAGExpressionAnalyzerHelper::buildCastFunctionInternal(
    DAGExpressionAnalyzer * analyzer,
    const Names & argument_names,
    bool in_union,
    const tipb::FieldType & field_type,
    const ExpressionActionsPtr & actions)
{
    static const String tidb_cast_name = "tidb_cast";

    String result_name = genFuncString(tidb_cast_name, argument_names, {nullptr}, {&field_type});
    if (actions->getSampleBlock().has(result_name))
        return result_name;

    FunctionBuilderPtr function_builder = FunctionFactory::instance().get(tidb_cast_name, analyzer->getContext());
    auto * function_builder_tidb_cast = dynamic_cast<FunctionBuilderTiDBCast *>(function_builder.get());
    function_builder_tidb_cast->setInUnion(in_union);
    function_builder_tidb_cast->setTiDBFieldType(field_type);

    const ExpressionAction & apply_function
        = ExpressionAction::applyFunction(function_builder, argument_names, result_name, nullptr);
    actions->add(apply_function);
    return result_name;
}

/// buildCastFunction build tidb_cast function
String DAGExpressionAnalyzerHelper::buildCastFunction(
    DAGExpressionAnalyzer * analyzer,
    const tipb::Expr & expr,
    const ExpressionActionsPtr & actions)
{
    if (expr.children_size() != 1)
        throw TiFlashException("Cast function only support one argument", Errors::Coprocessor::BadRequest);
    if (!exprHasValidFieldType(expr))
        throw TiFlashException("CAST function without valid field type", Errors::Coprocessor::BadRequest);

    String name = analyzer->getActions(expr.children(0), actions);
    DataTypePtr expected_type = getDataTypeByFieldTypeForComputingLayer(expr.field_type());

    tipb::Expr type_expr = constructStringLiteralTiExpr(expected_type->getName());
    auto type_expr_name = analyzer->getActions(type_expr, actions);

    // todo extract in_union from tipb::Expr
    return buildCastFunctionInternal(analyzer, {name, type_expr_name}, false, expr.field_type(), actions);
}

String DAGExpressionAnalyzerHelper::buildSingleParamJsonRelatedFunctions(
    DAGExpressionAnalyzer * analyzer,
    const tipb::Expr & expr,
    const ExpressionActionsPtr & actions)
{
    auto func_name = getFunctionName(expr);
    if unlikely (expr.children_size() != 1)
        throw TiFlashException(
            fmt::format("{} function only support one argument", func_name),
            Errors::Coprocessor::BadRequest);
    if unlikely (!exprHasValidFieldType(expr))
        throw TiFlashException(
            fmt::format("{} function without valid field type", func_name),
            Errors::Coprocessor::BadRequest);

    const auto & input_expr = expr.children(0);
    String arg = analyzer->getActions(input_expr, actions);
    const auto & collator = getCollatorFromExpr(expr);
    String result_name = genFuncString(func_name, {arg}, {collator}, {&input_expr.field_type(), &expr.field_type()});
    if (actions->getSampleBlock().has(result_name))
        return result_name;

    const FunctionBuilderPtr & ifunction_builder = FunctionFactory::instance().get(func_name, analyzer->getContext());
    auto * function_build_ptr = ifunction_builder.get();
    if (auto * function_builder = dynamic_cast<DefaultFunctionBuilder *>(function_build_ptr); function_builder)
    {
        auto * function_impl = function_builder->getFunctionImpl().get();
        if (auto * function_cast_int_as_json = dynamic_cast<FunctionCastIntAsJson *>(function_impl);
            function_cast_int_as_json)
        {
            function_cast_int_as_json->setInputTiDBFieldType(input_expr.field_type());
        }
        else if (auto * function_cast_string_as_json = dynamic_cast<FunctionCastStringAsJson *>(function_impl);
                 function_cast_string_as_json)
        {
            function_cast_string_as_json->setInputTiDBFieldType(input_expr.field_type());
            function_cast_string_as_json->setOutputTiDBFieldType(expr.field_type());
        }
        else if (auto * function_cast_time_as_json = dynamic_cast<FunctionCastTimeAsJson *>(function_impl);
                 function_cast_time_as_json)
        {
            function_cast_time_as_json->setInputTiDBFieldType(input_expr.field_type());
        }
        else if (auto * function_json_unquote = dynamic_cast<FunctionJsonUnquote *>(function_impl);
                 function_json_unquote)
        {
            bool valid_check
                = !(isScalarFunctionExpr(input_expr) && input_expr.sig() == tipb::ScalarFuncSig::CastJsonAsString);
            function_json_unquote->setNeedValidCheck(valid_check);
        }
        else if (auto * function_cast_json_as_string = dynamic_cast<FunctionCastJsonAsString *>(function_impl);
                 function_cast_json_as_string)
        {
            function_cast_json_as_string->setOutputTiDBFieldType(expr.field_type());
        }
        else
        {
            throw Exception(fmt::format("Unexpected func {} in buildSingleParamJsonRelatedFunctions", func_name));
        }
    }
    else
    {
        throw Exception(fmt::format("Unexpected func {} in buildSingleParamJsonRelatedFunctions", func_name));
    }

    const ExpressionAction & action = ExpressionAction::applyFunction(ifunction_builder, {arg}, result_name, collator);
    actions->add(action);
    return result_name;
}

template <typename Impl>
String DAGExpressionAnalyzerHelper::buildDateAddOrSubFunction(
    DAGExpressionAnalyzer * analyzer,
    const tipb::Expr & expr,
    const ExpressionActionsPtr & actions)
{
    if (expr.children_size() != 3)
    {
        throw TiFlashException(
            fmt::format("{} function requires three arguments", Impl::name),
            Errors::Coprocessor::BadRequest);
    }
    String date_column = analyzer->getActions(expr.children(0), actions);
    String delta_column = analyzer->getActions(expr.children(1), actions);
    if (expr.children(2).tp() != tipb::ExprType::String)
    {
        throw TiFlashException(
            fmt::format("3rd argument of {} function must be string literal", Impl::name),
            Errors::Coprocessor::BadRequest);
    }
    String unit = expr.children(2).val();
    if (Impl::unit_to_func_name_map.find(unit) == Impl::unit_to_func_name_map.end())
        throw TiFlashException(
            fmt::format("{} function does not support unit {} yet.", Impl::name, unit),
            Errors::Coprocessor::Unimplemented);
    String func_name = Impl::unit_to_func_name_map.find(unit)->second;
    const auto & delta_column_type = removeNullable(actions->getSampleBlock().getByName(delta_column).type);
    if (!delta_column_type->isNumber())
    {
        // convert to numeric first
        Names arg_names;
        arg_names.push_back(delta_column);
        delta_column = analyzer->applyFunction("toInt64OrNull", arg_names, actions, nullptr);
    }
    else if (!delta_column_type->isInteger())
    {
        // convert to integer
        Names arg_names;
        arg_names.push_back(delta_column);
        delta_column = analyzer->applyFunction("round", arg_names, actions, nullptr);
    }
    Names argument_names;
    argument_names.push_back(date_column);
    argument_names.push_back(delta_column);
    return analyzer->applyFunction(func_name, argument_names, actions, nullptr);
}

String DAGExpressionAnalyzerHelper::buildBitwiseFunction(
    DAGExpressionAnalyzer * analyzer,
    const tipb::Expr & expr,
    const ExpressionActionsPtr & actions)
{
    const String & func_name = getFunctionName(expr);
    Names argument_names;
    // We should convert arguments to UInt64.
    // See https://github.com/pingcap/tics/issues/1756
    DataTypePtr uint64_type = std::make_shared<DataTypeUInt64>();
    const Block & sample_block = actions->getSampleBlock();
    for (const auto & child : expr.children())
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

String DAGExpressionAnalyzerHelper::buildRoundFunction(
    DAGExpressionAnalyzer * analyzer,
    const tipb::Expr & expr,
    const ExpressionActionsPtr & actions)
{
    // ROUND(x) -> ROUND(x, 0)
    if (expr.children_size() != 1)
        throw TiFlashException("Invalid arguments of ROUND function", Errors::Coprocessor::BadRequest);

    auto input_arg_name = analyzer->getActions(expr.children(0), actions);

    auto const_zero = constructInt64LiteralTiExpr(0);
    auto const_zero_arg_name = analyzer->getActions(const_zero, actions);

    Names argument_names;
    argument_names.push_back(std::move(input_arg_name));
    argument_names.push_back(std::move(const_zero_arg_name));

    return analyzer->applyFunction("tidbRoundWithFrac", argument_names, actions, getCollatorFromExpr(expr));
}

String DAGExpressionAnalyzerHelper::buildRegexpFunction(
    DAGExpressionAnalyzer * analyzer,
    const tipb::Expr & expr,
    const ExpressionActionsPtr & actions)
{
    const String & func_name = getFunctionName(expr);
    Names argument_names;
    for (const auto & child : expr.children())
    {
        String name = analyzer->getActions(child, actions);
        argument_names.push_back(name);
    }
    TiDB::TiDBCollatorPtr collator = getCollatorFromExpr(expr);
    if (expr.sig() == tipb::ScalarFuncSig::RegexpReplaceSig || expr.sig() == tipb::ScalarFuncSig::RegexpSig)
    {
        /// according to https://github.com/pingcap/tidb/blob/v5.0.0/expression/builtin_like.go#L126,
        /// For binary collation, it will use RegexpXXXSig, otherwise it will use RegexpXXXUTF8Sig
        /// Need to set the collator explicitly because `getCollatorFromExpr` will return nullptr
        /// if new collation is not enabled.
        collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::BINARY);
    }
    return analyzer->applyFunction(func_name, argument_names, actions, collator);
}

String DAGExpressionAnalyzerHelper::buildGroupingFunction(
    DAGExpressionAnalyzer * analyzer,
    const tipb::Expr & expr,
    const ExpressionActionsPtr & actions)
{
    const String & func_name = getFunctionName(expr);
    Names argument_names;
    for (const auto & child : expr.children())
    {
        String name = analyzer->getActions(child, actions);
        argument_names.push_back(name);
    }

    // Get the result by function name and parameters is **NOT** enough, grouping functions like: grouping(a) and grouping(b) will be rewritten as the same signature:
    //  grouping(gid), grouping(gid) with different metadata.
    // In TiDB, we don't reuse the action by the signature name, while TiFlash does, we should distinguish different grouping function out from their metadata.
    String result_name = genFuncString(func_name, argument_names, {getCollatorFromExpr(expr)});
    // grouping function's metadata has naturally been encoded as proto-message as string, just appending them to the result name as new grouping functions' result_name.
    result_name += expr.val();
    if (actions->getSampleBlock().has(result_name))
        return result_name;

    FunctionBuilderPtr function_builder = FunctionFactory::instance().get(func_name, analyzer->getContext());
    auto * function_builder_grouping = dynamic_cast<FunctionBuilderGrouping *>(function_builder.get());
    function_builder_grouping->setExpr(expr);

    const ExpressionAction & apply_function
        = ExpressionAction::applyFunction(function_builder, argument_names, result_name, nullptr);
    actions->add(apply_function);
    return result_name;
}

String DAGExpressionAnalyzerHelper::buildDefaultFunction(
    DAGExpressionAnalyzer * analyzer,
    const tipb::Expr & expr,
    const ExpressionActionsPtr & actions)
{
    const String & func_name = getFunctionName(expr);
    Names argument_names;
    for (const auto & child : expr.children())
    {
        String name = analyzer->getActions(child, actions);
        argument_names.push_back(name);
    }
    return analyzer->applyFunction(func_name, argument_names, actions, getCollatorFromExpr(expr));
}

String DAGExpressionAnalyzerHelper::buildFunction(
    DAGExpressionAnalyzer * analyzer,
    const tipb::Expr & expr,
    const ExpressionActionsPtr & actions)
{
    const String & func_name = getFunctionName(expr);
    if (function_builder_map.count(func_name) != 0)
    {
        return function_builder_map[func_name](analyzer, expr, actions);
    }
    else
    {
        return buildDefaultFunction(analyzer, expr, actions);
    }
}

DAGExpressionAnalyzerHelper::FunctionBuilderMap DAGExpressionAnalyzerHelper::function_builder_map(
    {{"in", DAGExpressionAnalyzerHelper::buildInFunction},
     {"notIn", DAGExpressionAnalyzerHelper::buildInFunction},
     {"globalIn", DAGExpressionAnalyzerHelper::buildInFunction},
     {"globalNotIn", DAGExpressionAnalyzerHelper::buildInFunction},
     {"tidbIn", DAGExpressionAnalyzerHelper::buildInFunction},
     {"tidbNotIn", DAGExpressionAnalyzerHelper::buildInFunction},
     {"ifNull", DAGExpressionAnalyzerHelper::buildIfNullFunction},
     {"multiIf", DAGExpressionAnalyzerHelper::buildMultiIfFunction},
     {"tidb_cast", DAGExpressionAnalyzerHelper::buildCastFunction},
     {"cast_int_as_json", DAGExpressionAnalyzerHelper::buildSingleParamJsonRelatedFunctions},
     {"cast_string_as_json", DAGExpressionAnalyzerHelper::buildSingleParamJsonRelatedFunctions},
     {"cast_time_as_json", DAGExpressionAnalyzerHelper::buildSingleParamJsonRelatedFunctions},
     {"cast_json_as_string", DAGExpressionAnalyzerHelper::buildSingleParamJsonRelatedFunctions},
     {"json_unquote", DAGExpressionAnalyzerHelper::buildSingleParamJsonRelatedFunctions},
     {"and", DAGExpressionAnalyzerHelper::buildLogicalFunction},
     {"or", DAGExpressionAnalyzerHelper::buildLogicalFunction},
     {"xor", DAGExpressionAnalyzerHelper::buildLogicalFunction},
     {"binary_and", DAGExpressionAnalyzerHelper::buildLogicalFunction},
     {"binary_or", DAGExpressionAnalyzerHelper::buildLogicalFunction},
     {"binary_xor", DAGExpressionAnalyzerHelper::buildLogicalFunction},
     {"not", DAGExpressionAnalyzerHelper::buildLogicalFunction},
     {"bitAnd", DAGExpressionAnalyzerHelper::buildBitwiseFunction},
     {"bitOr", DAGExpressionAnalyzerHelper::buildBitwiseFunction},
     {"bitXor", DAGExpressionAnalyzerHelper::buildBitwiseFunction},
     {"bitNot", DAGExpressionAnalyzerHelper::buildBitwiseFunction},
     {"bitShiftLeft", DAGExpressionAnalyzerHelper::buildBitwiseFunction},
     {"bitShiftRight", DAGExpressionAnalyzerHelper::buildBitwiseFunction},
     {"leftUTF8", DAGExpressionAnalyzerHelper::buildLeftUTF8Function},
     {"date_add", DAGExpressionAnalyzerHelper::buildDateAddOrSubFunction<DateAdd>},
     {"date_sub", DAGExpressionAnalyzerHelper::buildDateAddOrSubFunction<DateSub>},
     {"regexp", DAGExpressionAnalyzerHelper::buildRegexpFunction},
     {"replaceRegexpAll", DAGExpressionAnalyzerHelper::buildRegexpFunction},
     {"tidbRound", DAGExpressionAnalyzerHelper::buildRoundFunction},
     {"grouping", DAGExpressionAnalyzerHelper::buildGroupingFunction}});

} // namespace DB
