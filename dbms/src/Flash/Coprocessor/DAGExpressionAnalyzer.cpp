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
#include <AggregateFunctions/AggregateFunctionGroupConcat.h>
#include <Columns/ColumnSet.h>
#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Common/FmtUtils.h>
#include <Common/Logger.h>
#include <Common/TiFlashException.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/FieldToDataType.h>
#include <DataTypes/getLeastSupertype.h>
#include <Flash/Coprocessor/AggregationInterpreterHelper.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzerHelper.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/JoinInterpreterHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsTiDBConversion.h>
#include <Functions/minus.h>
#include <Functions/plus.h>
#include <Interpreters/Context.h>
#include <Interpreters/Expand.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Set.h>
#include <Interpreters/Settings.h>
#include <Interpreters/convertFieldToType.h>
#include <Parsers/ASTIdentifier.h>
#include <TiDB/Decode/TypeMapping.h>
#include <WindowFunctions/WindowFunctionFactory.h>
#include <tipb/executor.pb.h>
#include <tipb/expression.pb.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNSUPPORTED_METHOD;
} // namespace ErrorCodes

DAGExpressionAnalyzer::DAGExpressionAnalyzer(std::vector<NameAndTypePair> source_columns_, const Context & context_)
    : source_columns(std::move(source_columns_))
    , context(context_)
{}

DAGExpressionAnalyzer::DAGExpressionAnalyzer(const Block & sample_block, const Context & context_)
    : context(context_)
{
    source_columns.reserve(sample_block.columns());
    for (const auto & col : sample_block)
        source_columns.emplace_back(col.name, col.type);
}

extern const String count_second_stage;
extern const String sum_on_partial_result;

namespace
{
bool isUInt8Type(const DataTypePtr & type)
{
    return removeNullable(type)->getTypeId() == TypeIndex::UInt8;
}

tipb::Expr constructTZExpr(const TimezoneInfo & dag_timezone_info)
{
    return dag_timezone_info.is_name_based ? constructStringLiteralTiExpr(dag_timezone_info.timezone_name)
                                           : constructInt64LiteralTiExpr(dag_timezone_info.timezone_offset);
}

String getAggFuncName(const tipb::Expr & expr, const tipb::Aggregation & agg, const Settings & settings)
{
    String agg_func_name = getAggFunctionName(expr);

    static const String count_distinct_func_name = "countDistinct";
    if (expr.has_distinct() && agg_func_name == count_distinct_func_name)
        return settings.count_distinct_implementation;

    static const String sum_func_name = "sum";
    if (agg.group_by_size() == 0 && agg_func_name == sum_func_name && expr.has_field_type()
        && !getDataTypeByFieldTypeForComputingLayer(expr.field_type())->isNullable())
    {
        /// this is a little hack: if the query does not have group by column, and the result of sum is not nullable, then the sum
        /// must be the second stage for count, in this case we should return 0 instead of null if the input is empty.
        return count_second_stage;
    }

    // sum functions in mpp are multistage and we need to distinguish them with function name.
    // "sum" represents the first stage.
    // "sum_on_partial_result" represents other stages whose input is partial result.
    // Return type of sum function in different stages is calculated in different ways which is determined
    // by function name, so we need to distinguish them with function names.
    if (AggregationInterpreterHelper::isSumOnPartialResults(expr))
        return sum_on_partial_result;

    return agg_func_name;
}

/// return `duplicated Agg/Window function`->getReturnType if duplicated.
/// or not return nullptr.
template <typename Descriptions>
DataTypePtr findDuplicateAggWindowFunc(const String & func_string, const Descriptions & descriptions)
{
    for (const auto & description : descriptions)
    {
        if (description.column_name == func_string)
        {
            if constexpr (std::is_same_v<Descriptions, AggregateDescriptions>)
            {
                auto return_type = description.function->getReturnType();
                assert(return_type);
                return return_type;
            }
            else
            {
                static_assert(std::is_same_v<Descriptions, WindowFunctionDescriptions>);
                auto return_type = description.window_function->getReturnType();
                assert(return_type);
                return return_type;
            }
        }
    }
    return nullptr;
}

/// Generate AggregateDescription and append it to AggregateDescriptions if need.
/// And append output column to aggregated_columns.
void appendAggDescription(
    const Names & arg_names,
    const DataTypes & arg_types,
    TiDB::TiDBCollators & arg_collators,
    const String & agg_func_name,
    AggregateDescriptions & aggregate_descriptions,
    NamesAndTypes & aggregated_columns,
    bool empty_input_as_null,
    const Context & context)
{
    assert(arg_names.size() == arg_collators.size() && arg_names.size() == arg_types.size());

    AggregateDescription aggregate;
    aggregate.argument_names = arg_names;
    String func_string = genFuncString(agg_func_name, aggregate.argument_names, arg_collators);
    if (auto duplicated_return_type = findDuplicateAggWindowFunc(func_string, aggregate_descriptions))
    {
        // agg function duplicate, don't need to build again.
        aggregated_columns.emplace_back(func_string, duplicated_return_type);
        return;
    }

    aggregate.column_name = func_string;
    aggregate.parameters = Array();
    aggregate.function
        = AggregateFunctionFactory::instance().get(context, agg_func_name, arg_types, {}, 0, empty_input_as_null);
    aggregate.function->setCollators(arg_collators);

    aggregated_columns.emplace_back(func_string, aggregate.function->getReturnType());

    aggregate_descriptions.emplace_back(std::move(aggregate));
}

/// Generate WindowFunctionDescription and append it to WindowDescription if need.
void appendWindowDescription(
    const Names & arg_names,
    const DataTypes & arg_types,
    TiDB::TiDBCollators & arg_collators,
    const String & window_func_name,
    WindowDescription & window_description,
    NamesAndTypes & source_columns,
    NamesAndTypes & window_columns)
{
    assert(arg_names.size() == arg_collators.size() && arg_names.size() == arg_types.size());

    String func_string = genFuncString(window_func_name, arg_names, arg_collators);
    if (auto duplicated_return_type
        = findDuplicateAggWindowFunc(func_string, window_description.window_functions_descriptions))
    {
        // window function duplicate, don't need to build again.
        source_columns.emplace_back(func_string, duplicated_return_type);
        return;
    }

    WindowFunctionDescription window_function_description;
    window_function_description.argument_names = arg_names;
    window_function_description.column_name = func_string;
    window_function_description.window_function = WindowFunctionFactory::instance().get(window_func_name, arg_types);
    DataTypePtr result_type = window_function_description.window_function->getReturnType();
    window_description.window_functions_descriptions.emplace_back(std::move(window_function_description));
    window_columns.emplace_back(func_string, result_type);
    source_columns.emplace_back(func_string, result_type);
}

void setAuxiliaryColumnInfoImpl(
    const String & aux_col_name,
    const Block & tmp_block,
    Int32 & range_auxiliary_column_index,
    TypeIndex & aux_col_type,
    bool & is_order_by_col_nullable)
{
    if (!aux_col_name.empty())
    {
        // Set auxiliary columns' indexes
        size_t aux_col_idx = tmp_block.getPositionByName(aux_col_name);
        range_auxiliary_column_index = aux_col_idx;

        // Set auxiliary columns' types
        const auto & col_and_name = tmp_block.getByName(aux_col_name);
        auto data_type = col_and_name.type;
        if (data_type->isNullable())
        {
            is_order_by_col_nullable = true;
            const auto & nullable_data_type = static_cast<const DataTypeNullable &>(*data_type);
            aux_col_type = nullable_data_type.getNestedType()->getTypeId();
        }
        else
        {
            is_order_by_col_nullable = false;
            aux_col_type = data_type->getTypeId();
        }
    }
}

// We need auxiliary columns' info when finding the start or end boundary of the frame
void setAuxiliaryColumnInfo(
    ExpressionActionsPtr & actions,
    WindowDescription & window_desc,
    const String & begin_aux_col_name,
    const String & end_aux_col_name,
    const tipb::Window & window)
{
    // Execute this function only when the frame type is Range
    if (window.frame().type() != tipb::WindowFrameType::Ranges)
        return;

    if (begin_aux_col_name.empty() && end_aux_col_name.empty())
        return;

    const Block & tmp_block = actions->getSampleBlock();
    if (!begin_aux_col_name.empty())
        setAuxiliaryColumnInfoImpl(
            begin_aux_col_name,
            tmp_block,
            window_desc.frame.begin_range_auxiliary_column_index,
            window_desc.begin_aux_col_type,
            window_desc.is_begin_aux_col_nullable);
    if (!end_aux_col_name.empty())
        setAuxiliaryColumnInfoImpl(
            end_aux_col_name,
            tmp_block,
            window_desc.frame.end_range_auxiliary_column_index,
            window_desc.end_aux_col_type,
            window_desc.is_end_aux_col_nullable);
}

void setOrderByColumnTypeAndDirectionForRangeFrame(
    WindowDescription & window_desc,
    const ExpressionActionsPtr & actions,
    const tipb::Window & window)
{
    // Execute this function only when the frame type is Range
    if (window.frame().type() != tipb::WindowFrameType::Ranges)
        return;

    if (!window.frame().start().has_frame_range() && !window.frame().end().has_frame_range())
        return;

    RUNTIME_CHECK_MSG(
        !window_desc.order_by.empty(),
        "Order by column should not be empty when the frame type is range");

    const Block & sample_block = actions->getSampleBlock();
    const String & order_by_col_name = window_desc.order_by[0].column_name;
    const ColumnWithTypeAndName & order_by_col_type_and_name = sample_block.getByName(order_by_col_name);

    if (order_by_col_type_and_name.type->isNullable())
    {
        window_desc.is_order_by_col_nullable = true;
        const auto & nullable_data_type = static_cast<const DataTypeNullable &>(*order_by_col_type_and_name.type);
        window_desc.order_by_col_type = nullable_data_type.getNestedType()->getTypeId();
    }
    else
    {
        window_desc.is_order_by_col_nullable = false;
        window_desc.order_by_col_type = order_by_col_type_and_name.type->getTypeId();
    }
    window_desc.is_desc = (window_desc.order_by[0].direction == -1);
}

// Add a function generating a new auxiliary column that help the implementation of range frame type
std::pair<String, String> addRangeFrameAuxiliaryFunctionAction(
    DAGExpressionAnalyzer * analyzer,
    ExpressionActionsPtr & actions,
    const tipb::Window & window)
{
    // Execute this function only when the frame type is Range
    if (window.frame().type() != tipb::WindowFrameType::Ranges)
        return std::make_pair("", "");

    String begin_aux_col_name;
    String end_aux_col_name;
    if (window.frame().start().has_frame_range())
        begin_aux_col_name
            = DAGExpressionAnalyzerHelper::buildFunction(analyzer, window.frame().start().frame_range(), actions);

    if (window.frame().end().has_frame_range())
        end_aux_col_name
            = DAGExpressionAnalyzerHelper::buildFunction(analyzer, window.frame().end().frame_range(), actions);

    return std::make_pair(begin_aux_col_name, end_aux_col_name);
}

WindowDescription createAndInitWindowDesc(DAGExpressionAnalyzer * const analyzer, const tipb::Window & window)
{
    WindowDescription window_description;
    window_description.partition_by = analyzer->getWindowSortDescription(window.partition_by());
    window_description.order_by = analyzer->getWindowSortDescription(window.order_by());
    if (window.has_frame())
    {
        window_description.setWindowFrame(window.frame());
    }

    return window_description;
}

void buildActionsBeforeWindow(
    DAGExpressionAnalyzer * analyzer,
    WindowDescription & window_desc,
    ExpressionActionsChain & chain,
    const tipb::Window & window)
{
    auto actions = chain.getLastActions();

    // Prepare auxiliary function for range frame type
    auto aux_col_names = addRangeFrameAuxiliaryFunctionAction(analyzer, actions, window);

    analyzer->appendWindowColumns(window_desc, window, actions);
    // set required output for window funcs's arguments.
    for (const auto & window_function_description : window_desc.window_functions_descriptions)
    {
        for (const auto & argument_name : window_function_description.argument_names)
            chain.getLastStep().required_output.push_back(argument_name);
    }

    window_desc.before_window = actions;
    if (!aux_col_names.first.empty())
        chain.getLastStep().required_output.push_back(aux_col_names.first);
    if (!aux_col_names.second.empty())
        chain.getLastStep().required_output.push_back(aux_col_names.second);

    chain.finalize();
    chain.clear();
    setAuxiliaryColumnInfo(actions, window_desc, aux_col_names.first, aux_col_names.second, window);
}

void buildActionsAfterWindow(
    DAGExpressionAnalyzer * const analyze,
    WindowDescription & window_desc,
    ExpressionActionsChain & chain,
    const tipb::Window & window,
    size_t source_size)
{
    auto & after_window_step = analyze->initAndGetLastStep(chain);
    analyze->appendCastAfterWindow(after_window_step.actions, window, source_size);
    window_desc.after_window_columns = analyze->getCurrentInputColumns();
    analyze->appendSourceColumnsToRequireOutput(after_window_step);
    window_desc.after_window = chain.getLastActions();
    chain.finalize();
    chain.clear();
}

std::optional<String> findFirstRow(
    const AggregateDescriptions & aggregate_descriptions,
    const String & arg_name,
    const DataTypePtr & arg_type)
{
    for (const auto & desc : aggregate_descriptions)
    {
        if (desc.function->getName() == "first_row" && desc.argument_names[0] == arg_name)
        {
            RUNTIME_CHECK(desc.function->getReturnType()->equals(*arg_type));
            return {desc.column_name};
        }
    }
    return {};
}
} // namespace

ExpressionActionsChain::Step & DAGExpressionAnalyzer::initAndGetLastStep(ExpressionActionsChain & chain) const
{
    initChain(chain);
    return chain.getLastStep();
}

void DAGExpressionAnalyzer::fillArgumentDetail(
    const ExpressionActionsPtr & actions,
    const tipb::Expr & arg,
    Names & arg_names,
    DataTypes & arg_types,
    TiDB::TiDBCollators & arg_collators)
{
    arg_names.push_back(getActions(arg, actions));
    arg_types.push_back(actions->getSampleBlock().getByName(arg_names.back()).type);
    arg_collators.push_back(removeNullable(arg_types.back())->isString() ? getCollatorFromExpr(arg) : nullptr);
}

void DAGExpressionAnalyzer::buildGroupConcat(
    const tipb::Expr & expr,
    const ExpressionActionsPtr & actions,
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
    DataTypes types;

    /// more than one args will be combined to one
    if (child_size == 1 && expr.order_by_size() == 0)
    {
        /// only one arg
        Names arg_names;
        fillArgumentDetail(actions, expr.children(0), arg_names, types, arg_collators);
        arg_name = arg_names.back();
        all_columns_names_and_types.emplace_back(arg_name, types[0]);
    }
    else
    {
        /// args... -> tuple(args...)
        arg_name = buildTupleFunctionForGroupConcat(
            expr,
            sort_description,
            all_columns_names_and_types,
            arg_collators,
            actions);
        only_one_column = false;
        types.push_back(actions->getSampleBlock().getByName(arg_name).type);
    }
    aggregate.argument_names.push_back(arg_name);

    /// the separator
    arg_name = getActions(expr.children(child_size), actions);
    if (expr.children(child_size).tp() == tipb::String)
    {
        const ColumnConst * col_delim
            = checkAndGetColumnConstStringOrFixedString(actions->getSampleBlock().getByName(arg_name).column.get());
        if (col_delim == nullptr)
        {
            throw Exception("the separator of group concat should not be invalid!");
        }
        delimiter = col_delim->getValue<String>();
    }

    String func_string = genFuncString(agg_func_name, aggregate.argument_names, arg_collators);
    /// return directly if the agg is duplicated
    if (auto duplicated_return_type = findDuplicateAggWindowFunc(func_string, aggregate_descriptions))
    {
        aggregated_columns.emplace_back(func_string, duplicated_return_type);
        return;
    }

    aggregate.column_name = func_string;
    aggregate.parameters = Array();
    /// if there is group by clause, there is no need to consider the empty input case
    aggregate.function
        = AggregateFunctionFactory::instance().get(context, agg_func_name, types, {}, 0, result_is_nullable);

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

void DAGExpressionAnalyzer::buildCommonAggFunc(
    const tipb::Expr & expr,
    const ExpressionActionsPtr & actions,
    const String & agg_func_name,
    AggregateDescriptions & aggregate_descriptions,
    NamesAndTypes & aggregated_columns,
    bool empty_input_as_null)
{
    auto child_size = expr.children_size();
    Names arg_names;
    DataTypes arg_types;
    TiDB::TiDBCollators arg_collators;

    for (Int32 i = 0; i < child_size; ++i)
    {
        fillArgumentDetail(actions, expr.children(i), arg_names, arg_types, arg_collators);
    }
    // For count(not null column), we can transform it to count() to avoid the cost of convertToFullColumn.
    if (expr.tp() == tipb::ExprType::Count && !expr.has_distinct() && child_size == 1 && !arg_types[0]->isNullable())
    {
        arg_names.clear();
        arg_types.clear();
        arg_collators.clear();
    }
    appendAggDescription(
        arg_names,
        arg_types,
        arg_collators,
        agg_func_name,
        aggregate_descriptions,
        aggregated_columns,
        empty_input_as_null,
        context);
}

void DAGExpressionAnalyzer::buildAggGroupBy(
    const google::protobuf::RepeatedPtrField<tipb::Expr> & group_by,
    const ExpressionActionsPtr & actions,
    AggregateDescriptions & aggregate_descriptions,
    NamesAndTypes & aggregated_columns,
    Names & aggregation_keys,
    std::unordered_set<String> & agg_key_set,
    KeyRefAggFuncMap & key_ref_agg_func,
    bool group_by_collation_sensitive,
    std::unordered_map<String, TiDB::TiDBCollatorPtr> & collators)
{
    for (const tipb::Expr & expr : group_by)
    {
        String name = getActions(expr, actions);
        bool duplicated_key = agg_key_set.find(name) != agg_key_set.end();
        if (!duplicated_key)
        {
            /// note this assume that column with the same name has the same collator
            /// need double check this assumption when we support agg with collation
            aggregation_keys.push_back(name);
            agg_key_set.emplace(name);
            RUNTIME_CHECK(collators.insert({name, nullptr}).second);
        }
        /// when group_by_collation_sensitive is true, TiFlash will do the aggregation with collation
        /// info, since the aggregation in TiFlash is actually the partial stage, and TiDB always do
        /// the final stage of the aggregation, even if TiFlash do the aggregation without collation
        /// info, the correctness of the query result is guaranteed by TiDB itself, so add a flag to
        /// let TiDB/TiFlash to decide whether aggregate the data with collation info or not
        if (group_by_collation_sensitive)
        {
            auto type = actions->getSampleBlock().getByName(name).type;
            TiDB::TiDBCollatorPtr collator = nullptr;
            if (removeNullable(type)->isString())
                collator = getCollatorFromExpr(expr);
            if (!duplicated_key)
                collators[name] = collator;
            if (collator != nullptr)
            {
                auto first_row_name = findFirstRow(aggregate_descriptions, name, type);
                String agg_func_name;
                if (first_row_name)
                {
                    agg_func_name = *first_row_name;
                    // Got here when this group by key has its corresponding first_row agg func.
                    aggregated_columns.emplace_back(name, type);
                }
                else
                {
                    /// if the column is a string with collation info, the `sort_key` of the column is used during
                    /// aggregation, but we can not reconstruct the origin column by `sort_key`, so add an extra
                    /// extra aggregation function first_row(group_by_column) here as the output of the group by column
                    TiDB::TiDBCollators arg_collators{collator};
                    appendAggDescription(
                        {name},
                        {type},
                        arg_collators,
                        "first_row",
                        aggregate_descriptions,
                        aggregated_columns,
                        false,
                        context);
                    agg_func_name = aggregate_descriptions.back().column_name;
                }
                key_ref_agg_func.insert({name, agg_func_name});
            }
            else
            {
                aggregated_columns.emplace_back(name, actions->getSampleBlock().getByName(name).type);
            }
        }
        else
        {
            aggregated_columns.emplace_back(name, actions->getSampleBlock().getByName(name).type);
        }
    }
#ifndef NDEBUG
    for (const auto & p : key_ref_agg_func)
    {
        LOG_TRACE(Logger::get(), "key_ref_agg_func optimization: {} ref {}", p.first, p.second);
    }
#endif
}

void DAGExpressionAnalyzer::tryEliminateFirstRow(
    const Names & aggregation_keys,
    const std::unordered_map<String, TiDB::TiDBCollatorPtr> & collators,
    AggFuncRefKeyMap & agg_func_ref_key,
    AggregateDescriptions & aggregate_descriptions)
{
    // Assume aggregate_keys and collators are corresponding one by one.
    // This is assured by buildAggGroupBy().
    RUNTIME_CHECK(aggregation_keys.size() == collators.size());

    for (const auto & key_name : aggregation_keys)
    {
        for (const auto & agg_desc : aggregate_descriptions)
        {
            const auto & func_name = agg_desc.function->getName();
            const auto & collator_iter = collators.find(key_name);
            RUNTIME_CHECK(collator_iter != collators.end());
            if (collator_iter->second == nullptr && func_name == "first_row" && agg_desc.argument_names[0] == key_name)
            {
                agg_func_ref_key.insert({agg_desc.column_name, key_name});
            }
        }
    }

    if (agg_func_ref_key.empty())
        return;

    AggregateDescriptions tmp_aggregate_descriptions;
    tmp_aggregate_descriptions.reserve(aggregate_descriptions.size() - agg_func_ref_key.size());
    for (const auto & desc : aggregate_descriptions)
    {
        if (agg_func_ref_key.find(desc.column_name) == agg_func_ref_key.end())
        {
            tmp_aggregate_descriptions.push_back(desc);
        }
    }
    aggregate_descriptions = tmp_aggregate_descriptions;

#ifndef NDEBUG
    for (const auto & p : agg_func_ref_key)
    {
        LOG_TRACE(Logger::get(), "agg_func_ref_key optimization: {} ref {}", p.first, p.second);
    }
#endif
}

void DAGExpressionAnalyzer::buildAggFuncs(
    const tipb::Aggregation & aggregation,
    const ExpressionActionsPtr & actions,
    AggregateDescriptions & aggregate_descriptions,
    NamesAndTypes & aggregated_columns)
{
    for (const tipb::Expr & expr : aggregation.agg_func())
    {
        if (expr.tp() == tipb::ExprType::GroupConcat)
        {
            buildGroupConcat(
                expr,
                actions,
                getAggFuncName(expr, aggregation, context.getSettingsRef()),
                aggregate_descriptions,
                aggregated_columns,
                aggregation.group_by().empty());
        }
        else
        {
            /// if there is group by clause, there is no need to consider the empty input case
            bool empty_input_as_null = aggregation.group_by().empty();
            buildCommonAggFunc(
                expr,
                actions,
                getAggFuncName(expr, aggregation, context.getSettingsRef()),
                aggregate_descriptions,
                aggregated_columns,
                empty_input_as_null);
        }
    }
}

bool isWindowFunctionsValid(const tipb::Window & window)
{
    bool has_agg_func = false;
    bool has_window_func = false;
    for (const tipb::Expr & expr : window.func_desc())
    {
        has_agg_func = has_agg_func || isAggFunctionExpr(expr);
        has_window_func = has_window_func || isWindowFunctionExpr(expr);
    }

    return !(has_agg_func && has_window_func);
}

SortDescription DAGExpressionAnalyzer::getWindowSortDescription(
    const ::google::protobuf::RepeatedPtrField<tipb::ByItem> & by_items) const
{
    NamesAndTypes by_item_columns;
    by_item_columns.reserve(by_items.size());

    for (const tipb::ByItem & by_item : by_items)
    {
        if (!isColumnExpr(by_item.expr()))
        {
            throw TiFlashException("must be column expr.", Errors::Coprocessor::BadRequest);
        }
        by_item_columns.emplace_back(getColumnNameAndTypeForColumnExpr(by_item.expr(), getCurrentInputColumns()));
    }

    return getSortDescription(by_item_columns, by_items);
}


void DAGExpressionAnalyzer::appendSourceColumnsToRequireOutput(ExpressionActionsChain::Step & step) const
{
    for (const auto & col : getCurrentInputColumns())
    {
        step.required_output.push_back(col.name);
    }
}

void DAGExpressionAnalyzer::buildLeadLag(
    const tipb::Expr & expr,
    const ExpressionActionsPtr & actions,
    const String & window_func_name,
    WindowDescription & window_description,
    NamesAndTypes & source_columns,
    NamesAndTypes & window_columns)
{
    auto child_size = expr.children_size();
    RUNTIME_CHECK_MSG(
        child_size >= 1 && child_size <= 3,
        "arguments num of lead/lag must >= 1 and <= 3, but {}",
        child_size);

    Names arg_names;
    DataTypes arg_types;
    TiDB::TiDBCollators arg_collators;

    if (child_size <= 2)
    {
        for (Int32 i = 0; i < child_size; ++i)
        {
            fillArgumentDetail(actions, expr.children(i), arg_names, arg_types, arg_collators);
        }
    }
    else // child_size == 3
    {
        const auto & sample_block = actions->getSampleBlock();
        auto get_name_type = [&](const tipb::Expr & arg_expr) -> std::pair<String, DataTypePtr> {
            auto arg_name = getActions(arg_expr, actions);
            auto arg_type = sample_block.getByName(arg_name).type;
            return {std::move(arg_name), std::move(arg_type)};
        };
        auto [first_arg_name, first_arg_type] = get_name_type(expr.children(0));
        auto [third_arg_name, third_arg_type] = get_name_type(expr.children(2));

        auto final_type = getLeastSupertype({first_arg_type, third_arg_type});
        auto append_cast_if_need = [&](String & name, DataTypePtr & type) {
            if (!final_type->equals(*type))
            {
                name = appendCast(final_type, actions, name);
                type = final_type;
            }
        };
        append_cast_if_need(first_arg_name, first_arg_type);
        append_cast_if_need(third_arg_name, third_arg_type);

        auto fill_arg_detail = [&](const tipb::Expr & arg_expr, const String & arg_name, const DataTypePtr & arg_type) {
            arg_names.push_back(arg_name);
            arg_types.push_back(arg_type);
            arg_collators.push_back(removeNullable(arg_type)->isString() ? getCollatorFromExpr(arg_expr) : nullptr);
        };
        fill_arg_detail(expr.children(0), first_arg_name, first_arg_type);
        fillArgumentDetail(actions, expr.children(1), arg_names, arg_types, arg_collators);
        fill_arg_detail(expr.children(2), third_arg_name, third_arg_type);
    }

    appendWindowDescription(
        arg_names,
        arg_types,
        arg_collators,
        window_func_name,
        window_description,
        source_columns,
        window_columns);
}

void DAGExpressionAnalyzer::buildCommonWindowFunc(
    const tipb::Expr & expr,
    const ExpressionActionsPtr & actions,
    const String & window_func_name,
    WindowDescription & window_description,
    NamesAndTypes & source_columns,
    NamesAndTypes & window_columns)
{
    auto child_size = expr.children_size();
    Names arg_names;
    DataTypes arg_types;
    TiDB::TiDBCollators arg_collators;
    for (Int32 i = 0; i < child_size; ++i)
    {
        fillArgumentDetail(actions, expr.children(i), arg_names, arg_types, arg_collators);
    }

    appendWindowDescription(
        arg_names,
        arg_types,
        arg_collators,
        window_func_name,
        window_description,
        source_columns,
        window_columns);
}

// This function will add new window function culumns to source_column
void DAGExpressionAnalyzer::appendWindowColumns(
    WindowDescription & window_description,
    const tipb::Window & window,
    const ExpressionActionsPtr & actions)
{
    RUNTIME_CHECK_MSG(window.func_desc_size() != 0, "window executor without agg/window expression.");
    RUNTIME_CHECK_MSG(isWindowFunctionsValid(window), "can not have window and agg functions together in one window.");

    NamesAndTypes window_columns;
    for (const tipb::Expr & expr : window.func_desc())
    {
        RUNTIME_CHECK_MSG(isWindowFunctionExpr(expr), "Now Window Operator only support window function.");
        if (expr.tp() == tipb::ExprType::Lead || expr.tp() == tipb::ExprType::Lag)
        {
            buildLeadLag(
                expr,
                actions,
                getWindowFunctionName(expr),
                window_description,
                source_columns,
                window_columns);
        }
        else
        {
            buildCommonWindowFunc(
                expr,
                actions,
                getWindowFunctionName(expr),
                window_description,
                source_columns,
                window_columns);
        }
    }
    window_description.add_columns = window_columns;
}

WindowDescription DAGExpressionAnalyzer::buildWindowDescription(const tipb::Window & window)
{
    ExpressionActionsChain chain;
    ExpressionActionsChain::Step & step = initAndGetLastStep(chain);
    appendSourceColumnsToRequireOutput(step);

    size_t source_size = getCurrentInputColumns().size();

    WindowDescription window_description = createAndInitWindowDesc(this, window);
    setOrderByColumnTypeAndDirectionForRangeFrame(window_description, step.actions, window);
    buildActionsBeforeWindow(this, window_description, chain, window);
    buildActionsAfterWindow(this, window_description, chain, window, source_size);

    return window_description;
}

String DAGExpressionAnalyzer::applyFunction(
    const String & func_name,
    const Names & arg_names,
    const ExpressionActionsPtr & actions,
    const TiDB::TiDBCollatorPtr & collator)
{
    String result_name = genFuncString(func_name, arg_names, {collator});
    if (actions->getSampleBlock().has(result_name))
        return result_name;
    const FunctionBuilderPtr & function_builder = FunctionFactory::instance().get(func_name, context);
    const ExpressionAction & action
        = ExpressionAction::applyFunction(function_builder, arg_names, result_name, collator);
    actions->add(action);
    return result_name;
}

String DAGExpressionAnalyzer::buildFilterColumn(
    const ExpressionActionsPtr & actions,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & conditions,
    bool null_as_false)
{
    String filter_column_name;
    if (conditions.size() == 1)
    {
        filter_column_name = getActions(conditions[0], actions, true);
        if (isColumnExpr(conditions[0])
            && (!exprHasValidFieldType(conditions[0])
                /// if the column is not UInt8 type, we already add some convert function to convert it ot UInt8 type
                || isUInt8Type(getDataTypeByFieldTypeForComputingLayer(conditions[0].field_type()))))
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
        for (const auto & condition : conditions)
            arg_names.push_back(getActions(condition, actions, true));
        // connect all the conditions by logical and
        String fun_name = "and";
        if (context.getSettingsRef().use_two_value_logic_op_for_top_filter && null_as_false)
            fun_name = "two_value_and";
        filter_column_name = applyFunction(fun_name, arg_names, actions, nullptr);
    }
    return filter_column_name;
}

std::tuple<ExpressionActionsPtr, String, ExpressionActionsPtr> DAGExpressionAnalyzer::buildPushDownFilter(
    const google::protobuf::RepeatedPtrField<tipb::Expr> & conditions,
    bool null_as_false)
{
    assert(!conditions.empty());

    ExpressionActionsChain chain;
    initChain(chain);
    String filter_column_name = appendWhere(chain, conditions, null_as_false);
    ExpressionActionsPtr before_where = chain.getLastActions();
    chain.addStep();

    // remove useless tmp column and keep the schema of local streams and remote streams the same.
    for (const auto & col : getCurrentInputColumns())
    {
        chain.getLastStep().required_output.push_back(col.name);
    }
    ExpressionActionsPtr project_after_where = chain.getLastActions();
    chain.finalize();
    chain.clear();

    RUNTIME_CHECK(!project_after_where->getActions().empty());
    return {before_where, filter_column_name, project_after_where};
}

String DAGExpressionAnalyzer::appendWhere(
    ExpressionActionsChain & chain,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & conditions,
    bool null_as_false)
{
    auto & last_step = initAndGetLastStep(chain);

    String filter_column_name = buildFilterColumn(last_step.actions, conditions, null_as_false);

    last_step.required_output.push_back(filter_column_name);
    return filter_column_name;
}

String DAGExpressionAnalyzer::convertToUInt8(const ExpressionActionsPtr & actions, const String & column_name)
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
    else if (checkDataTypeArray<DataTypeFloat32>(org_type.get()))
    {
        tipb::Expr const_expr = constructZeroVectorFloat32TiExpr();
        auto const_expr_name = getActions(const_expr, actions);
        return applyFunction("notEquals", {column_name, const_expr_name}, actions, nullptr);
    }
    throw TiFlashException(
        fmt::format("Filter on {} is not supported.", org_type->getName()),
        Errors::Coprocessor::Unimplemented);
}

NamesAndTypes DAGExpressionAnalyzer::buildWindowOrderColumns(const tipb::Sort & window_sort) const
{
    if (window_sort.byitems_size() == 0)
    {
        throw TiFlashException("window executor without order by exprs", Errors::Coprocessor::BadRequest);
    }
    NamesAndTypes order_columns;
    order_columns.reserve(window_sort.byitems_size());

    for (const tipb::ByItem & order_by : window_sort.byitems())
    {
        if (!isColumnExpr(order_by.expr()))
        {
            throw TiFlashException("must be column expr.", Errors::Coprocessor::BadRequest);
        }
        order_columns.emplace_back(getColumnNameAndTypeForColumnExpr(order_by.expr(), getCurrentInputColumns()));
    }
    return order_columns;
}

NamesAndTypes DAGExpressionAnalyzer::buildOrderColumns(
    const ExpressionActionsPtr & actions,
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

GroupingSets DAGExpressionAnalyzer::buildExpandGroupingColumns(
    const tipb::Expand & expand,
    const ExpressionActionsPtr & actions)
{
    GroupingSets group_sets_columns;
    std::map<String, bool> map_grouping_col;
    group_sets_columns.reserve(expand.grouping_sets().size());
    for (const auto & group_set : expand.grouping_sets())
    {
        GroupingSet group_set_columns;
        group_set_columns.reserve(group_set.grouping_exprs().size());
        for (const auto & group_exprs : group_set.grouping_exprs())
        {
            GroupingColumnNames group_exprs_columns;
            group_exprs_columns.reserve(group_exprs.grouping_expr().size());
            for (const auto & group_expr : group_exprs.grouping_expr())
            {
                String cp_name = getActions(group_expr, actions);
                // tidb expression computation is based on column index offset child's chunk schema, change to ck block column name here.
                group_exprs_columns.emplace_back(cp_name);
                map_grouping_col.insert(std::pair<String, bool>(cp_name, true));
            }
            // move here, cause basic string is copied from input cols.
            group_set_columns.emplace_back(std::move(group_exprs_columns));
        }
        group_sets_columns.emplace_back(std::move(group_set_columns));
    }
    // change the original source column to be nullable, and add a new column for groupingID.
    for (auto & mutable_one : source_columns)
    {
        if (map_grouping_col[mutable_one.name])
            mutable_one.type = makeNullable(mutable_one.type);
    }
    source_columns.emplace_back(Expand::grouping_identifier_column_name, Expand::grouping_identifier_column_type);
    return group_sets_columns;
}

const std::vector<NameAndTypePair> & DAGExpressionAnalyzer::getCurrentInputColumns() const
{
    return source_columns;
}

String DAGExpressionAnalyzer::appendTimeZoneCast(
    const String & tz_col,
    const String & ts_col,
    const String & func_name,
    const ExpressionActionsPtr & actions)
{
    String cast_expr_name = applyFunction(func_name, {ts_col, tz_col}, actions, nullptr);
    return cast_expr_name;
}

std::pair<bool, std::vector<String>> DAGExpressionAnalyzer::buildExtraCastsAfterTS(
    const ExpressionActionsPtr & actions,
    const std::vector<UInt8> & may_need_add_cast_column,
    const TiDB::ColumnInfos & table_scan_columns)
{
    bool has_cast = false;
    std::vector<String> casted_columns;
    casted_columns.reserve(may_need_add_cast_column.size());
    // For TimeZone
    tipb::Expr tz_expr = constructTZExpr(context.getTimezoneInfo());
    String tz_col = getActions(tz_expr, actions);
    static const String convert_time_zone_form_utc = "ConvertTimeZoneFromUTC";
    static const String convert_time_zone_by_offset = "ConvertTimeZoneByOffsetFromUTC";
    const String & timezone_func_name
        = context.getTimezoneInfo().is_name_based ? convert_time_zone_form_utc : convert_time_zone_by_offset;

    // For Duration
    String fsp_col;
    static const String dur_func_name = "FunctionConvertDurationFromNanos";
    for (size_t i = 0; i < may_need_add_cast_column.size(); ++i)
    {
        String casted_name = source_columns[i].name;
        if (!context.getTimezoneInfo().is_utc_timezone && may_need_add_cast_column[i]
            && table_scan_columns[i].tp == TiDB::TypeTimestamp)
        {
            casted_name = appendTimeZoneCast(tz_col, source_columns[i].name, timezone_func_name, actions);
            has_cast = true;
        }

        if (may_need_add_cast_column[i] && table_scan_columns[i].tp == TiDB::TypeTime)
        {
            if (table_scan_columns[i].decimal > 6)
                throw Exception("fsp must <= 6", ErrorCodes::LOGICAL_ERROR);
            const auto fsp = table_scan_columns[i].decimal < 0 ? 0 : table_scan_columns[i].decimal;
            tipb::Expr fsp_expr = constructInt64LiteralTiExpr(fsp);
            fsp_col = getActions(fsp_expr, actions);
            casted_name = appendDurationCast(fsp_col, source_columns[i].name, dur_func_name, actions);
            // We will replace the source_columns[i] with the casted column later
            // so we need to update the type of the source_column[i]
            source_columns[i].type = actions->getSampleBlock().getByName(casted_name).type;
            has_cast = true;
        }

        casted_columns.emplace_back(std::move(casted_name));
    }

    return {has_cast, casted_columns};
}

bool DAGExpressionAnalyzer::appendExtraCastsAfterTS(
    ExpressionActionsChain & chain,
    const std::vector<UInt8> & may_need_add_cast_column,
    const TiDBTableScan & table_scan)
{
    auto & step = initAndGetLastStep(chain);
    auto & actions = step.actions;

    auto [has_cast, casted_columns]
        = buildExtraCastsAfterTS(actions, may_need_add_cast_column, table_scan.getColumns());

    if (!has_cast)
        return false;

    // Add a projection to replace the original columns with the casted columns.
    // For example:
    // we have a block with columns (a int64, b float, c int64)
    // after the cast, the block will be (a int64, b float, c int64, casted_c MyDuration)
    // After this projection, the block will be (a int64, b float, c MyDuration)
    NamesWithAliases project_cols;
    for (size_t i = 0; i < may_need_add_cast_column.size(); ++i)
        project_cols.emplace_back(casted_columns[i], source_columns[i].name);
    actions->add(ExpressionAction::project(project_cols));

    for (auto & col : source_columns)
        step.required_output.push_back(col.name);

    return true;
}

String DAGExpressionAnalyzer::appendDurationCast(
    const String & fsp_expr,
    const String & dur_expr,
    const String & func_name,
    const ExpressionActionsPtr & actions)
{
    return applyFunction(func_name, {dur_expr, fsp_expr}, actions, nullptr);
}

std::tuple<bool, Names, Names> DAGExpressionAnalyzer::buildJoinKey(
    const ExpressionActionsPtr & actions,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & keys,
    const JoinKeyTypes & join_key_types)
{
    bool has_actions_of_keys = false;

    Names key_names;
    Names original_key_names;

    UniqueNameGenerator unique_name_generator;
    for (int i = 0; i < keys.size(); ++i)
    {
        const auto & key = keys.at(i);
        bool has_actions = key.tp() != tipb::ExprType::ColumnRef;

        String key_name = getActions(key, actions);
        original_key_names.push_back(key_name);

        DataTypePtr current_type = actions->getSampleBlock().getByName(key_name).type;
        const auto & join_key_type = join_key_types[i];
        if (!removeNullable(current_type)->equals(*removeNullable(join_key_type.key_type)))
        {
            /// need to convert to key type
            key_name = join_key_type.is_incompatible_decimal
                ? applyFunction("formatDecimal", {key_name}, actions, nullptr)
                : appendCast(join_key_type.key_type, actions, key_name);
            has_actions = true;
        }
        if (has_actions)
        {
            String updated_key_name = unique_name_generator.toUniqueName(key_name);
            /// duplicated key names, in Clickhouse join, it is assumed that here is no duplicated
            /// key names, so just copy a key with new name
            if (key_name != updated_key_name)
            {
                actions->add(ExpressionAction::copyColumn(key_name, updated_key_name));
                key_name = updated_key_name;
            }
        }
        key_names.push_back(key_name);
        has_actions_of_keys |= has_actions;
    }

    return {has_actions_of_keys, std::move(key_names), std::move(original_key_names)};
}

bool DAGExpressionAnalyzer::appendJoinKeyAndJoinFilters(
    ExpressionActionsChain & chain,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & keys,
    const JoinKeyTypes & join_key_types,
    Names & key_names,
    Names & original_key_names,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & filters,
    String & filter_column_name)
{
    initChain(chain);
    ExpressionActionsPtr actions = chain.getLastActions();

    bool ret = false;
    std::tie(ret, key_names, original_key_names) = buildJoinKey(actions, keys, join_key_types);

    if (!filters.empty())
    {
        ret = true;
        filter_column_name = appendWhere(chain, filters);
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
    /// PhysicalPlanNode we add column_prefix in
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

String DAGExpressionAnalyzer::appendNullAwareSemiJoinEqColumn(
    ExpressionActionsChain & chain,
    const Names & probe_key_names,
    const Names & build_key_names,
    const TiDB::TiDBCollators & collators)
{
    RUNTIME_ASSERT(probe_key_names.size() == build_key_names.size());
    if (probe_key_names.empty())
        return "";

    auto & last_step = initAndGetLastStep(chain);

    Names and_arg_names(probe_key_names.size());
    for (size_t i = 0; i < probe_key_names.size(); ++i)
    {
        Names arg_names;
        arg_names.emplace_back(probe_key_names[i]);
        arg_names.emplace_back(build_key_names[i]);
        const TiDB::TiDBCollatorPtr & collator = i < collators.size() ? collators[i] : nullptr;
        and_arg_names[i] = applyFunction("equals", arg_names, last_step.actions, collator);
    }
    return and_arg_names.size() == 1 ? and_arg_names[0]
                                     : applyFunction("and", and_arg_names, last_step.actions, nullptr);
}

void DAGExpressionAnalyzer::appendRuntimeFilterProperties(RuntimeFilterPtr & runtime_filter)
{
    NameAndTypePair name_and_type;
    name_and_type = getColumnNameAndTypeForColumnExpr(runtime_filter->getSourceExpr(), getCurrentInputColumns());
    runtime_filter->setSourceColumnName(name_and_type.name);
    Block header;
    std::shared_ptr<Set> in_values_set;
    const auto & settings = context.getSettingsRef();
    switch (runtime_filter->getRFType())
    {
    case tipb::IN:
        in_values_set = std::make_shared<Set>(
            SizeLimits(settings.rf_max_in_value_set, settings.max_bytes_in_set, settings.set_overflow_mode),
            TiDB::TiDBCollators{getCollatorFromExpr(runtime_filter->getSourceExpr())});
        header.insert(ColumnWithTypeAndName(name_and_type.type->createColumn(), name_and_type.type, "_" + toString(1)));
        in_values_set->setHeader(header);
        runtime_filter->setINValuesSet(in_values_set);
        runtime_filter->setTimezoneInfo(context.getTimezoneInfo());
        break;
    case tipb::MIN_MAX:
    case tipb::BLOOM_FILTER:
        // todo
        break;
    }
}

void DAGExpressionAnalyzer::appendCastAfterWindow(
    const ExpressionActionsPtr & actions,
    const tipb::Window & window,
    size_t window_columns_start_index)
{
    bool need_update_source_columns = false;
    NamesAndTypes updated_window_columns;

    auto update_cast_column = [&](const tipb::Expr & expr, const NameAndTypePair & origin_column) {
        String updated_name = appendCastForFunctionExpr(expr, actions, origin_column.name);
        if (origin_column.name != updated_name)
        {
            DataTypePtr type = actions->getSampleBlock().getByName(updated_name).type;
            updated_window_columns.emplace_back(updated_name, type);
            need_update_source_columns = true;
        }
        else
        {
            updated_window_columns.emplace_back(origin_column.name, origin_column.type);
        }
    };

    for (size_t i = 0; i < window_columns_start_index; ++i)
    {
        updated_window_columns.emplace_back(source_columns[i]);
    }

    assert(window.func_desc_size() + window_columns_start_index == source_columns.size());
    for (Int32 i = 0; i < window.func_desc_size(); ++i)
    {
        update_cast_column(window.func_desc(i), source_columns[window_columns_start_index + i]);
    }

    if (need_update_source_columns)
    {
        std::swap(source_columns, updated_window_columns);
    }
}

void DAGExpressionAnalyzer::appendCastAfterAgg(
    const ExpressionActionsPtr & actions,
    const tipb::Aggregation & aggregation)
{
    bool need_update_source_columns = false;
    std::vector<NameAndTypePair> updated_aggregated_columns;

    auto update_cast_column = [&](const tipb::Expr & expr, const NameAndTypePair & origin_column) {
        String updated_name = appendCastForFunctionExpr(expr, actions, origin_column.name);
        if (origin_column.name != updated_name)
        {
            DataTypePtr type = actions->getSampleBlock().getByName(updated_name).type;
            updated_aggregated_columns.emplace_back(updated_name, type);
            need_update_source_columns = true;
        }
        else
        {
            updated_aggregated_columns.emplace_back(origin_column.name, origin_column.type);
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

// TODO: Maybe no need to append copy column action, just use same column name to reference column.
ExpressionActionsPtr DAGExpressionAnalyzer::appendCopyColumnAfterAgg(
    const NamesAndTypes & aggregated_columns,
    const KeyRefAggFuncMap & key_ref_agg_func,
    const AggFuncRefKeyMap & agg_func_ref_key)
{
    // aggregated_columns.size() == key_ref_agg_func.size() + agg_func_ref_key.size() happens when:
    // select group_concat(distinct c1) from t where id = 0;
    // The first stage of HashAgg will only has one group by expr and no agg func expr.
    // So aggregated_columns is [first_row(c1)], key_ref_agg_func is [c1->first_row(c1)], agg_func_ref_key is empty.
    RUNTIME_CHECK(aggregated_columns.size() >= key_ref_agg_func.size() + agg_func_ref_key.size());
    auto actual_agg_output_col_cnt = aggregated_columns.size() - key_ref_agg_func.size() - agg_func_ref_key.size();
    NamesAndTypes agg_output_columns;
    agg_output_columns.reserve(actual_agg_output_col_cnt);
    for (const auto & col : aggregated_columns)
    {
        const bool found_in_key_ref_agg_func = key_ref_agg_func.find(col.name) != key_ref_agg_func.end();
        const bool found_in_agg_func_ref_key = agg_func_ref_key.find(col.name) != agg_func_ref_key.end();

        // Agg col cannot exists both in key_ref_agg_func and agg_func_ref_key.
        // Because key_ref_agg_func is only for column with collator,
        // and agg_func_ref_key is only for column without collator.
        RUNTIME_CHECK(!(found_in_key_ref_agg_func && found_in_agg_func_ref_key));

        // If agg col doesn't exist neither, it means Aggregator should output it.
        // Otherwise this col should reference some column of the output column of Aggregator.
        if (!found_in_key_ref_agg_func && !found_in_agg_func_ref_key)
        {
            agg_output_columns.push_back(col);
        }
    }
    RUNTIME_CHECK(!agg_output_columns.empty());

    auto expr_after_agg_actions = PhysicalPlanHelper::newActions(agg_output_columns);
    for (const auto & pair : key_ref_agg_func)
    {
        const auto & key_name = pair.first;
        const auto & agg_func_name = pair.second;
        expr_after_agg_actions->add(ExpressionAction::copyColumn(agg_func_name, key_name));
    }

    for (const auto & pair : agg_func_ref_key)
    {
        const auto & agg_func_name = pair.first;
        const auto & key_name = pair.second;
        expr_after_agg_actions->add(ExpressionAction::copyColumn(key_name, agg_func_name));
    }
    reset(aggregated_columns);
    return expr_after_agg_actions;
}

NamesWithAliases DAGExpressionAnalyzer::genNonRootFinalProjectAliases(const String & column_prefix) const
{
    NamesWithAliases final_project_aliases;
    UniqueNameGenerator unique_name_generator;
    for (const auto & element : getCurrentInputColumns())
        final_project_aliases.emplace_back(
            element.name,
            unique_name_generator.toUniqueName(column_prefix + element.name));
    return final_project_aliases;
}

NamesWithAliases DAGExpressionAnalyzer::genRootFinalProjectAliases(
    const String & column_prefix,
    const std::vector<Int32> & output_offsets) const
{
    NamesWithAliases final_project_aliases;
    const auto & current_columns = getCurrentInputColumns();
    UniqueNameGenerator unique_name_generator;
    for (auto i : output_offsets)
    {
        final_project_aliases.emplace_back(
            current_columns[i].name,
            unique_name_generator.toUniqueName(column_prefix + current_columns[i].name));
    }
    return final_project_aliases;
}

void DAGExpressionAnalyzer::appendCastForRootFinalProjection(
    const ExpressionActionsPtr & actions,
    const std::vector<tipb::FieldType> & require_schema,
    const std::vector<Int32> & output_offsets,
    bool need_append_timezone_cast,
    const BoolVec & need_append_type_cast_vec)
{
    tipb::Expr tz_expr = constructTZExpr(context.getTimezoneInfo());
    String tz_col;
    String tz_cast_func_name
        = context.getTimezoneInfo().is_name_based ? "ConvertTimeZoneToUTC" : "ConvertTimeZoneByOffsetToUTC";

    const auto & current_columns = getCurrentInputColumns();
    NamesAndTypes after_cast_columns = current_columns;

    for (size_t index = 0; index < output_offsets.size(); ++index)
    {
        UInt32 offset = output_offsets[index];
        assert(offset < current_columns.size());
        assert(offset < require_schema.size());
        assert(offset < after_cast_columns.size());

        /// for all the columns that need to be returned, if the type is timestamp, then convert
        /// the timestamp column to UTC based, refer to appendTimeZoneCastsAfterTS for more details
        if ((need_append_timezone_cast && require_schema[offset].tp() == TiDB::TypeTimestamp)
            || need_append_type_cast_vec[index])
        {
            const String & origin_column_name = current_columns[offset].name;
            String updated_name = origin_column_name;
            auto updated_type = current_columns[offset].type;
            /// first add timestamp cast
            if (need_append_timezone_cast && require_schema[offset].tp() == TiDB::TypeTimestamp)
            {
                if (tz_col.empty())
                    tz_col = getActions(tz_expr, actions);
                updated_name = appendTimeZoneCast(tz_col, updated_name, tz_cast_func_name, actions);
            }
            /// then add type cast
            if (need_append_type_cast_vec[index])
            {
                updated_type = getDataTypeByFieldTypeForComputingLayer(require_schema[offset]);
                updated_name = appendCast(updated_type, actions, updated_name);
            }
            after_cast_columns[offset].name = updated_name;
            after_cast_columns[offset].type = updated_type;
        }
    }

    source_columns = std::move(after_cast_columns);
}

std::pair<bool, BoolVec> DAGExpressionAnalyzer::isCastRequiredForRootFinalProjection(
    const std::vector<tipb::FieldType> & require_schema,
    const std::vector<Int32> & output_offsets) const
{
    /// TiDB can not guarantee that the field type in DAG request is accurate, so in order to make things work,
    /// TiFlash will append extra type cast if needed.
    const auto & current_columns = getCurrentInputColumns();
    bool need_append_type_cast = false;
    BoolVec need_append_type_cast_vec;
    /// we need to append type cast for root final projection if necessary
    for (UInt32 i : output_offsets)
    {
        const auto & actual_type = current_columns[i].type;
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
    return std::make_pair(need_append_type_cast, std::move(need_append_type_cast_vec));
}

NamesWithAliases DAGExpressionAnalyzer::buildFinalProjection(
    const ExpressionActionsPtr & actions,
    const std::vector<tipb::FieldType> & schema,
    const std::vector<Int32> & output_offsets,
    const String & column_prefix,
    bool keep_session_timezone_info)
{
    if (unlikely(output_offsets.empty()))
        throw Exception("DAGRequest without output_offsets", ErrorCodes::LOGICAL_ERROR);

    bool need_append_timezone_cast = !keep_session_timezone_info && !context.getTimezoneInfo().is_utc_timezone;
    auto [need_append_type_cast, need_append_type_cast_vec]
        = isCastRequiredForRootFinalProjection(schema, output_offsets);
    assert(need_append_type_cast_vec.size() == output_offsets.size());

    if (need_append_timezone_cast || need_append_type_cast)
    {
        // after appendCastForRootFinalProjection, source_columns has been modified.
        appendCastForRootFinalProjection(
            actions,
            schema,
            output_offsets,
            need_append_timezone_cast,
            need_append_type_cast_vec);
    }

    // generate project aliases from source_columns.
    return genRootFinalProjectAliases(column_prefix, output_offsets);
}

String DAGExpressionAnalyzer::alignReturnType(
    const tipb::Expr & expr,
    const ExpressionActionsPtr & actions,
    const String & expr_name,
    bool force_uint8)
{
    DataTypePtr orig_type = actions->getSampleBlock().getByName(expr_name).type;
    if (force_uint8 && isUInt8Type(orig_type))
        return expr_name;
    String updated_name = appendCastForFunctionExpr(expr, actions, expr_name);
    DataTypePtr updated_type = actions->getSampleBlock().getByName(updated_name).type;
    if (force_uint8 && !isUInt8Type(updated_type))
        updated_name = convertToUInt8(actions, updated_name);
    return updated_name;
}

void DAGExpressionAnalyzer::initChain(ExpressionActionsChain & chain) const
{
    if (chain.steps.empty())
    {
        const auto & columns = getCurrentInputColumns();
        std::unordered_set<String> column_name_set;
        NamesAndTypesList column_list;
        for (const auto & col : columns)
        {
            if (column_name_set.find(col.name) == column_name_set.end())
            {
                column_list.emplace_back(col.name, col.type);
                column_name_set.emplace(col.name);
            }
        }
        chain.steps.emplace_back(std::make_shared<ExpressionActions>(column_list));
    }
}

String DAGExpressionAnalyzer::appendCast(
    const DataTypePtr & target_type,
    const ExpressionActionsPtr & actions,
    const String & expr_name)
{
    // need to add cast function
    // first construct the second argument
    tipb::Expr type_expr = constructStringLiteralTiExpr(target_type->getName());
    auto type_expr_name = getActions(type_expr, actions);
    String cast_expr_name = applyFunction("CAST", {expr_name, type_expr_name}, actions, nullptr);
    return cast_expr_name;
}

String DAGExpressionAnalyzer::appendCastForFunctionExpr(
    const tipb::Expr & expr,
    const ExpressionActionsPtr & actions,
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
        if (expected_type->equals(*actual_type))
            return expr_name;
        if (expected_type->isNullable() && !actual_type->isNullable())
        {
            /// if TiDB require nullable type while TiFlash get not null type
            /// don't add convert if the nested type is the same since just
            /// convert the type to nullable is meaningless and will affect
            /// the performance of TiFlash
            if (removeNullable(expected_type)->equals(*actual_type))
            {
                LOG_TRACE(
                    context.getDAGContext()->log,
                    "Skip implicit cast for column {}, expected type {}, actual type {}",
                    expr_name,
                    expected_type->getName(),
                    actual_type->getName());
                return expr_name;
            }
        }
        LOG_TRACE(
            context.getDAGContext()->log,
            "Add implicit cast for column {}, expected type {}, actual type {}",
            expr_name,
            expected_type->getName(),
            actual_type->getName());
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
    const auto & settings = context.getSettingsRef();
    SetPtr set = std::make_shared<Set>(
        SizeLimits(settings.max_rows_in_set, settings.max_bytes_in_set, settings.set_overflow_mode),
        TiDB::TiDBCollators{getCollatorFromExpr(expr)});

    auto remaining_exprs = set->createFromDAGExpr(set_element_types, expr, create_ordered_set);
    prepared_sets[&expr] = std::make_shared<DAGSet>(std::move(set), std::move(remaining_exprs));
}

String DAGExpressionAnalyzer::getActions(
    const tipb::Expr & expr,
    const ExpressionActionsPtr & actions,
    bool output_as_uint8_type)
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
            column.column
                = target_type->createColumnConst(1, convertFieldToType(value, *target_type, flash_type.get()));
            column.name = ret;
            column.type = target_type;
            actions->add(ExpressionAction::addColumn(column));
        }
        if (expr.field_type().tp() == TiDB::TypeTimestamp && !context.getTimezoneInfo().is_utc_timezone)
        {
            /// append timezone cast for timestamp literal
            tipb::Expr tz_expr = constructTZExpr(context.getTimezoneInfo());
            String func_name
                = context.getTimezoneInfo().is_name_based ? "ConvertTimeZoneFromUTC" : "ConvertTimeZoneByOffsetFromUTC";
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
        ret = DAGExpressionAnalyzerHelper::buildFunction(this, expr, actions);
    }
    else
    {
        throw TiFlashException(
            fmt::format("Unsupported expr type: {}", getTypeName(expr)),
            Errors::Coprocessor::Unimplemented);
    }

    ret = alignReturnType(expr, actions, ret, output_as_uint8_type);
    return ret;
}

String DAGExpressionAnalyzer::buildTupleFunctionForGroupConcat(
    const tipb::Expr & expr,
    SortDescription & sort_desc,
    NamesAndTypes & names_and_types,
    TiDB::TiDBCollators & collators,
    const ExpressionActionsPtr & actions)
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

    NamesAndTypes order_columns;
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

} // namespace DB
