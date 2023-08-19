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

#include <Common/TiFlashException.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/getLeastSupertype.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/JoinInterpreterHelper.h>
#include <Interpreters/Context.h>
#include <Interpreters/Join.h>
#include <Storages/Transaction/TypeMapping.h>
#include <fmt/format.h>

#include <unordered_map>

namespace DB
{
namespace ErrorCodes
{
extern const int NO_COMMON_TYPE;
} // namespace ErrorCodes

namespace JoinInterpreterHelper
{
namespace
{
std::pair<ASTTableJoin::Kind, size_t> getJoinKindAndBuildSideIndex(const tipb::Join & join)
{
    static const std::unordered_map<tipb::JoinType, ASTTableJoin::Kind> equal_join_type_map{
        {tipb::JoinType::TypeInnerJoin, ASTTableJoin::Kind::Inner},
        {tipb::JoinType::TypeLeftOuterJoin, ASTTableJoin::Kind::Left},
        {tipb::JoinType::TypeRightOuterJoin, ASTTableJoin::Kind::Right},
        {tipb::JoinType::TypeSemiJoin, ASTTableJoin::Kind::Inner},
        {tipb::JoinType::TypeAntiSemiJoin, ASTTableJoin::Kind::Anti},
        {tipb::JoinType::TypeLeftOuterSemiJoin, ASTTableJoin::Kind::LeftSemi},
        {tipb::JoinType::TypeAntiLeftOuterSemiJoin, ASTTableJoin::Kind::LeftAnti}};
    static const std::unordered_map<tipb::JoinType, ASTTableJoin::Kind> cartesian_join_type_map{
        {tipb::JoinType::TypeInnerJoin, ASTTableJoin::Kind::Cross},
        {tipb::JoinType::TypeLeftOuterJoin, ASTTableJoin::Kind::Cross_Left},
        {tipb::JoinType::TypeRightOuterJoin, ASTTableJoin::Kind::Cross_Right},
        {tipb::JoinType::TypeSemiJoin, ASTTableJoin::Kind::Cross},
        {tipb::JoinType::TypeAntiSemiJoin, ASTTableJoin::Kind::Cross_Anti},
        {tipb::JoinType::TypeLeftOuterSemiJoin, ASTTableJoin::Kind::Cross_LeftSemi},
        {tipb::JoinType::TypeAntiLeftOuterSemiJoin, ASTTableJoin::Kind::Cross_LeftAnti}};

    const auto & join_type_map = join.left_join_keys_size() == 0 ? cartesian_join_type_map : equal_join_type_map;
    auto join_type_it = join_type_map.find(join.join_type());
    if (unlikely(join_type_it == join_type_map.end()))
        throw TiFlashException("Unknown join type in dag request", Errors::Coprocessor::BadRequest);

    ASTTableJoin::Kind kind = join_type_it->second;

    /// in DAG request, inner part is the build side, however for TiFlash implementation,
    /// the build side must be the right side, so need to swap the join side if needed
    /// 1. for (cross) inner join, there is no problem in this swap.
    /// 2. for (cross) semi/anti-semi join, the build side is always right, needn't swap.
    /// 3. for non-cross left/right join, there is no problem in this swap.
    /// 4. for cross left join, the build side is always right, needn't and can't swap.
    /// 5. for cross right join, the build side is always left, so it will always swap and change to cross left join.
    /// note that whatever the build side is, we can't support cross-right join now.

    size_t build_side_index = 0;
    switch (kind)
    {
    case ASTTableJoin::Kind::Cross_Right:
        build_side_index = 0;
        break;
    case ASTTableJoin::Kind::Cross_Left:
        build_side_index = 1;
        break;
    default:
        build_side_index = join.inner_idx();
    }
    assert(build_side_index == 0 || build_side_index == 1);

    // should swap join side.
    if (build_side_index != 1)
    {
        switch (kind)
        {
        case ASTTableJoin::Kind::Left:
            kind = ASTTableJoin::Kind::Right;
            break;
        case ASTTableJoin::Kind::Right:
            kind = ASTTableJoin::Kind::Left;
            break;
        case ASTTableJoin::Kind::Cross_Right:
            kind = ASTTableJoin::Kind::Cross_Left;
        default:; // just `default`, for other kinds, don't need to change kind.
        }
    }

    return {kind, build_side_index};
}

JoinKeyType geCommonTypeForJoinOn(const DataTypePtr & left_type, const DataTypePtr & right_type)
{
    try
    {
        return {getLeastSupertype({left_type, right_type}), false};
    }
    catch (DB::Exception & e)
    {
        if (e.code() == ErrorCodes::NO_COMMON_TYPE
            && removeNullable(left_type)->isDecimal()
            && removeNullable(right_type)->isDecimal())
        {
            // fix https://github.com/pingcap/tiflash/issues/4519
            // String is the common type for all types, it is always safe to choose String.
            // But then we need to use `FunctionFormatDecimal` to format decimal.
            // For example 0.1000000000 is equal to 0.10000000000000000000, but the original strings are not equal.
            RUNTIME_ASSERT(!left_type->onlyNull() || !right_type->onlyNull());
            auto fall_back_type = std::make_shared<DataTypeString>();
            bool make_nullable = left_type->isNullable() || right_type->isNullable();
            return {make_nullable ? makeNullable(fall_back_type) : fall_back_type, true};
        }
        else
        {
            throw;
        }
    }
}

JoinKeyTypes getJoinKeyTypes(const tipb::Join & join)
{
    if (unlikely(join.left_join_keys_size() != join.right_join_keys_size()))
        throw TiFlashException("size of join.left_join_keys != size of join.right_join_keys", Errors::Coprocessor::BadRequest);
    JoinKeyTypes join_key_types;
    for (int i = 0; i < join.left_join_keys_size(); ++i)
    {
        if (unlikely(!exprHasValidFieldType(join.left_join_keys(i)) || !exprHasValidFieldType(join.right_join_keys(i))))
            throw TiFlashException("Join key without field type", Errors::Coprocessor::BadRequest);
        auto left_type = getDataTypeByFieldTypeForComputingLayer(join.left_join_keys(i).field_type());
        auto right_type = getDataTypeByFieldTypeForComputingLayer(join.right_join_keys(i).field_type());
        join_key_types.emplace_back(geCommonTypeForJoinOn(left_type, right_type));
    }
    return join_key_types;
}

TiDB::TiDBCollators getJoinKeyCollators(const tipb::Join & join, const JoinKeyTypes & join_key_types)
{
    TiDB::TiDBCollators collators;
    size_t join_key_size = join_key_types.size();
    if (join.probe_types_size() == static_cast<int>(join_key_size) && join.build_types_size() == join.probe_types_size())
        for (size_t i = 0; i < join_key_size; ++i)
        {
            // Don't need to check the collate for decimal format string.
            if (removeNullable(join_key_types[i].key_type)->isString() && !join_key_types[i].is_incompatible_decimal)
            {
                if (unlikely(join.probe_types(i).collate() != join.build_types(i).collate()))
                    throw TiFlashException("Join with different collators on the join key", Errors::Coprocessor::BadRequest);
                collators.push_back(getCollatorFromFieldType(join.probe_types(i)));
            }
            else
                collators.push_back(nullptr);
        }
    return collators;
}

std::tuple<ExpressionActionsPtr, String, String> doGenJoinOtherConditionAction(
    const Context & context,
    const tipb::Join & join,
    const NamesAndTypes & source_columns)
{
    if (join.other_conditions_size() == 0 && join.other_eq_conditions_from_in_size() == 0)
        return {nullptr, "", ""};

    DAGExpressionAnalyzer dag_analyzer(source_columns, context);
    ExpressionActionsChain chain;

    String filter_column_for_other_condition;
    if (join.other_conditions_size() > 0)
    {
        std::vector<const tipb::Expr *> condition_vector;
        for (const auto & c : join.other_conditions())
        {
            condition_vector.push_back(&c);
        }
        filter_column_for_other_condition = dag_analyzer.appendWhere(chain, condition_vector);
    }

    String filter_column_for_other_eq_condition;
    if (join.other_eq_conditions_from_in_size() > 0)
    {
        std::vector<const tipb::Expr *> condition_vector;
        for (const auto & c : join.other_eq_conditions_from_in())
        {
            condition_vector.push_back(&c);
        }
        filter_column_for_other_eq_condition = dag_analyzer.appendWhere(chain, condition_vector);
    }

    return {chain.getLastActions(), std::move(filter_column_for_other_condition), std::move(filter_column_for_other_eq_condition)};
}
} // namespace

TiFlashJoin::TiFlashJoin(const tipb::Join & join_) // NOLINT(cppcoreguidelines-pro-type-member-init)
    : join(join_)
    , join_key_types(getJoinKeyTypes(join_))
    , join_key_collators(getJoinKeyCollators(join_, join_key_types))
{
    std::tie(kind, build_side_index) = getJoinKindAndBuildSideIndex(join);
    strictness = isSemiJoin() ? ASTTableJoin::Strictness::Any : ASTTableJoin::Strictness::All;
}

String TiFlashJoin::genMatchHelperName(const Block & header1, const Block & header2) const
{
    if (!isLeftSemiFamily())
    {
        return "";
    }

    size_t i = 0;
    String match_helper_name = fmt::format("{}{}", Join::match_helper_prefix, i);
    while (header1.has(match_helper_name) || header2.has(match_helper_name))
    {
        match_helper_name = fmt::format("{}{}", Join::match_helper_prefix, ++i);
    }
    return match_helper_name;
}

NamesAndTypes TiFlashJoin::genColumnsForOtherJoinFilter(
    const Block & left_input_header,
    const Block & right_input_header,
    const ExpressionActionsPtr & probe_prepare_join_actions) const
{
#ifndef NDEBUG
    auto is_prepare_actions_valid = [](const Block & origin_block, const ExpressionActionsPtr & prepare_actions) {
        const Block & prepare_sample_block = prepare_actions->getSampleBlock();
        for (const auto & p : origin_block)
        {
            if (!prepare_sample_block.has(p.name))
                return false;
        }
        return true;
    };
    if (unlikely(!is_prepare_actions_valid(build_side_index == 1 ? left_input_header : right_input_header, probe_prepare_join_actions)))
    {
        throw TiFlashException("probe_prepare_join_actions isn't valid", Errors::Coprocessor::Internal);
    }
#endif

    /// columns_for_other_join_filter is a vector of columns used
    /// as the input columns when compiling other join filter.
    /// Note the order in the column vector is very important:
    /// first the columns in left_input_header, then followed
    /// by the columns in right_input_header, if there are other
    /// columns generated before compile other join filter, then
    /// append the extra columns afterwards. In order to figure out
    /// whether a given column is already in the column vector or
    /// not quickly, we use another set to store the column names.

    /// The order of columns must be {left_input, right_input, extra columns},
    /// because tidb requires the input schema of join to be {left_input, right_input}.
    /// Extra columns are appended to prevent extra columns from being repeatedly generated.

    NamesAndTypes columns_for_other_join_filter;
    std::unordered_set<String> column_set_for_origin_columns;

    auto append_origin_columns = [&columns_for_other_join_filter, &column_set_for_origin_columns](const Block & header, bool make_nullable) {
        for (const auto & p : header)
        {
            columns_for_other_join_filter.emplace_back(p.name, make_nullable ? makeNullable(p.type) : p.type);
            column_set_for_origin_columns.emplace(p.name);
        }
    };
    append_origin_columns(left_input_header, join.join_type() == tipb::JoinType::TypeRightOuterJoin);
    append_origin_columns(right_input_header, join.join_type() == tipb::JoinType::TypeLeftOuterJoin);

    /// append the columns generated by probe side prepare join actions.
    /// the new columns are
    /// - filter_column and related temporary columns
    /// - join keys and related temporary columns
    auto append_new_columns = [&columns_for_other_join_filter, &column_set_for_origin_columns](const Block & header, bool make_nullable) {
        for (const auto & p : header)
        {
            if (column_set_for_origin_columns.find(p.name) == column_set_for_origin_columns.end())
                columns_for_other_join_filter.emplace_back(p.name, make_nullable ? makeNullable(p.type) : p.type);
        }
    };
    bool make_nullable = build_side_index == 1
        ? join.join_type() == tipb::JoinType::TypeRightOuterJoin
        : join.join_type() == tipb::JoinType::TypeLeftOuterJoin;
    append_new_columns(probe_prepare_join_actions->getSampleBlock(), make_nullable);

    return columns_for_other_join_filter;
}

/// all the columns from build side streams should be added after join, even for the join key.
NamesAndTypesList TiFlashJoin::genColumnsAddedByJoin(
    const Block & build_side_header,
    const String & match_helper_name) const
{
    NamesAndTypesList columns_added_by_join;
    bool make_nullable = isTiFlashLeftJoin();
    for (auto const & p : build_side_header)
    {
        columns_added_by_join.emplace_back(p.name, make_nullable ? makeNullable(p.type) : p.type);
    }
    if (!match_helper_name.empty())
    {
        columns_added_by_join.emplace_back(match_helper_name, Join::match_helper_type);
    }
    return columns_added_by_join;
}

NamesAndTypes TiFlashJoin::genJoinOutputColumns(
    const Block & left_input_header,
    const Block & right_input_header,
    const String & match_helper_name) const
{
    NamesAndTypes join_output_columns;
    auto append_output_columns = [&join_output_columns](const Block & header, bool make_nullable) {
        for (auto const & p : header)
        {
            join_output_columns.emplace_back(p.name, make_nullable ? makeNullable(p.type) : p.type);
        }
    };

    append_output_columns(left_input_header, join.join_type() == tipb::JoinType::TypeRightOuterJoin);
    if (!isSemiJoin())
    {
        /// for semi join, the columns from right table will be ignored
        append_output_columns(right_input_header, join.join_type() == tipb::JoinType::TypeLeftOuterJoin);
    }

    if (!match_helper_name.empty())
    {
        join_output_columns.emplace_back(match_helper_name, Join::match_helper_type);
    }

    return join_output_columns;
}

std::tuple<ExpressionActionsPtr, String, String> TiFlashJoin::genJoinOtherConditionAction(
    const Context & context,
    const Block & left_input_header,
    const Block & right_input_header,
    const ExpressionActionsPtr & probe_side_prepare_join) const
{
    auto columns_for_other_join_filter
        = genColumnsForOtherJoinFilter(
            left_input_header,
            right_input_header,
            probe_side_prepare_join);

    return doGenJoinOtherConditionAction(context, join, columns_for_other_join_filter);
}

std::tuple<ExpressionActionsPtr, Names, String> prepareJoin(
    const Context & context,
    const Block & input_header,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & keys,
    const JoinKeyTypes & join_key_types,
    bool left,
    bool is_right_out_join,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & filters)
{
    NamesAndTypes source_columns;
    for (auto const & p : input_header)
        source_columns.emplace_back(p.name, p.type);
    DAGExpressionAnalyzer dag_analyzer(std::move(source_columns), context);
    ExpressionActionsChain chain;
    Names key_names;
    String filter_column_name;
    dag_analyzer.appendJoinKeyAndJoinFilters(chain, keys, join_key_types, key_names, left, is_right_out_join, filters, filter_column_name);
    return {chain.getLastActions(), std::move(key_names), std::move(filter_column_name)};
}
} // namespace JoinInterpreterHelper
} // namespace DB
