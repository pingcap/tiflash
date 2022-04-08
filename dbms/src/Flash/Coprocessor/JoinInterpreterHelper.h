// Copyright 2022 PingCAP, Ltd.
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

#pragma once

#include <Common/FailPoint.h>
#include <Common/FmtUtils.h>
#include <Common/TiFlashException.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/getLeastSupertype.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Interpreters/Context.h>
#include <Interpreters/Join.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Storages/Transaction/Collator.h>
#include <Storages/Transaction/TypeMapping.h>
#include <tipb/executor.pb.h>

#include <tuple>

namespace DB
{
namespace FailPoints
{
extern const char pause_after_copr_streams_acquired[];
extern const char minimum_block_size_for_cross_join[];
} // namespace FailPoints

namespace JoinInterpreterHelper
{
bool isLeftSemiFamily(const tipb::Join & join)
{
    return join.join_type() == tipb::JoinType::TypeLeftOuterSemiJoin || join.join_type() == tipb::JoinType::TypeAntiLeftOuterSemiJoin;
}

bool isSemiJoin(const tipb::Join & join)
{
    return join.join_type() == tipb::JoinType::TypeSemiJoin || join.join_type() == tipb::JoinType::TypeAntiSemiJoin || isLeftSemiFamily(join);
}

bool isTiflashLeftJoin(const ASTTableJoin::Kind & kind)
{
    return kind == ASTTableJoin::Kind::Left || kind == ASTTableJoin::Kind::Cross_Left;
}

bool isTiflashRightJoin(const ASTTableJoin::Kind & kind)
{
    return kind == ASTTableJoin::Kind::Right;
}

// ASTTableJoin::Kind, build_side_index
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
    if (join_type_it == join_type_map.end())
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
    if (kind == ASTTableJoin::Kind::Cross_Right)
        build_side_index = 0;
    else if (kind == ASTTableJoin::Kind::Cross_Left)
        build_side_index = 1;
    else
    {
        build_side_index = join.inner_idx();
        if (build_side_index == 0)
        {
            if (kind == ASTTableJoin::Kind::Left)
                kind = ASTTableJoin::Kind::Right;
            else if (kind == ASTTableJoin::Kind::Right)
                kind = ASTTableJoin::Kind::Left;
            else if (kind == ASTTableJoin::Kind::Cross_Right)
                kind = ASTTableJoin::Kind::Cross_Left;
        }
    }

    return {kind, build_side_index};
}

String genMatcherNameForLeftSemiFamily(const Block & left_header, const Block & right_header)
{
    size_t i = 0;
    String match_helper_name = fmt::format("{}{}", Join::match_helper_prefix, i);
    while (left_header.has(match_helper_name) || right_header.has(match_helper_name))
    {
        match_helper_name = fmt::format("{}{}", Join::match_helper_prefix, ++i);
    }
    return match_helper_name;
}

/// ClickHouse require join key to be exactly the same type
/// TiDB only require the join key to be the same category
/// for example decimal(10,2) join decimal(20,0) is allowed in
/// TiDB and will throw exception in ClickHouse
DataTypes getJoinKeyTypes(const tipb::Join & join)
{
    DataTypes key_types;
    for (int i = 0; i < join.left_join_keys().size(); ++i)
    {
        if (!exprHasValidFieldType(join.left_join_keys(i)) || !exprHasValidFieldType(join.right_join_keys(i)))
            throw TiFlashException("Join key without field type", Errors::Coprocessor::BadRequest);
        DataTypes types;
        types.emplace_back(getDataTypeByFieldTypeForComputingLayer(join.left_join_keys(i).field_type()));
        types.emplace_back(getDataTypeByFieldTypeForComputingLayer(join.right_join_keys(i).field_type()));
        DataTypePtr common_type = getLeastSupertype(types);
        key_types.emplace_back(common_type);
    }
    return key_types;
}

TiDB::TiDBCollators getJoinKeyCollators(const tipb::Join & join, const DataTypes & key_types)
{
    TiDB::TiDBCollators collators;
    size_t key_size = key_types.size();
    if (join.probe_types_size() == static_cast<int>(key_size) && join.build_types_size() == join.probe_types_size())
    {
        for (size_t i = 0; i < key_size; ++i)
        {
            if (removeNullable(key_types[i])->isString())
            {
                if (join.probe_types(i).collate() != join.build_types(i).collate())
                    throw TiFlashException("Join with different collators on the join key", Errors::Coprocessor::BadRequest);
                collators.push_back(getCollatorFromFieldType(join.probe_types(i)));
            }
            else
            {
                collators.push_back(nullptr);
            }
        }
    }
    return collators;
}

ExpressionActionsPtr genJoinOtherConditionAction(
    const Context & context,
    const tipb::Join & join,
    const NamesAndTypes & source_columns,
    String & filter_column_for_other_condition,
    String & filter_column_for_other_eq_condition)
{
    if (join.other_conditions_size() == 0 && join.other_eq_conditions_from_in_size() == 0)
        return nullptr;
    DAGExpressionAnalyzer dag_analyzer(source_columns, context);
    ExpressionActionsChain chain;
    std::vector<const tipb::Expr *> condition_vector;
    if (join.other_conditions_size() > 0)
    {
        for (const auto & c : join.other_conditions())
        {
            condition_vector.push_back(&c);
        }
        filter_column_for_other_condition = dag_analyzer.appendWhere(chain, condition_vector);
    }
    if (join.other_eq_conditions_from_in_size() > 0)
    {
        condition_vector.clear();
        for (const auto & c : join.other_eq_conditions_from_in())
        {
            condition_vector.push_back(&c);
        }
        filter_column_for_other_eq_condition = dag_analyzer.appendWhere(chain, condition_vector);
    }
    return chain.getLastActions();
}

ExpressionActionsPtr prepareJoin(
    const Context & context,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & keys,
    const DataTypes & key_types,
    const Block & header,
    Names & key_names,
    bool left,
    bool is_right_out_join,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & filters,
    String & filter_column_name)
{
    NamesAndTypes source_columns;
    for (auto const & p : header.getNamesAndTypesList())
        source_columns.emplace_back(p.name, p.type);
    DAGExpressionAnalyzer dag_analyzer(std::move(source_columns), context);
    ExpressionActionsChain chain;
    dag_analyzer.appendJoinKeyAndJoinFilters(chain, keys, key_types, key_names, left, is_right_out_join, filters, filter_column_name);
    return chain.getLastActions();
}

// join_output_columns, join_ptr, columns_added_by_join, prepare_build_actions, prepare_probe_actions
std::tuple<NamesAndTypes, JoinPtr, NamesAndTypesList, ExpressionActionsPtr, ExpressionActionsPtr>
handleJoin(const Context & context, const tipb::Join & join, size_t build_side_index, const ASTTableJoin::Kind & kind, const std::vector<Block> & headers)
{
    assert(headers.size() == 2);

    bool swap_join_side = build_side_index == 0;

    const Block & probe_header = headers[1 - build_side_index];
    const Block & build_header = headers[build_side_index];

    /// (cartesian) (anti) left semi join.
    const bool is_left_semi_family = JoinInterpreterHelper::isLeftSemiFamily(join);
    const bool is_semi_join = JoinInterpreterHelper::isSemiJoin(join);

    NamesAndTypes join_output_columns;
    /// columns_for_other_join_filter is a vector of columns used
    /// as the input columns when compiling other join filter.
    /// Note the order in the column vector is very important:
    /// first the columns in input_streams_vec[0], then followed
    /// by the columns in input_streams_vec[1], if there are other
    /// columns generated before compile other join filter, then
    /// append the extra columns afterwards. In order to figure out
    /// whether a given column is already in the column vector or
    /// not quickly, we use another set to store the column names
    NamesAndTypes columns_for_other_join_filter;
    std::unordered_set<String> column_set_for_other_join_filter;
    bool make_nullable = join.join_type() == tipb::JoinType::TypeRightOuterJoin;
    for (auto const & p : headers[0].getNamesAndTypesList())
    {
        join_output_columns.emplace_back(p.name, make_nullable ? makeNullable(p.type) : p.type);
        columns_for_other_join_filter.emplace_back(p.name, make_nullable ? makeNullable(p.type) : p.type);
        column_set_for_other_join_filter.emplace(p.name);
    }
    make_nullable = join.join_type() == tipb::JoinType::TypeLeftOuterJoin;
    for (auto const & p : headers[1].getNamesAndTypesList())
    {
        if (!is_semi_join)
            /// for semi join, the columns from right table will be ignored
            join_output_columns.emplace_back(p.name, make_nullable ? makeNullable(p.type) : p.type);
        /// however, when compiling join's other condition, we still need the columns from right table
        columns_for_other_join_filter.emplace_back(p.name, make_nullable ? makeNullable(p.type) : p.type);
        column_set_for_other_join_filter.emplace(p.name);
    }

    bool is_tiflash_left_join = JoinInterpreterHelper::isTiflashLeftJoin(kind);
    bool is_tiflash_right_join = JoinInterpreterHelper::isTiflashRightJoin(kind);
    /// all the columns from right table should be added after join, even for the join key
    NamesAndTypesList columns_added_by_join;
    make_nullable = is_tiflash_left_join;
    for (auto const & p : build_header.getNamesAndTypesList())
    {
        columns_added_by_join.emplace_back(p.name, make_nullable ? makeNullable(p.type) : p.type);
    }

    String match_helper_name;
    if (is_left_semi_family)
    {
        match_helper_name = JoinInterpreterHelper::genMatcherNameForLeftSemiFamily(headers[0], headers[1]);
        assert(!match_helper_name.empty());
        columns_added_by_join.emplace_back(match_helper_name, Join::match_helper_type);
        join_output_columns.emplace_back(match_helper_name, Join::match_helper_type);
    }

    DataTypes join_key_types = JoinInterpreterHelper::getJoinKeyTypes(join);
    TiDB::TiDBCollators collators = JoinInterpreterHelper::getJoinKeyCollators(join, join_key_types);

    Names left_key_names, right_key_names;
    String left_filter_column_name, right_filter_column_name;

    /// add necessary transformation if the join key is an expression

    auto prepare_probe_actions = prepareJoin(
        context,
        swap_join_side ? join.right_join_keys() : join.left_join_keys(),
        join_key_types,
        probe_header,
        left_key_names,
        true,
        is_tiflash_right_join,
        swap_join_side ? join.right_conditions() : join.left_conditions(),
        left_filter_column_name);

    auto prepare_build_actions = prepareJoin(
        context,
        swap_join_side ? join.left_join_keys() : join.right_join_keys(),
        join_key_types,
        build_header,
        right_key_names,
        false,
        is_tiflash_right_join,
        swap_join_side ? join.left_conditions() : join.right_conditions(),
        right_filter_column_name);

    String other_filter_column_name, other_eq_filter_from_in_column_name;
    for (auto const & p : prepare_probe_actions->getSampleBlock().getNamesAndTypesList())
    {
        if (column_set_for_other_join_filter.find(p.name) == column_set_for_other_join_filter.end())
            columns_for_other_join_filter.emplace_back(p.name, p.type);
    }
    for (auto const & p : prepare_build_actions->getSampleBlock().getNamesAndTypesList())
    {
        if (column_set_for_other_join_filter.find(p.name) == column_set_for_other_join_filter.end())
            columns_for_other_join_filter.emplace_back(p.name, p.type);
    }

    ExpressionActionsPtr other_condition_expr = JoinInterpreterHelper::genJoinOtherConditionAction(
        context,
        join,
        columns_for_other_join_filter,
        other_filter_column_name,
        other_eq_filter_from_in_column_name);

    const Settings & settings = context.getSettingsRef();
    // todo max_threads need refactor.
    size_t join_build_concurrency = settings.join_concurrent_build ? static_cast<size_t>(settings.max_threads) : 1;
    size_t max_block_size_for_cross_join = settings.max_block_size;
    fiu_do_on(FailPoints::minimum_block_size_for_cross_join, { max_block_size_for_cross_join = 1; });

    ASTTableJoin::Strictness strictness = is_semi_join ? ASTTableJoin::Strictness::Any : ASTTableJoin::Strictness::All;

    JoinPtr join_ptr = std::make_shared<Join>(
        left_key_names,
        right_key_names,
        true,
        SizeLimits(settings.max_rows_in_join, settings.max_bytes_in_join, settings.join_overflow_mode),
        kind,
        strictness,
        context.getDAGContext()->log->identifier(),
        join_build_concurrency,
        collators,
        left_filter_column_name,
        right_filter_column_name,
        other_filter_column_name,
        other_eq_filter_from_in_column_name,
        other_condition_expr,
        max_block_size_for_cross_join,
        match_helper_name);

    return {join_output_columns, join_ptr, columns_added_by_join, prepare_build_actions, prepare_probe_actions};
}
} // namespace JoinInterpreterHelper
} // namespace DB