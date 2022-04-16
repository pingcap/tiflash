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

#include <Common/TiFlashException.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/getLeastSupertype.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/JoinInterpreterHelper.h>
#include <Interpreters/Context.h>
#include <Interpreters/Join.h>
#include <Storages/Transaction/TypeMapping.h>
#include <fmt/format.h>

#include <unordered_map>

namespace DB::JoinInterpreterHelper
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
    {
        build_side_index = 0;
        kind = ASTTableJoin::Kind::Cross_Left;
    }
    else if (kind == ASTTableJoin::Kind::Cross_Left)
    {
        build_side_index = 1;
    }
    else
    {
        build_side_index = join.inner_idx();
        if (build_side_index == 0)
        {
            if (kind == ASTTableJoin::Kind::Left)
            {
                kind = ASTTableJoin::Kind::Right;
            }
            else if (kind == ASTTableJoin::Kind::Right)
            {
                kind = ASTTableJoin::Kind::Left;
            }
            else
            {
                // just `else`, for other kinds, don't need to change kind.
            }
        }
    }

    return {kind, build_side_index};
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

/// (cartesian) (anti) left semi join.
bool isLeftSemiFamily(const tipb::Join & join)
{
    return join.join_type() == tipb::JoinType::TypeLeftOuterSemiJoin || join.join_type() == tipb::JoinType::TypeAntiLeftOuterSemiJoin;
}

bool isSemiJoin(const tipb::Join & join)
{
    return join.join_type() == tipb::JoinType::TypeSemiJoin || join.join_type() == tipb::JoinType::TypeAntiSemiJoin || isLeftSemiFamily(join);
}

ASTTableJoin::Strictness getStrictness(const tipb::Join & join)
{
    return isSemiJoin(join) ? ASTTableJoin::Strictness::Any : ASTTableJoin::Strictness::All;
}

String genMatchHelperName(const tipb::Join & join, const Block & header1, const Block & header2)
{
    if (!isLeftSemiFamily(join))
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

const google::protobuf::RepeatedPtrField<tipb::Expr> & getBuildJoinKeys(const tipb::Join & join, size_t build_side_index)
{
    return build_side_index == 1 ? join.right_join_keys() : join.left_join_keys();
}

const google::protobuf::RepeatedPtrField<tipb::Expr> & getProbeJoinKeys(const tipb::Join & join, size_t build_side_index)
{
    return build_side_index == 0 ? join.right_join_keys() : join.left_join_keys();
}

const google::protobuf::RepeatedPtrField<tipb::Expr> & getBuildConditions(const tipb::Join & join, size_t build_side_index)
{
    return build_side_index == 1 ? join.right_conditions() : join.left_conditions();
}

const google::protobuf::RepeatedPtrField<tipb::Expr> & getProbeConditions(const tipb::Join & join, size_t build_side_index)
{
    return build_side_index == 0 ? join.right_conditions() : join.left_conditions();
}

bool isTiflashLeftJoin(const ASTTableJoin::Kind & kind)
{
    return kind == ASTTableJoin::Kind::Left || kind == ASTTableJoin::Kind::Cross_Left;
}

bool isTiflashRightJoin(const ASTTableJoin::Kind & kind)
{
    return kind == ASTTableJoin::Kind::Right;
}

NamesAndTypes genJoinOutputColumns(
    const tipb::Join & join,
    const Block & left_input_header,
    const Block & right_input_header,
    const String & match_helper_name)
{
    NamesAndTypes join_output_columns;
    auto append_output_columns = [&join_output_columns](const Block & header, bool make_nullable) {
        for (auto const & p : header)
        {
            join_output_columns.emplace_back(p.name, make_nullable ? makeNullable(p.type) : p.type);
        }
    };

    append_output_columns(left_input_header, join.join_type() == tipb::JoinType::TypeRightOuterJoin);
    if (!JoinInterpreterHelper::isSemiJoin(join))
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

/// all the columns from build side streams should be added after join, even for the join key.
NamesAndTypesList genColumnsAddedByJoin(
    const ASTTableJoin::Kind & kind,
    const Block & build_side_header,
    const String & match_helper_name)
{
    NamesAndTypesList columns_added_by_join;
    bool make_nullable = isTiflashLeftJoin(kind);
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

NamesAndTypes genColumnsForOtherJoinFilter(
    const tipb::Join & join,
    const Block & left_input_header,
    const Block & right_input_header,
    const ExpressionActionsPtr & prepare_join_actions1,
    const ExpressionActionsPtr & prepare_join_actions2)
{
    /// columns_for_other_join_filter is a vector of columns used
    /// as the input columns when compiling other join filter.
    /// Note the order in the column vector is very important:
    /// first the columns in left_input_header, then followed
    /// by the columns in right_input_header, if there are other
    /// columns generated before compile other join filter, then
    /// append the extra columns afterwards. In order to figure out
    /// whether a given column is already in the column vector or
    /// not quickly, we use another set to store the column names

    NamesAndTypes columns_for_other_join_filter;
    std::unordered_set<String> column_set_for_other_join_filter;

    auto append_output_columns = [&columns_for_other_join_filter, &column_set_for_other_join_filter](const Block & header, bool make_nullable) {
        for (auto const & p : header)
        {
            columns_for_other_join_filter.emplace_back(p.name, make_nullable ? makeNullable(p.type) : p.type);
            column_set_for_other_join_filter.emplace(p.name);
        }
    };
    append_output_columns(left_input_header, join.join_type() == tipb::JoinType::TypeRightOuterJoin);
    append_output_columns(right_input_header, join.join_type() == tipb::JoinType::TypeLeftOuterJoin);

    auto append_unique_columns = [&columns_for_other_join_filter, &column_set_for_other_join_filter](const Block & header) {
        for (auto const & p : header)
        {
            if (column_set_for_other_join_filter.find(p.name) == column_set_for_other_join_filter.end())
                columns_for_other_join_filter.emplace_back(p.name, p.type);
        }
    };
    append_unique_columns(prepare_join_actions1->getSampleBlock());
    append_unique_columns(prepare_join_actions2->getSampleBlock());

    return columns_for_other_join_filter;
}

ExpressionActionsPtr prepareJoin(
    const Context & context,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & keys,
    const DataTypes & key_types,
    const Block & input_header,
    Names & key_names,
    bool left,
    bool is_right_out_join,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & filters,
    String & filter_column_name)
{
    NamesAndTypes source_columns;
    for (auto const & p : input_header)
        source_columns.emplace_back(p.name, p.type);
    DAGExpressionAnalyzer dag_analyzer(std::move(source_columns), context);
    ExpressionActionsChain chain;
    dag_analyzer.appendJoinKeyAndJoinFilters(chain, keys, key_types, key_names, left, is_right_out_join, filters, filter_column_name);
    return chain.getLastActions();
}


std::tuple<ExpressionActionsPtr, String, String> genJoinOtherConditionAction(
    const Context & context,
    const tipb::Join & join,
    const NamesAndTypes & source_columns)
{
    if (join.other_conditions_size() == 0 && join.other_eq_conditions_from_in_size() == 0)
        return {nullptr, "", ""};

    DAGExpressionAnalyzer dag_analyzer(source_columns, context);
    ExpressionActionsChain chain;
    std::vector<const tipb::Expr *> condition_vector;

    String filter_column_for_other_condition;
    if (join.other_conditions_size() > 0)
    {
        for (const auto & c : join.other_conditions())
        {
            condition_vector.push_back(&c);
        }
        filter_column_for_other_condition = dag_analyzer.appendWhere(chain, condition_vector);
    }

    String filter_column_for_other_eq_condition;
    if (join.other_eq_conditions_from_in_size() > 0)
    {
        condition_vector.clear();
        for (const auto & c : join.other_eq_conditions_from_in())
        {
            condition_vector.push_back(&c);
        }
        filter_column_for_other_eq_condition = dag_analyzer.appendWhere(chain, condition_vector);
    }

    return {chain.getLastActions(), filter_column_for_other_condition, filter_column_for_other_eq_condition};
}
} // namespace DB::JoinInterpreterHelper