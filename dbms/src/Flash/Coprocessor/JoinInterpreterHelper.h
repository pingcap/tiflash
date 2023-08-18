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

#pragma once

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Storages/Transaction/Collator.h>
#include <tipb/executor.pb.h>

#include <functional>
#include <tuple>

namespace DB
{
class Context;

struct JoinKeyType
{
    DataTypePtr key_type;
    bool is_incompatible_decimal;
};
using JoinKeyTypes = std::vector<JoinKeyType>;

namespace JoinInterpreterHelper
{
struct TiFlashJoin
{
    explicit TiFlashJoin(const tipb::Join & join_);

    const tipb::Join & join;

    ASTTableJoin::Kind kind;
    size_t build_side_index = 0;

    JoinKeyTypes join_key_types;
    TiDB::TiDBCollators join_key_collators;

    ASTTableJoin::Strictness strictness;

    /// (cartesian) (anti) left semi join.
    bool isLeftSemiFamily() const { return join.join_type() == tipb::JoinType::TypeLeftOuterSemiJoin || join.join_type() == tipb::JoinType::TypeAntiLeftOuterSemiJoin; }

    bool isSemiJoin() const { return join.join_type() == tipb::JoinType::TypeSemiJoin || join.join_type() == tipb::JoinType::TypeAntiSemiJoin || isLeftSemiFamily(); }

    const google::protobuf::RepeatedPtrField<tipb::Expr> & getBuildJoinKeys() const
    {
        return build_side_index == 1 ? join.right_join_keys() : join.left_join_keys();
    }

    const google::protobuf::RepeatedPtrField<tipb::Expr> & getProbeJoinKeys() const
    {
        return build_side_index == 0 ? join.right_join_keys() : join.left_join_keys();
    }

    const google::protobuf::RepeatedPtrField<tipb::Expr> & getBuildConditions() const
    {
        return build_side_index == 1 ? join.right_conditions() : join.left_conditions();
    }

    const google::protobuf::RepeatedPtrField<tipb::Expr> & getProbeConditions() const
    {
        return build_side_index == 0 ? join.right_conditions() : join.left_conditions();
    }

    bool isTiFlashLeftJoin() const { return kind == ASTTableJoin::Kind::Left || kind == ASTTableJoin::Kind::Cross_Left; }

    /// Cross_Right join will be converted to Cross_Left join, so no need to check Cross_Right
    bool isTiFlashRightJoin() const { return kind == ASTTableJoin::Kind::Right; }

    /// return a name that is unique in header1 and header2 for left semi family join,
    /// return "" for everything else.
    String genMatchHelperName(const Block & header1, const Block & header2) const;

    /// columns_added_by_join
    /// = join_output_columns - probe_side_columns
    /// = build_side_columns + match_helper_name
    NamesAndTypesList genColumnsAddedByJoin(
        const Block & build_side_header,
        const String & match_helper_name) const;

    /// The columns output by join will be:
    /// {columns of left_input, columns of right_input, match_helper_name}
    NamesAndTypes genJoinOutputColumns(
        const Block & left_input_header,
        const Block & right_input_header,
        const String & match_helper_name) const;

    /// @other_condition_expr: generates other_filter_column and other_eq_filter_from_in_column
    /// @other_filter_column_name: column name of `and(other_cond1, other_cond2, ...)`
    /// @other_eq_filter_from_in_column_name: column name of `and(other_eq_cond1_from_in, other_eq_cond2_from_in, ...)`
    /// such as
    ///   `select * from t1 where col1 in (select col2 from t2 where t1.col2 = t2.col3)`
    ///   - other_filter is `t1.col2 = t2.col3`
    ///   - other_eq_filter_from_in_column is `t1.col1 = t2.col2`
    ///
    /// new columns from build side prepare join actions cannot be appended.
    /// because the input that other filter accepts is
    /// {left_input_columns, right_input_columns, new_columns_from_probe_side_prepare, match_helper_name}.
    std::tuple<ExpressionActionsPtr, String, String> genJoinOtherConditionAction(
        const Context & context,
        const Block & left_input_header,
        const Block & right_input_header,
        const ExpressionActionsPtr & probe_side_prepare_join) const;

    NamesAndTypes genColumnsForOtherJoinFilter(
        const Block & left_input_header,
        const Block & right_input_header,
        const ExpressionActionsPtr & probe_prepare_join_actions) const;
};

/// @join_prepare_expr_actions: generates join key columns and join filter column
/// @key_names: column names of keys.
/// @filter_column_name: column name of `and(filters)`
std::tuple<ExpressionActionsPtr, Names, String> prepareJoin(
    const Context & context,
    const Block & input_header,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & keys,
    const JoinKeyTypes & join_key_types,
    bool left,
    bool is_right_out_join,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & filters);
} // namespace JoinInterpreterHelper
} // namespace DB
