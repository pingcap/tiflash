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

struct JoinOtherConditions
{
    String other_cond_name;
    String other_eq_cond_from_in_name;
    ExpressionActionsPtr other_cond_expr;

    /// null_aware_eq_cond is used for checking the null-aware equal condition for not-matched rows.
    String null_aware_eq_cond_name;
    ExpressionActionsPtr null_aware_eq_cond_expr;

    /// Validate this JoinConditions and return error message if any.
    String validate(bool is_null_aware_semi_join) const
    {
        if ((!other_cond_name.empty() || !other_eq_cond_from_in_name.empty()) && other_cond_expr == nullptr)
            return "other_cond_name and/or other_eq_cond_from_in_name are not empty but other_cond_expr is nullptr";
        if (other_cond_name.empty() && other_eq_cond_from_in_name.empty() && other_cond_expr != nullptr)
            return "other_cond_name and other_eq_cond_from_in_name are empty but other_cond_expr is not nullptr";

        if (is_null_aware_semi_join)
        {
            if (null_aware_eq_cond_name.empty() || null_aware_eq_cond_expr == nullptr)
                return "null-aware semi join does not have null_aware_eq_cond_name or null_aware_eq_cond_expr is nullptr";
            if (!other_eq_cond_from_in_name.empty())
                return "null-aware semi join should not have other_eq_cond_from_in_name";
        }

        return "";
    }
};

namespace JoinInterpreterHelper
{
struct TiFlashJoin
{
    TiFlashJoin(const tipb::Join & join_, bool is_test);

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

    /// The columns output by join will be:
    /// {columns of left_input, columns of right_input, match_helper_name}
    NamesAndTypes genJoinOutputColumns(
        const Block & left_input_header,
        const Block & right_input_header,
        const String & match_helper_name) const;

    /// Example 1:
    ///   `select * from t1 inner join t2 on t1.col1 = t2.col1 and t1.col2 > t2.col2`
    ///   - join key are `t1.col1` and `t2.col1`
    ///   - other_cond is `t1.col2 > t2.col2`
    ///
    /// Example 2:
    ///   `select * from t1 where col1 not in (select col1 from t2 where t1.col2 = t2.col2 and t1.col3 > t2.col3)`
    ///   There are several possibilities which depends on TiDB.
    ///   1. cartesian anti semi join.
    ///      - join key is empty
    ///      - other_cond is `t1.col2 = t2.col2 and t1.col3 > t2.col3`
    ///      - other_cond_from_in is `t1.col1 = t2.col1`
    ///   2. anti semi join.
    ///      - join keys are `t1.col2` and `t2.col2`
    ///      - other_cond is `t1.col3 > t2.col3`
    ///      - other_cond_from_in is `t1.col1 = t2.col1`
    ///   3. null-aware anti semi join
    ///      - join keys are `t1.col1` and `t2.col1`
    ///      - other_cond is `t1.col2 = t2.col2 and t1.col3 > t2.col3`
    ///      - null_aware_eq_cond is `t1.col1 = t2.col1`
    ///
    /// Note that new columns from build side prepare join actions cannot be appended.
    /// because the input that other filter accepts is
    /// {left_input_columns, right_input_columns, new_columns_from_probe_side_prepare, match_helper_name}.
    JoinOtherConditions genJoinOtherConditionsAction(
        const Context & context,
        const Block & left_input_header,
        const Block & right_input_header,
        const ExpressionActionsPtr & probe_side_prepare_join,
        const Names & probe_key_names,
        const Names & build_key_names) const;

    NamesAndTypes genColumnsForOtherJoinFilter(
        const Block & left_input_header,
        const Block & right_input_header,
        const ExpressionActionsPtr & probe_prepare_join_actions) const;
};

/// @join_prepare_expr_actions: generates join key columns and join filter column
/// @key_names: column names of keys.
/// @original_key_names: original column names of keys.(only used for null-aware semi join)
/// @filter_column_name: column name of `and(filters)`
std::tuple<ExpressionActionsPtr, Names, Names, String> prepareJoin(
    const Context & context,
    const Block & input_header,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & keys,
    const JoinKeyTypes & join_key_types,
    bool left,
    bool is_right_out_join,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & filters);
} // namespace JoinInterpreterHelper
} // namespace DB
