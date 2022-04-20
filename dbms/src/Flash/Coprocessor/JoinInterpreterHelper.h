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

#include <tuple>

namespace DB
{
class Context;
struct DAGPipeline;

namespace JoinInterpreterHelper
{
struct TiflashJoin
{
    explicit TiflashJoin(const tipb::Join & join_);

    const tipb::Join & join;

    ASTTableJoin::Kind kind;
    size_t build_side_index;

    DataTypes join_key_types;
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

    bool isTiflashLeftJoin() const { return kind == ASTTableJoin::Kind::Left || kind == ASTTableJoin::Kind::Cross_Left; }

    /// Cross_Right join will be converted to Cross_Left join, so no need to check Cross_Right
    bool isTiflashRightJoin() const { return kind == ASTTableJoin::Kind::Right; }
};

/// other_condition_expr, other_filter_column_name, other_eq_filter_from_in_column_name
std::tuple<ExpressionActionsPtr, String, String> genJoinOtherConditionAction(
    const Context & context,
    const tipb::Join & join,
    NamesAndTypes & source_columns);

void prepareJoin(
    const Context & context,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & keys,
    const DataTypes & key_types,
    DAGPipeline & pipeline,
    Names & key_names,
    bool left,
    bool is_right_out_join,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & filters,
    String & filter_column_name);
} // namespace JoinInterpreterHelper
} // namespace DB