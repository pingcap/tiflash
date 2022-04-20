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

namespace DB
{
class Context;

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
    bool isLeftSemiFamily() { return join.join_type() == tipb::JoinType::TypeLeftOuterSemiJoin || join.join_type() == tipb::JoinType::TypeAntiLeftOuterSemiJoin; }

    bool isSemiJoin() { return join.join_type() == tipb::JoinType::TypeSemiJoin || join.join_type() == tipb::JoinType::TypeAntiSemiJoin || isLeftSemiFamily(); }

    const google::protobuf::RepeatedPtrField<tipb::Expr> & getBuildJoinKeys()
    {
        return build_side_index == 1 ? join.right_join_keys() : join.left_join_keys();
    }

    const google::protobuf::RepeatedPtrField<tipb::Expr> & getProbeJoinKeys()
    {
        return build_side_index == 0 ? join.right_join_keys() : join.left_join_keys();
    }

    const google::protobuf::RepeatedPtrField<tipb::Expr> & getBuildConditions()
    {
        return build_side_index == 1 ? join.right_conditions() : join.left_conditions();
    }

    const google::protobuf::RepeatedPtrField<tipb::Expr> & getProbeConditions()
    {
        return build_side_index == 0 ? join.right_conditions() : join.left_conditions();
    }

    bool isTiflashLeftJoin() { return kind == ASTTableJoin::Kind::Left || kind == ASTTableJoin::Kind::Cross_Left; }

    /// Cross_Right join will be converted to Cross_Left join, so no need to check Cross_Right
    bool isTiflashRightJoin() { return kind == ASTTableJoin::Kind::Right; }
};
} // namespace JoinInterpreterHelper
} // namespace DB