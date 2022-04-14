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

#include <DataTypes/IDataType.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Storages/Transaction/Collator.h>
#include <tipb/executor.pb.h>

#include <tuple>

namespace DB::JoinInterpreterHelper
{
std::pair<ASTTableJoin::Kind, size_t> getJoinKindAndBuildSideIndex(const tipb::Join & join);

DataTypes getJoinKeyTypes(const tipb::Join & join);

TiDB::TiDBCollators getJoinKeyCollators(const tipb::Join & join, const DataTypes & key_types);

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

/// return a name that is unique in header1 and header2.
String genMatchHelperNameForLeftSemiFamily(const Block & header1, const Block & header2);

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
} // namespace DB::JoinInterpreterHelper