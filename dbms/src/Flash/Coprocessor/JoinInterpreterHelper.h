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
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Storages/Transaction/Collator.h>
#include <tipb/executor.pb.h>

#include <tuple>

namespace DB
{
class Context;

namespace JoinInterpreterHelper
{
std::pair<ASTTableJoin::Kind, size_t> getJoinKindAndBuildSideIndex(const tipb::Join & join);

DataTypes getJoinKeyTypes(const tipb::Join & join);

TiDB::TiDBCollators getJoinKeyCollators(const tipb::Join & join, const DataTypes & key_types);

/// (cartesian) (anti) left semi join.
bool isLeftSemiFamily(const tipb::Join & join);

bool isSemiJoin(const tipb::Join & join);

ASTTableJoin::Strictness getStrictness(const tipb::Join & join);

/// return a name that is unique in header1 and header2 for left semi family join,
/// return "" for everything else.
String genMatchHelperName(const tipb::Join & join, const Block & header1, const Block & header2);

const google::protobuf::RepeatedPtrField<tipb::Expr> & getBuildJoinKeys(const tipb::Join & join, size_t build_side_index);

const google::protobuf::RepeatedPtrField<tipb::Expr> & getProbeJoinKeys(const tipb::Join & join, size_t build_side_index);

const google::protobuf::RepeatedPtrField<tipb::Expr> & getBuildConditions(const tipb::Join & join, size_t build_side_index);

const google::protobuf::RepeatedPtrField<tipb::Expr> & getProbeConditions(const tipb::Join & join, size_t build_side_index);

bool isTiflashLeftJoin(const ASTTableJoin::Kind & kind);

bool isTiflashRightJoin(const ASTTableJoin::Kind & kind);

NamesAndTypes genJoinOutputColumns(
    const tipb::Join & join,
    const Block & left_input_header,
    const Block & right_input_header,
    const String & match_helper_name);

NamesAndTypesList genColumnsAddedByJoin(
    const ASTTableJoin::Kind & kind,
    const Block & build_side_header,
    const String & match_helper_name);

NamesAndTypes genColumnsForOtherJoinFilter(
    const tipb::Join & join,
    const Block & left_input_header,
    const Block & right_input_header,
    const ExpressionActionsPtr & prepare_join_actions1,
    const ExpressionActionsPtr & prepare_join_actions2);

ExpressionActionsPtr prepareJoin(
    const Context & context,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & keys,
    const DataTypes & key_types,
    const Block & input_header,
    Names & key_names,
    bool left,
    bool is_right_out_join,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & filters,
    String & filter_column_name);

/// return {other_condition_expr, other_filter_column_name, other_eq_filter_from_in_column_name}
std::tuple<ExpressionActionsPtr, String, String> genJoinOtherConditionAction(
    const Context & context,
    const tipb::Join & join,
    const NamesAndTypes & source_columns);
} // namespace JoinInterpreterHelper
} // namespace DB