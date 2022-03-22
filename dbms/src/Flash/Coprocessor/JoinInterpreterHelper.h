#pragma once

#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Storages/Transaction/Collator.h>
#include <tipb/executor.pb.h>

#include <memory>
#include <utility>

namespace DB::JoinInterpreterHelper
{
std::pair<ASTTableJoin::Kind, size_t> getJoinKindAndBuildSideIndex(const tipb::Join & join);

DataTypes getJoinKeyTypes(const tipb::Join & join);

TiDB::TiDBCollators getJoinKeyCollators(const tipb::Join & join, const DataTypes & join_key_types);

/// generate a name not contained by left_header and right_header.
String genMatchHelperNameForLeftSemiFamily(const Block & left_header, const Block & right_header);
} // namespace DB::JoinInterpreterHelper
