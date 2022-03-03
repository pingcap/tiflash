#pragma once

#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Storages/Transaction/Collator.h>
#include <tipb/executor.pb.h>

#include <memory>
#include <utility>

namespace DB
{
namespace JoinInterpreterHelper
{
std::pair<ASTTableJoin::Kind, size_t> getJoinKindAndBuildSideIndex(const tipb::Join & join);

/// ClickHouse require join key to be exactly the same type
/// TiDB only require the join key to be the same category
/// for example decimal(10,2) join decimal(20,0) is allowed in
/// TiDB and will throw exception in ClickHouse
DataTypes getJoinKeyTypes(const tipb::Join & join);

TiDB::TiDBCollators getJoinKeyCollators(const tipb::Join & join, const DataTypes & join_key_types);

String genMatchHelperNameForLeftSemiFamily(const Block & left_header, const Block & right_header);
} // namespace namespace
} // namespace DB
