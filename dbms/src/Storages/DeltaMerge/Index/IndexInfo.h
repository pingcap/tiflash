// Copyright 2024 PingCAP, Inc.
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

#include <Storages/KVStore/Types.h>
#include <TiDB/Schema/VectorIndex.h>

namespace TiDB
{
struct TableInfo;
struct ColumnInfo;
struct IndexInfo;
} // namespace TiDB

namespace DB
{
class Logger;
using LoggerPtr = std::shared_ptr<Logger>;
} // namespace DB
namespace DB::DM
{
enum class IndexType
{
    Vector = 1,
};

struct LocalIndexInfo
{
    IndexType type;
    // If the index is defined on TiDB::ColumnInfo, use EmptyIndexID as index_id
    IndexID index_id = DB::EmptyIndexID;
    // Which column_id the index is built on
    ColumnID column_id = DB::EmptyColumnID;
    // Now we only support vector index.
    // In the future, we may support more types of indexes, using std::variant.
    TiDB::VectorIndexDefinitionPtr index_definition;
};

using LocalIndexInfos = std::vector<LocalIndexInfo>;
using LocalIndexInfosPtr = std::shared_ptr<LocalIndexInfos>;
using LocalIndexInfosSnapshot = std::shared_ptr<const LocalIndexInfos>;

LocalIndexInfosPtr initLocalIndexInfos(const TiDB::TableInfo & table_info, const LoggerPtr & logger);
LocalIndexInfosPtr generateLocalIndexInfos(
    const LocalIndexInfosSnapshot & existing_indexes,
    const TiDB::TableInfo & new_table_info,
    const LoggerPtr & logger);

} // namespace DB::DM
