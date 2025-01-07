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

#include <Common/Exception.h>
#include <Storages/DeltaMerge/Index/LocalIndexInfo_fwd.h>
#include <Storages/DeltaMerge/dtpb/index_file.pb.h>
#include <Storages/KVStore/Types.h>
#include <TiDB/Schema/TiDB.h>
#include <TiDB/Schema/VectorIndex.h>

namespace TiDB
{
struct TableInfo;
} // namespace TiDB

namespace DB
{
class Logger;
using LoggerPtr = std::shared_ptr<Logger>;
} // namespace DB
namespace DB::DM
{

struct LocalIndexInfo
{
    TiDB::ColumnarIndexKind kind;
    // If the index is defined on TiDB::ColumnInfo, use EmptyIndexID as index_id
    IndexID index_id = DB::EmptyIndexID;
    // Which column_id the index is built on
    ColumnID column_id = DB::EmptyColumnID;

    TiDB::VectorIndexDefinitionPtr def_vector_index = nullptr;

    dtpb::IndexFileKind getKindAsDtpb() const
    {
        switch (kind)
        {
        case TiDB::ColumnarIndexKind::Vector:
            return dtpb::IndexFileKind::VECTOR_INDEX;
        default:
            RUNTIME_CHECK_MSG(false, "Unsupported index kind: {}", magic_enum::enum_name(kind));
        }
    }
};

LocalIndexInfosPtr initLocalIndexInfos(const TiDB::TableInfo & table_info, const LoggerPtr & logger);

struct LocalIndexInfosChangeset
{
    LocalIndexInfosPtr new_local_index_infos;
    std::vector<IndexID> dropped_indexes;
};

// Generate a changeset according to `existing_indexes` and `new_table_info`
// If there are newly added or dropped indexes according to `new_table_info`,
// return a changeset with changeset.new_local_index_infos != nullptr
LocalIndexInfosChangeset generateLocalIndexInfos(
    const LocalIndexInfosSnapshot & existing_indexes,
    const TiDB::TableInfo & new_table_info,
    const LoggerPtr & logger);

} // namespace DB::DM
