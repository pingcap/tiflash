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

#include <Common/Decimal.h>
#include <Common/Exception.h>
#include <Common/MyTime.h>
#include <IO/ReadBufferFromString.h>
#include <Storages/Transaction/StorageEngineType.h>
#include <Storages/Transaction/Types.h>
#include <TiDB/Schema/ColumnInfo.h>
#include <TiDB/Schema/IndexInfo.h>
#include <TiDB/Schema/SchemaNameMapper.h>
#include <TiDB/Schema/SchemaState.h>
#include <TiDB/Schema/PartitionInfo.h>


namespace TiDB
{
using DB::ColumnID;
using DB::TableID;


struct TableInfo
{
    TableInfo() = default;

    TableInfo(const TableInfo &) = default;

    TableInfo & operator=(const TableInfo &) = default;

    explicit TableInfo(Poco::JSON::Object::Ptr json);

    explicit TableInfo(const String & table_info_json);

    String serialize() const;

    void deserialize(const String & json_str);

    void deserialize(Poco::JSON::Object::Ptr obj);

    // The meaning of this ID changed after we support TiDB partition table.
    // It is the physical table ID, i.e. table ID for non-partition table,
    // and partition ID for partition table,
    // whereas field `belonging_table_id` below actually means the table ID this partition belongs to.
    TableID id = DB::InvalidTableID;
    String name;
    // Columns are listed in the order in which they appear in the schema.
    std::vector<ColumnInfo> columns;
    /// index_infos stores the index info from TiDB. But we do not store all
    /// the index infos because most of the index info is useless in TiFlash.
    /// If is_common_handle = true, the primary index info is stored
    /// otherwise, all of the index info are ignored
    std::vector<IndexInfo> index_infos;
    SchemaState state = StateNone;
    bool pk_is_handle = false;
    /// when is_common_handle = true, it means this table is a clustered index table
    bool is_common_handle = false;
    String comment;
    Timestamp update_timestamp = 0;
    bool is_partition_table = false;
    TableID belonging_table_id = DB::InvalidTableID;
    PartitionInfo partition;
    // If the table is view, we should ignore it.
    bool is_view = false;
    // If the table is sequence, we should ignore it.
    bool is_sequence = false;
    Int64 schema_version = DEFAULT_UNSPECIFIED_SCHEMA_VERSION;

    // The TiFlash replica info persisted by TiDB
    TiFlashReplicaInfo replica_info;

    ::TiDB::StorageEngine engine_type = ::TiDB::StorageEngine::UNSPECIFIED;

    ColumnID getColumnID(const String & name) const;
    String getColumnName(ColumnID id) const;

    const ColumnInfo & getColumnInfo(ColumnID id) const;

    std::optional<std::reference_wrapper<const ColumnInfo>> getPKHandleColumn() const;

    TableInfoPtr producePartitionTableInfo(TableID table_or_partition_id, const DB::SchemaNameMapper & name_mapper) const;

    bool isLogicalPartitionTable() const { return is_partition_table && belonging_table_id == DB::InvalidTableID && partition.enable; }

    /// should not be called if is_common_handle = false.
    /// when use IndexInfo, please avoid to use the offset info
    /// the offset value may be wrong in some cases,
    /// due to we will not update IndexInfo except RENAME DDL action,
    /// but DDL like add column / drop column may change the offset of columns
    /// Thus, please be very careful when you must have to use offset information !!!!!
    const IndexInfo & getPrimaryIndexInfo() const { return index_infos[0]; }

    IndexInfo & getPrimaryIndexInfo() { return index_infos[0]; }
};
} // namespace TiDB