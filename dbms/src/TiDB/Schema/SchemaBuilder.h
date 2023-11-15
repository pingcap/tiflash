// Copyright 2023 PingCAP, Inc.
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

#include <Interpreters/Context_fwd.h>
#include <Storages/KVStore/TMTStorages.h>
#include <TiDB/Schema/DatabaseInfoCache.h>
#include <TiDB/Schema/SchemaGetter.h>
#include <TiDB/Schema/TableIDMap.h>

namespace DB
{

template <typename Getter, typename NameMapper>
struct SchemaBuilder
{
private:
    NameMapper name_mapper;

    Getter & getter;

    Context & context;

    DatabaseInfoCache & databases;

    TableIDMap & table_id_map;

    const KeyspaceID keyspace_id;

    LoggerPtr log;

public:
    SchemaBuilder(Getter & getter_, Context & context_, DatabaseInfoCache & dbs_, TableIDMap & table_id_map_)
        : getter(getter_)
        , context(context_)
        , databases(dbs_)
        , table_id_map(table_id_map_)
        , keyspace_id(getter_.getKeyspaceID())
        , log(Logger::get(fmt::format("keyspace={}", keyspace_id)))
    {}

    void applyDiff(const SchemaDiff & diff);

    void syncAllSchema();

    void dropAllSchema();

    bool applyTable(DatabaseID database_id, TableID logical_table_id, TableID physical_table_id);

private:
    void applyDropSchema(DatabaseID schema_id);

    /// Parameter db_name should be mapped.
    void applyDropSchema(const String & db_name);

    bool applyCreateSchema(DatabaseID schema_id);

    void applyCreateSchema(const TiDB::DBInfoPtr & db_info);

    void applyCreateStorageInstance(const TiDB::DBInfoPtr & db_info, const TiDB::TableInfoPtr & table_info);

    void applyDropTable(DatabaseID database_id, TableID table_id);

    void applyRecoverTable(DatabaseID database_id, TiDB::TableID table_id);

    bool tryRecoverPhysicalTable(const TiDB::DBInfoPtr & db_info, const TiDB::TableInfoPtr & table_info);

    /// Parameter schema_name should be mapped.
    void applyDropPhysicalTable(const String & db_name, TableID table_id);

    void applyPartitionDiff(DatabaseID database_id, TableID table_id);
    void applyPartitionDiff(
        const TiDB::DBInfoPtr & db_info,
        const TiDB::TableInfoPtr & table_info,
        const ManageableStoragePtr & storage);

    void applyRenameTable(DatabaseID database_id, TiDB::TableID table_id);

    void applyRenameLogicalTable(
        const TiDB::DBInfoPtr & new_db_info,
        const TiDB::TableInfoPtr & new_table_info,
        const ManageableStoragePtr & storage);

    void applyRenamePhysicalTable(
        const TiDB::DBInfoPtr & new_db_info,
        const TiDB::TableInfo & new_table_info,
        const ManageableStoragePtr & storage);

    void applySetTiFlashReplica(DatabaseID database_id, TableID table_id);

    void applyCreateTable(DatabaseID database_id, TableID table_id);

    void applyExchangeTablePartition(const SchemaDiff & diff);
};

} // namespace DB
