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
#include <Storages/Transaction/TMTStorages.h>
#include <TiDB/Schema/SchemaGetter.h>

namespace DB
{
using KeyspaceDatabaseMap = std::unordered_map<KeyspaceDatabaseID, TiDB::DBInfoPtr, boost::hash<KeyspaceDatabaseID>>;
template <typename Getter, typename NameMapper>
struct SchemaBuilder
{
    NameMapper name_mapper;

    Getter & getter;

    Context & context;

    KeyspaceDatabaseMap & databases;

    Int64 target_version;

    const KeyspaceID keyspace_id;

    LoggerPtr log;

    SchemaBuilder(Getter & getter_, Context & context_, KeyspaceDatabaseMap & dbs_, Int64 version)
        : getter(getter_)
        , context(context_)
        , databases(dbs_)
        , target_version(version)
        , keyspace_id(getter_.getKeyspaceID())
        , log(Logger::get(fmt::format("keyspace={}", keyspace_id)))
    {}

    void applyDiff(const SchemaDiff & diff);

    void syncAllSchema();

    void dropAllSchema();

private:
    void applyDropSchema(DatabaseID schema_id);

    /// Parameter db_name should be mapped.
    void applyDropSchema(const String & db_name);

    void applyRecoverSchema(DatabaseID database_id);

    bool applyCreateSchema(DatabaseID schema_id);

    void applyCreateSchema(const TiDB::DBInfoPtr & db_info);

    void applyCreateTable(const TiDB::DBInfoPtr & db_info, TableID table_id);

    void applyCreateLogicalTable(const TiDB::DBInfoPtr & db_info, const TiDB::TableInfoPtr & table_info);

    void applyCreatePhysicalTable(const TiDB::DBInfoPtr & db_info, const TiDB::TableInfoPtr & table_info);

    void applyDropTable(const TiDB::DBInfoPtr & db_info, TableID table_id);

    /// Parameter schema_name should be mapped.
    void applyDropPhysicalTable(const String & db_name, TableID table_id);

    void applyPartitionDiff(const TiDB::DBInfoPtr & db_info, TableID table_id);

    void applyPartitionDiff(const TiDB::DBInfoPtr & db_info, const TiDB::TableInfoPtr & table_info, const ManageableStoragePtr & storage, bool drop_part_if_not_exist);
    TiDB::DBInfoPtr tryFindDatabaseByPartitionTable(const TiDB::DBInfoPtr & db_info, const String & part_table_name);

    void applyAlterTable(const TiDB::DBInfoPtr & db_info, TableID table_id);

    void applyAlterLogicalTable(const TiDB::DBInfoPtr & db_info, const TiDB::TableInfoPtr & table_info, const ManageableStoragePtr & storage);

    void applyAlterPhysicalTable(const TiDB::DBInfoPtr & db_info, const TiDB::TableInfoPtr & table_info, const ManageableStoragePtr & storage);

    void applyRenameTable(const TiDB::DBInfoPtr & new_db_info, TiDB::TableID table_id);

    void applyRenameLogicalTable(const TiDB::DBInfoPtr & new_db_info, const TiDB::TableInfoPtr & new_table_info, const ManageableStoragePtr & storage);

    void applyRenamePhysicalTable(const TiDB::DBInfoPtr & new_db_info, const TiDB::TableInfo & new_table_info, const ManageableStoragePtr & storage);

    void applyExchangeTablePartition(const SchemaDiff & diff);

    void applySetTiFlashReplica(const TiDB::DBInfoPtr & db_info, TableID table_id);
    void applySetTiFlashReplicaOnLogicalTable(const TiDB::DBInfoPtr & db_info, const TiDB::TableInfoPtr & table_info, const ManageableStoragePtr & storage);
    void applySetTiFlashReplicaOnPhysicalTable(const TiDB::DBInfoPtr & db_info, const TiDB::TableInfoPtr & table_info, const ManageableStoragePtr & storage);
};

} // namespace DB
