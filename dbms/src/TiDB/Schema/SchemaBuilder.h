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

#include <Interpreters/Context_fwd.h>
#include <Storages/Transaction/TMTStorages.h>
#include <TiDB/Schema/SchemaGetter.h>

namespace DB
{
template <typename Getter, typename NameMapper>
struct SchemaBuilder
{
    NameMapper name_mapper;

    Getter & getter;

    Context & context;

    std::shared_mutex & shared_mutex_for_databases;

    std::unordered_map<DB::DatabaseID, TiDB::DBInfoPtr> & databases;

    std::shared_mutex & shared_mutex_for_table_id_map;

    std::unordered_map<DB::TableID, DB::DatabaseID> & table_id_to_database_id;

    std::unordered_map<DB::TableID, DB::TableID> & partition_id_to_logical_id;

    const KeyspaceID keyspace_id;

    LoggerPtr log;

    SchemaBuilder(Getter & getter_, Context & context_, std::unordered_map<DB::DatabaseID, TiDB::DBInfoPtr> & dbs_, std::unordered_map<DB::TableID, DB::DatabaseID> & table_id_to_database_id_, std::unordered_map<DB::TableID, DB::TableID> & partition_id_to_logical_id_, std::shared_mutex & shared_mutex_for_table_id_map_, std::shared_mutex & shared_mutex_for_databases_)
        : getter(getter_)
        , context(context_)
        , shared_mutex_for_databases(shared_mutex_for_databases_)
        , databases(dbs_)
        , table_id_to_database_id(table_id_to_database_id_)
        , shared_mutex_for_table_id_map(shared_mutex_for_table_id_map_)
        , partition_id_to_logical_id(partition_id_to_logical_id_)
        , keyspace_id(getter_.getKeyspaceID())
        , log(Logger::get(fmt::format("keyspace={}", keyspace_id)))
    {}

    void applyDiff(const SchemaDiff & diff);

    void syncAllSchema();

    void dropAllSchema();

    void applyTable(DatabaseID database_id, TableID logical_table_id, TableID physical_table_id);

private:
    void applyDropSchema(DatabaseID schema_id);

    /// Parameter db_name should be mapped.
    void applyDropSchema(const String & db_name);

    bool applyCreateSchema(DatabaseID schema_id);

    void applyCreateSchema(const TiDB::DBInfoPtr & db_info);

    void applyCreatePhysicalTable(const TiDB::DBInfoPtr & db_info, const TiDB::TableInfoPtr & table_info);

    void applyDropTable(DatabaseID database_id, TableID table_id);

    void applyRecoverTable(DatabaseID database_id, TiDB::TableID table_id);

    void applyRecoverPhysicalTable(const TiDB::DBInfoPtr & db_info, const TiDB::TableInfoPtr & table_info);

    /// Parameter schema_name should be mapped.
    void applyDropPhysicalTable(const String & db_name, TableID table_id);

    void applyPartitionDiff(DatabaseID database_id, TableID table_id);

    void applyPartitionDiff(const TiDB::DBInfoPtr & db_info, const TiDB::TableInfoPtr & table_info, const ManageableStoragePtr & storage);

    void applyRenameTable(DatabaseID database_id, TiDB::TableID table_id);

    void applyRenameLogicalTable(const TiDB::DBInfoPtr & new_db_info, const TiDB::TableInfoPtr & new_table_info, const ManageableStoragePtr & storage);

    void applyRenamePhysicalTable(const TiDB::DBInfoPtr & new_db_info, const TiDB::TableInfo & new_table_info, const ManageableStoragePtr & storage);

    void applySetTiFlashReplica(DatabaseID database_id, TableID table_id);

    // not safe for concurrent use, please acquire shared_mutex_for_table_id_map lock first
    void emplacePartitionTableID(TableID partition_id, TableID table_id);
    void emplaceTableID(TableID table_id, DatabaseID database_id);

    void applyCreateTable(DatabaseID database_id, TableID table_id);

    void applyExchangeTablePartition(const SchemaDiff & diff);
};

} // namespace DB
