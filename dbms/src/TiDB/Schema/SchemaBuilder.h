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
#include <TiDB/Schema/SchemaGetter.h>

namespace DB
{
/// TableIDMap use to store the mapping between table_id -> database_id and partition_id -> logical_table_id
struct TableIDMap
{
    LoggerPtr & log;
    std::shared_mutex shared_mutex_for_table_id_map;
    std::unordered_map<DB::TableID, DB::DatabaseID> table_id_to_database_id;
    std::unordered_map<DB::TableID, DB::TableID> partition_id_to_logical_id;

    explicit TableIDMap(LoggerPtr & log_)
        : log(log_)
    {}

    void erase(DB::TableID table_id)
    {
        std::unique_lock lock(shared_mutex_for_table_id_map);
        table_id_to_database_id.erase(table_id);
        partition_id_to_logical_id.erase(table_id);
    }

    void clear()
    {
        std::unique_lock lock(shared_mutex_for_table_id_map);
        table_id_to_database_id.clear();
        partition_id_to_logical_id.clear();
    }

    void emplaceTableID(TableID table_id, DatabaseID database_id)
    {
        std::unique_lock lock(shared_mutex_for_table_id_map);
        if (table_id_to_database_id.find(table_id) != table_id_to_database_id.end())
        {
            LOG_WARNING(log, "table {} is already exists in table_id_to_database_id ", table_id);
            table_id_to_database_id[table_id] = database_id;
        }
        else
            table_id_to_database_id.emplace(table_id, database_id);
    }

    void emplacePartitionTableID(TableID partition_id, TableID table_id)
    {
        std::unique_lock lock(shared_mutex_for_table_id_map);
        if (partition_id_to_logical_id.find(partition_id) != partition_id_to_logical_id.end())
        {
            LOG_WARNING(log, "partition id {} is already exists in partition_id_to_logical_id ", partition_id);
            partition_id_to_logical_id[partition_id] = table_id;
        }
        else
            partition_id_to_logical_id.emplace(partition_id, table_id);
    }

    void eraseTableIDOrLogError(TableID table_id)
    {
        std::unique_lock lock(shared_mutex_for_table_id_map);
        if (table_id_to_database_id.find(table_id) != table_id_to_database_id.end())
            table_id_to_database_id.erase(table_id);
        else
            LOG_ERROR(log, "table_id {} not in table_id_to_database_id", table_id);
    }

    void erasePartitionTableIDOrLogError(TableID table_id)
    {
        std::unique_lock lock(shared_mutex_for_table_id_map);
        if (partition_id_to_logical_id.find(table_id) != partition_id_to_logical_id.end())
        {
            partition_id_to_logical_id.erase(table_id);
        }
        else
        {
            LOG_ERROR(log, "table_id {} not in partition_id_to_logical_id", table_id);
        }
    }

    std::vector<TableID> findTablesByDatabaseID(DatabaseID database_id)
    {
        std::shared_lock lock(shared_mutex_for_table_id_map);
        std::vector<TableID> tables;
        for (auto & table_id : table_id_to_database_id)
        {
            if (table_id.second == database_id)
            {
                tables.emplace_back(table_id.first);
            }
        }
        return tables;
    }

    bool tableIDInTwoMaps(TableID table_id)
    {
        std::shared_lock<std::shared_mutex> lock(shared_mutex_for_table_id_map);
        return !(
            table_id_to_database_id.find(table_id) == table_id_to_database_id.end()
            && partition_id_to_logical_id.find(table_id) == partition_id_to_logical_id.end());
    }

    bool tableIDInDatabaseIdMap(TableID table_id)
    {
        std::shared_lock<std::shared_mutex> lock(shared_mutex_for_table_id_map);
        return !(table_id_to_database_id.find(table_id) == table_id_to_database_id.end());
    }

    // if not find，than return -1
    DatabaseID findTableIDInDatabaseMap(TableID table_id)
    {
        std::shared_lock<std::shared_mutex> lock(shared_mutex_for_table_id_map);
        auto database_iter = table_id_to_database_id.find(table_id);
        if (database_iter == table_id_to_database_id.end())
            return -1;

        return database_iter->second;
    }

    // if not find，than return -1
    TableID findTableIDInPartitionMap(TableID partition_id)
    {
        std::shared_lock<std::shared_mutex> lock(shared_mutex_for_table_id_map);
        auto logical_table_iter = partition_id_to_logical_id.find(partition_id);
        if (logical_table_iter == partition_id_to_logical_id.end())
            return -1;

        return logical_table_iter->second;
    }
};

template <typename Getter, typename NameMapper>
struct SchemaBuilder
{
    NameMapper name_mapper;

    Getter & getter;

    Context & context;

    std::shared_mutex & shared_mutex_for_databases;

    std::unordered_map<DB::DatabaseID, TiDB::DBInfoPtr> & databases;

    TableIDMap & table_id_map;

    const KeyspaceID keyspace_id;

    LoggerPtr log;

    SchemaBuilder(
        Getter & getter_,
        Context & context_,
        std::unordered_map<DB::DatabaseID, TiDB::DBInfoPtr> & dbs_,
        TableIDMap & table_id_map_,
        std::shared_mutex & shared_mutex_for_databases_)
        : getter(getter_)
        , context(context_)
        , shared_mutex_for_databases(shared_mutex_for_databases_)
        , databases(dbs_)
        , table_id_map(table_id_map_)
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
