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
    explicit TableIDMap(const LoggerPtr & log_)
        : log(log_)
    {}

    void erase(DB::TableID table_id)
    {
        std::unique_lock lock(mtx_id_mapping);
        table_id_to_database_id.erase(table_id);
        partition_id_to_logical_id.erase(table_id);
    }

    void clear()
    {
        std::unique_lock lock(mtx_id_mapping);
        table_id_to_database_id.clear();
        partition_id_to_logical_id.clear();
    }

    void emplaceTableID(TableID table_id, DatabaseID database_id)
    {
        std::unique_lock lock(mtx_id_mapping);
        doEmplaceTableID(table_id, database_id, "", lock);
    }

    void emplacePartitionTableID(TableID partition_id, TableID table_id)
    {
        std::unique_lock lock(mtx_id_mapping);
        doEmplacePartitionTableID(partition_id, table_id, "", lock);
    }

    void exchangeTablePartition(
        DatabaseID non_partition_database_id,
        TableID non_partition_table_id,
        DatabaseID partition_database_id,
        TableID partition_logical_table_id,
        TableID partition_physical_table_id);

    std::vector<TableID> findTablesByDatabaseID(DatabaseID database_id) const
    {
        std::shared_lock lock(mtx_id_mapping);
        std::vector<TableID> tables;
        for (const auto & table_id : table_id_to_database_id)
        {
            if (table_id.second == database_id)
            {
                tables.emplace_back(table_id.first);
            }
        }
        return tables;
    }

    bool tableIDInTwoMaps(TableID table_id) const
    {
        std::shared_lock<std::shared_mutex> lock(mtx_id_mapping);
        return !(
            table_id_to_database_id.find(table_id) == table_id_to_database_id.end()
            && partition_id_to_logical_id.find(table_id) == partition_id_to_logical_id.end());
    }

    bool tableIDInDatabaseIdMap(TableID table_id) const
    {
        std::shared_lock<std::shared_mutex> lock(mtx_id_mapping);
        return !(table_id_to_database_id.find(table_id) == table_id_to_database_id.end());
    }

    // if not find，than return -1
    DatabaseID findTableIDInDatabaseMap(TableID table_id) const
    {
        std::shared_lock<std::shared_mutex> lock(mtx_id_mapping);
        auto database_iter = table_id_to_database_id.find(table_id);
        if (database_iter == table_id_to_database_id.end())
            return -1;

        return database_iter->second;
    }

    // if not find，than return -1
    TableID findTableIDInPartitionMap(TableID partition_id) const
    {
        std::shared_lock<std::shared_mutex> lock(mtx_id_mapping);
        auto logical_table_iter = partition_id_to_logical_id.find(partition_id);
        if (logical_table_iter == partition_id_to_logical_id.end())
            return -1;

        return logical_table_iter->second;
    }

    std::tuple<bool, DatabaseID, TableID> findDatabaseIDAndLogicalTableID(TableID physical_table_id) const;

private:
    void doEmplaceTableID(
        TableID table_id,
        DatabaseID database_id,
        std::string_view log_prefix,
        const std::unique_lock<std::shared_mutex> &);
    void doEmplacePartitionTableID(
        TableID partition_id,
        TableID table_id,
        std::string_view log_prefix,
        const std::unique_lock<std::shared_mutex> &);

private:
    LoggerPtr log;
    mutable std::shared_mutex mtx_id_mapping;
    std::unordered_map<DB::TableID, DB::DatabaseID> table_id_to_database_id;
    std::unordered_map<DB::TableID, DB::TableID> partition_id_to_logical_id;
};

template <typename Getter, typename NameMapper>
struct SchemaBuilder
{
private:
    NameMapper name_mapper;

    Getter & getter;

    Context & context;

    std::shared_mutex & shared_mutex_for_databases;

    std::unordered_map<DB::DatabaseID, TiDB::DBInfoPtr> & databases;

    TableIDMap & table_id_map;

    const KeyspaceID keyspace_id;

    LoggerPtr log;

public:
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
