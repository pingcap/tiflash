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

#include <Common/Logger.h>
#include <Storages/KVStore/Types.h>

#include <shared_mutex>
#include <unordered_map>

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

    // Return all partition_ids' belonging to the database.
    // Note that the normal table or the logical table of partitioned table is excluded.
    std::map<TableID, DatabaseID> getAllPartitionsBelongDatabase() const;

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
} // namespace DB
