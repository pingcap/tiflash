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

#include <TiDB/Schema/TableIDMap.h>
#include <common/logger_useful.h>

namespace DB
{

void TableIDMap::doEmplaceTableID(
    TableID table_id,
    DatabaseID database_id,
    std::string_view log_prefix,
    const std::unique_lock<std::shared_mutex> &)
{
    if (auto iter = table_id_to_database_id.find(table_id); //
        iter != table_id_to_database_id.end())
    {
        if (iter->second != database_id)
        {
            LOG_WARNING(
                log,
                "{}table_id to database_id is being overwrite, table_id={}"
                " old_database_id={} new_database_id={}",
                log_prefix,
                table_id,
                iter->second,
                database_id);
            iter->second = database_id;
        }
    }
    else
        table_id_to_database_id.emplace(table_id, database_id);
}

void TableIDMap::doEmplacePartitionTableID(
    TableID partition_id,
    TableID table_id,
    std::string_view log_prefix,
    const std::unique_lock<std::shared_mutex> &)
{
    if (auto iter = partition_id_to_logical_id.find(partition_id); //
        iter != partition_id_to_logical_id.end())
    {
        if (iter->second != table_id)
        {
            LOG_WARNING(
                log,
                "{}partition_id to table_id is being overwrite, physical_table_id={}"
                " old_logical_table_id={} new_logical_table_id={}",
                log_prefix,
                partition_id,
                iter->second,
                table_id);
            iter->second = table_id;
        }
    }
    else
        partition_id_to_logical_id.emplace(partition_id, table_id);
}

void TableIDMap::exchangeTablePartition(
    DatabaseID non_partition_database_id,
    TableID non_partition_table_id,
    DatabaseID /*partition_database_id*/,
    TableID partition_logical_table_id,
    TableID partition_physical_table_id)
{
    // Change all under the same lock
    std::unique_lock lock(mtx_id_mapping);
    // erase the non partition table
    if (auto iter = table_id_to_database_id.find(non_partition_table_id); iter != table_id_to_database_id.end())
        table_id_to_database_id.erase(iter);
    else
        LOG_WARNING(
            log,
            "ExchangeTablePartition: non partition table not in table_id_to_database_id, table_id={}",
            non_partition_table_id);

    // make the partition table to be a non-partition table
    doEmplaceTableID(partition_physical_table_id, non_partition_database_id, "ExchangeTablePartition: ", lock);

    // remove the partition table to logical table mapping
    if (auto iter = partition_id_to_logical_id.find(partition_physical_table_id);
        iter != partition_id_to_logical_id.end())
    {
        partition_id_to_logical_id.erase(iter);
    }
    else
    {
        LOG_WARNING(
            log,
            "ExchangeTablePartition: partition table not in partition_id_to_logical_id, physical_table_id={}",
            partition_physical_table_id);
    }

    // make the non partition table as a partition to logical table
    doEmplacePartitionTableID(non_partition_table_id, partition_logical_table_id, "ExchangeTablePartition: ", lock);
}

std::tuple<bool, DatabaseID, TableID> TableIDMap::findDatabaseIDAndLogicalTableID(TableID physical_table_id) const
{
    std::shared_lock<std::shared_mutex> lock(mtx_id_mapping);
    DatabaseID database_id = -1;
    if (auto database_iter = table_id_to_database_id.find(physical_table_id);
        database_iter != table_id_to_database_id.end())
    {
        database_id = database_iter->second;
        // This is a non-partition table or the logical_table of partition table.
        return {true, database_id, physical_table_id};
    }

    /// if we can't find physical_table_id in table_id_to_database_id,
    /// we should first try to find it in partition_id_to_logical_id because it could be the pysical_table_id of partition tables
    TableID logical_table_id = -1;
    if (auto logical_table_iter = partition_id_to_logical_id.find(physical_table_id);
        logical_table_iter != partition_id_to_logical_id.end())
    {
        logical_table_id = logical_table_iter->second;
        // try to get the database_id of logical_table_id
        if (auto database_iter = table_id_to_database_id.find(logical_table_id);
            database_iter != table_id_to_database_id.end())
        {
            database_id = database_iter->second;
            // This is a non-partition table or the logical_table of partition table.
            return {true, database_id, logical_table_id};
        }
    }
    return {false, 0, 0};
}

std::map<TableID, DatabaseID> TableIDMap::getAllPartitionsBelongDatabase() const
{
    std::shared_lock lock(mtx_id_mapping);
    std::map<TableID, DatabaseID> part_to_db;
    for (const auto & [part_id, logical_table_id] : partition_id_to_logical_id)
    {
        auto iter = table_id_to_database_id.find(logical_table_id);
        if (iter != table_id_to_database_id.end())
        {
            part_to_db[part_id] = iter->second;
            continue;
        }
        // something wrong happen
        LOG_WARNING(
            log,
            "Can not find logical_table_id to database_id for partition, physical_table_id={} "
            "logical_table_id={}",
            part_id,
            logical_table_id);
    }
    return part_to_db;
}

} // namespace DB
