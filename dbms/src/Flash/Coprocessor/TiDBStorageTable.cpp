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

#include <Common/TiFlashMetrics.h>
#include <Flash/Coprocessor/TiDBStorageTable.h>
#include <Interpreters/Context.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>
#include <fmt/format.h>

namespace DB
{
TiDBStorageTable::TiDBStorageTable(
    const tipb::Executor * table_scan_,
    const String & executor_id_,
    Context & context_,
    const String & req_id)
    : context(context_)
    , tmt(context.getTMTContext())
    , log(Logger::get("TiDBStorageTable", req_id))
    , tidb_table_scan(table_scan_, executor_id_, *context.getDAGContext())
    , storages_with_structure_lock(getAndLockStorages(tidb_table_scan))
    , storage_for_logical_table(getStorage(tidb_table_scan.getLogicalTableID()))
    , schema(getSchemaForTableScan(tidb_table_scan))
{}

void TiDBStorageTable::releaseAlterLock()
{
    RUNTIME_ASSERT(!alter_locks_released, log, "alter locks has been released.");
    // The DeltaTree engine ensures that once input streams are created, the caller can get a consistent result
    // from those streams even if DDL operations are applied. Release the alter lock so that reading does not
    // block DDL operations, keep the drop lock so that the storage not to be dropped during reading.
    for (auto storage_with_lock : storages_with_structure_lock)
    {
        drop_locks.emplace_back(std::get<1>(std::move(storage_with_lock.second.lock).release()));
    }
    alter_locks_released = true;
}

const TableLockHolders & TiDBStorageTable::getDropLocks() const
{
    RUNTIME_ASSERT(alter_locks_released, log, "alter locks has not been released.");
    return drop_locks;
}

const ManageableStoragePtr & TiDBStorageTable::getStorage(TableID table_id) const
{
    auto it = storages_with_structure_lock.find(table_id);
    RUNTIME_ASSERT(it != storages_with_structure_lock.end(), log, "can't find storage for table_id: {}.", table_id);
    return it->second.storage;
}

IDsAndStorageWithStructureLocks TiDBStorageTable::getAndLockStorages(const TiDBTableScan & table_scan)
{
    Int64 query_schema_version = context.getSettingsRef().schema_version;
    auto logical_table_id = table_scan.getLogicalTableID();
    IDsAndStorageWithStructureLocks storages_with_lock;
    if (unlikely(query_schema_version == DEFAULT_UNSPECIFIED_SCHEMA_VERSION))
    {
        auto logical_table_storage = tmt.getStorages().get(logical_table_id);
        if (!logical_table_storage)
        {
            throw TiFlashException(fmt::format("Table {} doesn't exist.", logical_table_id), Errors::Table::NotExists);
        }
        storages_with_lock[logical_table_id] = {logical_table_storage, logical_table_storage->lockStructureForShare(context.getCurrentQueryId())};
        if (table_scan.isPartitionTableScan())
        {
            for (auto const physical_table_id : table_scan.getPhysicalTableIDs())
            {
                auto physical_table_storage = tmt.getStorages().get(physical_table_id);
                if (!physical_table_storage)
                {
                    throw TiFlashException(fmt::format("Table {} doesn't exist.", physical_table_id), Errors::Table::NotExists);
                }
                storages_with_lock[physical_table_id] = {physical_table_storage, physical_table_storage->lockStructureForShare(context.getCurrentQueryId())};
            }
        }
        return storages_with_lock;
    }

    auto global_schema_version = tmt.getSchemaSyncer()->getCurrentVersion();

    /// Align schema version under the read lock.
    /// Return: [storage, table_structure_lock, storage_schema_version, ok]
    auto get_and_lock_storage = [&](bool schema_synced, TableID table_id) -> std::tuple<ManageableStoragePtr, TableStructureLockHolder, Int64, bool> {
        /// Get storage in case it's dropped then re-created.
        // If schema synced, call getTable without try, leading to exception on table not existing.
        auto table_store = tmt.getStorages().get(table_id);
        if (!table_store)
        {
            if (schema_synced)
                throw TiFlashException(fmt::format("Table {} doesn't exist.", table_id), Errors::Table::NotExists);
            else
                return {{}, {}, {}, false};
        }

        if (table_store->engineType() != ::TiDB::StorageEngine::TMT && table_store->engineType() != ::TiDB::StorageEngine::DT)
        {
            throw TiFlashException(
                fmt::format(
                    "Specifying schema_version for non-managed storage: {}, table: {}, id: {} is not allowed",
                    table_store->getName(),
                    table_store->getTableName(),
                    table_id),
                Errors::Coprocessor::Internal);
        }

        auto lock = table_store->lockStructureForShare(context.getCurrentQueryId());

        /// Check schema version, requiring TiDB/TiSpark and TiFlash both use exactly the same schema.
        // We have three schema versions, two in TiFlash:
        // 1. Storage: the version that this TiFlash table (storage) was last altered.
        // 2. Global: the version that TiFlash global schema is at.
        // And one from TiDB/TiSpark:
        // 3. Query: the version that TiDB/TiSpark used for this query.
        auto storage_schema_version = table_store->getTableInfo().schema_version;
        // Not allow storage > query in any case, one example is time travel queries.
        if (storage_schema_version > query_schema_version)
            throw TiFlashException(
                fmt::format("Table {} schema version {} newer than query schema version {}", table_id, storage_schema_version, query_schema_version),
                Errors::Table::SchemaVersionError);
        // From now on we have storage <= query.
        // If schema was synced, it implies that global >= query, as mentioned above we have storage <= query, we are OK to serve.
        if (schema_synced)
            return {table_store, lock, storage_schema_version, true};
        // From now on the schema was not synced.
        // 1. storage == query, TiDB/TiSpark is using exactly the same schema that altered this table, we are just OK to serve.
        // 2. global >= query, TiDB/TiSpark is using a schema older than TiFlash global, but as mentioned above we have storage <= query,
        // meaning that the query schema is still newer than the time when this table was last altered, so we are still OK to serve.
        if (storage_schema_version == query_schema_version || global_schema_version >= query_schema_version)
            return {table_store, lock, storage_schema_version, true};
        // From now on we have global < query.
        // Return false for outer to sync and retry.
        return {nullptr, {}, DEFAULT_UNSPECIFIED_SCHEMA_VERSION, false};
    };

    auto get_and_lock_storages = [&](bool schema_synced) -> std::tuple<std::vector<ManageableStoragePtr>, std::vector<TableStructureLockHolder>, std::vector<Int64>, bool> {
        std::vector<ManageableStoragePtr> table_storages;
        std::vector<TableStructureLockHolder> table_locks;
        std::vector<Int64> table_schema_versions;
        auto [logical_table_storage, logical_table_lock, logical_table_storage_schema_version, ok] = get_and_lock_storage(schema_synced, logical_table_id);
        if (!ok)
            return {{}, {}, {}, false};
        table_storages.emplace_back(std::move(logical_table_storage));
        table_locks.emplace_back(std::move(logical_table_lock));
        table_schema_versions.push_back(logical_table_storage_schema_version);
        if (!table_scan.isPartitionTableScan())
        {
            return {table_storages, table_locks, table_schema_versions, true};
        }
        for (auto const physical_table_id : table_scan.getPhysicalTableIDs())
        {
            auto [physical_table_storage, physical_table_lock, physical_table_storage_schema_version, ok] = get_and_lock_storage(schema_synced, physical_table_id);
            if (!ok)
            {
                return {{}, {}, {}, false};
            }
            table_storages.emplace_back(std::move(physical_table_storage));
            table_locks.emplace_back(std::move(physical_table_lock));
            table_schema_versions.push_back(physical_table_storage_schema_version);
        }
        return {table_storages, table_locks, table_schema_versions, true};
    };

    auto log_schema_version = [&](const String & result, const std::vector<Int64> & storage_schema_versions) {
        FmtBuffer buffer;
        buffer.fmtAppend("Table {} schema {} Schema version [storage, global, query]: [{}, {}, {}]", logical_table_id, result, storage_schema_versions[0], global_schema_version, query_schema_version);
        if (table_scan.isPartitionTableScan())
        {
            assert(storage_schema_versions.size() == 1 + table_scan.getPhysicalTableIDs().size());
            for (size_t i = 0; i < table_scan.getPhysicalTableIDs().size(); ++i)
            {
                const auto physical_table_id = table_scan.getPhysicalTableIDs()[i];
                buffer.fmtAppend(", Table {} schema {} Schema version [storage, global, query]: [{}, {}, {}]", physical_table_id, result, storage_schema_versions[1 + i], global_schema_version, query_schema_version);
            }
        }
        return buffer.toString();
    };

    auto sync_schema = [&] {
        auto start_time = Clock::now();
        GET_METRIC(tiflash_schema_trigger_count, type_cop_read).Increment();
        tmt.getSchemaSyncer()->syncSchemas(context);
        auto schema_sync_cost = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start_time).count();
        LOG_FMT_INFO(log, "Table {} schema sync cost {}ms.", logical_table_id, schema_sync_cost);
    };

    /// Try get storage and lock once.
    auto [storages, locks, storage_schema_versions, ok] = get_and_lock_storages(false);
    if (ok)
    {
        LOG_FMT_INFO(log, "{}", log_schema_version("OK, no syncing required.", storage_schema_versions));
    }
    else
    /// If first try failed, sync schema and try again.
    {
        LOG_FMT_INFO(log, "not OK, syncing schemas.");

        sync_schema();

        std::tie(storages, locks, storage_schema_versions, ok) = get_and_lock_storages(true);
        if (ok)
        {
            LOG_FMT_INFO(log, "{}", log_schema_version("OK after syncing.", storage_schema_versions));
        }
        else
            throw TiFlashException("Shouldn't reach here", Errors::Coprocessor::Internal);
    }
    for (size_t i = 0; i < storages.size(); ++i)
    {
        auto const table_id = storages[i]->getTableInfo().id;
        storages_with_lock[table_id] = {std::move(storages[i]), std::move(locks[i])};
    }
    return storages_with_lock;
}

NamesAndTypes TiDBStorageTable::getSchemaForTableScan(const TiDBTableScan & table_scan)
{
    Int64 max_columns_to_read = context.getSettingsRef().max_columns_to_read;
    // todo handle alias column
    if (max_columns_to_read && table_scan.getColumnSize() > max_columns_to_read)
    {
        throw TiFlashException(
            fmt::format("Limit for number of columns to read exceeded. Requested: {}, maximum: {}", table_scan.getColumnSize(), max_columns_to_read),
            Errors::BroadcastJoin::TooManyColumns);
    }

    NamesAndTypes schema_columns;
    String handle_column_name = MutableSupport::tidb_pk_column_name;
    if (auto pk_handle_col = storage_for_logical_table->getTableInfo().getPKHandleColumn())
        handle_column_name = pk_handle_col->get().name;

    for (Int32 i = 0; i < table_scan.getColumnSize(); ++i)
    {
        auto const & ci = table_scan.getColumns()[i];
        ColumnID cid = ci.column_id();

        // Column ID -1 return the handle column
        String name;
        if (cid == TiDBPkColumnID)
            name = handle_column_name;
        else if (cid == ExtraTableIDColumnID)
            name = MutableSupport::extra_table_id_column_name;
        else
            name = storage_for_logical_table->getTableInfo().getColumnName(cid);
        if (cid == ExtraTableIDColumnID)
        {
            NameAndTypePair extra_table_id_column_pair = {name, MutableSupport::extra_table_id_column_type};
            schema_columns.emplace_back(std::move(extra_table_id_column_pair));
        }
        else
        {
            auto pair = storage_for_logical_table->getColumns().getPhysical(name);
            schema_columns.emplace_back(std::move(pair));
        }
    }

    return schema_columns;
}
} // namespace DB