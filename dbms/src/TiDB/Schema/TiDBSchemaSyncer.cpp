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

#include <Common/TiFlashMetrics.h>
#include <TiDB/Schema/TiDBSchemaSyncer.h>
#include <common/types.h>

#include <mutex>
#include <shared_mutex>

namespace DB
{
template <bool mock_getter, bool mock_mapper>
bool TiDBSchemaSyncer<mock_getter, mock_mapper>::syncSchemas(Context & context)
{
    std::lock_guard<std::mutex> lock(mutex_for_sync_schema);

    GET_METRIC(tiflash_sync_schema_applying).Increment();

    auto getter = createSchemaGetter(keyspace_id);
    const Int64 version = getter.getVersion();

    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(tiflash_schema_apply_duration_seconds).Observe(watch.elapsedSeconds());
        GET_METRIC(tiflash_sync_schema_applying).Decrement();
    });

    if (version == SchemaGetter::SchemaVersionNotExist)
    {
        // Tables and databases are already tombstoned and waiting for GC.
        if (cur_version == SchemaGetter::SchemaVersionNotExist)
        {
            return false;
        }

        LOG_INFO(log, "Start to drop schemas. schema version key not exists, keyspace should be deleted");

        // The key range of the given keyspace is deleted by `UnsafeDestroyRange`, so the return result
        // of `SchemaGetter::listDBs` is not reliable. Directly mark all databases and tables of this keyspace
        // as a tombstone and let the SchemaSyncService drop them physically.
        dropAllSchema(context);
        cur_version = SchemaGetter::SchemaVersionNotExist;
    }
    else
    {
        if (version <= cur_version)
        {
            return false;
        }

        LOG_INFO(
            log,
            "Start to sync schemas. current version is: {} and try to sync schema version to: {}",
            cur_version,
            version);

        if (cur_version <= 0)
        {
            // first load all db and tables
            Int64 version_after_load_all = syncAllSchemas(context, getter, version);
            cur_version = version_after_load_all;
        }
        else
        {
            // After the feature concurrent DDL, TiDB does `update schema version` before `set schema diff`, and they are done in separate transactions.
            // So TiFlash may see a schema version X but no schema diff X, meaning that the transaction of schema diff X has not been committed or has
            // been aborted.
            // However, TiDB makes sure that if we get a schema version X, then the schema diff X-1 must exist. Otherwise the transaction of schema diff
            // X-1 is aborted and we can safely ignore it.
            // Since TiDB can not make sure the schema diff of the latest schema version X is not empty, under this situation we should set the `cur_version`
            // to X-1 and try to fetch the schema diff X next time.
            Int64 version_after_load_diff = syncSchemaDiffs(context, getter, version);
            if (version_after_load_diff != -1)
            {
                cur_version = version_after_load_diff;
            }
            else
            {
                // when diff->regenerate_schema_map == true, we use syncAllSchemas to reload all schemas
                cur_version = syncAllSchemas(context, getter, version);
            }
        }
    }

    LOG_INFO(
        log,
        "End sync schema, version has been updated to {}{}",
        cur_version,
        cur_version == version ? "" : "(latest diff is empty)");
    return true;
}

template <bool mock_getter, bool mock_mapper>
Int64 TiDBSchemaSyncer<mock_getter, mock_mapper>::syncSchemaDiffs(
    Context & context,
    Getter & getter,
    Int64 latest_version)
{
    Int64 used_version = cur_version;
    // TODO:try to use parallel to speed up
    while (used_version < latest_version)
    {
        used_version++;
        std::optional<SchemaDiff> diff = getter.getSchemaDiff(used_version);

        if (used_version == latest_version && !diff)
        {
            --used_version;
            break;
        }

        if (diff->regenerate_schema_map)
        {
            // If `schema_diff.regenerate_schema_map` == true, return `-1` directly, let TiFlash reload schema info from TiKV.
            LOG_INFO(log, "Meets a schema diff with regenerate_schema_map flag");
            return -1;
        }

        SchemaBuilder<Getter, NameMapper> builder(getter, context, databases, table_id_map, shared_mutex_for_databases);
        builder.applyDiff(*diff);
    }
    return used_version;
}

template <bool mock_getter, bool mock_mapper>
Int64 TiDBSchemaSyncer<mock_getter, mock_mapper>::syncAllSchemas(Context & context, Getter & getter, Int64 version)
{
    if (!getter.checkSchemaDiffExists(version))
    {
        --version;
    }
    SchemaBuilder<Getter, NameMapper> builder(getter, context, databases, table_id_map, shared_mutex_for_databases);
    builder.syncAllSchema();

    return version;
}

template <bool mock_getter, bool mock_mapper>
std::tuple<bool, DatabaseID, TableID> TiDBSchemaSyncer<mock_getter, mock_mapper>::findDatabaseIDAndTableID(
    TableID physical_table_id)
{
    auto database_id = table_id_map.findTableIDInDatabaseMap(physical_table_id);
    TableID logical_table_id = physical_table_id;
    if (database_id == -1)
    {
        /// if we can't find physical_table_id in table_id_to_database_id,
        /// we should first try to find it in partition_id_to_logical_id because it could be the pysical_table_id of partition tables
        logical_table_id = table_id_map.findTableIDInPartitionMap(physical_table_id);
        if (logical_table_id != -1)
            database_id = table_id_map.findTableIDInDatabaseMap(logical_table_id);
    }

    if (database_id != -1 and logical_table_id != -1)
    {
        return std::make_tuple(true, database_id, logical_table_id);
    }

    return std::make_tuple(false, 0, 0);
}


/// we don't need a lock at the beginning of syncTableSchema,
/// we will catch the AlterLock for storage later.
template <bool mock_getter, bool mock_mapper>
bool TiDBSchemaSyncer<mock_getter, mock_mapper>::syncTableSchema(Context & context, TableID physical_table_id)
{
    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(tiflash_schema_apply_duration_seconds, type_sync_table_schema_apply_duration)
            .Observe(watch.elapsedSeconds());
    });

    LOG_INFO(log, "Start sync table schema, table_id={}", physical_table_id);
    auto getter = createSchemaGetter(keyspace_id);

    // get logical_table_id and database_id based on physical_table_id,
    // if the table is a partition table, logical_table_id != physical_table_id, otherwise, logical_table_id = physical_table_id;
    auto [find, database_id, logical_table_id] = findDatabaseIDAndTableID(physical_table_id);
    if (!find)
    {
        LOG_WARNING(
            log,
            "Can't find related database_id and logical_table_id from table_id_map, try to syncSchemas. "
            "physical_table_id={}",
            physical_table_id);
        GET_METRIC(tiflash_schema_trigger_count, type_sync_table_schema).Increment();
        syncSchemas(context);
        std::tie(find, database_id, logical_table_id) = findDatabaseIDAndTableID(physical_table_id);
        if (!find)
        {
            LOG_ERROR(
                log,
                "Still can't find related database_id and logical_table_id from table_id_map, physical_table_id={}",
                physical_table_id);
            return false;
        }
    }

    SchemaBuilder<Getter, NameMapper> builder(getter, context, databases, table_id_map, shared_mutex_for_databases);
    builder.applyTable(database_id, logical_table_id, physical_table_id);

    return true;
}

template class TiDBSchemaSyncer<false, false>;
template class TiDBSchemaSyncer<true, false>;
template class TiDBSchemaSyncer<true, true>;

} // namespace DB
