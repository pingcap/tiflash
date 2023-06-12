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
    auto getter = createSchemaGetter(keyspace_id);
    Int64 version = getter.getVersion();

    Stopwatch watch;
    SCOPE_EXIT({ GET_METRIC(tiflash_schema_apply_duration_seconds).Observe(watch.elapsedSeconds()); });

    if (version == SchemaGetter::SchemaVersionNotExist)
    {
        // Tables and databases are already tombstoned and waiting for GC.
        if (cur_version == SchemaGetter::SchemaVersionNotExist)
        {
            return false;
        }

        LOG_INFO(log, "Start to drop schemas. schema version key not exists, keyspace should be deleted");
        GET_METRIC(tiflash_schema_apply_count, type_drop_keyspace).Increment();

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
            LOG_INFO(log, " version {} is the same as cur_version {}, so do nothing", version, cur_version);
            return false;
        }

        LOG_INFO(log, "Start to sync schemas. current version is: {} and try to sync schema version to: {}", cur_version, version);
        GET_METRIC(tiflash_schema_apply_count, type_diff).Increment();

        if (cur_version <= 0)
        {
            // first load all db and tables
            Int64 version_after_load_all = syncAllSchemas(context, getter, version);

            cur_version = version_after_load_all;
            GET_METRIC(tiflash_schema_apply_count, type_full).Increment();
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

    LOG_INFO(log, "End sync schema, version has been updated to {}{}", cur_version, cur_version == version ? "" : "(latest diff is empty)");
    return true;
}

template <bool mock_getter, bool mock_mapper>
Int64 TiDBSchemaSyncer<mock_getter, mock_mapper>::syncSchemaDiffs(Context & context, Getter & getter, Int64 latest_version)
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

        if (!diff)
        {
            LOG_ERROR(log, "Diff in version {} is empty", used_version);
            continue;
        }

        if (diff->regenerate_schema_map)
        {
            // If `schema_diff.regenerate_schema_map` == true, return `-1` direclty, let TiFlash reload schema info from TiKV.
            LOG_INFO(log, "Meets a schema diff with regenerate_schema_map flag");
            return -1;
        }

        SchemaBuilder<Getter, NameMapper> builder(getter, context, databases, table_id_to_database_id, partition_id_to_logical_id, shared_mutex_for_table_id_map, shared_mutex_for_databases);
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
    SchemaBuilder<Getter, NameMapper> builder(getter, context, databases, table_id_to_database_id, partition_id_to_logical_id, shared_mutex_for_table_id_map, shared_mutex_for_databases);
    builder.syncAllSchema();

    return version;
}

template <bool mock_getter, bool mock_mapper>
std::tuple<bool, DatabaseID, TableID> TiDBSchemaSyncer<mock_getter, mock_mapper>::findDatabaseIDAndTableID(TableID table_id_)
{
    std::shared_lock<std::shared_mutex> lock(shared_mutex_for_table_id_map);

    auto database_iter = table_id_to_database_id.find(table_id_);
    DatabaseID database_id;
    TableID table_id = table_id_;
    bool find = false;
    if (database_iter == table_id_to_database_id.end())
    {
        /// if we can't find table_id in table_id_to_database_id,
        /// we should first try to find it in partition_id_to_logical_id because it could be the pysical table_id of partition tables
        auto logical_table_iter = partition_id_to_logical_id.find(table_id_);
        if (logical_table_iter != partition_id_to_logical_id.end())
        {
            table_id = logical_table_iter->second;
            database_iter = table_id_to_database_id.find(table_id);
            if (database_iter != table_id_to_database_id.end())
            {
                database_id = database_iter->second;
                find = true;
            }
        }
    }
    else
    {
        database_id = database_iter->second;
        find = true;
    }

    if (find)
    {
        return std::make_tuple(true, database_id, table_id);
    }

    return std::make_tuple(false, 0, 0);
}

/// Help: do we need a lock for syncTableSchema for each table?
/// I roughly think we don't need a lock here, because we will catch the lock for storage later.
/// but I'm not quite sure.
template <bool mock_getter, bool mock_mapper>
bool TiDBSchemaSyncer<mock_getter, mock_mapper>::syncTableSchema(Context & context, TableID physical_table_id)
{
    LOG_INFO(log, "Start sync table schema, table_id: {}", physical_table_id);
    auto getter = createSchemaGetter(keyspace_id);

    // get logical_table_id and database_id based on physical_table_id,
    // if the table is a partition table, logical_table_id != physical_table_id, otherwise, logical_table_id = physical_table_id;
    auto [find, database_id, logical_table_id] = findDatabaseIDAndTableID(physical_table_id);
    if (!find)
    {
        LOG_WARNING(log, "Can't find table_id {} in table_id_to_database_id and map partition_id_to_logical_id, try to syncSchemas", physical_table_id);
        syncSchemas(context);
        std::tie(find, database_id, logical_table_id) = findDatabaseIDAndTableID(physical_table_id);
        if (!find)
        {
            LOG_ERROR(log, "Still can't find table_id {} in table_id_to_database_id and map partition_id_to_logical_id", physical_table_id);
            return false;
        }
    }

    SchemaBuilder<Getter, NameMapper> builder(getter, context, databases, table_id_to_database_id, partition_id_to_logical_id, shared_mutex_for_table_id_map, shared_mutex_for_databases);
    builder.applyTable(database_id, logical_table_id, physical_table_id);

    return true;
}

template class TiDBSchemaSyncer<false, false>;
template class TiDBSchemaSyncer<true, false>;
template class TiDBSchemaSyncer<true, true>;

} // namespace DB