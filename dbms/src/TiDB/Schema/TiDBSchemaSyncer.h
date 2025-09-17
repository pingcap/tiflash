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
#include <Debug/MockSchemaGetter.h>
#include <Debug/MockSchemaNameMapper.h>
#include <TiDB/Schema/DatabaseInfoCache.h>
#include <TiDB/Schema/TableIDMap.h>
#include <pingcap/kv/Cluster.h>

#include <ext/scope_guard.h>

namespace DB
{
using KVClusterPtr = std::shared_ptr<pingcap::kv::Cluster>;

/// The schema syncer for given keyspace
template <bool mock_getter, bool mock_mapper>
class TiDBSchemaSyncer : public SchemaSyncer
{
    using Getter = std::conditional_t<mock_getter, MockSchemaGetter, SchemaGetter>;

    using NameMapper = std::conditional_t<mock_mapper, MockSchemaNameMapper, SchemaNameMapper>;

private:
    // The KV cluster to get schema info from TiKV cluster
    KVClusterPtr cluster;

    const KeyspaceID keyspace_id;
    LoggerPtr log;

    // Ensure `syncSchemasByGetter` will only executed by one thread.
    std::mutex mutex_for_sync_schema;
    // The current applied schema version. Should only accessed in `syncSchemasByGetter` and protected by `mutex_for_sync_schema`
    Int64 cur_version;

    // Cache of DatabaseID -> DatabaseInfo in this keyspace
    DatabaseInfoCache databases;
    // Cache of TableIDMap in this keyspace
    TableIDMap table_id_map;

public:
    TiDBSchemaSyncer(KVClusterPtr cluster_, KeyspaceID keyspace_id_)
        : cluster(std::move(cluster_))
        , keyspace_id(keyspace_id_)
        , log(Logger::get(fmt::format("keyspace={}", keyspace_id)))
        , cur_version(0)
        , table_id_map(log)
    {}

    /*
     * Sync all tables' schemas based on schema diff. This method mainly update the TableID mapping of this keyspace.
     */
    bool syncSchemas(Context & context) override;

    /*
     * Sync the table's inner schema(like add columns, modify columns, etc) for given physical_table_id
     * This function could be called concurrently when the schema not matches during reading or writing
     */
    bool syncTableSchema(Context & context, TableID physical_table_id) override;

    /*
     * When the table is physically dropped from the TiFlash node, use this method to unregister
     * the TableID mapping.
     */
    void removeTableID(TableID table_id) override { table_id_map.erase(table_id); }

    /**
      * Drop all schema of a given keyspace.
      * When a keyspace is removed, drop all its databases and tables.
      */
    void dropAllSchema(Context & context) override;

    /*
     * Clear all states.
     * just for testing restart
     */
    void reset() override
    {
        databases.clear();
        table_id_map.clear();
        cur_version = 0;
    }

    TiDB::DBInfoPtr getDBInfoByName(const String & database_name) override
    {
        return databases.getDBInfoByName(database_name);
    }

private:
    Getter createSchemaGetter(KeyspaceID keyspace_id);
    bool syncSchemasByGetter(Context & context, Getter & getter);

    Int64 syncSchemaDiffs(Context & context, Getter & getter, Int64 before_sync_diff_version, Int64 latest_version);
    Int64 syncAllSchemas(Context & context, Getter & getter, Int64 version);

    std::tuple<bool, String> trySyncTableSchema(
        Context & context,
        TableID physical_table_id,
        Getter & getter,
        bool force,
        const char * next_action);
};

} // namespace DB
