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
#include <Common/Stopwatch.h>
#include <Debug/MockSchemaGetter.h>
#include <Debug/MockSchemaNameMapper.h>
#include <TiDB/Schema/SchemaBuilder.h>
#include <TiDB/Schema/TiDB.h>
#include <pingcap/kv/Cluster.h>
#include <pingcap/kv/Snapshot.h>

#include <ext/scope_guard.h>

namespace DB
{
using KVClusterPtr = std::shared_ptr<pingcap::kv::Cluster>;
namespace ErrorCodes
{
extern const int FAIL_POINT_ERROR;
};

/// The schema syncer for given keyspace
template <bool mock_getter, bool mock_mapper>
class TiDBSchemaSyncer : public SchemaSyncer
{
    using Getter = std::conditional_t<mock_getter, MockSchemaGetter, SchemaGetter>;

    using NameMapper = std::conditional_t<mock_mapper, MockSchemaNameMapper, SchemaNameMapper>;

private:
    KVClusterPtr cluster;

    const KeyspaceID keyspace_id;

    Int64 cur_version;

    // for syncSchemas
    std::mutex mutex_for_sync_schema;

    // mutex for databases
    std::shared_mutex shared_mutex_for_databases;

    std::unordered_map<DB::DatabaseID, TiDB::DBInfoPtr> databases;

    LoggerPtr log;

    TableIDMap table_id_map;

    Getter createSchemaGetter(KeyspaceID keyspace_id)
    {
        [[maybe_unused]] auto tso = cluster->pd_client->getTS();
        if constexpr (mock_getter)
        {
            return Getter();
        }
        else
        {
            return Getter(cluster.get(), tso, keyspace_id);
        }
    }

    std::tuple<bool, DatabaseID, TableID> findDatabaseIDAndTableID(TableID physical_table_id);

public:
    TiDBSchemaSyncer(KVClusterPtr cluster_, KeyspaceID keyspace_id_)
        : cluster(std::move(cluster_))
        , keyspace_id(keyspace_id_)
        , cur_version(0)
        , log(Logger::get(fmt::format("keyspace={}", keyspace_id)))
        , table_id_map(log)
    {}

    /*
     * Sync all tables' schemas based on schema diff. This method mainly update the TableID mapping of this keyspace.
     */
    bool syncSchemas(Context & context) override;

    /*
     * Sync the table's inner schema(like add columns, modify columns, etc) for given physical_table_id
     * This function will be called concurrently when the schema not matches during reading or writing
     */
    bool syncTableSchema(Context & context, TableID physical_table_id) override;

    /*
     * When the table is physically dropped from the TiFlash node, use this method to unregister
     * the TableID mapping.
     */
    void removeTableID(TableID table_id) override { table_id_map.erase(table_id); }

private:
    Int64 syncSchemaDiffs(Context & context, Getter & getter, Int64 latest_version);
    Int64 syncAllSchemas(Context & context, Getter & getter, Int64 version);

    TiDB::DBInfoPtr getDBInfoByName(const String & database_name) override
    {
        std::shared_lock<std::shared_mutex> lock(shared_mutex_for_databases);

        auto it = std::find_if(databases.begin(), databases.end(), [&](const auto & pair) {
            return pair.second->name == database_name;
        });
        if (it == databases.end())
            return nullptr;
        return it->second;
    }

    TiDB::DBInfoPtr getDBInfoByMappedName(const String & mapped_database_name) override
    {
        std::shared_lock<std::shared_mutex> lock(shared_mutex_for_databases);

        auto it = std::find_if(databases.begin(), databases.end(), [&](const auto & pair) {
            return NameMapper().mapDatabaseName(*pair.second) == mapped_database_name;
        });
        if (it == databases.end())
            return nullptr;
        return it->second;
    }

    void dropAllSchema(Context & context) override
    {
        auto getter = createSchemaGetter(keyspace_id);
        SchemaBuilder<Getter, NameMapper> builder(getter, context, databases, table_id_map, shared_mutex_for_databases);
        builder.dropAllSchema();
    }

    // clear all states.
    // just for testing restart
    void reset() override
    {
        {
            std::unique_lock<std::shared_mutex> lock(shared_mutex_for_databases);
            databases.clear();
        }

        table_id_map.clear();
        cur_version = 0;
    }
};

} // namespace DB
