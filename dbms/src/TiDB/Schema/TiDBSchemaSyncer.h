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

#include <Common/Logger.h>
#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <Debug/MockSchemaGetter.h>
#include <Debug/MockSchemaNameMapper.h>
#include <Storages/Transaction/TiDB.h>
#include <TiDB/Schema/SchemaBuilder.h>
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

template <bool mock_getter, bool mock_mapper>
class TiDBSchemaSyncer : public SchemaSyncer
{
    using Getter = std::conditional_t<mock_getter, MockSchemaGetter, SchemaGetter>;

    using NameMapper = std::conditional_t<mock_mapper, MockSchemaNameMapper, SchemaNameMapper>;

private:
    KVClusterPtr cluster;

    KeyspaceID keyspace_id;

    Int64 cur_version;

    // for syncSchemas
    std::mutex mutex_for_sync_schema;

    // mutex for databases
    std::shared_mutex shared_mutex_for_databases;

    std::unordered_map<DB::DatabaseID, TiDB::DBInfoPtr> databases;

    // mutex for table_id_to_database_id and partition_id_to_logical_id;
    std::shared_mutex shared_mutex_for_table_id_map;

    std::unordered_map<DB::TableID, DB::DatabaseID> table_id_to_database_id;

    /// we have to store partition_id --> logical_id here,
    /// otherwise, when the first written to a partition table, we can't get the table_info based on its table_id
    std::unordered_map<DB::TableID, DB::TableID> partition_id_to_logical_id;

    LoggerPtr log;

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
    {}

    Int64 syncSchemaDiffs(Context & context, Getter & getter, Int64 latest_version);

    bool syncSchemas(Context & context) override;

    Int64 syncAllSchemas(Context & context, Getter & getter, Int64 version);

    bool syncTableSchema(Context & context, TableID physical_table_id) override;

    void removeTableID(TableID table_id) override
    {
        std::unique_lock<std::shared_mutex> lock(shared_mutex_for_table_id_map);
        auto it = table_id_to_database_id.find(table_id);
        if (it == table_id_to_database_id.end())
        {
            LOG_WARNING(log, "table_id {} is already moved in schemaSyncer", table_id);
        }
        else
        {
            table_id_to_database_id.erase(it);
        }

        if (partition_id_to_logical_id.find(table_id) != partition_id_to_logical_id.end())
        {
            partition_id_to_logical_id.erase(table_id);
        }
    }

    TiDB::DBInfoPtr getDBInfoByName(const String & database_name) override
    {
        std::shared_lock<std::shared_mutex> lock(shared_mutex_for_databases);

        auto it = std::find_if(databases.begin(), databases.end(), [&](const auto & pair) { return pair.second->name == database_name; });
        if (it == databases.end())
            return nullptr;
        return it->second;
    }

    TiDB::DBInfoPtr getDBInfoByMappedName(const String & mapped_database_name) override
    {
        std::shared_lock<std::shared_mutex> lock(shared_mutex_for_databases);

        auto it = std::find_if(databases.begin(), databases.end(), [&](const auto & pair) { return NameMapper().mapDatabaseName(*pair.second) == mapped_database_name; });
        if (it == databases.end())
            return nullptr;
        return it->second;
    }

    void dropAllSchema(Context & context) override
    {
        auto getter = createSchemaGetter(keyspace_id);
        SchemaBuilder<Getter, NameMapper> builder(getter, context, databases, table_id_to_database_id, partition_id_to_logical_id, shared_mutex_for_table_id_map, shared_mutex_for_databases);
        builder.dropAllSchema();
    }

    // just for test
    void reset() override
    {
        std::unique_lock<std::shared_mutex> lock(shared_mutex_for_databases);
        databases.clear();

        std::unique_lock<std::shared_mutex> lock_table(shared_mutex_for_table_id_map);
        table_id_to_database_id.clear();
        partition_id_to_logical_id.clear();
        cur_version = 0;
    }
};

} // namespace DB
