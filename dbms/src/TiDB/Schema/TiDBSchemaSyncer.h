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

    std::mutex schema_mutex; // mutex for ?

    Int64 cur_version;

    std::unordered_map<DB::DatabaseID, TiDB::DBInfoPtr> databases; // 这个什么时候会用到呢

    std::shared_mutex shared_mutex_for_table_id_map; // mutex for table_id_to_database_id and partition_id_to_logical_id;

    std::unordered_map<DB::TableID, DB::DatabaseID> table_id_to_database_id;

    std::unordered_map<DB::TableID, DB::TableID> partition_id_to_logical_id; // 这个我们只存分区表的对应关系，不存这个的话，如果分区表写入的时候，你只知道分表的 table_id，没有 table_info 的时候会拿不到 tableInfo

    LoggerPtr log;

    explicit TiDBSchemaSyncer(KVClusterPtr cluster_, KeyspaceID keyspace_id_)
        : cluster(std::move(cluster_))
        , keyspace_id(keyspace_id_)
        , cur_version(0)
        , log(Logger::get(fmt::format("keyspace={}", keyspace_id)))
    {}

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

    std::vector<TiDB::DBInfoPtr> fetchAllDBs(KeyspaceID keyspace_id) override
    {
        auto getter = createSchemaGetter(keyspace_id);
        return getter.listDBs();
    }

public:
    Int64 syncSchemaDiffs(Context & context, Getter & getter, Int64 latest_version);

    bool syncSchemas(Context & context) override;

    // just use when cur_version = 0
    bool syncAllSchemas(Context & context, Getter & getter, Int64 version);

    bool syncTableSchema(Context & context, TableID table_id_) override;

    TiDB::DBInfoPtr getDBInfoByName(const String & database_name) override
    {
        std::lock_guard lock(schema_mutex);

        auto it = std::find_if(databases.begin(), databases.end(), [&](const auto & pair) { return pair.second->name == database_name; });
        if (it == databases.end())
            return nullptr;
        return it->second;
    }

    TiDB::DBInfoPtr getDBInfoByMappedName(const String & mapped_database_name) override
    {
        std::lock_guard lock(schema_mutex);

        auto it = std::find_if(databases.begin(), databases.end(), [&](const auto & pair) { return NameMapper().mapDatabaseName(*pair.second) == mapped_database_name; });
        if (it == databases.end())
            return nullptr;
        return it->second;
    }

    void dropAllSchema(Context & context) override
    {
        auto getter = createSchemaGetter(keyspace_id);
        SchemaBuilder<Getter, NameMapper> builder(getter, context, databases, -1);
        builder.dropAllSchema();
    }
};

} // namespace DB
