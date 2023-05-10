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

    std::shared_mutex shared_mutex_for_databases; // mutex for databases?

    std::unordered_map<DB::DatabaseID, TiDB::DBInfoPtr> databases; // 这个什么时候会用到呢

    std::shared_mutex shared_mutex_for_table_id_map; // mutex for table_id_to_database_id and partition_id_to_logical_id;

    std::unordered_map<DB::TableID, DB::DatabaseID> table_id_to_database_id;

    std::unordered_map<DB::TableID, DB::TableID> partition_id_to_logical_id; // 这个我们只存分区表的对应关系，不存这个的话，如果分区表写入的时候，你只知道分表的 table_id，没有 table_info 的时候会拿不到 tableInfo

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

    std::vector<TiDB::DBInfoPtr> fetchAllDBs(KeyspaceID keyspace_id) override
    {
        auto getter = createSchemaGetter(keyspace_id);
        return getter.listDBs();
    }

public:
    TiDBSchemaSyncer(KVClusterPtr cluster_, KeyspaceID keyspace_id_)
        : cluster(std::move(cluster_))
        , keyspace_id(keyspace_id_)
        , cur_version(0)
        , log(Logger::get(fmt::format("keyspace={}", keyspace_id)))
    {}

    Int64 syncSchemaDiffs(Context & context, Getter & getter, Int64 latest_version);

    // background 的逻辑本身可以保证同时一个keyspace 只会有一个 线程在做 syncSchema，所以 syncSchema 本身不需要加锁来避免多个同时跑
    // 不过 syncSchemas 可以跟 syncTableSchema 一起跑么？
    // syncSchema 主要是更新两个 map，特定 ddl 会更新表本身。syncTableSchema 主要是更新表本身。
    // 因为 map 和 表本身都各自上锁，应该能保证两个并行跑也不会出问题。不过都要在改 map 和 改表前做确定，do only once，不要多次重复
    // TODO:目前拍脑袋觉得是可以一起跑的，但是后面还是要看看有没有什么 corner case
    bool syncSchemas(Context & context) override;

    // just use when cur_version = 0
    bool syncAllSchemas(Context & context, Getter & getter, Int64 version);

    bool syncTableSchema(Context & context, TableID table_id_) override;

    void removeTableID(TableID table_id) override {
        shared_mutex_for_table_id_map.lock();
        auto it = table_id_to_database_id.find(table_id);
        if (it == table_id_to_database_id.end()) {
            LOG_ERROR(log, "table_id {} is already moved in schemaSyncer", table_id);
        }
        else {
            table_id_to_database_id.erase(it);
        }
        
        if (partition_id_to_logical_id.find(table_id) != partition_id_to_logical_id.end()) {
            partition_id_to_logical_id.erase(table_id);
        }
        shared_mutex_for_table_id_map.unlock();
    }

    TiDB::DBInfoPtr getDBInfoByName(const String & database_name) override
    {
        std::shared_lock<std::shared_mutex> lock(shared_mutex_for_databases);
        lock.lock();

        auto it = std::find_if(databases.begin(), databases.end(), [&](const auto & pair) { return pair.second->name == database_name; });
        if (it == databases.end())
            return nullptr;
        return it->second;
    }

    TiDB::DBInfoPtr getDBInfoByMappedName(const String & mapped_database_name) override
    {
        std::shared_lock<std::shared_mutex> lock(shared_mutex_for_databases);
        lock.lock();

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
        lock.lock();
        databases.clear();

        shared_mutex_for_table_id_map.lock();
        table_id_to_database_id.clear();
        partition_id_to_logical_id.clear();
        shared_mutex_for_table_id_map.unlock();
    }
};

} // namespace DB
