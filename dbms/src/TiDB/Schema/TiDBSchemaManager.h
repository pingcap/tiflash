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

#include <TiDB/Schema/TiDBSchemaSyncer.h>

namespace DB {

class TiDBSchemaSyncerManager {
public:
    explicit TiDBSchemaSyncerManager(KVClusterPtr cluster_, bool mock_getter_, bool mock_mapper_) : 
            cluster(cluster_)
            ,mock_getter(mock_getter_)
            ,mock_mapper(mock_mapper_) {}

    SchemaSyncerPtr getSchemaSyncer(KeyspaceID keyspace_id){
        auto syncer = schema_syncers.find(keyspace_id);
        return syncer == schema_syncers.end() ? nullptr : syncer->second;
    }

    SchemaSyncerPtr createSchemaSyncer(KeyspaceID keyspace_id) {
        if (!mock_getter and !mock_mapper) {
            auto schema_syncer = std::static_pointer_cast<SchemaSyncer>(std::make_shared<TiDBSchemaSyncer<false, false>>(cluster, keyspace_id));
            schema_syncers[keyspace_id] = schema_syncer ;
            return schema_syncer;
        } else if (mock_getter and mock_mapper) {
            // for mock test
            auto schema_syncer = std::static_pointer_cast<SchemaSyncer>(std::make_shared<TiDBSchemaSyncer<true, true>>(cluster, keyspace_id));
            schema_syncers[keyspace_id] = schema_syncer ;
            return schema_syncer;
        }
        
        // for unit test
        auto schema_syncer = std::static_pointer_cast<SchemaSyncer>(std::make_shared<TiDBSchemaSyncer<true, false>>(cluster, keyspace_id));
        schema_syncers[keyspace_id] = schema_syncer ;
        return schema_syncer;
    }

    bool syncSchemas(Context & context, KeyspaceID keyspace_id){
        std::cout << " begin syncSchemas " << std::endl;
        // 先暴力加 unique lock
        std::unique_lock<std::shared_mutex> lock(schema_syncers_mutex);
        auto schema_syncer = getSchemaSyncer(keyspace_id);
        if (schema_syncer == nullptr) {
            schema_syncer = createSchemaSyncer(keyspace_id);
        }
        return schema_syncer->syncSchemas(context);
    }

    // TODO:是不是这一层也要加锁感觉，不然是不是会出问题？
    bool syncTableSchema(Context & context, KeyspaceID keyspace_id, TableID table_id){
        std::cout << " begin syncTableSchema " << std::endl;
        std::unique_lock<std::shared_mutex> lock(schema_syncers_mutex);
        auto schema_syncer = getSchemaSyncer(keyspace_id);
        if (schema_syncer == nullptr) {
            schema_syncer = createSchemaSyncer(keyspace_id);
        }
        return schema_syncer->syncTableSchema(context, table_id);
    }

    void reset(KeyspaceID keyspace_id){
        std::shared_lock<std::shared_mutex> lock(schema_syncers_mutex);
        auto schema_syncer = getSchemaSyncer(keyspace_id);
        if (schema_syncer == nullptr) {
            LOG_ERROR(Logger::get("TiDBSchemaSyncerManager"), "SchemaSyncer not found for keyspace_id: {}", keyspace_id);
            return;
        }
        schema_syncer->reset();
    }

    // TODO:那返回地方要处理 nullptr
    TiDB::DBInfoPtr getDBInfoByName(KeyspaceID keyspace_id, const String & database_name){
        std::shared_lock<std::shared_mutex> lock(schema_syncers_mutex);
        auto schema_syncer = getSchemaSyncer(keyspace_id);
        if (schema_syncer == nullptr) {
            LOG_ERROR(Logger::get("TiDBSchemaSyncerManager"), "SchemaSyncer not found for keyspace_id: {}", keyspace_id);
            return nullptr;
        }
        return schema_syncer->getDBInfoByName(database_name);
    }

    // TODO:那返回地方要处理 nullptr
    TiDB::DBInfoPtr getDBInfoByMappedName(KeyspaceID keyspace_id, const String & mapped_database_name)
    {
        std::shared_lock<std::shared_mutex> lock(schema_syncers_mutex);
        auto schema_syncer = getSchemaSyncer(keyspace_id);
        if (schema_syncer == nullptr) {
            //schema_syncer = createSchemaSyncer(keyspace_id);
            LOG_ERROR(Logger::get("TiDBSchemaSyncerManager"), "SchemaSyncer not found for keyspace_id: {}", keyspace_id);
            return nullptr;
        }
        return schema_syncer->getDBInfoByMappedName(mapped_database_name);
    }

    bool removeSchemaSyncer(KeyspaceID keyspace_id) {
        std::unique_lock<std::shared_mutex> lock(schema_syncers_mutex);
        auto schema_syncer = getSchemaSyncer(keyspace_id);
        if (schema_syncer == nullptr) {
            LOG_ERROR(Logger::get("TiDBSchemaSyncerManager"), "SchemaSyncer not found for keyspace_id: {}", keyspace_id);
            return false;
        }
        schema_syncers.erase(keyspace_id);
        return true;
    }

    void removeTableID(KeyspaceID keyspace_id, TableID table_id) {
        std::shared_lock<std::shared_mutex> lock(schema_syncers_mutex);
        auto schema_syncer = getSchemaSyncer(keyspace_id);
        if (schema_syncer == nullptr) {
            LOG_ERROR(Logger::get("TiDBSchemaSyncerManager"), "SchemaSyncer not found for keyspace_id: {}", keyspace_id);
        }
        schema_syncer->removeTableID(table_id);
    }

private:
    std::shared_mutex schema_syncers_mutex;

    KVClusterPtr cluster;

    const bool mock_getter;
    const bool mock_mapper;

    std::unordered_map<KeyspaceID, SchemaSyncerPtr> schema_syncers;
};
}