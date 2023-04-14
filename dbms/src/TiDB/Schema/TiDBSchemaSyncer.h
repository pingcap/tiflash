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

#include "Common/Exception.h"
#include "Common/StackTrace.h"
#include "Common/UniThreadPool.h"
#include "Interpreters/Context.h"
#include "Storages/Transaction/Types.h"
#include "TiDB/Schema/SchemaGetter.h"
#include "common/logger_useful.h"
#include "common/types.h"

namespace std
{
template <>
struct hash<pair<DB::DatabaseID, DB::TableID>>
{
    size_t operator()(const pair<DB::DatabaseID, DB::TableID> & pair) const
    {
        size_t seed = 0;
        boost::hash_combine(seed, boost::hash_value(pair.first));
        boost::hash_combine(seed, boost::hash_value(pair.second));
        return seed;
    }
};
} // namespace std
namespace DB
{
using KVClusterPtr = std::shared_ptr<pingcap::kv::Cluster>;
namespace ErrorCodes
{
extern const int FAIL_POINT_ERROR;
};

using SchemaVerMap = std::unordered_map<KeyspaceID, Int64>;

template <bool mock_getter, bool mock_mapper>
struct TiDBSchemaSyncer : public SchemaSyncer
{
    using Getter = std::conditional_t<mock_getter, MockSchemaGetter, SchemaGetter>;

    using NameMapper = std::conditional_t<mock_mapper, MockSchemaNameMapper, SchemaNameMapper>;

    KVClusterPtr cluster;

    static constexpr Int64 maxNumberOfDiffs = 100;

    SchemaVerMap cur_versions;

    std::mutex schema_mutex;

    KeyspaceDatabaseMap databases;

    LoggerPtr log;

    explicit TiDBSchemaSyncer(KVClusterPtr cluster_)
        : cluster(std::move(cluster_))
        , log(Logger::get())
    {}

    // bool isTooOldSchema(Int64 cur_ver, Int64 new_version) { return cur_ver == 0 || new_version - cur_ver > maxNumberOfDiffs; }
    bool isTooOldSchema(Int64 cur_ver) { return cur_ver == 0;}

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

    // just for test
    // It clear all synced database info and reset the `cur_version` to 0.
    // All info will fetch from the `getter` again the next time
    // `syncSchemas` is call.
    void reset() override
    {
        std::lock_guard lock(schema_mutex);

        databases.clear();
        cur_versions.clear();
    }

    std::vector<TiDB::DBInfoPtr> fetchAllDBs(KeyspaceID keyspace_id) override
    {
        auto getter = createSchemaGetter(keyspace_id);
        return getter.listDBs();
    }

    Int64 getCurrentVersion(KeyspaceID keyspace_id) override
    {
        std::lock_guard lock(schema_mutex);
        auto it = cur_versions.find(keyspace_id);
        if (it == cur_versions.end())
            return 0;
        return it->second;
    }

    // After all tables have been physical dropped, remove the `cur_version` of given keyspace
    void removeCurrentVersion(KeyspaceID keyspace_id) override
    {
        std::lock_guard lock(schema_mutex);
        cur_versions.erase(keyspace_id);
    }

    bool syncSchemas(Context & context, KeyspaceID keyspace_id) override
    {
        std::lock_guard lock(schema_mutex);
        auto ks_log = log->getChild(fmt::format("keyspace={}", keyspace_id));
        auto cur_version = cur_versions.try_emplace(keyspace_id, 0).first->second;
        auto getter = createSchemaGetter(keyspace_id);

        Int64 version = getter.getVersion();

        Stopwatch watch;
        SCOPE_EXIT({ GET_METRIC(tiflash_schema_apply_duration_seconds).Observe(watch.elapsedSeconds()); });

        // Show whether the schema mutex is held for a long time or not.
        GET_METRIC(tiflash_schema_applying).Set(1.0);
        SCOPE_EXIT({ GET_METRIC(tiflash_schema_applying).Set(0.0); });

        // If the schema version not exists, drop all schemas.
        if (version == SchemaGetter::SchemaVersionNotExist)
        {
            // Tables and databases are already tombstoned and waiting for GC.
            if (SchemaGetter::SchemaVersionNotExist == cur_version)
                return false;

            LOG_INFO(ks_log, "Start to drop schemas. schema version key not exists, keyspace should be deleted");
            GET_METRIC(tiflash_schema_apply_count, type_drop_keyspace).Increment();

            // The key range of the given keyspace is deleted by `UnsafeDestroyRange`, so the return result
            // of `SchemaGetter::listDBs` is not reliable. Directly mark all databases and tables of this keyspace
            // as a tombstone and let the SchemaSyncService drop them physically.
            dropAllSchema(getter, context);
            cur_versions[keyspace_id] = SchemaGetter::SchemaVersionNotExist;
        }
        else
        {
            if (version <= cur_version)
                return false;

            LOG_INFO(ks_log, "Start to sync schemas. current version is: {} and try to sync schema version to: {}", cur_version, version);
            GET_METRIC(tiflash_schema_apply_count, type_diff).Increment();

            // After the feature concurrent DDL, TiDB does `update schema version` before `set schema diff`, and they are done in separate transactions.
            // So TiFlash may see a schema version X but no schema diff X, meaning that the transaction of schema diff X has not been committed or has
            // been aborted.
            // However, TiDB makes sure that if we get a schema version X, then the schema diff X-1 must exist. Otherwise the transaction of schema diff
            // X-1 is aborted and we can safely ignore it.
            // Since TiDB can not make sure the schema diff of the latest schema version X is not empty, under this situation we should set the `cur_version`
            // to X-1 and try to fetch the schema diff X next time.
            Int64 version_after_load_diff = 0;
            version_after_load_diff = tryLoadSchemaDiffs(getter, cur_version, version, context, ks_log); 
            while (version_after_load_diff == -1)
            {
                version_after_load_diff = tryLoadSchemaDiffs(getter, cur_version, version, context, ks_log); 
                LOG_ERROR(log, "tryLoadSchemaDiffs Failed");
            }
            cur_versions[keyspace_id] = version_after_load_diff;
        }

        // TODO: (keyspace) attach keyspace id to the metrics.
        GET_METRIC(tiflash_schema_version).Set(cur_version);
        LOG_INFO(ks_log, "End sync schema, version has been updated to {}{}", cur_version, cur_version == version ? "" : "(latest diff is empty)");
        return true;
    }

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

    Int64 getDiffTables(std::vector<std::optional<SchemaDiff>> & diffs,
                        std::unordered_map<std::pair<DatabaseID, TableID>, bool> & apply_tables,
                        std::unordered_map<std::pair<DatabaseID, TableID>, bool> & drop_tables,
                        std::vector<DatabaseID> & create_database_ids,
                        std::vector<DatabaseID> & drop_database_ids)
    {
        // 我们可以先 create databases，然后 apply tables，随后 drop tables 和 drop databases ids
        // 对于同一个table，如果出现了 drop table 的操作，我们就去除他原来在 apply tables 的操作，如果出现了 recover table 的操作，我们就去除他在 drop tables 的操作
        for (const auto & diff : diffs)
        {
            LOG_INFO(DB::Logger::get("hyy"), "diff info is {} with schema_id is {}", static_cast<int>(diff->type), diff->schema_id);
            if (diff.has_value())
            {
                if (diff->regenerate_schema_map)
                {
                    // If `schema_diff.regenerate_schema_map` == true, return `-1` direclty, let TiFlash reload schema info from TiKV.
                    LOG_INFO(log, "Meets a schema diff with regenerate_schema_map flag");
                    return -1;
                }
                switch (diff->type)
                {
                case SchemaActionType::CreateSchema:
                {
                    create_database_ids.push_back(diff->schema_id);
                    break;
                }
                case SchemaActionType::DropSchema:
                {
                    drop_database_ids.push_back(diff->schema_id);
                    break;
                }
                case SchemaActionType::CreateTables:
                case SchemaActionType::RenameTables:
                {
                    for (auto && opt : diff->affected_opts)
                    {
                        std::pair<DatabaseID, TableID> pair(opt.schema_id, opt.table_id);
                        if (apply_tables.find(pair) == apply_tables.end())
                        {
                            apply_tables[pair] = true;
                        }
                    }
                    break;
                }
                case SchemaActionType::RecoverTable:
                {
                    std::pair<DatabaseID, TableID> pair(diff->schema_id, diff->table_id);
                    if (drop_tables.find(pair) != drop_tables.end())
                    {
                        drop_tables.erase(pair);
                    }
                    if (apply_tables.find(pair) == apply_tables.end())
                    {
                        apply_tables[pair] = true;
                    }
                    break;
                }
                case SchemaActionType::DropTable: // TODO:话说我们这边要额外加判断么，比如不应该同个table出现两个drop？
                case SchemaActionType::DropView:
                {
                    std::pair<DatabaseID, TableID> pair(diff->schema_id, diff->table_id);
                    if (drop_tables.find(pair) == drop_tables.end())
                    {
                        drop_tables[pair] = true;
                        // delete item in apply_tables
                        if (apply_tables.find(pair) != apply_tables.end())
                        {
                            apply_tables.erase(pair);
                        }
                    }
                    break;
                }
                case SchemaActionType::TruncateTable: // 等于先删了一个，再建了一个
                {
                    std::pair<DatabaseID, TableID> drop_pair(diff->schema_id, diff->old_table_id);
                    std::pair<DatabaseID, TableID> create_pair(diff->schema_id, diff->table_id);
                    if (drop_tables.find(drop_pair) == drop_tables.end())
                    {
                        drop_tables[drop_pair] = true;
                        // delete item in apply_tables
                        if (apply_tables.find(drop_pair) != apply_tables.end())
                        {
                            apply_tables.erase(drop_pair);
                        }
                    }
                    if (apply_tables.find(create_pair) == apply_tables.end())
                    {
                        apply_tables[create_pair] = true;
                    }
                    break;
                }
                case SchemaActionType::CreateTable:
                case SchemaActionType::AddColumn:
                case SchemaActionType::AddColumns:
                case SchemaActionType::DropColumn:
                case SchemaActionType::DropColumns:
                case SchemaActionType::ModifyColumn:
                case SchemaActionType::SetDefaultValue:
                // Add primary key change primary keys to not null, so it's equal to alter table for tiflash.
                case SchemaActionType::AddPrimaryKey:
                case SchemaActionType::RenameTable:
                case SchemaActionType::AddTablePartition:
                case SchemaActionType::DropTablePartition:
                case SchemaActionType::TruncateTablePartition:
                case SchemaActionType::ActionReorganizePartition:
                case SchemaActionType::SetTiFlashReplica:
                {
                    std::pair<DatabaseID, TableID> pair(diff->schema_id, diff->table_id);
                    if (apply_tables.find(pair) == apply_tables.end())
                    {
                        apply_tables[pair] = true;
                    }
                    break;
                }
                case SchemaActionType::ExchangeTablePartition:
                {
                    // 两问题，一个是 throw exception 跟 log error 在这里有什么本质区别么，2. 为什么会拿不到 table_info??看起来跟同一个 database 有一点关系，不知道具体是什么关系。
                    LOG_INFO(log, "ExchangeTablePartition info diff->schema_id is {}, diff->table_id is {}, diff->affected_opts[0].schema_id is {}, diff->old_table_id is {}, diff->affected_opts[0].table_id is {}", diff->schema_id, diff->table_id, diff->affected_opts[0].schema_id, diff->old_table_id, diff->affected_opts[0].table_id);
                    /// Table_id in diff is the partition id of which will be exchanged,
                    /// Schema_id in diff is the non-partition table's schema id
                    /// Old_table_id in diff is the non-partition table's table id
                    /// Table_id in diff.affected_opts[0] is the table id of the partition table
                    /// Schema_id in diff.affected_opts[0] is the schema id of the partition table
                    //std::pair<DatabaseID, TableID> new_non_partition_pair(diff->schema_id, diff->table_id);
                    std::pair<DatabaseID, TableID> new_non_partition_pair(diff->schema_id, diff->table_id); // 这个就会出问题？？？？（2，89）
                    //std::pair<DatabaseID, TableID> new_partition_pair(diff->affected_opts[0].schema_id, diff->old_table_id);
                    std::pair<DatabaseID, TableID> new_partition_belongs_pair(diff->affected_opts[0].schema_id, diff->affected_opts[0].table_id);
                   // std::pair<DatabaseID, TableID> old_non_partition_pair(diff->schema_id, diff->old_table_id);//？再考虑过
                    //std::pair<DatabaseID, TableID> partition_pair(diff->affected_opts[0].schema_id, diff->old_table_id);
                    if (apply_tables.find(new_non_partition_pair) == apply_tables.end())
                    {
                        apply_tables[new_non_partition_pair] = true;
                    }
                    // if (apply_tables.find(new_partition_pair) == apply_tables.end())
                    // {
                    //     apply_tables[new_partition_pair] = true;
                    // }
                    if (apply_tables.find(new_partition_belongs_pair) == apply_tables.end())
                    {
                        apply_tables[new_partition_belongs_pair] = true;
                    }

                    // if (drop_tables.find(old_non_partition_pair) == drop_tables.end())
                    // {
                    //     drop_tables[old_non_partition_pair] = true;
                    //     // delete item in apply_tables
                    //     if (apply_tables.find(old_non_partition_pair) != apply_tables.end())
                    //     {
                    //         apply_tables.erase(old_non_partition_pair);
                    //     }
                    // }
                
                    break;
                }
                default:
                {
                    if (diff->type < SchemaActionType::MaxRecognizedType)
                    {
                        LOG_INFO(log, "Ignore change type: {}", int(diff->type));
                    }
                    else
                    { // >= SchemaActionType::MaxRecognizedType
                        LOG_ERROR(log, "Unsupported change type: {}", int(diff->type));
                    }

                    break;
                }
                }
            }
        }

        return 0;
    }
    // Return Values
    // - if latest schema diff is not empty, return the (latest_version)
    // - if latest schema diff is empty, return the (latest_version - 1)
    // - if schema_diff.regenerate_schema_map == true, need reload all schema info from TiKV, return (-1)
    // - if error happens, return (-1)
    Int64 tryLoadSchemaDiffs(Getter & getter, Int64 cur_version, Int64 latest_version, Context & context, const LoggerPtr & ks_log)
    {
        if (cur_version == 0)
        {
            loadAllSchema(getter, cur_version, context);
        }

        LOG_DEBUG(ks_log, "Try load schema diffs.");

        Int64 used_version = cur_version;
        // First get all schema diff from `cur_version` to `latest_version`. Only apply the schema diff(s) if we fetch all
        // schema diff without any exception.
        std::vector<std::optional<SchemaDiff>> diffs;
        while (used_version < latest_version)
        {
            used_version++;
            diffs.push_back(getter.getSchemaDiff(used_version));
        }
        LOG_DEBUG(ks_log, "End load schema diffs with total {} entries.", diffs.size());
        if (diffs.empty())
        {
            LOG_WARNING(ks_log, "Schema Diff is empty.");
            return -1;
        }
        // Since the latest schema diff may be empty, and schemaBuilder may need to update the latest version for storageDeltaMerge,
        // Thus we need check whether latest schema diff is empty or not before begin to builder.applyDiff.
        if (!diffs.back())
        {
            --used_version;
            diffs.pop_back();
        }

        std::unordered_map<std::pair<DatabaseID, TableID>, bool> apply_tables;
        std::unordered_map<std::pair<DatabaseID, TableID>, bool> drop_tables;
        std::vector<DatabaseID> create_database_ids;
        std::vector<DatabaseID> drop_database_ids;
        // 根据 diffs 整理一波 database_id, table_id 的 pairs
        auto ret = getDiffTables(diffs, apply_tables, drop_tables, create_database_ids, drop_database_ids);
        if (ret == -1)
        {
            return ret;
        }

        LOG_INFO(log, "apply_tables size is {}, drop_tables size is {}, create_database_ids size is {}, drop_database_ids size is {}",
            apply_tables.size(), drop_tables.size(), create_database_ids.size(), drop_database_ids.size());

        SchemaBuilder<Getter, NameMapper> builder(getter, context, databases, used_version);

        // size_t default_num_threads = std::max(4UL, std::thread::hardware_concurrency()) * context.getSettingsRef().init_thread_count_scale;

        // auto schema_apply_thread_pool = ThreadPool(default_num_threads, default_num_threads / 2, default_num_threads * 2);

        try
        {
            // 并发做 create databases
            // 并发处理 apply tables
            // 并发处理 drop tables
            // 并发处理 drop databases
            // create databases
            {
                //auto schema_apply_wait_group = schema_apply_thread_pool.waitGroup();
                for (const auto & create_database_id : create_database_ids)
                {
                    LOG_INFO(DB::Logger::get("hyy"), "create database {}", create_database_id);
                    //schema_apply_wait_group->schedule([&] { builder.applyCreateSchema(create_database_id); });
                    builder.applyCreateSchema(create_database_id); 
                }

                //schema_apply_wait_group->wait();
            }

            {
                // apply tables
                //auto schema_apply_wait_group = schema_apply_thread_pool.waitGroup();
                for (const auto & [key, value] : apply_tables)
                {
                    const auto & database_id = key.first;
                    const auto & table_id = key.second;
                    LOG_INFO(DB::Logger::get("hyy"), "apply tables with database_id:{}, table_id:{}", database_id, table_id);
                    //schema_apply_wait_group->schedule([&] { builder.applyVariousDiff(database_id, table_id); });
                    builder.applyVariousDiff(database_id, table_id);
                }
                //schema_apply_wait_group->wait();
            }
            
            {
                //auto schema_apply_wait_group = schema_apply_thread_pool.waitGroup();
                // drop tables
                for (const auto & [key, value] : drop_tables)
                {
                    const auto & database_id = key.first;
                    const auto & table_id = key.second;
                    //schema_apply_wait_group->schedule([&] { builder.applyDropTable(database_id, table_id); });
                    builder.applyDropTable(database_id, table_id);
                }

                //schema_apply_wait_group->wait();
            }
            
            {
                //auto schema_apply_wait_group = schema_apply_thread_pool.waitGroup();
                // drop databases
                for (const auto & drop_database_id : drop_database_ids)
                {
                    //schema_apply_wait_group->schedule([&] { builder.applyDropSchema(drop_database_id); });
                    builder.applyDropSchema(drop_database_id);
                }

                //schema_apply_wait_group->wait();
            }
            
        }
        catch (TiFlashException & e)
        {
            if (!e.getError().is(Errors::DDL::StaleSchema))
            {
                GET_METRIC(tiflash_schema_apply_count, type_failed).Increment();
            }
            LOG_ERROR(log, "apply diff meets exception : {} \n stack is {}", e.displayText(), e.getStackTrace().toString());
            return -1;
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::FAIL_POINT_ERROR)
            {
                throw;
            }
            GET_METRIC(tiflash_schema_apply_count, type_failed).Increment();
            LOG_ERROR(ks_log, "apply diff meets exception : {} \n stack is {}", e.displayText(), e.getStackTrace().toString());
            return -1;
        }
        catch (Poco::Exception & e)
        {
            GET_METRIC(tiflash_schema_apply_count, type_failed).Increment();
            LOG_ERROR(ks_log, "apply diff meets exception : {}", e.displayText());
            return -1;
        }
        catch (std::exception & e)
        {
            GET_METRIC(tiflash_schema_apply_count, type_failed).Increment();
            LOG_ERROR(ks_log, "apply diff meets exception : {}", e.what());
            return -1;
        }

        return used_version;
    }

    Int64 loadAllSchema(Getter & getter, Int64 version, Context & context)
    {
        if (!getter.checkSchemaDiffExists(version))
        {
            --version;
        }
        SchemaBuilder<Getter, NameMapper> builder(getter, context, databases, version);
        builder.syncAllSchema();
        return version;
    }

    void dropAllSchema(Getter & getter, Context & context)
    {
        SchemaBuilder<Getter, NameMapper> builder(getter, context, databases, -1);
        builder.dropAllSchema();
    }
};

} // namespace DB
