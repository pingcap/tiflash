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
struct TiDBSchemaSyncer : public SchemaSyncer
{
    using Getter = std::conditional_t<mock_getter, MockSchemaGetter, SchemaGetter>;

    using NameMapper = std::conditional_t<mock_mapper, MockSchemaNameMapper, SchemaNameMapper>;

    KVClusterPtr cluster;

    static constexpr Int64 maxNumberOfDiffs = 100;

    Int64 cur_version;

    std::mutex schema_mutex;

    std::unordered_map<DB::DatabaseID, TiDB::DBInfoPtr> databases;

    LoggerPtr log;

    explicit TiDBSchemaSyncer(KVClusterPtr cluster_)
        : cluster(std::move(cluster_))
        , cur_version(0)
        , log(Logger::get())
    {}

    bool isTooOldSchema(Int64 cur_ver, Int64 new_version) { return cur_ver == 0 || new_version - cur_ver > maxNumberOfDiffs; }

    Getter createSchemaGetter()
    {
        if constexpr (mock_getter)
        {
            return Getter();
        }
        else
        {
            auto tso = PDClientHelper::getTSO(cluster->pd_client, PDClientHelper::get_tso_maxtime);
            return Getter(cluster.get(), tso);
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
        cur_version = 0;
    }

    std::vector<TiDB::DBInfoPtr> fetchAllDBs() override
    {
        auto getter = createSchemaGetter();
        return getter.listDBs();
    }

    Int64 getCurrentVersion() override
    {
        std::lock_guard lock(schema_mutex);
        return cur_version;
    }

    bool syncSchemas(Context & context) override
    {
        std::lock_guard lock(schema_mutex);

        auto getter = createSchemaGetter();
        Int64 version = getter.getVersion();
        if (version <= cur_version)
        {
            return false;
        }
        Stopwatch watch;
        SCOPE_EXIT({ GET_METRIC(tiflash_schema_apply_duration_seconds).Observe(watch.elapsedSeconds()); });

        LOG_INFO(log, "Start to sync schemas. current version is: {} and try to sync schema version to: {}", cur_version, version);

        // Show whether the schema mutex is held for a long time or not.
        GET_METRIC(tiflash_schema_applying).Set(1.0);
        SCOPE_EXIT({ GET_METRIC(tiflash_schema_applying).Set(0.0); });

        GET_METRIC(tiflash_schema_apply_count, type_diff).Increment();
        // After the feature concurrent DDL, TiDB does `update schema version` before `set schema diff`, and they are done in separate transactions.
        // So TiFlash may see a schema version X but no schema diff X, meaning that the transaction of schema diff X has not been committed or has
        // been aborted.
        // However, TiDB makes sure that if we get a schema version X, then the schema diff X-1 must exist. Otherwise the transaction of schema diff
        // X-1 is aborted and we can safely ignore it.
        // Since TiDB can not make sure the schema diff of the latest schema version X is not empty, under this situation we should set the `cur_version`
        // to X-1 and try to fetch the schema diff X next time.
        Int64 version_after_load_diff = 0;
        if (version_after_load_diff = tryLoadSchemaDiffs(getter, version, context); version_after_load_diff == -1)
        {
            GET_METRIC(tiflash_schema_apply_count, type_full).Increment();
            version_after_load_diff = loadAllSchema(getter, version, context);
        }
        cur_version = version_after_load_diff;
        GET_METRIC(tiflash_schema_version).Set(cur_version);
        LOG_INFO(log, "End sync schema, version has been updated to {}{}", cur_version, cur_version == version ? "" : "(latest diff is empty)");
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

    // Return Values
    // - if latest schema diff is not empty, return the (latest_version)
    // - if latest schema diff is empty, return the (latest_version - 1)
    // - if error happend, return (-1)
    Int64 tryLoadSchemaDiffs(Getter & getter, Int64 latest_version, Context & context)
    {
        if (isTooOldSchema(cur_version, latest_version))
        {
            return -1;
        }

        LOG_DEBUG(log, "Try load schema diffs.");

        Int64 used_version = cur_version;
        // First get all schema diff from `cur_version` to `latest_version`. Only apply the schema diff(s) if we fetch all
        // schema diff without any exception.
        std::vector<std::optional<SchemaDiff>> diffs;
        while (used_version < latest_version)
        {
            used_version++;
            diffs.push_back(getter.getSchemaDiff(used_version));
        }
        LOG_DEBUG(log, "End load schema diffs with total {} entries.", diffs.size());


        if (diffs.empty())
        {
            LOG_WARNING(log, "Schema Diff is empty.");
            return -1;
        }
        // Since the latest schema diff may be empty, and schemaBuilder may need to update the latest version for storageDeltaMerge,
        // Thus we need check whether latest schema diff is empty or not before begin to builder.applyDiff.
        if (!diffs.back())
        {
            --used_version;
            diffs.pop_back();
        }

        SchemaBuilder<Getter, NameMapper> builder(getter, context, databases, used_version);

        try
        {
            for (size_t diff_index = 0; diff_index < diffs.size(); ++diff_index)
            {
                const auto & schema_diff = diffs[diff_index];

                if (!schema_diff)
                {
                    // If `schema diff` got empty `schema diff`(it's not the latest one, due to we check it before), we should just skip it.
                    //
                    // example:
                    //  - `cur_version` is 1, `latest_version` is 10
                    //  - The schema diff of schema version [2,4,6] is empty, Then we just skip it.
                    //  - The schema diff of schema version 10 is empty, Then we should just apply version into 9(which we check it before)
                    LOG_WARNING(log, "Skip the schema diff from version {}. ", cur_version + diff_index + 1);
                    continue;
                }
                builder.applyDiff(*schema_diff);
            }
        }
        catch (TiFlashException & e)
        {
            if (!e.getError().is(Errors::DDL::StaleSchema))
            {
                GET_METRIC(tiflash_schema_apply_count, type_failed).Increment();
            }
            LOG_WARNING(log, "apply diff meets exception : {} \n stack is {}", e.displayText(), e.getStackTrace().toString());
            return -1;
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::FAIL_POINT_ERROR)
            {
                throw;
            }
            GET_METRIC(tiflash_schema_apply_count, type_failed).Increment();
            LOG_WARNING(log, "apply diff meets exception : {} \n stack is {}", e.displayText(), e.getStackTrace().toString());
            return -1;
        }
        catch (Poco::Exception & e)
        {
            GET_METRIC(tiflash_schema_apply_count, type_failed).Increment();
            LOG_WARNING(log, "apply diff meets exception : {}", e.displayText());
            return -1;
        }
        catch (std::exception & e)
        {
            GET_METRIC(tiflash_schema_apply_count, type_failed).Increment();
            LOG_WARNING(log, "apply diff meets exception : {}", e.what());
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
};

} // namespace DB
