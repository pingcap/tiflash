#pragma once

#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <Debug/MockSchemaGetter.h>
#include <Debug/MockSchemaNameMapper.h>
#include <Storages/Transaction/SchemaBuilder.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiDB.h>
#include <pingcap/kv/Cluster.h>
#include <pingcap/kv/Snapshot.h>

#include <ext/scope_guard.h>

namespace DB
{
template <bool mock_getter>
struct TiDBSchemaSyncer : public SchemaSyncer
{
    using Getter = std::conditional_t<mock_getter, MockSchemaGetter, SchemaGetter>;

    using NameMapper = std::conditional_t<mock_getter, MockSchemaNameMapper, SchemaNameMapper>;

    KVClusterPtr cluster;

    const Int64 maxNumberOfDiffs = 100;

    Int64 cur_version;

    std::mutex schema_mutex;

    std::unordered_map<DB::DatabaseID, TiDB::DBInfoPtr> databases;

    Poco::Logger * log;

    TiDBSchemaSyncer(KVClusterPtr cluster_)
        : cluster(cluster_)
        , cur_version(0)
        , log(&Poco::Logger::get("SchemaSyncer"))
    {}

    bool isTooOldSchema(Int64 cur_ver, Int64 new_version) { return cur_ver == 0 || new_version - cur_ver > maxNumberOfDiffs; }

    Getter createSchemaGetter()
    {
        [[maybe_unused]] auto tso = cluster->pd_client->getTS();
        if constexpr (mock_getter)
        {
            return Getter();
        }
        else
        {
            return Getter(cluster.get(), tso);
        }
    }

    // just for test
    void reset() override
    {
        std::lock_guard<std::mutex> lock(schema_mutex);

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
        std::lock_guard<std::mutex> lock(schema_mutex);
        return cur_version;
    }

    bool syncSchemas(Context & context) override
    {
        std::lock_guard<std::mutex> lock(schema_mutex);

        auto getter = createSchemaGetter();
        Int64 version = getter.getVersion();
        if (version <= cur_version)
        {
            return false;
        }
        Stopwatch watch;
        SCOPE_EXIT({ GET_METRIC(tiflash_schema_apply_duration_seconds).Observe(watch.elapsedSeconds()); });

        LOG_INFO(log,
                 "start to sync schemas. current version is: " + std::to_string(cur_version)
                     + " and try to sync schema version to: " + std::to_string(version));

        // Show whether the schema mutex is held for a long time or not.
        GET_METRIC(tiflash_schema_applying).Set(1.0);
        SCOPE_EXIT({ GET_METRIC(tiflash_schema_applying).Set(0.0); });

        GET_METRIC(tiflash_schema_apply_count, type_diff).Increment();
        if (!tryLoadSchemaDiffs(getter, version, context))
        {
            GET_METRIC(tiflash_schema_apply_count, type_full).Increment();
            loadAllSchema(getter, version, context);
        }
        cur_version = version;
        GET_METRIC(tiflash_schema_version).Set(cur_version);
        LOG_INFO(log, "end sync schema, version has been updated to " + std::to_string(cur_version));
        return true;
    }

    TiDB::DBInfoPtr getDBInfoByName(const String & database_name) override
    {
        std::lock_guard<std::mutex> lock(schema_mutex);

        auto it = std::find_if(databases.begin(), databases.end(), [&](const auto & pair) { return pair.second->name == database_name; });
        if (it == databases.end())
            return nullptr;
        return it->second;
    }

    TiDB::DBInfoPtr getDBInfoByMappedName(const String & mapped_database_name) override
    {
        std::lock_guard<std::mutex> lock(schema_mutex);

        auto it = std::find_if(databases.begin(), databases.end(), [&](const auto & pair) { return NameMapper().mapDatabaseName(*pair.second) == mapped_database_name; });
        if (it == databases.end())
            return nullptr;
        return it->second;
    }

    bool tryLoadSchemaDiffs(Getter & getter, Int64 version, Context & context)
    {
        if (isTooOldSchema(cur_version, version))
        {
            return false;
        }

        LOG_DEBUG(log, "try load schema diffs.");

        SchemaBuilder<Getter, NameMapper> builder(getter, context, databases, version);

        Int64 used_version = cur_version;
        std::vector<SchemaDiff> diffs;
        while (used_version < version)
        {
            used_version++;
            diffs.push_back(getter.getSchemaDiff(used_version));
        }
        LOG_DEBUG(log, "end load schema diffs.");
        try
        {
            for (const auto & diff : diffs)
            {
                builder.applyDiff(diff);
            }
        }
        catch (TiFlashException & e)
        {
            if (!e.getError().is(Errors::DDL::StaleSchema))
            {
                GET_METRIC(tiflash_schema_apply_count, type_failed).Increment();
            }
            LOG_WARNING(log, "apply diff meets exception : " << e.displayText() << " \n stack is " << e.getStackTrace().toString());
            return false;
        }
        catch (Exception & e)
        {
            GET_METRIC(tiflash_schema_apply_count, type_failed).Increment();
            LOG_WARNING(log, "apply diff meets exception : " << e.displayText() << " \n stack is " << e.getStackTrace().toString());
            return false;
        }
        catch (Poco::Exception & e)
        {
            GET_METRIC(tiflash_schema_apply_count, type_failed).Increment();
            LOG_WARNING(log, "apply diff meets exception : " << e.displayText() << " \n");
            return false;
        }
        catch (std::exception & e)
        {
            GET_METRIC(tiflash_schema_apply_count, type_failed).Increment();
            LOG_WARNING(log, "apply diff meets exception : " << e.what() << " \n");
            return false;
        }
        return true;
    }

    void loadAllSchema(Getter & getter, Int64 version, Context & context)
    {
        SchemaBuilder<Getter, NameMapper> builder(getter, context, databases, version);
        builder.syncAllSchema();
    }
};

} // namespace DB
