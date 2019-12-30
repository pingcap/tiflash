#pragma once

#include <Debug/MockSchemaGetter.h>
#include <Storages/Transaction/SchemaBuilder.h>
#include <Storages/Transaction/TMTContext.h>
#include <pingcap/kv/Cluster.h>
#include <pingcap/kv/Snapshot.h>

namespace DB
{

template <bool mock_getter>
struct TiDBSchemaSyncer : public SchemaSyncer
{

    using Getter = std::conditional_t<mock_getter, MockSchemaGetter, SchemaGetter>;


    KVClusterPtr cluster;

    const Int64 maxNumberOfDiffs = 100;

    Int64 cur_version;

    std::mutex schema_mutex;

    std::unordered_map<DB::DatabaseID, String> databases;

    Logger * log;

    TiDBSchemaSyncer(KVClusterPtr cluster_) : cluster(cluster_), cur_version(0), log(&Logger::get("SchemaSyncer")) {}

    bool isTooOldSchema(Int64 cur_ver, Int64 new_version) { return cur_ver == 0 || new_version - cur_ver > maxNumberOfDiffs; }

    Getter createSchemaGetter(UInt64 tso [[maybe_unused]])
    {
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

    Int64 getCurrentVersion() override
    {
        std::lock_guard<std::mutex> lock(schema_mutex);
        return cur_version;
    }

    bool syncSchemas(Context & context) override
    {
        std::lock_guard<std::mutex> lock(schema_mutex);

        auto tso = cluster->pd_client->getTS();
        auto getter = createSchemaGetter(tso);
        Int64 version = getter.getVersion();
        if (version <= cur_version)
        {
            return false;
        }
        LOG_INFO(log,
            "start to sync schemas. current version is: " + std::to_string(cur_version)
                + " and try to sync schema version to: " + std::to_string(version));
        if (!tryLoadSchemaDiffs(getter, version, context))
        {
            loadAllSchema(getter, version, context);
        }
        cur_version = version;
        LOG_INFO(log, "end sync schema, version has been updated to " + std::to_string(cur_version));
        return true;
    }

    bool tryLoadSchemaDiffs(Getter & getter, Int64 version, Context & context)
    {
        if (isTooOldSchema(cur_version, version))
        {
            return false;
        }

        LOG_DEBUG(log, "try load schema diffs.");

        SchemaBuilder<Getter> builder(getter, context, databases, version);

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
        catch (Exception & e)
        {
            LOG_ERROR(log, "apply diff meets exception : " + e.displayText());
            return false;
        }
        return true;
    }

    void loadAllSchema(Getter & getter, Int64 version, Context & context)
    {
        SchemaBuilder<Getter> builder(getter, context, databases, version);
        builder.syncAllSchema();
    }
};

} // namespace DB
