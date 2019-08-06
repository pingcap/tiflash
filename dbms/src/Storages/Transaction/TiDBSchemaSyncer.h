#pragma once

#include <Debug/MockSchemaGetter.h>
#include <Storages/Transaction/SchemaBuilder.h>
#include <Storages/Transaction/TMTContext.h>
#include <tikv/Snapshot.h>

namespace DB
{

template <bool mock_getter>
struct TiDBSchemaSyncer : public SchemaSyncer
{

    using Getter = std::conditional_t<mock_getter, MockSchemaGetter, SchemaGetter>;

    pingcap::pd::ClientPtr pd_client;
    pingcap::kv::RegionCachePtr region_cache;
    pingcap::kv::RpcClientPtr rpc_client;

    const Int64 maxNumberOfDiffs = 100;

    Int64 cur_version;

    std::mutex schema_mutex;

    std::unordered_map<DB::DatabaseID, String> databases;

    Logger * log;

    TiDBSchemaSyncer(pingcap::pd::ClientPtr pd_client_, pingcap::kv::RegionCachePtr region_cache_, pingcap::kv::RpcClientPtr rpc_client_)
        : pd_client(pd_client_), region_cache(region_cache_), rpc_client(rpc_client_), cur_version(0), log(&Logger::get("SchemaSyncer"))
    {}

    bool isTooOldSchema(Int64 cur_version, Int64 new_version) { return cur_version == 0 || new_version - cur_version > maxNumberOfDiffs; }

    Getter createSchemaGetter(UInt64 tso [[maybe_unused]])
    {
        if constexpr (mock_getter)
        {
            return Getter();
        }
        else
        {
            return Getter(region_cache, rpc_client, tso);
        }
    }

    bool syncSchemas(Context & context) override
    {
        std::lock_guard<std::mutex> lock(schema_mutex);

        auto tso = pd_client->getTS();
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
