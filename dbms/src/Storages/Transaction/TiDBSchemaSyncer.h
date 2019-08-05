#pragma once

#include <Storages/Transaction/SchemaBuilder.h>
#include <Storages/Transaction/TMTContext.h>
#include <tikv/Snapshot.h>

namespace DB
{

struct TiDBSchemaSyncer : public SchemaSyncer
{
    pingcap::pd::ClientPtr pdClient;
    pingcap::kv::RegionCachePtr regionCache;
    pingcap::kv::RpcClientPtr rpcClient;

    const Int64 maxNumberOfDiffs = 100;

    Int64 cur_version;

    std::mutex schema_mutex;

    std::unordered_map<DB::DatabaseID, String> databases;

    Logger * log;

    TiDBSchemaSyncer(pingcap::pd::ClientPtr pdClient_, pingcap::kv::RegionCachePtr regionCache_, pingcap::kv::RpcClientPtr rpcClient_)
        : pdClient(pdClient_), regionCache(regionCache_), rpcClient(rpcClient_), cur_version(0), log(&Logger::get("SchemaSyncer"))
    {}

    bool isTooOldSchema(Int64 cur_version, Int64 new_version) { return cur_version == 0 || new_version - cur_version > maxNumberOfDiffs; }

    bool syncSchemas(Context & context) override
    {
        std::lock_guard<std::mutex> lock(schema_mutex);

        auto tso = pdClient->getTS();
        SchemaGetter getter = SchemaGetter(regionCache, rpcClient, tso);
        Int64 version = getter.getVersion();
        if (version <= cur_version)
        {
            return false;
        }
        LOG_INFO(log, "start to sync schemas. current version is: " + std::to_string(cur_version) + " and try to sync schema version to: " + std::to_string(version));
        if (!tryLoadSchemaDiffs(getter, version, context))
        {
            loadAllSchema(getter, version, context);
        }
        cur_version = version;
        return true;
    }

    bool tryLoadSchemaDiffs(SchemaGetter & getter, Int64 version, Context & context)
    {
        if (isTooOldSchema(cur_version, version))
        {
            return false;
        }

        LOG_DEBUG(log, "try load schema diffs.");

        SchemaBuilder builder(getter, context, databases, version);

        Int64 used_version = cur_version;
        std::vector<SchemaDiff> diffs;
        while (used_version < version)
        {
            used_version++;
            diffs.push_back(getter.getSchemaDiff(used_version));
        }
        LOG_DEBUG(log, "end load schema diffs.");
        for (const auto & diff : diffs)
        {
            builder.applyDiff(diff);
        }
        return true;
    }

    bool loadAllSchema(SchemaGetter & getter, Int64 version, Context & context)
    {
        LOG_DEBUG(log, "try load all schemas.");

        std::vector<TiDB::DBInfoPtr> all_schema = getter.listDBs();

        for (auto db_info : all_schema)
        {
            LOG_DEBUG(log, "Load schema : " + db_info->name);
        }

        SchemaBuilder builder(getter, context, databases, version);

        std::set<TiDB::DatabaseID> db_ids;
        for (auto db : all_schema)
        {
            builder.updateDB(db);
            db_ids.insert(db->id);
        }
        // Drop databases;
        for (auto it = databases.begin(); it != databases.end(); it++)
        {
            if (db_ids.count(it->first) == 0)
            {
                builder.applyDropSchema(it->first);
            }
        }
        return true;
    }
};

} // namespace DB
