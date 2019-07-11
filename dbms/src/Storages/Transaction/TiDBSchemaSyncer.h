#pragma once

#include <Storages/Transaction/SchemaBuilder.h>
#include <Storages/Transaction/TMTContext.h>
#include <tikv/Snapshot.h>

namespace DB {

struct TiDBSchemaSyncer : public SchemaSyncer {
    pingcap::pd::ClientPtr      pdClient;
    pingcap::kv::RegionCachePtr regionCache;
    pingcap::kv::RpcClientPtr   rpcClient;

    const Int64 maxNumberOfDiffs = 100;

    Int64 cur_version;

    std::mutex schema_mutex;

    Logger * log;

    TiDBSchemaSyncer(pingcap::pd::ClientPtr pdClient_, pingcap::kv::RegionCachePtr regionCache_, pingcap::kv::RpcClientPtr rpcClient_)
        : pdClient(pdClient_), regionCache(regionCache_), rpcClient(rpcClient_), cur_version(0), log(&Logger::get("SchemaSyncer")) {}

    bool isTooOldSchema(Int64 cur_version, Int64 new_version) {
        return cur_version == 0 || new_version - cur_version > maxNumberOfDiffs;
    }

    bool syncSchemas(Context & context) override {
        std::lock_guard<std::mutex> lock(schema_mutex);

        LOG_INFO(log, "start to sync schemas. current version is: " + std::to_string(cur_version));

        auto tso = pdClient->getTS();
        LOG_DEBUG(log, "get schema use tso: " + std::to_string(tso));
        SchemaGetter getter = SchemaGetter(regionCache, rpcClient, tso);
        Int64 version = getter.getVersion();
        if (version <= cur_version) {
            return false;
        }
        LOG_INFO(log, "try to sync schema version to: " + std::to_string(version));
        if (!tryLoadSchemaDiffs(getter, version, context)) {
            loadAllSchema(getter, context);
        }
        cur_version = version;
        return true;
    }

    void syncSchema(TableID , Context & context, bool ) override {
        syncSchemas(context);
    }

    TableID getTableIdByName(const std::string & , const std::string & , Context & ) override {
        return 0;
    }

    bool tryLoadSchemaDiffs(SchemaGetter & getter, Int64 version, Context & context) {
        if (isTooOldSchema(cur_version, version)) {
            return false;
        }

        LOG_DEBUG(log, "try load schema diffs.");

        SchemaBuilder builder(getter, context);

        Int64 used_version = cur_version;
        std::vector<SchemaDiff> diffs;
        while (used_version < version) {
            used_version ++;
            diffs.push_back(getter.getSchemaDiff(used_version));
        }
        LOG_DEBUG(log, "end load schema diffs.");
        for (const auto & diff : diffs) {
            builder.applyDiff(diff);
        }
        return true;
    }

    bool loadAllSchema(SchemaGetter & getter, Context & context) {
        LOG_DEBUG(log, "try load all schemas.");

        std::vector<TiDB::DBInfoPtr> all_schema = getter.listDBs();

        for (auto db_info : all_schema) {
            LOG_DEBUG(log, "Load schema : " + db_info->name);
        }

        SchemaBuilder builder(getter, context);

        std::set<TiDB::DatabaseID> db_ids;
        for (auto db : all_schema) {
            builder.updateDB(db);
            db_ids.insert(db->id);
        }
        auto & tmt_context = context.getTMTContext();
        const auto & dbs = tmt_context.getStorages().databases;
        for (auto it = dbs.begin(); it != dbs.end(); it ++) {
            if (db_ids.count(it -> first) == 0) {
                builder.applyDropSchema(it -> first);
            }
        }
        return true;
    }

};

}
