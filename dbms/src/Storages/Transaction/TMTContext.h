#pragma once

#include <atomic>
#include <pd/IClient.h>

#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/RegionPartition.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTStorages.h>
#include <Storages/Transaction/TMTTableFlusher.h>

namespace DB
{

class Context;

class TMTContext
{
public:
    KVStorePtr kvstore;
    TMTStorages storages;
    TMTTableFlushers table_flushers;
    RegionPartition region_partition;

public:
    // TODO: get flusher args from config file
    explicit TMTContext(Context & context_, std::vector<String> addrs, size_t deadline_seconds = 5, size_t flush_threshold_rows = 1024);

    SchemaSyncerPtr getSchemaSyncer() const;
    void setSchemaSyncer(SchemaSyncerPtr);

    pingcap::pd::ClientPtr getPDClient() const;
    void setPDClient(pingcap::pd::ClientPtr);

    pingcap::kv::RegionCachePtr getRegionCache() const;

    pingcap::kv::RpcClientPtr getRpcClient();

    bool isEnabledDataHistoryVersionGc();
    void enableDataHistoryVersionGc(bool history_gc);

private:
    SchemaSyncerPtr             schema_syncer;
    pingcap::pd::ClientPtr      pd_client;
    pingcap::kv::RegionCachePtr region_cache;
    pingcap::kv::RpcClientPtr   rpc_client;

    std::atomic<bool> enabled_history_gc = false;

    mutable std::mutex mutex;
};

} // namespace DB
