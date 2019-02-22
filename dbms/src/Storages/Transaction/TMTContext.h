#pragma once

#include <pd/IClient.h>
#include <atomic>

#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/RegionPartition.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTStorages.h>

namespace DB
{

class Context;

class TMTContext
{
public:
    KVStorePtr kvstore;
    TMTStorages storages;
    RegionPartition region_partition;

public:
    // TODO: get flusher args from config file
    explicit TMTContext(Context & context_, std::vector<String> addrs);

    SchemaSyncerPtr getSchemaSyncer() const;
    void setSchemaSyncer(SchemaSyncerPtr);

    pingcap::pd::ClientPtr getPDClient() const;
    void setPDClient(pingcap::pd::ClientPtr);

    pingcap::kv::RegionCachePtr getRegionCache() const;

    pingcap::kv::RpcClientPtr getRpcClient();

private:
    SchemaSyncerPtr schema_syncer;
    pingcap::pd::ClientPtr pd_client;
    pingcap::kv::RegionCachePtr region_cache;
    pingcap::kv::RpcClientPtr rpc_client;

    mutable std::mutex mutex;
};

} // namespace DB
