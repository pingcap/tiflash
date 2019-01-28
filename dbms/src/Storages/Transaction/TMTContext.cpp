#include <Interpreters/Context.h>
#include <Storages/StorageMergeTree.h>
#include <pd/MockPDClient.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{

TMTContext::TMTContext(Context & context, std::vector<String> addrs, size_t flush_deadline_seconds, size_t flush_threshold_rows) :
    kvstore(std::make_shared<KVStore>(context.getPath() + "kvstore/", &context)),
    table_flushers(context, flush_deadline_seconds, flush_threshold_rows),
    region_partition(context.getPath() + "regmap/"),
    schema_syncer(std::make_shared<HttpJsonSchemaSyncer>()),
    pd_client(addrs.size() == 0 ? static_cast<pingcap::pd::IClient *>(new pingcap::pd::MockPDClient()) : static_cast<pingcap::pd::IClient *>(new pingcap::pd::Client(addrs))),
    region_cache(std::make_shared<pingcap::kv::RegionCache>(pd_client))
{}

SchemaSyncerPtr TMTContext::getSchemaSyncer() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return schema_syncer;
}

void TMTContext::setSchemaSyncer(SchemaSyncerPtr rhs)
{
    std::lock_guard<std::mutex> lock(mutex);
    schema_syncer = rhs;
}

pingcap::pd::ClientPtr TMTContext::getPDClient() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return pd_client;
}

void TMTContext::setPDClient(pingcap::pd::ClientPtr rhs)
{
    std::lock_guard<std::mutex> lock(mutex);
    pd_client = rhs;
}

pingcap::kv::RegionCachePtr TMTContext::getRegionCache() const {
    return region_cache;
}

pingcap::kv::RpcClientPtr TMTContext::getRpcClient() {
    if (rpc_client == nullptr)
        rpc_client = std::make_shared<pingcap::kv::RpcClient>();
    return rpc_client;
}

bool TMTContext::isEnabledDataHistoryVersionGc()
{
    return enabled_history_gc;
}

void TMTContext::enableDataHistoryVersionGc(bool history_gc)
{
    enabled_history_gc = history_gc;
}

} // namespace DB
