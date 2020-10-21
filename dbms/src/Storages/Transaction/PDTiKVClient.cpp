#include <Storages/Transaction/PDTiKVClient.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

constexpr int readIndexMaxBackoff = 10000;
Timestamp PDClientHelper::cached_gc_safe_point = 0;
std::chrono::time_point<std::chrono::system_clock> PDClientHelper::safe_point_last_update_time;

IndexReader::IndexReader(KVClusterPtr cluster_) : cluster(cluster_), log(&Logger::get("pingcap.index_read")) {}

ReadIndexResult::ReadIndexResult(UInt64 read_index_, bool region_unavailable_, bool region_epoch_not_match_, kvrpcpb::LockInfo * lock_info_)
    : read_index(read_index_),
      region_unavailable(region_unavailable_),
      region_epoch_not_match(region_epoch_not_match_),
      lock_info(lock_info_)
{}

struct ReadIndexClient : pingcap::kv::RegionClient
{
    ReadIndexClient(pingcap::kv::Cluster * cluster, const pingcap::kv::RegionVerID & id) : pingcap::kv::RegionClient(cluster, id) {}
    auto sendReqToRegion(pingcap::kv::Backoffer & bo,
        std::shared_ptr<kvrpcpb::ReadIndexRequest>
            request,
        const metapb::Region & meta,
        const metapb::Peer & peer,
        int timeout = pingcap::kv::dailTimeout)
    {
        pingcap::kv::RpcCall<kvrpcpb::ReadIndexRequest> rpc(request);
        for (;;)
        {
            const auto store = cluster->region_cache->getStore(bo, peer.store_id());
            const auto & store_addr = store.peer_addr.empty() ? store.addr : store.peer_addr;
            auto ctx = std::make_shared<pingcap::kv::RPCContext>(region_id, meta, peer, store_addr);
            rpc.setCtx(ctx);
            try
            {
                cluster->rpc_client->sendRequest(store_addr, rpc, timeout);
            }
            catch (const pingcap::Exception & e)
            {
                onSendFail(bo, e, ctx);
                continue;
            }
            auto resp = rpc.getResp();
            if (resp->has_region_error())
            {
                auto & region_error = resp->region_error();
                if (region_error.has_not_leader())
                {
                    throw pingcap::Exception("proxy can not find leader", pingcap::LeaderNotMatch);
                }
                else
                {
                    LOG_WARNING(log, "Region " << region_id.toString() << " find error: " << region_error.message());
                    onRegionError(bo, ctx, region_error);
                }
            }
            else
            {
                return resp;
            }
        }
    }
};

ReadIndexResult IndexReader::getReadIndex(const pingcap::kv::RegionVerID & region_id,
    const std::string & start_key,
    const std::string & end_key,
    UInt64 start_ts,
    UInt64 store_id)
{
    pingcap::kv::Backoffer bo(readIndexMaxBackoff);
    auto request = std::make_shared<kvrpcpb::ReadIndexRequest>();
    {
        request->set_start_ts(start_ts);
        auto key_range = request->add_ranges();
        key_range->set_start_key(start_key);
        key_range->set_end_key(end_key);
    }

    for (;;)
    {
        pingcap::kv::RegionPtr region_ptr;
        try
        {
            region_ptr = cluster->region_cache->getRegionByID(bo, region_id);
        }
        catch (pingcap::Exception & e)
        {
            LOG_WARNING(log, "Get Region By id " << region_id.toString() << " failed, error message is " << e.displayText());
            if (e.code() == pingcap::ErrorCodes::RegionUnavailable)
            {
                LOG_WARNING(log, "Region " << region_id.toString() << " not found");
                return {0, true, false};
            }
            bo.backoff(pingcap::kv::boPDRPC, e);
            continue;
        }
        const metapb::Peer * peer_ptr = nullptr;
        if (region_ptr)
        {
            if (region_ptr->peer.store_id() == store_id) // to be compatible with tiflash directly writing
                peer_ptr = &region_ptr->peer;
            else
            {
                for (const auto & peer : region_ptr->learners)
                {
                    if (peer.store_id() == store_id)
                    {
                        peer_ptr = &peer;
                        break;
                    }
                }
            }
        }
        // peer is strongly dependent on region version
        if (region_ptr == nullptr || peer_ptr == nullptr)
        {
            LOG_WARNING(log, "Region " << region_id.toString() << " not found");
            return {0, true, false};
        }
        auto region_id = region_ptr->verID();

        auto read_index_client = ReadIndexClient(cluster.get(), region_id);

        try
        {
            auto resp = read_index_client.sendReqToRegion(bo, request, region_ptr->meta, *peer_ptr);
            return {resp->read_index(), false, false, resp->release_locked()};
        }
        catch (pingcap::Exception & e)
        {
            LOG_WARNING(log, "Region " << region_id.toString() << " get index failed, error message is: " << e.displayText());
            if (e.code() == pingcap::LeaderNotMatch)
                return {0, true, false};
            return {0, false, true};
        }
    }
}

} // namespace DB
