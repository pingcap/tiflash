#include <Storages/Transaction/PDTiKVClient.h>

#include <Common/DNSCache.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

constexpr int readIndexMaxBackoff = 5000;
Timestamp PDClientHelper::cached_gc_safe_point = 0;
std::chrono::time_point<std::chrono::system_clock> PDClientHelper::safe_point_last_update_time;


IndexReader::IndexReader(
    KVClusterPtr cluster_, const pingcap::kv::RegionVerID & id_, const std::string & suggested_ip_, UInt16 suggested_port_)
    : pingcap::kv::RegionClient(cluster_.get(), id_),
      suggested_ip(suggested_ip_),
      suggested_port(suggested_port_),
      cluster(cluster_),
      log(&Logger::get("pingcap.index_read"))
{}

int64_t IndexReader::getReadIndex()
{
    pingcap::kv::Backoffer bo(readIndexMaxBackoff);

    for (;;)
    {
        auto region = cluster->region_cache->getRegionByID(bo, region_id);
        const auto & learners = region->learners;
        std::vector<metapb::Peer> candidate_learners;
        // By default, we should config true ip in our config file.
        // And we make sure that old config can also work.
        if (suggested_ip == "0.0.0.0")
            candidate_learners = learners;
        else
        {
            // Try to iterate all learners in pd as no accurate IP specified in config thus I don't know who 'I' am, otherwise only try 'myself'
            for (const auto & learner : learners)
            {
                std::string addr = cluster->region_cache->getStore(bo, learner.store_id()).addr;
                if (addr.empty())
                {
                    LOG_DEBUG(log, "learner address empty.");
                    continue;
                }
                // Assume net type of addr is AF_NET.
                auto socket_addr = DNSCache::instance().resolveHostAndPort(addr);
                std::string ip = socket_addr.host().toString();
                UInt16 port = socket_addr.port();
                if (ip.empty())
                {
                    LOG_WARNING(log, "cannot resolve address: " << addr);
                    continue;
                }
                if (ip == suggested_ip && port == suggested_port)
                {
                    candidate_learners.push_back(learner);
                    break;
                }
            }
        }

        // There are two cases that candidates may be empty:
        // 1. the learner lists is empty. It means there are no learner stores is up.
        // 2. the learner lists is not empty and we specify a local service address. But we don't find it in learner list, then we
        // fail the request directly.
        if (candidate_learners.empty())
        {
            cluster->region_cache->dropRegion(region_id);
            throw Exception("Cannot find store ip " + suggested_ip + " in region peers, region_id is " + std::to_string(region_id.id)
                    + ", maybe learner storage is not ready",
                ErrorCodes::LOGICAL_ERROR);
        }

        try
        {
            return getReadIndexFromLearners(bo, region->meta, candidate_learners)->read_index();
        }
        catch (const pingcap::Exception & e)
        {
            // all stores are failed, so we need drop the region.
            cluster->region_cache->dropRegion(region_id);
            bo.backoff(pingcap::kv::boTiKVRPC, e);
        }
    }
}

std::shared_ptr<::kvrpcpb::ReadIndexResponse> IndexReader::getReadIndexFromLearners(
    pingcap::kv::Backoffer & bo, const metapb::Region & meta, const std::vector<metapb::Peer> & learners)
{
    auto request = std::make_shared<kvrpcpb::ReadIndexRequest>();
    pingcap::kv::RpcCall<::kvrpcpb::ReadIndexRequest> rpc(request);
    for (const auto & learner : learners)
    {
        std::string addr = cluster->region_cache->getStore(bo, learner.store_id()).peer_addr;
        if (addr.size() == 0)
        {
            bo.backoff(pingcap::kv::boRegionMiss,
                pingcap::Exception(
                    "miss store, region id is: " + std::to_string(region_id.id) + " store id is: " + std::to_string(learner.store_id()),
                    pingcap::ErrorCodes::StoreNotReady));
            cluster->region_cache->dropStore(learner.store_id());
            continue;
        }
        auto ctx = std::make_shared<pingcap::kv::RPCContext>(region_id, meta, learner, addr);
        rpc.setCtx(ctx);
        try
        {
            cluster->rpc_client->sendRequest(addr, rpc);
        }
        catch (const pingcap::Exception & e)
        {
            LOG_WARNING(log, "send request to " + addr + " failed");
            // only drop this store. and retry again!
            cluster->region_cache->dropStore(learner.store_id());
            continue;
        }

        auto resp = rpc.getResp();
        if (resp->has_region_error())
        {
            LOG_WARNING(log, "send request to " << addr << " failed. because of meet region error: " << resp->region_error().message());
            onRegionError(bo, ctx, resp->region_error());
        }
        else
        {
            return resp;
        }
    }
    throw pingcap::Exception("all stores are failed, may be region info is out of date.", pingcap::ErrorCodes::StoreNotReady);
}

} // namespace DB
