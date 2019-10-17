#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"

#include <pingcap/kv/RegionClient.h>
#include <pingcap/pd/IClient.h>

#pragma GCC diagnostic pop

namespace DB
{

struct IndexReader : public pingcap::kv::RegionClient
{
    const std::string & suggested_address;

    IndexReader(pingcap::kv::RegionCachePtr cache_,
        pingcap::kv::RpcClientPtr client_,
        const pingcap::kv::RegionVerID & id,
        const std::string & suggested_address_)
        : pingcap::kv::RegionClient(cache_, client_, id), suggested_address(suggested_address_)
    {}

    int64_t getReadIndex()
    {
        auto request = new kvrpcpb::ReadIndexRequest();
        pingcap::kv::Backoffer bo(pingcap::kv::readIndexMaxBackoff);
        auto rpc_call = std::make_shared<pingcap::kv::RpcCall<kvrpcpb::ReadIndexRequest>>(request);

        auto region = cache->getRegionByID(bo, region_id);
        const auto & learners = region->learners;
        std::vector<metapb::Peer> candidate_learners;
        for (const auto learner : learners)
        {
            std::string addr = cache->getStoreAddr(bo, learner.store_id());
            if (addr.size() > 0 && addr == suggested_address)
            {
                candidate_learners.push_back(learner);
                break;
            }
        }
        if (candidate_learners.size() == 0)
            candidate_learners = std::move(learners);

        for (;;)
        {
            try
            {
                getReadIndexFromLearners(bo, region->meta, candidate_learners, rpc_call);
                return rpc_call->getResp()->read_index();
            }
            catch (const pingcap::Exception & e)
            {
                // all stores are failed, so we need drop the region.
                cache->dropRegion(region_id);
                bo.backoff(pingcap::kv::boTiKVRPC, e);
            }
        }
    }

private:
    void getReadIndexFromLearners(pingcap::kv::Backoffer & bo,
        const metapb::Region & meta,
        const std::vector<metapb::Peer> & learners,
        pingcap::kv::RpcCallPtr<kvrpcpb::ReadIndexRequest>
            rpc)
    {
        for (const auto learner : learners)
        {
            std::string addr = cache->getStoreAddr(bo, learner.store_id());
            if (addr == "")
            {
                bo.backoff(pingcap::kv::boRegionMiss,
                    pingcap::Exception(
                        "miss store, region id is: " + std::to_string(region_id.id) + " store id is: " + std::to_string(learner.store_id()),
                        pingcap::ErrorCodes::StoreNotReady));
                cache->dropStore(learner.store_id());
                continue;
            }
            auto ctx = std::make_shared<pingcap::kv::RPCContext>(region_id, meta, learner, addr);
            rpc->setCtx(ctx);
            try
            {
                client->sendRequest(addr, rpc);
            }
            catch (const pingcap::Exception & e)
            {
                // only drop this store. and retry again!
                cache->dropStore(learner.store_id());
                continue;
            }

            auto resp = rpc->getResp();
            if (resp->has_region_error())
            {
                onRegionError(bo, ctx, resp->region_error());
            }
            else
            {
                return;
            }
        }
    }
};

using IndexReaderPtr = std::shared_ptr<IndexReader>;

struct PDClientHelper
{

    static constexpr int get_safepoint_maxtime = 120000; // 120s. waiting pd recover.

    static uint64_t getGCSafePointWithRetry(pingcap::pd::ClientPtr pd_client)
    {
        pingcap::kv::Backoffer bo(get_safepoint_maxtime);
        for (;;)
        {
            try
            {
                auto safe_point = pd_client->getGCSafePoint();
                return safe_point;
            }
            catch (pingcap::Exception & e)
            {
                bo.backoff(pingcap::kv::boPDRPC, e);
            }
        }
    }
};


} // namespace DB
