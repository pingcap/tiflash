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

    int64_t getReadIndex();

private:
    void getReadIndexFromLearners(pingcap::kv::Backoffer & bo,
        const metapb::Region & meta,
        const std::vector<metapb::Peer> & learners,
        pingcap::kv::RpcCallPtr<kvrpcpb::ReadIndexRequest>
            rpc);
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
