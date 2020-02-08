#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Core/Types.h>
#include <pingcap/kv/RegionClient.h>
#include <pingcap/pd/IClient.h>

#pragma GCC diagnostic pop

#include <common/logger_useful.h>

// We define a shared ptr here, because TMTContext / SchemaSyncer / IndexReader all need to
// `share` the resource of cluster.
using KVClusterPtr = std::shared_ptr<pingcap::kv::Cluster>;


namespace DB
{


struct IndexReader
{
    pingcap::kv::RegionVerID region_id;
    KVClusterPtr cluster;
    Logger * log;

    IndexReader(KVClusterPtr cluster_, const pingcap::kv::RegionVerID & id);

    // if region is merged and this region is removed, the second value returns true.
    std::pair<uint64_t, bool> getReadIndex();
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
