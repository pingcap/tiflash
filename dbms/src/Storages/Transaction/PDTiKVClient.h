#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Core/Types.h>
#include <pingcap/kv/RegionClient.h>
#include <pingcap/pd/IClient.h>

#pragma GCC diagnostic pop

#include <Core/Types.h>
#include <Storages/Transaction/Types.h>
#include <common/logger_useful.h>

// We define a shared ptr here, because TMTContext / SchemaSyncer / IndexReader all need to
// `share` the resource of cluster.
using KVClusterPtr = std::shared_ptr<pingcap::kv::Cluster>;


namespace DB
{


struct IndexReader : public pingcap::kv::RegionClient
{
    std::string suggested_ip;
    UInt16 suggested_port;

    KVClusterPtr cluster;

    Logger * log;

    IndexReader(KVClusterPtr cluster_, const pingcap::kv::RegionVerID & id, const std::string & suggested_ip, UInt16 suggested_port);

    int64_t getReadIndex();

private:
    std::shared_ptr<::kvrpcpb::ReadIndexResponse> getReadIndexFromLearners(
        pingcap::kv::Backoffer & bo, const metapb::Region & meta, const std::vector<metapb::Peer> & learners);
};

using IndexReaderPtr = std::shared_ptr<IndexReader>;

struct PDClientHelper
{

    static constexpr int get_safepoint_maxtime = 120000; // 120s. waiting pd recover.

    static Timestamp getGCSafePointWithRetry(
        const pingcap::pd::ClientPtr & pd_client, bool ignore_cache = true, Int64 safe_point_update_interval_seconds = 30)
    {
        if (!ignore_cache)
        {
            // In case we cost too much to update safe point from PD.
            std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
            const auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - safe_point_last_update_time);
            const auto min_interval = std::max(Int64(1), safe_point_update_interval_seconds); // at least one second
            if (duration.count() < min_interval)
                return cached_gc_safe_point;
        }

        pingcap::kv::Backoffer bo(get_safepoint_maxtime);
        for (;;)
        {
            try
            {
                auto safe_point = pd_client->getGCSafePoint();
                cached_gc_safe_point = safe_point;
                safe_point_last_update_time = std::chrono::system_clock::now();
                return safe_point;
            }
            catch (pingcap::Exception & e)
            {
                bo.backoff(pingcap::kv::boPDRPC, e);
            }
        }
    }

private:
    static Timestamp cached_gc_safe_point;
    static std::chrono::time_point<std::chrono::system_clock> safe_point_last_update_time;
};


} // namespace DB
