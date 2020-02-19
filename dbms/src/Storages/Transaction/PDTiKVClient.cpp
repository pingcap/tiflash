#include <Common/DNSCache.h>
#include <Common/Exception.h>
#include <Storages/Transaction/PDTiKVClient.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

constexpr int readIndexMaxBackoff = 5000;
Timestamp PDClientHelper::cached_gc_safe_point = 0;
std::chrono::time_point<std::chrono::system_clock> PDClientHelper::safe_point_last_update_time;


IndexReader::IndexReader(KVClusterPtr cluster_) : cluster(cluster_), log(&Logger::get("pingcap.index_read")) {}

ReadIndexResult IndexReader::getReadIndex(const pingcap::kv::RegionVerID & region_id)
{
    pingcap::kv::Backoffer bo(readIndexMaxBackoff);
    auto request = std::make_shared<kvrpcpb::ReadIndexRequest>();

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
                LOG_WARNING(log, "Region " << region_id.toString() << " not found.");
                return {0, true, false};
            }
            bo.backoff(pingcap::kv::boPDRPC, e);
            continue;
        }
        if (region_ptr == nullptr)
        {
            LOG_WARNING(log, "Region " << region_id.toString() << " not found.");
            return {0, true, false};
        }
        auto region_id = region_ptr->verID();

        auto region_client = pingcap::kv::RegionClient(cluster.get(), region_id);

        try
        {
            UInt64 index = region_client.sendReqToRegion(bo, request)->read_index();
            return {index, false, false};
        }
        catch (pingcap::Exception & e)
        {
            LOG_WARNING(
                log, "Region " << region_id.toString() << " get index failed, error message is :" + e.displayText() + ", retry again.");
            return {0, false, true};
        }
    }
}

} // namespace DB
