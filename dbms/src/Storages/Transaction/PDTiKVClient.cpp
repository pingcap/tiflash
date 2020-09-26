#include <Common/DNSCache.h>
#include <Common/Exception.h>
#include <Storages/Transaction/PDTiKVClient.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

constexpr int readIndexMaxBackoff = 10000;
constexpr int maxRetryTime = 3;
Timestamp PDClientHelper::cached_gc_safe_point = 0;
std::chrono::time_point<std::chrono::system_clock> PDClientHelper::safe_point_last_update_time;


IndexReader::IndexReader(KVClusterPtr cluster_) : cluster(cluster_), log(&Logger::get("pingcap.index_read")) {}

ReadIndexResult::ReadIndexResult(UInt64 read_index_, bool region_unavailable_, bool region_epoch_not_match_, kvrpcpb::LockInfo * lock_info_)
    : read_index(read_index_),
      region_unavailable(region_unavailable_),
      region_epoch_not_match(region_epoch_not_match_),
      lock_info(lock_info_)
{}

ReadIndexResult IndexReader::getReadIndex(
    const pingcap::kv::RegionVerID & region_id, const std::string & start_key, const std::string & end_key, UInt64 start_ts)
{
    pingcap::kv::Backoffer bo(readIndexMaxBackoff);
    auto request = std::make_shared<kvrpcpb::ReadIndexRequest>();
    {
        request->set_start_ts(start_ts);
        auto key_range = request->add_ranges();
        key_range->set_start_key(start_key);
        key_range->set_end_key(end_key);
    }

    int retry_time = 0;
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
            auto resp = region_client.sendReqToRegion(bo, request);
            UInt64 index = resp->read_index();
            return {index, false, false, resp->release_locked()};
        }
        catch (pingcap::Exception & e)
        {
            LOG_WARNING(log, "Region " << region_id.toString() << " get index failed, error message is :" << e.displayText());
            retry_time++;
            // We try few times, may be cost several seconds, if it still fails, we should not waste too much time and report to tidb as soon.
            if (retry_time < maxRetryTime)
            {
                LOG_INFO(log, "read index retry");
                bo.backoff(pingcap::kv::boTiKVRPC, e);
                continue;
            }
            return {0, false, true};
        }
    }
}

} // namespace DB
