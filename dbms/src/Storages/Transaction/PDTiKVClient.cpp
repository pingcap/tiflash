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

IndexReader::IndexReader(KVClusterPtr cluster_, const pingcap::kv::RegionVerID & id_)
    : region_id(id_), cluster(cluster_), log(&Logger::get("pingcap.index_read"))
{}

std::pair<uint64_t, bool> IndexReader::getReadIndex()
{
    pingcap::kv::Backoffer bo(readIndexMaxBackoff);
    auto request = std::make_shared<kvrpcpb::ReadIndexRequest>();

    for (;;)
    {
        auto region_client = pingcap::kv::RegionClient(cluster.get(), region_id);

        try
        {
            uint64_t index = region_client.sendReqToRegion(bo, request)->read_index();
            return std::make_pair(index, false);
        }
        catch (pingcap::Exception & e)
        {
            LOG_WARNING(log, "Retry get read index");
            // If region epoch is not match , we shouldn't wait any more, because region epoch will never update here.
            if (e.code() == pingcap::ErrorCodes::RegionEpochNotMatch)
            {
                return std::make_pair(0, true);
            }
            bo.backoff(pingcap::kv::boTiKVRPC, e);
            continue;
        }
    }
}

} // namespace DB
