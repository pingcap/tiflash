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

int64_t IndexReader::getReadIndex()
{
    pingcap::kv::Backoffer bo(readIndexMaxBackoff);
    auto request = std::make_shared<kvrpcpb::ReadIndexRequest>();

    for (;;)
    {
        auto region_client = pingcap::kv::RegionClient(cluster.get(), region_id);

        try
        {
            return region_client.sendReqToRegion(bo, request)->read_index();
        }
        catch (pingcap::Exception & e)
        {
            LOG_WARNING(log, "Retry get read index");
            bo.backoff(pingcap::kv::boRegionMiss, e);
            continue;
        }
    }
}

} // namespace DB
