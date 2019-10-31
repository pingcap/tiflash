#include <Storages/Transaction/PDTiKVClient.h>

#include <Common/Exception.h>

#include <netdb.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

std::string getIP(const std::string & address)
{
    if (address.size() == 0)
        return "";
    size_t idx = address.find(":");
    if (idx == std::string::npos)
        return "";
    auto ip = address.substr(0, idx);
    return ip;
}

// convertAddr converts host name to network address.
// We assume the converted net type is AF_NET.
std::string IndexReader::convertAddr(const std::string & address)
{
    if (address.size() == 0)
        return "";
    size_t idx = address.find(":");
    if (idx == std::string::npos)
        return "";
    auto host = address.substr(0, idx);
    auto port = address.substr(idx + 1);

    char buf[1024];
    struct hostent buff_storage;
    struct hostent * result;
    int err = 0;
    gethostbyname_r(host.data(), &buff_storage, buf, sizeof(buf), &result, &err);
    // Suppose we always use IPv4 address.
    if (result == nullptr || result->h_addr == nullptr || err != 0)
    {
        LOG_ERROR(log, "Cannot resolve host name: " << address << " error message is :" << hstrerror(err));
        return "";
    }
    std::string addr;
    for (int i = 0; i < 4; i++)
    {
        addr.append(std::to_string(static_cast<uint32_t>((uint8_t)result->h_addr[i])));
        if (i != 3)
            addr.push_back('.');
    }
    return addr;
}

IndexReader::IndexReader(pingcap::kv::RegionCachePtr cache_,
    pingcap::kv::RpcClientPtr client_,
    const pingcap::kv::RegionVerID & id,
    const std::string & suggested_address_)
    : pingcap::kv::RegionClient(cache_, client_, id), suggested_address(suggested_address_), log(&Logger::get("pingcap.index_read"))
{}

int64_t IndexReader::getReadIndex()
{
    auto request = new kvrpcpb::ReadIndexRequest();
    pingcap::kv::Backoffer bo(pingcap::kv::readIndexMaxBackoff);
    auto rpc_call = std::make_shared<pingcap::kv::RpcCall<kvrpcpb::ReadIndexRequest>>(request);

    for (;;)
    {
        auto region = cache->getRegionByID(bo, region_id);
        const auto & learners = region->learners;
        const std::string suggested_ip = getIP(suggested_address);
        std::vector<metapb::Peer> candidate_learners;
        // By default, we should config true ip in our config file.
        // And we make sure that old config can also work.
        if (suggested_ip.size() == 0 || suggested_ip == "0.0.0.0")
            candidate_learners = learners;
        else
        {
            // Try to iterate all learners in pd as no accurate IP specified in config thus I don't know who 'I' am, otherwise only try 'myself'
            for (const auto & learner : learners)
            {
                std::string addr = cache->getStore(bo, learner.store_id()).addr;
                if (addr.size() > 0 && convertAddr(addr) == suggested_ip)
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
            cache->dropRegion(region_id);
            throw Exception("Cannot find store ip " + suggested_ip + " in region peers, region_id is " + std::to_string(region_id.id)
                    + ", maybe learner storage is not ready",
                ErrorCodes::LOGICAL_ERROR);
        }

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

void IndexReader::getReadIndexFromLearners(pingcap::kv::Backoffer & bo,
    const metapb::Region & meta,
    const std::vector<metapb::Peer> & learners,
    pingcap::kv::RpcCallPtr<kvrpcpb::ReadIndexRequest>
        rpc)
{
    for (const auto & learner : learners)
    {
        std::string addr = cache->getStore(bo, learner.store_id()).peer_addr;
        if (addr.size() == 0)
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
            LOG_WARNING(log, "send request to " + addr + " failed");
            // only drop this store. and retry again!
            cache->dropStore(learner.store_id());
            continue;
        }

        auto resp = rpc->getResp();
        if (resp->has_region_error())
        {
            LOG_WARNING(log, "send request to " + addr + " failed. because of MEET region error");
            onRegionError(bo, ctx, resp->region_error());
        }
        else
        {
            return;
        }
    }
    throw pingcap::Exception("all stores are failed, may be region info is out of date.", pingcap::ErrorCodes::StoreNotReady);
}

} // namespace DB
