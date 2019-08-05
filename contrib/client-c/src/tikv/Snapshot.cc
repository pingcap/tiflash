#include <common/CltException.h>
#include <tikv/Backoff.h>
#include <tikv/Scanner.h>
#include <tikv/Snapshot.h>

namespace pingcap
{
namespace kv
{

constexpr int scan_batch_size = 256;

//bool extractLockFromKeyErr()

std::string Snapshot::Get(const std::string & key)
{
    Backoffer bo(GetMaxBackoff);
    auto location = cache->locateKey(bo, key);
    auto regionClient = RegionClient(cache, client, location.region);
    auto request = new kvrpcpb::GetRequest();
    request->set_key(key);
    request->set_version(version);

    auto context = request->mutable_context();
    context->set_priority(::kvrpcpb::Normal);
    context->set_not_fill_cache(false);
    auto rpc_call = std::make_shared<RpcCall<kvrpcpb::GetRequest>>(request);

    regionClient.sendReqToRegion(bo, rpc_call, false);

    auto response = rpc_call->getResp();
    if (response->has_error())
    {
        throw Exception("has key error", LockError);
    }
    return response->value();
}

Scanner Snapshot::Scan(const std::string & begin, const std::string & end) { return Scanner(*this, begin, end, scan_batch_size); }

} // namespace kv
} // namespace pingcap
