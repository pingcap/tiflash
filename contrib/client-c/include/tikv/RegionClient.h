#pragma once

#include<tikv/Rpc.h>
#include<tikv/Region.h>
#include<tikv/Backoff.h>

namespace pingcap {
namespace kv {

struct RegionClient {
    RegionCachePtr      cache;
    RpcClientPtr        client;
    std::string         store_addr;
    RegionVerID         region_id;

    RegionClient(RegionCachePtr cache_, RpcClientPtr client_, const RegionVerID & id) : cache(cache_), client(client_), store_addr("you guess?"), region_id(id) {}

    int64_t getReadIndex() {
        auto request = new kvrpcpb::ReadIndexRequest();
        Backoffer bo(10000);
        auto rpc_call = std::make_shared<RpcCall<kvrpcpb::ReadIndexRequest>>(request);
        auto ctx = cache -> getRPCContext(bo, region_id, true);
        store_addr = ctx->addr;
        sendReqToRegion(bo, rpc_call, ctx);
        return rpc_call -> getResp() -> read_index();
    }

    template<typename T>
    void sendReqToRegion(Backoffer & bo, RpcCallPtr<T> rpc, RPCContextPtr rpc_ctx) {
        try {
            rpc -> setCtx(rpc_ctx);
            client -> sendRequest(store_addr, rpc);
        }
        catch(const Exception & e) {
            onSendFail(bo, e);
        }

    }

    void onSendFail(Backoffer & ,const Exception & e) {
        e.rethrow();
    }
};

using RegionClientPtr = std::shared_ptr<RegionClient>;

}
}

