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

    Logger * log;

    RegionClient(RegionCachePtr cache_, RpcClientPtr client_, const RegionVerID & id) : cache(cache_), client(client_), store_addr("you guess?"), region_id(id), log(&Logger::get("pingcap.tikv")) {}

    int64_t getReadIndex() {
        auto request = new kvrpcpb::ReadIndexRequest();
        Backoffer bo(readIndexMaxBackoff);
        auto rpc_call = std::make_shared<RpcCall<kvrpcpb::ReadIndexRequest>>(request);
        sendReqToRegion(bo, rpc_call, true);
        return rpc_call -> getResp() -> read_index();
    }

    template<typename T>
    void sendReqToRegion(Backoffer & bo, RpcCallPtr<T> rpc, bool learner) {
        for (;;) {
            RPCContextPtr ctx; 
            try {
                ctx = cache -> getRPCContext(bo, region_id, learner);
            } catch (const Exception & e) {
                onGetLearnerFail(bo, e);
                continue;
            }
            store_addr = ctx->addr;
            rpc -> setCtx(ctx);
            try {
                client -> sendRequest(store_addr, rpc);
            } catch(const Exception & e) {
                onSendFail(bo, e, ctx);
                continue;
            }
            auto resp = rpc -> getResp();
            if (resp -> has_region_error()) {
                onRegionError(bo, ctx, resp->region_error());
            } else {
                return;
            }
        }
    }

    void onRegionError(Backoffer & bo, RPCContextPtr rpc_ctx, const errorpb::Error & err) {
        if (err.has_not_leader()) {
            auto not_leader = err.not_leader();
            if (not_leader.has_leader()) {
                cache -> updateLeader(bo, rpc_ctx->region, not_leader.leader().store_id());
                bo.backoff(boUpdateLeader, Exception("not leader"));
            } else {
                cache -> dropRegion(rpc_ctx->region);
                bo.backoff(boRegionMiss, Exception("not leader"));
            }
            return;
        }

        if (err.has_store_not_match()) {
            cache -> dropStore(rpc_ctx->peer.store_id());
            return;
        }

        if (err.has_epoch_not_match()) {
            cache -> onRegionStale(rpc_ctx, err.epoch_not_match());
            return;
        }

        if (err.has_server_is_busy()) {
            bo.backoff(boServerBusy, Exception("server busy"));
            return;
        }

        if (err.has_stale_command()) {
            return;
        }

        if (err.has_raft_entry_too_large()) {
            throw Exception("entry too large");
        }

        cache -> dropRegion(rpc_ctx -> region);
    }

    void onGetLearnerFail(Backoffer & bo, const Exception & e) {
        log -> error("error found, retrying. The error msg is: "+ e.message());
        bo.backoff(boTiKVRPC, e);
    }

    void onSendFail(Backoffer & bo, const Exception & e, RPCContextPtr rpc_ctx) {
        cache->dropStoreOnSendReqFail(rpc_ctx, e);
        bo.backoff(boTiKVRPC, e);
    }
};

using RegionClientPtr = std::shared_ptr<RegionClient>;

}
}

