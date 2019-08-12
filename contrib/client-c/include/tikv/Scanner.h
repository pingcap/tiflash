#pragma once

#include <common/CltException.h>
#include <tikv/Backoff.h>
#include <tikv/Snapshot.h>
#include <tikv/Util.h>

namespace pingcap
{
namespace kv
{

struct Scanner
{
    Snapshot snap;
    std::string next_start_key;
    std::string end_key;
    int batch;

    std::vector<::kvrpcpb::KvPair> cache;
    size_t idx;
    bool valid;
    bool eof;


    Logger * log;

    Scanner(Snapshot & snapshot_, std::string start_key_, std::string end_key_, int batch_)
        : snap(snapshot_),
          next_start_key(start_key_),
          end_key(end_key_),
          batch(batch_),
          idx(0),
          valid(true),
          eof(false),
          log(&Logger::get("pingcap.tikv"))
    {
        next();
    }

    // TODO: This function has what kind of exception
    void next()
    {
        Backoffer bo(scanMaxBackoff);
        if (!valid)
        {
            throw Exception("the scanner is valid", LogicalError);
        }

        for (;;)
        {
            idx++;
            if (idx >= cache.size())
            {
                if (eof)
                {
                    valid = false;
                    return;
                }
                getData(bo);
                if (idx >= cache.size())
                {
                    continue;
                }
            }

            auto current = cache[idx];
            if (end_key.size() > 0 && current.key() >= end_key)
            {
                eof = true;
                valid = false;
            }

            return;
        }
    }

    std::string key()
    {
        if (valid)
            return cache[idx].key();
        return "";
    }

    std::string value()
    {
        if (valid)
            return cache[idx].value();
        return "";
    }

private:
    void getData(Backoffer & bo)
    {
        log->debug("get data for scanner");
        auto loc = snap.cache->locateKey(bo, next_start_key);
        auto req_end_key = end_key;
        if (req_end_key.size() > 0 && loc.end_key.size() > 0 && loc.end_key < req_end_key)
            req_end_key = loc.end_key;

        auto regionClient = RegionClient(snap.cache, snap.client, loc.region);
        auto request = new kvrpcpb::ScanRequest();
        request->set_start_key(next_start_key);
        request->set_end_key(req_end_key);
        request->set_limit(batch);
        request->set_version(snap.version);
        request->set_key_only(false);

        auto context = request->mutable_context();
        context->set_priority(::kvrpcpb::Normal);
        context->set_not_fill_cache(false);

        auto rpc_call = std::make_shared<RpcCall<kvrpcpb::ScanRequest>>(request);
        regionClient.sendReqToRegion(bo, rpc_call, false);

        // TODO Check safe point.

        auto responce = rpc_call->getResp();
        int pairs_size = responce->pairs_size();
        idx = 0;
        cache.clear();
        for (int i = 0; i < pairs_size; i++)
        {
            auto current = responce->pairs(i);
            if (current.has_error())
            {
                // process lock
                throw Exception("has key error", LockError);
            }
            cache.push_back(current);
        }

        log->debug("get pair size: " + std::to_string(pairs_size));

        if (pairs_size < batch)
        {
            next_start_key = loc.end_key;

            if (end_key.size() == 0 || (next_start_key) >= end_key)
            {
                eof = true;
            }

            return;
        }

        auto lastKey = responce->pairs(responce->pairs_size() - 1);
        next_start_key = prefixNext(lastKey.key());
    }
};

// end of namespace.
} // namespace kv
} // namespace pingcap
