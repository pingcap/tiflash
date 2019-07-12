#pragma once

#include <tikv/RegionClient.h>

namespace pingcap {
namespace kv {

class Scanner;

struct Snapshot {
    RegionCachePtr      cache;
    RpcClientPtr        client;
    const uint64_t      version;


    Snapshot(RegionCachePtr cache_, RpcClientPtr client_, uint64_t ver) : cache(cache_), client(client_), version(ver) {
    }

    std::string Get(const std::string & key);

    Scanner Scan(const std::string & begin, const std::string & end);
};

}
}
