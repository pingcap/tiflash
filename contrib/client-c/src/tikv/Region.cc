#include <tikv/Region.h>
#include <tikv/Exception.h>

namespace pingcap {
namespace kv {

RPCContextPtr RegionCache::getRPCContext(Backoffer & bo, const RegionVerID & id, bool is_learner) {
    RegionPtr region = getCachedRegion(bo, id);
    if (region == nullptr) {
        return nullptr;
    }
    const auto & meta = region -> meta;
    auto  peer = region -> peer;
    if (is_learner) {
        peer = region -> learner;
        if (!peer.IsInitialized()) {
            throw Exception("no learner");
        }
    }
    std::string addr = getStoreAddr(bo, peer.store_id());
    if (addr == "") {
        //dropRegion(id);
        return NULL;
    }
    return std::make_shared<RPCContext>(id, meta, peer, addr);
}

RegionPtr RegionCache::getCachedRegion(Backoffer & bo, const RegionVerID & id) {
    std::lock_guard<std::mutex> lock(region_mutex);
    auto it = regions.find(id);
    if (it == regions.end()) {
        auto region = loadRegionByID(bo, id.id);

        insertRegionToCache(region);

        return region;
    }
    return it->second;
}

//KeyLocation RegionCache::locateKey(Backoffer & bo, std::string key) {
//    RegionPtr region = searchCachedRegion(key);
//    if (region != nullptr) {
//        return KeyLocation (region -> verID() , region -> startKey(), region -> endKey());
//    }
//
//    region = loadRegion(bo, key);
//
//    insertRegionToCache(region);
//
//    return KeyLocation (region -> verID() , region -> startKey(), region -> endKey());
//}

RegionPtr RegionCache::loadRegionByID(Backoffer & bo, uint64_t region_id) {
    for(;;) {
        try {
            auto [meta, leader, learner] = pdClient->getRegionByID(region_id);
            if (!meta.IsInitialized()) {
                throw Exception("meta not found");
            }
            if (meta.peers_size() == 0) {
                throw Exception("receive Region with no peer.");
            }
            RegionPtr region = std::make_shared<Region>(meta, meta.peers(0), learner);
            if (leader.IsInitialized()) {
                region -> switchPeer(leader.store_id());
            }
            return region;
        } catch (const Exception & e) {
            bo.backoff(boPDRPC, e);
        }
    }
}

RegionPtr RegionCache::loadRegion(Backoffer & bo, std::string key) {
    for(;;) {
        try {
            auto [meta, leader, learner] = pdClient->getRegion(key);
            if (!meta.IsInitialized()) {
                throw Exception("meta not found");
            }
            if (meta.peers_size() == 0) {
                throw Exception("receive Region with no peer.");
            }
            RegionPtr region = std::make_shared<Region>(meta, meta.peers(0), learner);
            if (leader.IsInitialized()) {
                region -> switchPeer(leader.store_id());
            }
            return region;
        } catch (const Exception & e) {
            bo.backoff(boPDRPC, e);
        }
    }
}

std::string RegionCache::loadStoreAddr(Backoffer & bo, uint64_t id) {
    for (;;) {
        try {
            const auto & store = pdClient->getStore(id);
            return store.address();
        } catch (Exception & e) {
            bo.backoff(boPDRPC, e);
        }
    }
}

std::string RegionCache::reloadStoreAddr(Backoffer & bo, uint64_t id) {
    std::string addr = loadStoreAddr(bo, id);
    if (addr == "") {
        return "";
    }
    stores[id] = Store(id, addr);
    return addr;
}

std::string RegionCache::getStoreAddr(Backoffer & bo, uint64_t id) {
    std::lock_guard<std::mutex> lock(store_mutex);
    auto it = stores.find(id);
    if (it != stores.end()) {
        return it -> second.addr;
    }
    return reloadStoreAddr(bo, id);
}

//RegionPtr RegionCache::searchCachedRegion(std::string key) {
//    auto it = regions_map.upper_bound(key);
//    if (it != regions_map.end() && it->second->contains(key)) {
//        return it->second;
//    }
//    return nullptr;
//}

void RegionCache::insertRegionToCache(RegionPtr region) {
//    regions_map[region -> endKey()] = region;
    regions[region->verID()] = region;
}

}
}
