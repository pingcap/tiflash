#pragma once

#include <map>
#include <unordered_map>

#include <common/Log.h>
#include <kvproto/errorpb.pb.h>
#include <kvproto/metapb.pb.h>
#include <pd/Client.h>
#include <tikv/Backoff.h>

namespace pingcap
{
namespace kv
{

struct Store
{
    uint64_t id;
    std::string addr;
    std::map<std::string, std::string> labels;

    Store() {}
    Store(uint64_t id_, const std::string & addr_, const std::map<std::string, std::string> & labels_)
        : id(id_), addr(addr_), labels(labels_)
    {}
    Store(Store &&) = default;
    Store(const Store &) = default;
    Store & operator=(const Store & rhs)
    {
        id = rhs.id;
        addr = rhs.addr;
        labels = rhs.labels;
        return *this;
    }
};

struct RegionVerID
{
    uint64_t id;
    uint64_t confVer;
    uint64_t ver;

    RegionVerID(uint64_t id_, uint64_t conf_ver, uint64_t ver_) : id(id_), confVer(conf_ver), ver(ver_) {}

    bool operator==(const RegionVerID & rhs) const { return id == rhs.id && confVer == rhs.confVer && ver == rhs.ver; }
};

} // namespace kv
} // namespace pingcap

namespace std
{
template <>
struct hash<pingcap::kv::RegionVerID>
{
    using argument_type = pingcap::kv::RegionVerID;
    using result_type = size_t;
    size_t operator()(const pingcap::kv::RegionVerID & key) const { return key.id ^ key.confVer ^ key.ver; }
};
} // namespace std

namespace pingcap
{
namespace kv
{

struct Region
{
    metapb::Region meta;
    metapb::Peer peer;
    metapb::Peer learner;

    Region(const metapb::Region & meta_, const metapb::Peer & peer_) : meta(meta_), peer(peer_), learner(metapb::Peer::default_instance())
    {}

    Region(const metapb::Region & meta_, const metapb::Peer & peer_, const metapb::Peer & learner_)
        : meta(meta_), peer(peer_), learner(learner_)
    {}

    const std::string & startKey() { return meta.start_key(); }

    const std::string & endKey() { return meta.end_key(); }

    bool contains(const std::string & key) { return key >= startKey() && key < endKey(); }

    RegionVerID verID()
    {
        return RegionVerID{
            meta.id(),
            meta.region_epoch().conf_ver(),
            meta.region_epoch().version(),
        };
    }

    bool switchPeer(uint64_t store_id)
    {
        for (int i = 0; i < meta.peers_size(); i++)
        {
            if (store_id == meta.peers(i).store_id())
            {
                peer = meta.peers(i);
                return true;
            }
        }
        return false;
    }
};

using RegionPtr = std::shared_ptr<Region>;

struct KeyLocation
{
    RegionVerID region;
    std::string start_key;
    std::string end_key;

    KeyLocation(const RegionVerID & region_, const std::string & start_key_, const std::string & end_key_)
        : region(region_), start_key(start_key_), end_key(end_key_)
    {}
};

struct RPCContext
{
    RegionVerID region;
    metapb::Region meta;
    metapb::Peer peer;
    std::string addr;

    RPCContext(const RegionVerID & region_, const metapb::Region & meta_, const metapb::Peer & peer_, const std::string & addr_)
        : region(region_), meta(meta_), peer(peer_), addr(addr_)
    {}
};

using RPCContextPtr = std::shared_ptr<RPCContext>;

class RegionCache
{
public:
    RegionCache(pd::ClientPtr pdClient_, std::string key_, std::string value_)
        : pdClient(pdClient_), learner_key(std::move(key_)), learner_value(std::move(value_)), log(&Logger::get("pingcap.tikv"))
    {}

    RPCContextPtr getRPCContext(Backoffer & bo, const RegionVerID & id, bool is_learner);

    void updateLeader(Backoffer & bo, const RegionVerID & region_id, uint64_t leader_store_id);

    KeyLocation locateKey(Backoffer & bo, const std::string & key);

    void dropRegion(const RegionVerID &);

    void dropStore(uint64_t failed_store_id);

    void dropStoreOnSendReqFail(RPCContextPtr & ctx, const Exception & exc);

    void onRegionStale(RPCContextPtr ctx, const errorpb::EpochNotMatch & epoch_not_match);

private:
    RegionPtr getCachedRegion(Backoffer & bo, const RegionVerID & id);

    RegionPtr loadRegion(Backoffer & bo, std::string key);

    metapb::Peer selectLearner(Backoffer & bo, const std::vector<metapb::Peer> & slaves);

    RegionPtr loadRegionByID(Backoffer & bo, uint64_t region_id);

    metapb::Store loadStore(Backoffer & bo, uint64_t id);

    Store reloadStore(Backoffer & bo, uint64_t id);

    std::string getStoreAddr(Backoffer & bo, uint64_t id);
    Store getStore(Backoffer & bo, uint64_t id);

    RegionPtr searchCachedRegion(const std::string & key);

    void insertRegionToCache(RegionPtr region);

    std::map<std::string, RegionPtr> regions_map;

    std::unordered_map<RegionVerID, RegionPtr> regions;

    std::map<uint64_t, Store> stores;

    pd::ClientPtr pdClient;

    std::mutex region_mutex;

    std::mutex store_mutex;

    std::string learner_key;

    std::string learner_value;

    Logger * log;
};

using RegionCachePtr = std::shared_ptr<RegionCache>;

} // namespace kv
} // namespace pingcap
