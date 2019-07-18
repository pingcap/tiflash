#pragma once

#include <condition_variable>

#include <Storages/Transaction/TiKVKeyValue.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/metapb.pb.h>
#include <kvproto/raft_serverpb.pb.h>
#pragma GCC diagnostic pop

namespace pingcap::kv
{
struct RegionVerID;
}

namespace DB
{

using RegionRange = std::pair<TiKVKey, TiKVKey>;

class RegionMeta
{
public:
    RegionMeta(metapb::Peer peer_, raft_serverpb::RaftApplyState apply_state_, const UInt64 applied_term_,
        raft_serverpb::RegionLocalState region_state_)
        : peer(std::move(peer_)),
          apply_state(std::move(apply_state_)),
          applied_term(applied_term_),
          region_state(std::move(region_state_)),
          region_id(region_state.region().id())
    {}

    RegionMeta(metapb::Peer peer_, metapb::Region region, raft_serverpb::RaftApplyState apply_state_)
        : peer(std::move(peer_)),
          apply_state(std::move(apply_state_)),
          applied_term(apply_state.truncated_state().term()),
          region_id(region.id())
    {
        *region_state.mutable_region() = std::move(region);
    }

    RegionMeta(RegionMeta && meta);

    RegionID regionId() const;
    UInt64 peerId() const;
    UInt64 storeId() const;

    UInt64 appliedIndex() const;
    UInt64 appliedTerm() const;

    RegionRange getRange() const;

    metapb::Peer getPeer() const;
    pingcap::kv::RegionVerID getRegionVerID() const;

    UInt64 version() const;

    UInt64 confVer() const;

    raft_serverpb::RaftApplyState getApplyState() const;

    void setApplied(UInt64 index, UInt64 term);
    void notifyAll();

    std::string toString(bool dump_status = true) const;

    enginepb::CommandResponse toCommandResponse() const;

    std::tuple<size_t, UInt64> serialize(WriteBuffer & buf) const;

    static RegionMeta deserialize(ReadBuffer & buf);

    raft_serverpb::PeerState peerState() const;
    void setPeerState(const raft_serverpb::PeerState peer_state_);

    void assignRegionMeta(RegionMeta && other);

    friend bool operator==(const RegionMeta & meta1, const RegionMeta & meta2);

    void waitIndex(UInt64 index);
    bool checkIndex(UInt64 index);

    bool isPeerRemoved() const;

    void execChangePeer(const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response, UInt64 index, UInt64 term);
    void execCompactLog(const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response, UInt64 index, UInt64 term);

private:
    void doSetRegion(const metapb::Region & region);

    void doSetApplied(UInt64 index, UInt64 term);

    bool doCheckIndex(UInt64 index) const;

private:
    metapb::Peer peer;

    // raft_serverpb::RaftApplyState contains applied_index_ and it's truncated_state_ can be used for CompactLog.
    raft_serverpb::RaftApplyState apply_state;
    UInt64 applied_term;

    raft_serverpb::RegionLocalState region_state;

    mutable std::mutex mutex;
    std::condition_variable cv;
    const RegionID region_id;
};

// TODO: Integrate initialApplyState to MockTiKV

// When we create a region peer, we should initialize its log term/index > 0,
// so that we can force the follower peer to sync the snapshot first.
static constexpr UInt64 RAFT_INIT_LOG_TERM = 5;
static constexpr UInt64 RAFT_INIT_LOG_INDEX = 5;

inline raft_serverpb::RaftApplyState initialApplyState()
{
    raft_serverpb::RaftApplyState state;
    state.set_applied_index(RAFT_INIT_LOG_INDEX);
    state.mutable_truncated_state()->set_index(RAFT_INIT_LOG_INDEX);
    state.mutable_truncated_state()->set_term(RAFT_INIT_LOG_TERM);
    return state;
}

} // namespace DB
