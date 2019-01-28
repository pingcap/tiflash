#pragma once

#include <Storages/Transaction/TiKVKeyValue.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tikv/Region.h>
#pragma GCC diagnostic pop

namespace DB
{

using RegionRange = std::pair<TiKVKey, TiKVKey>;

class RegionMeta
{
public:
    RegionMeta(const metapb::Peer & peer_, const metapb::Region & region_, const raft_serverpb::RaftApplyState & apply_state_)
        : peer(peer_), region(region_), apply_state(apply_state_), applied_term(apply_state.truncated_state().term())
    {}

    RegionMeta(const metapb::Peer & peer_, const metapb::Region & region_, const raft_serverpb::RaftApplyState & apply_state_,
        UInt64 applied_term_, bool pending_remove_)
        : peer(peer_), region(region_), apply_state(apply_state_), applied_term(applied_term_), pending_remove(pending_remove_)
    {}

    RegionMeta(RegionMeta && meta);
    RegionMeta(const RegionMeta & meta);

    RegionID regionId() const;
    UInt64 peerId() const;
    UInt64 storeId() const;

    UInt64 appliedIndex() const;
    UInt64 appliedTerm() const;

    RegionRange getRange() const;

    metapb::Peer getPeer() const;
    metapb::Region getRegion() const;
    pingcap::kv::RegionVerID getRegionVerID() const {
        return pingcap::kv::RegionVerID {
            region.id(),
            region.region_epoch().conf_ver(),
            region.region_epoch().version()
        };
    }

    raft_serverpb::RaftApplyState getApplyState() const;

    void setRegion(const metapb::Region & region);
    void setApplied(UInt64 index, UInt64 term);

    std::string toString(bool dump_status = true) const;

    enginepb::CommandResponse toCommandResponse() const;

    size_t serializeSize() const;
    size_t serialize(WriteBuffer & buf);

    static RegionMeta deserialize(ReadBuffer & buf);

    bool isPendingRemove() const;
    void setPendingRemove();

    void swap(RegionMeta & other)
    {
        peer.Swap(&other.peer);
        region.Swap(&other.region);
        apply_state.Swap(&other.apply_state);
        std::swap(applied_term, other.applied_term);
        std::swap(pending_remove, other.pending_remove);
    }

    friend bool operator==(const RegionMeta & meta1, const RegionMeta & meta2)
    {
        return meta1.peer == meta2.peer && meta1.region == meta2.region && meta1.apply_state == meta2.apply_state
            && meta1.applied_term == meta2.applied_term;
    }

    void wait_index(UInt64 index);

private:
    metapb::Peer peer;
    metapb::Region region;
    raft_serverpb::RaftApplyState apply_state;
    UInt64 applied_term;

    bool pending_remove = false;

    mutable std::mutex mutex;
    std::condition_variable cv;
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
