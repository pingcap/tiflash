#pragma once

#include <Storages/Transaction/RegionState.h>

#include <condition_variable>

namespace pingcap::kv
{
struct RegionVerID;
}

namespace DB
{

struct RegionMergeResult;
class Region;
class MetaRaftCommandDelegate;
class RegionRaftCommandDelegate;
using TerminateWaitIndex = bool;

struct RegionMetaSnapshot
{
    RegionVersion ver;
    RegionVersion conf_ver;
    ImutRegionRangePtr range;
    metapb::Peer peer;
};

class RegionMeta
{
public:
    RegionMeta(metapb::Peer peer_, raft_serverpb::RaftApplyState apply_state_, const UInt64 applied_term_,
        raft_serverpb::RegionLocalState region_state_);

    RegionMeta(metapb::Peer peer_, metapb::Region region, raft_serverpb::RaftApplyState apply_state_);

    RegionMeta(RegionMeta && meta);

    RegionID regionId() const;
    UInt64 peerId() const;
    UInt64 storeId() const;

    UInt64 appliedIndex() const;
    UInt64 appliedTerm() const;

    ImutRegionRangePtr getRange() const;

    metapb::Peer getPeer() const;
    pingcap::kv::RegionVerID getRegionVerID() const;

    UInt64 version() const;

    UInt64 confVer() const;

    raft_serverpb::RaftApplyState getApplyState() const;

    void setApplied(UInt64 index, UInt64 term);
    void notifyAll() const;

    std::string toString(bool dump_status = true) const;

    std::tuple<size_t, UInt64> serialize(WriteBuffer & buf) const;

    static RegionMeta deserialize(ReadBuffer & buf);

    raft_serverpb::PeerState peerState() const;
    void setPeerState(const raft_serverpb::PeerState peer_state_);

    void assignRegionMeta(RegionMeta && other);

    friend bool operator==(const RegionMeta & meta1, const RegionMeta & meta2);

    TerminateWaitIndex waitIndex(UInt64 index, std::function<bool(void)> &&) const;
    bool checkIndex(UInt64 index) const;

    RegionMetaSnapshot dumpRegionMetaSnapshot() const;
    MetaRaftCommandDelegate & makeRaftCommandDelegate();

    metapb::Region getMetaRegion() const;
    raft_serverpb::MergeState getMergeState() const;

private:
    RegionMeta() = delete;
    friend class MetaRaftCommandDelegate;

    void doSetRegion(const metapb::Region & region);
    void doSetApplied(UInt64 index, UInt64 term);
    bool doCheckIndex(UInt64 index) const;
    bool doCheckPeerRemoved() const;

private:
    metapb::Peer peer;

    // raft_serverpb::RaftApplyState contains applied_index_ and it's truncated_state_ can be used for CompactLog.
    raft_serverpb::RaftApplyState apply_state;
    UInt64 applied_term;

    RegionState region_state;

    mutable std::mutex mutex;
    mutable std::condition_variable cv;
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

class MetaRaftCommandDelegate : public RegionMeta, private boost::noncopyable
{
    friend class RegionRaftCommandDelegate;

    MetaRaftCommandDelegate() = delete;

    const metapb::Peer & getPeer() const;
    const raft_serverpb::RaftApplyState & applyState() const;
    const RegionState & regionState() const;

    void execChangePeer(const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response, UInt64 index, UInt64 term);
    void execPrepareMerge(const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response, UInt64 index, UInt64 term);
    void execCommitMerge(const RegionMergeResult & result, UInt64 index, UInt64 term, const MetaRaftCommandDelegate & source_meta,
        const raft_cmdpb::AdminResponse & response);
    RegionMergeResult checkBeforeCommitMerge(const raft_cmdpb::AdminRequest & request, const MetaRaftCommandDelegate & source_meta) const;
    void execRollbackMerge(
        const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response, const UInt64 index, const UInt64 term);
};

} // namespace DB
