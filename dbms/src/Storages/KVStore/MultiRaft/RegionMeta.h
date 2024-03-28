// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Storages/KVStore/MultiRaft/RegionState.h>

#include <condition_variable>

namespace pingcap::kv
{
struct RegionVerID;
}

namespace DB
{
namespace tests
{
class KVStoreTestBase;
class RegionKVStoreOldTest;
} // namespace tests

struct RegionMergeResult;
class Region;
class MetaRaftCommandDelegate;
class RegionRaftCommandDelegate;
enum class WaitIndexStatus
{
    Finished,
    Terminated, // Read index is terminated due to upper layer.
    Timeout,
};
struct WaitIndexResult
{
    WaitIndexStatus status{WaitIndexStatus::Finished};
    // the applied index before wait index
    UInt64 prev_index = 0;
    // the applied index when wait index finish
    UInt64 current_index = 0;
};

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
    RegionMeta(
        metapb::Peer peer_,
        raft_serverpb::RaftApplyState apply_state_,
        UInt64 applied_term_,
        raft_serverpb::RegionLocalState region_state_);

    RegionMeta(metapb::Peer peer_, metapb::Region region, raft_serverpb::RaftApplyState apply_state_);

    RegionMeta(RegionMeta && rhs);

    RegionID regionId() const;
    UInt64 peerId() const;
    UInt64 storeId() const;

    UInt64 appliedIndex() const;
    UInt64 appliedIndexTerm() const;
    UInt64 truncateIndex() const;

    ImutRegionRangePtr getRange() const;

    metapb::Peer getPeer() const;
    void setPeer(metapb::Peer &&);

    UInt64 version() const;

    UInt64 confVer() const;

    raft_serverpb::RaftApplyState clonedApplyState() const;
    raft_serverpb::RegionLocalState clonedRegionState() const;

    void setApplied(UInt64 index, UInt64 term);
    void notifyAll() const;

    std::string toString(bool dump_status = true) const;

    std::tuple<size_t, UInt64> serialize(WriteBuffer & buf) const;
    static RegionMeta deserialize(ReadBuffer & buf);

    raft_serverpb::PeerState peerState() const;
    void setPeerState(const raft_serverpb::PeerState & peer_state_);

    void assignRegionMeta(RegionMeta && rhs);

    friend bool operator==(const RegionMeta & meta1, const RegionMeta & meta2);

    // Wait until the applied index reach `index` and return WaitIndexResult::Finished.
    // If `timeout_ms` == 0, it waits infinite except `check_running` return false.
    //    `timeout_ms` != 0 and not reaching `index` after waiting for `timeout_ms`, Return WaitIndexResult::Timeout.
    // If `check_running` return false, returns WaitIndexResult::Terminated
    WaitIndexResult waitIndex(UInt64 index, UInt64 timeout_ms, std::function<bool(void)> && check_running) const;
    bool checkIndex(UInt64 index) const;

    RegionMetaSnapshot dumpRegionMetaSnapshot() const;
    MetaRaftCommandDelegate & makeRaftCommandDelegate();

    const metapb::Region & getMetaRegion() const;
    metapb::Region cloneMetaRegion() const;
    const raft_serverpb::MergeState & getMergeState() const;
    raft_serverpb::MergeState cloneMergeState() const;
    const RegionState & getRegionState() const;
    RegionState & debugMutRegionState();
    RegionMeta clone() const
    {
        std::lock_guard lock(mutex);
        return RegionMeta(peer, region_state.getRegion(), apply_state);
    }

    RegionMeta() = delete;

private:
    friend class MetaRaftCommandDelegate;
    friend class tests::KVStoreTestBase;
    friend class tests::RegionKVStoreOldTest;

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

class MetaRaftCommandDelegate
    : public RegionMeta
    , private boost::noncopyable
{
    friend class RegionRaftCommandDelegate;
    friend class tests::KVStoreTestBase;
    friend class tests::RegionKVStoreOldTest;

    const metapb::Peer & getPeer() const;
    const raft_serverpb::RaftApplyState & applyState() const;
    const RegionState & regionState() const;

    void execChangePeer(
        const raft_cmdpb::AdminRequest & request,
        const raft_cmdpb::AdminResponse & response,
        UInt64 index,
        UInt64 term);
    void execPrepareMerge(
        const raft_cmdpb::AdminRequest & request,
        const raft_cmdpb::AdminResponse & response,
        UInt64 index,
        UInt64 term);
    void execCommitMerge(
        const RegionMergeResult & result,
        UInt64 index,
        UInt64 term,
        const MetaRaftCommandDelegate & source_meta,
        const raft_cmdpb::AdminResponse & response);
    RegionMergeResult checkBeforeCommitMerge(
        const raft_cmdpb::AdminRequest & request,
        const MetaRaftCommandDelegate & source_meta) const;
    void execRollbackMerge(
        const raft_cmdpb::AdminRequest & request,
        const raft_cmdpb::AdminResponse & response,
        UInt64 index,
        UInt64 term);

public:
    static RegionMergeResult computeRegionMergeResult(
        const metapb::Region & source_region,
        const metapb::Region & target_region);

    MetaRaftCommandDelegate() = delete;
};

} // namespace DB
