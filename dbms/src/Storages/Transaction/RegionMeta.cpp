#include <Storages/Transaction/RegionExecutionResult.h>
#include <Storages/Transaction/RegionMeta.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#include <pingcap/kv/RegionCache.h>
#pragma GCC diagnostic pop

namespace DB
{

std::tuple<size_t, UInt64> RegionMeta::serialize(WriteBuffer & buf) const
{
    std::lock_guard<std::mutex> lock(mutex);

    size_t size = 0;
    size += writeBinary2(peer, buf);
    size += writeBinary2(apply_state, buf);
    size += writeBinary2(applied_term, buf);
    size += writeBinary2(region_state.getBase(), buf);
    return {size, apply_state.applied_index()};
}

RegionMeta RegionMeta::deserialize(ReadBuffer & buf)
{
    auto peer = readPeer(buf);
    auto apply_state = readApplyState(buf);
    auto applied_term = readBinary2<UInt64>(buf);
    auto region_state = readRegionLocalState(buf);
    return RegionMeta(std::move(peer), std::move(apply_state), applied_term, std::move(region_state));
}

RegionID RegionMeta::regionId() const { return region_id; }

UInt64 RegionMeta::peerId() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return peer.id();
}

UInt64 RegionMeta::storeId() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return peer.store_id();
}

metapb::Peer RegionMeta::getPeer() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return peer;
}

pingcap::kv::RegionVerID RegionMeta::getRegionVerID() const
{
    std::lock_guard<std::mutex> lock(mutex);

    return pingcap::kv::RegionVerID{regionId(), region_state.getConfVersion(), region_state.getVersion()};
}

raft_serverpb::RaftApplyState RegionMeta::getApplyState() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return apply_state;
}

void RegionMeta::doSetRegion(const metapb::Region & region)
{
    if (regionId() != region.id())
        throw Exception(std::string(__PRETTY_FUNCTION__) + ": region id is not equal, should not happen", ErrorCodes::LOGICAL_ERROR);

    region_state.setRegion(region);
}

void RegionMeta::setApplied(UInt64 index, UInt64 term)
{
    std::lock_guard<std::mutex> lock(mutex);
    doSetApplied(index, term);
}

void RegionMeta::doSetApplied(UInt64 index, UInt64 term)
{
    apply_state.set_applied_index(index);
    applied_term = term;
}

void RegionMeta::notifyAll() const { cv.notify_all(); }

UInt64 RegionMeta::appliedIndex() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return apply_state.applied_index();
}

UInt64 RegionMeta::appliedTerm() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return applied_term;
}

RegionMeta::RegionMeta(RegionMeta && rhs) : region_id(rhs.regionId())
{
    std::lock_guard<std::mutex> lock(rhs.mutex);

    peer = std::move(rhs.peer);
    apply_state = std::move(rhs.apply_state);
    applied_term = rhs.applied_term;
    region_state = std::move(rhs.region_state);
}

ImutRegionRangePtr RegionMeta::getRange() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return region_state.getRange();
}

std::string RegionMeta::toString(bool dump_status) const
{
    std::stringstream ss;
    ss << "[region " << regionId();
    if (dump_status)
    {
        UInt64 term = 0;
        UInt64 index = 0;
        {
            std::lock_guard<std::mutex> lock(mutex);
            term = applied_term;
            index = apply_state.applied_index();
        }
        ss << ", applied: term " << term << " index " << index;
    }
    ss << "]";
    return ss.str();
}

raft_serverpb::PeerState RegionMeta::peerState() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return region_state.getState();
}

void RegionMeta::setPeerState(const raft_serverpb::PeerState peer_state_)
{
    std::lock_guard<std::mutex> lock(mutex);
    region_state.setState(peer_state_);
}

TerminateWaitIndex RegionMeta::waitIndex(UInt64 index, std::function<bool(void)> && check_running) const
{
    std::unique_lock<std::mutex> lock(mutex);
    TerminateWaitIndex res = false;
    cv.wait(lock, [&] {
        if (!check_running())
        {
            res = true;
            return true;
        }
        return doCheckIndex(index);
    });
    return res;
}

bool RegionMeta::checkIndex(UInt64 index) const
{
    std::lock_guard<std::mutex> lock(mutex);
    return doCheckIndex(index);
}

bool RegionMeta::doCheckIndex(UInt64 index) const
{
    return region_state.getState() != raft_serverpb::PeerState::Normal || apply_state.applied_index() >= index;
}

UInt64 RegionMeta::version() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return region_state.getVersion();
}

UInt64 RegionMeta::confVer() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return region_state.getConfVersion();
}

void RegionMeta::assignRegionMeta(RegionMeta && rhs)
{
    std::lock_guard<std::mutex> lock(mutex);

    if (regionId() != rhs.regionId())
        throw Exception(std::string(__PRETTY_FUNCTION__) + ": region_id not equal, should not happen", ErrorCodes::LOGICAL_ERROR);

    peer = std::move(rhs.peer);
    apply_state = std::move(rhs.apply_state);
    applied_term = rhs.applied_term;
    region_state = std::move(rhs.region_state);
}

void MetaRaftCommandDelegate::execChangePeer(
    const raft_cmdpb::AdminRequest &, const raft_cmdpb::AdminResponse & response, UInt64 index, UInt64 term)
{
    std::lock_guard<std::mutex> lock(mutex);

    const auto & new_region = response.change_peer().region();

    doSetRegion(new_region);
    if (doCheckPeerRemoved())
        region_state.setState(raft_serverpb::PeerState::Tombstone);
    else
        region_state.setState(raft_serverpb::PeerState::Normal);
    region_state.clearMergeState();
    doSetApplied(index, term);
}

RegionMergeResult MetaRaftCommandDelegate::checkBeforeCommitMerge(
    const raft_cmdpb::AdminRequest & request, const MetaRaftCommandDelegate & source_meta) const
{
    auto & commit_merge_request = request.commit_merge();
    auto & source_region = commit_merge_request.source();

    switch (auto state = source_meta.region_state.getState())
    {
        case raft_serverpb::PeerState::Merging:
        case raft_serverpb::PeerState::Normal:
            break;
        default:
        {
            throw Exception(std::string(__PRETTY_FUNCTION__) + ": " + toString(false)
                    + " unexpected state of source region: " + raft_serverpb::PeerState_Name(state),
                ErrorCodes::LOGICAL_ERROR);
        }
    }

    if (!(source_region == source_meta.region_state.getRegion()))
        throw Exception(std::string(__PRETTY_FUNCTION__) + ": source_region not match exist region", ErrorCodes::LOGICAL_ERROR);

    RegionMergeResult res;

    res.version = std::max(source_region.region_epoch().version(), region_state.getVersion()) + 1;

    if (source_region.start_key().empty())
    {
        res.source_at_left = true;
    }
    else
    {
        res.source_at_left = source_region.end_key() == region_state.getRegion().start_key();
    }

    return res;
}

static void CheckRegionForMergeCmd(const raft_cmdpb::AdminResponse & response, const RegionState & region_state)
{
    if (response.has_split() && !(response.split().left() == region_state.getRegion()))
        throw Exception(std::string(__PRETTY_FUNCTION__) + ": current region:\n" + region_state.getRegion().ShortDebugString()
                + "\nexpect:\n" + response.split().left().ShortDebugString() + "\nshould not happen",
            ErrorCodes::LOGICAL_ERROR);
}

void MetaRaftCommandDelegate::execRollbackMerge(
    const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response, const UInt64 index, const UInt64 term)
{
    auto & rollback_request = request.rollback_merge();

    if (region_state.getState() != raft_serverpb::PeerState::Merging)
        throw Exception(std::string(__PRETTY_FUNCTION__) + ": region state is " + raft_serverpb::PeerState_Name(region_state.getState()),
            ErrorCodes::LOGICAL_ERROR);
    if (region_state.getMergeState().commit() != rollback_request.commit())
        throw Exception(std::string(__PRETTY_FUNCTION__) + ": merge commit index " + DB::toString(region_state.getMergeState().commit())
                + " != " + DB::toString(rollback_request.commit()),
            ErrorCodes::LOGICAL_ERROR);

    std::lock_guard<std::mutex> lock(mutex);
    const auto version = region_state.getVersion() + 1;
    region_state.setVersion(version);
    region_state.setState(raft_serverpb::PeerState::Normal);
    region_state.clearMergeState();
    doSetApplied(index, term);

    CheckRegionForMergeCmd(response, region_state);
}

void MetaRaftCommandDelegate::execCommitMerge(const RegionMergeResult & res, UInt64 index, UInt64 term,
    const MetaRaftCommandDelegate & source_meta, const raft_cmdpb::AdminResponse & response)
{
    std::lock_guard<std::mutex> lock(mutex);
    region_state.setVersion(res.version);
    if (res.source_at_left)
        region_state.setStartKey(source_meta.region_state.getRegion().start_key());
    else
        region_state.setEndKey(source_meta.region_state.getRegion().end_key());

    region_state.setState(raft_serverpb::PeerState::Normal);
    region_state.clearMergeState();
    doSetApplied(index, term);

    CheckRegionForMergeCmd(response, region_state);
}

void MetaRaftCommandDelegate::execPrepareMerge(
    const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response, UInt64 index, UInt64 term)
{
    auto & prepare_merge_request = request.prepare_merge();
    auto & target = prepare_merge_request.target();

    std::lock_guard<std::mutex> lock(mutex);
    auto & region = region_state.getRegion();
    auto region_version = region.region_epoch().version() + 1;
    region_state.setVersion(region_version);

    auto conf_version = region.region_epoch().conf_ver() + 1;
    region_state.setConfVersion(conf_version);

    auto & merge_state = region_state.getMutMergeState();
    merge_state.set_min_index(prepare_merge_request.min_index());
    *merge_state.mutable_target() = target;
    merge_state.set_commit(index);

    region_state.setState(raft_serverpb::PeerState::Merging);
    doSetApplied(index, term);

    CheckRegionForMergeCmd(response, region_state);
}

bool RegionMeta::doCheckPeerRemoved() const
{
    if (region_state.getRegion().peers().empty())
        throw Exception(std::string(__PRETTY_FUNCTION__) + ": got empty peers, should not happen", ErrorCodes::LOGICAL_ERROR);

    for (const auto & region_peer : region_state.getRegion().peers())
    {
        if (region_peer.id() == peer.id())
            return false;
    }
    return true;
}

bool operator==(const RegionMeta & meta1, const RegionMeta & meta2)
{
    std::lock_guard<std::mutex> lock1(meta1.mutex);
    std::lock_guard<std::mutex> lock2(meta2.mutex);

    return meta1.peer == meta2.peer && meta1.apply_state == meta2.apply_state && meta1.applied_term == meta2.applied_term
        && meta1.region_state == meta2.region_state;
}

RegionMetaSnapshot RegionMeta::dumpRegionMetaSnapshot() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return {region_state.getVersion(), region_state.getConfVersion(), region_state.getRange(), peer};
}

MetaRaftCommandDelegate & RegionMeta::makeRaftCommandDelegate()
{
    static_assert(sizeof(MetaRaftCommandDelegate) == sizeof(RegionMeta));
    return static_cast<MetaRaftCommandDelegate &>(*this);
}

const metapb::Peer & MetaRaftCommandDelegate::getPeer() const { return peer; }
const raft_serverpb::RaftApplyState & MetaRaftCommandDelegate::applyState() const { return apply_state; }
const RegionState & MetaRaftCommandDelegate::regionState() const { return region_state; }

RegionMeta::RegionMeta(metapb::Peer peer_, raft_serverpb::RaftApplyState apply_state_, const UInt64 applied_term_,
    raft_serverpb::RegionLocalState region_state_)
    : peer(std::move(peer_)),
      apply_state(std::move(apply_state_)),
      applied_term(applied_term_),
      region_state(std::move(region_state_)),
      region_id(region_state.getRegion().id())
{}

RegionMeta::RegionMeta(metapb::Peer peer_, metapb::Region region, raft_serverpb::RaftApplyState apply_state_)
    : peer(std::move(peer_)),
      apply_state(std::move(apply_state_)),
      applied_term(apply_state.truncated_state().term()),
      region_id(region.id())
{
    region_state.setRegion(std::move(region));
}

metapb::Region RegionMeta::getMetaRegion() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return region_state.getRegion();
}

raft_serverpb::MergeState RegionMeta::getMergeState() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return region_state.getMergeState();
}

} // namespace DB
