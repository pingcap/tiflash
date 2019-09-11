#include <Storages/Transaction/RaftCommandResult.h>
#include <Storages/Transaction/RegionMeta.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tikv/Region.h>
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
        throw Exception("[RegionMeta::doSetRegion] region id is not equal, should not happen", ErrorCodes::LOGICAL_ERROR);

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

enginepb::CommandResponse RegionMeta::toCommandResponse() const
{
    std::lock_guard<std::mutex> lock(mutex);
    enginepb::CommandResponse resp;
    resp.mutable_header()->set_region_id(regionId());
    resp.mutable_apply_state()->CopyFrom(apply_state);
    resp.set_applied_term(applied_term);
    return resp;
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

void RegionMeta::waitIndex(UInt64 index) const
{
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait(lock, [this, index] { return doCheckIndex(index); });
}

bool RegionMeta::checkIndex(UInt64 index) const
{
    std::lock_guard<std::mutex> lock(mutex);
    return doCheckIndex(index);
}

bool RegionMeta::doCheckIndex(UInt64 index) const
{
    return region_state.getState() == raft_serverpb::PeerState::Tombstone || apply_state.applied_index() >= index;
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
        throw Exception("[RegionMeta::assignRegionMeta] region_id not equal, should not happen", ErrorCodes::LOGICAL_ERROR);

    peer = std::move(rhs.peer);
    apply_state = std::move(rhs.apply_state);
    applied_term = rhs.applied_term;
    region_state = std::move(rhs.region_state);
}

void MetaRaftCommandDelegate::execChangePeer(
    const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response, UInt64 index, UInt64 term)
{
    std::lock_guard<std::mutex> lock(mutex);

    const auto & change_peer_request = request.change_peer();
    const auto & new_region = response.change_peer().region();

    bool pending_remove = false;
    switch (change_peer_request.change_type())
    {
        case eraftpb::ConfChangeType::AddNode:
        case eraftpb::ConfChangeType::AddLearnerNode:
        {
            // change the peers of region, add conf_ver.
            doSetRegion(new_region);
            break;
        }
        case eraftpb::ConfChangeType::RemoveNode:
        {
            if (peer.id() == change_peer_request.peer().id())
                pending_remove = true;

            doSetRegion(new_region);
            break;
        }
        default:
            throw Exception("[RegionMeta::execChangePeer] unsupported cmd", ErrorCodes::LOGICAL_ERROR);
    }

    if (pending_remove)
        region_state.setState(raft_serverpb::PeerState::Tombstone);
    else
        region_state.setState(raft_serverpb::PeerState::Normal);
    region_state.clearMergeState();
    doSetApplied(index, term);
}

void MetaRaftCommandDelegate::execCompactLog(
    const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse &, const UInt64 index, const UInt64 term)
{
    const auto & compact_log_request = request.compact_log();

    std::lock_guard<std::mutex> lock(mutex);

    apply_state.mutable_truncated_state()->set_term(compact_log_request.compact_term());
    apply_state.mutable_truncated_state()->set_index(compact_log_request.compact_index());

    doSetApplied(index, term);
}

bool RegionMeta::isPeerRemoved() const
{
    std::lock_guard<std::mutex> lock(mutex);

    if (region_state.getState() == raft_serverpb::PeerState::Tombstone)
        return true;

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

std::tuple<RegionVersion, RegionVersion, ImutRegionRangePtr> RegionMeta::dumpVersionRange() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return {region_state.getVersion(), region_state.getConfVersion(), region_state.getRange()};
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
