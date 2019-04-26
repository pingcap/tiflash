#include <Storages/Transaction/RegionMeta.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tikv/Region.h>
#pragma GCC diagnostic pop

namespace DB
{

// TODO We need encoding version here, otherwise it is impossible to handle structure update.
size_t RegionMeta::serialize(WriteBuffer & buf) const
{
    std::lock_guard<std::mutex> lock(mutex);

    size_t size = 0;
    size += writeBinary2(peer, buf);
    size += writeBinary2(region, buf);
    size += writeBinary2(apply_state, buf);
    size += writeBinary2(applied_term, buf);
    size += writeBinary2(pending_remove, buf);
    return size;
}

RegionMeta RegionMeta::deserialize(ReadBuffer & buf)
{
    auto peer = readPeer(buf);
    auto region = readRegion(buf);
    auto apply_state = readApplyState(buf);
    auto applied_term = readBinary2<UInt64>(buf);
    auto pending_remove = readBinary2<bool>(buf);
    return RegionMeta(peer, region, apply_state, applied_term, pending_remove);
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

metapb::Region RegionMeta::getRegion() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return region;
}

pingcap::kv::RegionVerID RegionMeta::getRegionVerID() const
{
    std::lock_guard<std::mutex> lock(mutex);

    return pingcap::kv::RegionVerID{regionId(), region.region_epoch().conf_ver(), region.region_epoch().version()};
}

const raft_serverpb::RaftApplyState & RegionMeta::getApplyState() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return apply_state;
}

void RegionMeta::doSetRegion(const metapb::Region & region)
{
    if (regionId() != region.id())
        throw Exception("RegionMeta::doSetRegion region_id not equal, should not happen", ErrorCodes::LOGICAL_ERROR);

    this->region = region;
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

void RegionMeta::notifyAll() { cv.notify_all(); }

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
    region = std::move(rhs.region);
    apply_state = std::move(rhs.apply_state);
    applied_term = rhs.applied_term;
    pending_remove = rhs.pending_remove;
}

RegionMeta::RegionMeta(const RegionMeta & rhs) : region_id(rhs.regionId())
{
    std::lock_guard<std::mutex> lock(rhs.mutex);

    peer = rhs.peer;
    region = rhs.region;
    apply_state = rhs.apply_state;
    applied_term = rhs.applied_term;
    pending_remove = rhs.pending_remove;
}

RegionRange RegionMeta::getRange() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return {TiKVKey(region.start_key()), TiKVKey(region.end_key())};
}

std::string RegionMeta::toString(bool dump_status) const
{
    std::stringstream ss;
    std::lock_guard<std::mutex> lock(mutex);
    ss << "region[id: " << regionId();
    if (dump_status)
        ss << ", term: " << applied_term << ", applied_index: " << apply_state.applied_index();
    ss << "]";
    return ss.str();
}

bool RegionMeta::isPendingRemove() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return pending_remove;
}

void RegionMeta::setPendingRemove()
{
    std::lock_guard<std::mutex> lock(mutex);
    doSetPendingRemove();
}

void RegionMeta::doSetPendingRemove() { pending_remove = true; }

void RegionMeta::waitIndex(UInt64 index)
{
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait(lock, [this, index] { return doCheckIndex(index); });
}

bool RegionMeta::checkIndex(UInt64 index)
{
    std::lock_guard<std::mutex> lock(mutex);
    return doCheckIndex(index);
}

bool RegionMeta::doCheckIndex(UInt64 index) { return pending_remove || apply_state.applied_index() >= index; }

UInt64 RegionMeta::version() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return region.region_epoch().version();
}

UInt64 RegionMeta::confVer() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return region.region_epoch().conf_ver();
}

void RegionMeta::assignRegionMeta(RegionMeta && rhs)
{
    std::lock_guard<std::mutex> lock(mutex);

    if (regionId() != rhs.regionId())
        throw Exception("RegionMeta::assignRegionMeta region_id not equal, should not happen", ErrorCodes::LOGICAL_ERROR);

    peer = std::move(rhs.peer);
    region = std::move(rhs.region);
    apply_state = std::move(rhs.apply_state);
    applied_term = rhs.applied_term;
    pending_remove = rhs.pending_remove;
}

void RegionMeta::execChangePeer(
    const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response, UInt64 index, UInt64 term)
{
    const auto & change_peer_request = request.change_peer();
    const auto & new_region = response.change_peer().region();

    switch (change_peer_request.change_type())
    {
        case eraftpb::ConfChangeType::AddNode:
        case eraftpb::ConfChangeType::AddLearnerNode:
        {
            std::lock_guard<std::mutex> lock(mutex);

            // change the peers of region, add conf_ver.
            doSetRegion(new_region);
            doSetApplied(index, term);
            return;
        }
        case eraftpb::ConfChangeType::RemoveNode:
        {
            const auto & peer = change_peer_request.peer();

            std::lock_guard<std::mutex> lock(mutex);

            doSetRegion(new_region);

            if (this->peer.id() == peer.id())
                doSetPendingRemove();

            doSetApplied(index, term);
            return;
        }
        default:
            throw Exception("execChangePeer: unsupported cmd", ErrorCodes::LOGICAL_ERROR);
    }
}

bool RegionMeta::isPeerRemoved() const
{
    std::lock_guard<std::mutex> lock(mutex);

    if (pending_remove)
        return true;
    for (auto region_peer : region.peers())
    {
        if (region_peer.id() == peer.id())
            return false;
    }
    return true;
}

} // namespace DB
