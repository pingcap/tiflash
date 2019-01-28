#include <Storages/Transaction/RegionMeta.h>

namespace DB
{

// TODO We need encoding version here, otherwise it is impossible to handle structure update.
// Maybe use protobuf directly.

size_t RegionMeta::serializeSize() const
{
    std::lock_guard<std::mutex> lock(mutex);

    auto peer_size = sizeof(UInt64) + sizeof(UInt64) + sizeof(bool);
    // TODO: this region_size not right, 4 bytes missed
    auto region_size = sizeof(UInt64)
        + sizeof(UInt32) + KEY_SIZE_WITHOUT_TS
        + sizeof(UInt32) + KEY_SIZE_WITHOUT_TS
        + sizeof(UInt64) + sizeof(UInt64);
    region_size += peer_size * region.peers_size();
    auto apply_state_size = sizeof(UInt64) + sizeof(UInt64) + sizeof(UInt64);
    return peer_size + region_size + apply_state_size + sizeof(UInt64) + sizeof(bool);
}

size_t RegionMeta::serialize(WriteBuffer & buf)
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

RegionID RegionMeta::regionId() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return region.id();
}

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

raft_serverpb::RaftApplyState RegionMeta::getApplyState() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return apply_state;
}

void RegionMeta::setRegion(const metapb::Region & region_)
{
    std::lock_guard<std::mutex> lock(mutex);
    region = region_;
}

void RegionMeta::setApplied(UInt64 index, UInt64 term)
{
    {
        std::lock_guard<std::mutex> lock(mutex);
        apply_state.set_applied_index(index);
        applied_term = term;
    }
    cv.notify_all();
}

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
    resp.mutable_header()->set_region_id(region.id());
    resp.mutable_apply_state()->CopyFrom(apply_state);
    resp.set_applied_term(applied_term);
    return resp;
}

RegionMeta::RegionMeta(RegionMeta && rhs)
{
    std::lock_guard<std::mutex> lock1(mutex);
    std::lock_guard<std::mutex> lock2(rhs.mutex);

    peer = std::move(rhs.peer);
    region = std::move(rhs.region);
    apply_state = std::move(rhs.apply_state);
    applied_term = rhs.applied_term;
}

RegionMeta::RegionMeta(const RegionMeta & rhs)
{
    std::lock_guard<std::mutex> lock1(mutex);
    std::lock_guard<std::mutex> lock2(rhs.mutex);

    peer = rhs.peer;
    region = rhs.region;
    apply_state = rhs.apply_state;
    applied_term = rhs.applied_term;
}

RegionRange RegionMeta::getRange() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return {TiKVKey(region.start_key()), TiKVKey(region.end_key())};
}

std::string RegionMeta::toString(bool dump_status) const
{
    std::lock_guard<std::mutex> lock(mutex);
    std::string status_str = !dump_status ? "" : ", term: " + DB::toString(applied_term) +
        ", applied_index: " + DB::toString(apply_state.applied_index());
    return "region[id: " + DB::toString(region.id()) + status_str + "]";
}

bool RegionMeta::isPendingRemove() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return pending_remove;
}

void RegionMeta::setPendingRemove()
{
    std::lock_guard<std::mutex> lock(mutex);
    pending_remove = true;
}

void RegionMeta::wait_index(UInt64 index) {
    std::cout<<"get index: "<<apply_state.applied_index()<<" wanted index "<< index <<" region id: "<<region.id()<<std::endl;
    std::unique_lock<std::mutex> lk(mutex);
    cv.wait(lk, [this, index]{
            return apply_state.applied_index() >= index;
            });
}

} // namespace DB
