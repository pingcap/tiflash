#include <Storages/Transaction/SerializationHelper.h>

namespace DB
{

size_t writeBinary2(const metapb::Peer & peer, WriteBuffer & buf)
{
    writeIntBinary((UInt64)peer.id(), buf);
    writeIntBinary((UInt64)peer.store_id(), buf);
    writeBinary(peer.is_learner(), buf);
    return sizeof(UInt64) + sizeof(UInt64) + sizeof(bool);
}

metapb::Peer readPeer(ReadBuffer & buf)
{
    metapb::Peer peer;
    peer.set_id(readBinary2<UInt64>(buf));
    peer.set_store_id(readBinary2<UInt64>(buf));
    peer.set_is_learner(readBinary2<bool>(buf));
    return peer;
}

size_t writeBinary2(const metapb::Region & region, WriteBuffer & buf)
{
    size_t size = 0;
    size += writeBinary2((UInt64)region.id(), buf);
    size += writeBinary2(region.start_key(), buf);
    size += writeBinary2(region.end_key(), buf);

    size += writeBinary2((UInt64)region.region_epoch().conf_ver(), buf);
    size += writeBinary2((UInt64)region.region_epoch().version(), buf);

    size += writeBinary2(region.peers_size(), buf);
    for (const auto & peer : region.peers())
    {
        size += writeBinary2(peer, buf);
    }
    return size;
}

metapb::Region readRegion(ReadBuffer & buf)
{
    metapb::Region region;

    region.set_id(readBinary2<UInt64>(buf));
    region.set_start_key(readBinary2<std::string>(buf));
    region.set_end_key(readBinary2<std::string>(buf));

    region.mutable_region_epoch()->set_conf_ver(readBinary2<UInt64>(buf));
    region.mutable_region_epoch()->set_version(readBinary2<UInt64>(buf));

    int peer_size = readBinary2<int>(buf);
    for (int i = 0; i < peer_size; ++i)
    {
        *(region.mutable_peers()->Add()) = readPeer(buf);
    }
    return region;
}

size_t writeBinary2(const raft_serverpb::RaftApplyState & state, WriteBuffer & buf)
{
    size_t size = 0;
    size += writeBinary2((UInt64)state.applied_index(), buf);
    size += writeBinary2((UInt64)state.truncated_state().index(), buf);
    size += writeBinary2((UInt64)state.truncated_state().term(), buf);
    return size;
}

raft_serverpb::RaftApplyState readApplyState(ReadBuffer & buf)
{
    raft_serverpb::RaftApplyState state;
    state.set_applied_index(readBinary2<UInt64>(buf));
    state.mutable_truncated_state()->set_index(readBinary2<UInt64>(buf));
    state.mutable_truncated_state()->set_term(readBinary2<UInt64>(buf));
    return state;
}

bool operator==(const metapb::Peer & peer1, const metapb::Peer & peer2)
{
    return peer1.id() == peer2.id() && peer1.store_id() == peer2.store_id() && peer1.is_learner() == peer2.is_learner();
}

bool operator==(const metapb::Region & region1, const metapb::Region & region2)
{
    if (region1.id() != region2.id())
        return false;
    if (region1.peers_size() != region2.peers_size())
        return false;
    for (int i = 0; i < region1.peers_size(); ++i)
    {
        if (!(region1.peers(i) == region2.peers(i)))
            return false;
    }
    return true;
}

bool operator==(const raft_serverpb::RaftApplyState & state1, const raft_serverpb::RaftApplyState & state2)
{
    return state1.applied_index() == state2.applied_index() //
        && state1.truncated_state().index() == state2.truncated_state().index()
        && state1.truncated_state().term() == state2.truncated_state().term();
}

} // namespace DB
