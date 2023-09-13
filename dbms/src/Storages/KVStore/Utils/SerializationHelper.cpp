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

#include <Storages/KVStore/Utils/SerializationHelper.h>

namespace DB
{
size_t writeBinary2(const metapb::Peer & peer, WriteBuffer & buf)
{
    writeIntBinary((UInt64)peer.id(), buf); // NOLINT
    writeIntBinary((UInt64)peer.store_id(), buf); // NOLINT
    writeIntBinary((UInt8)peer.role(), buf); // NOLINT
    return sizeof(UInt64) + sizeof(UInt64) + sizeof(bool);
}

metapb::Peer readPeer(ReadBuffer & buf)
{
    metapb::Peer peer;
    peer.set_id(readBinary2<UInt64>(buf));
    peer.set_store_id(readBinary2<UInt64>(buf));
    peer.set_role(static_cast<metapb::PeerRole>(readBinary2<UInt8>(buf)));
    return peer;
}

size_t writeBinary2(const metapb::Region & region, WriteBuffer & buf)
{
    size_t size = 0;
    size += writeBinary2((UInt64)region.id(), buf); // NOLINT
    size += writeBinary2(region.start_key(), buf);
    size += writeBinary2(region.end_key(), buf);

    size += writeBinary2((UInt64)region.region_epoch().conf_ver(), buf); // NOLINT
    size += writeBinary2((UInt64)region.region_epoch().version(), buf); // NOLINT

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

    auto peer_size = readBinary2<Int32>(buf);
    for (Int32 i = 0; i < peer_size; ++i)
    {
        *(region.mutable_peers()->Add()) = readPeer(buf);
    }
    return region;
}

raft_serverpb::MergeState readMergeState(ReadBuffer & buf)
{
    raft_serverpb::MergeState merge_state;
    *merge_state.mutable_target() = readRegion(buf);
    merge_state.set_commit(readBinary2<UInt64>(buf));
    merge_state.set_min_index(readBinary2<UInt64>(buf));
    return merge_state;
}

raft_serverpb::RegionLocalState readRegionLocalState(ReadBuffer & buf)
{
    raft_serverpb::RegionLocalState region_state;
    *region_state.mutable_region() = readRegion(buf);
    region_state.set_state((raft_serverpb::PeerState)readBinary2<Int32>(buf)); // NOLINT
    bool has_merge_state = readBinary2<bool>(buf);
    if (has_merge_state)
    {
        *region_state.mutable_merge_state() = readMergeState(buf);
    }
    return region_state;
}

size_t writeBinary2(const raft_serverpb::RaftApplyState & state, WriteBuffer & buf)
{
    size_t size = 0;
    size += writeBinary2((UInt64)state.applied_index(), buf); // NOLINT
    size += writeBinary2((UInt64)state.truncated_state().index(), buf); // NOLINT
    size += writeBinary2((UInt64)state.truncated_state().term(), buf); // NOLINT
    return size;
}

size_t writeBinary2(const raft_serverpb::MergeState & state, WriteBuffer & buf)
{
    size_t size = 0;
    size += writeBinary2(state.target(), buf);
    size += writeBinary2(state.commit(), buf);
    size += writeBinary2(state.min_index(), buf);
    return size;
}

size_t writeBinary2(const raft_serverpb::RegionLocalState & region_state, WriteBuffer & buf)
{
    size_t size = 0;
    size += writeBinary2(region_state.region(), buf);
    size += writeBinary2((Int32)region_state.state(), buf); // NOLINT

    if (region_state.has_merge_state())
    {
        size += writeBinary2(true, buf);
        size += writeBinary2(region_state.merge_state(), buf);
    }
    else
        size += writeBinary2(false, buf);

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
    return peer1.id() == peer2.id() && peer1.store_id() == peer2.store_id() && peer1.role() == peer2.role();
}

bool operator==(const metapb::Region & region1, const metapb::Region & region2)
{
    if (region1.id() != region2.id() || region1.start_key() != region2.start_key()
        || region1.end_key() != region2.end_key())
        return false;
    if (region1.region_epoch().version() != region2.region_epoch().version()
        || region1.region_epoch().conf_ver() != region2.region_epoch().conf_ver())
        return false;
    if (region1.peers_size() != region2.peers_size())
        return false;
    for (Int32 i = 0; i < region1.peers_size(); ++i)
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

bool operator==(const raft_serverpb::MergeState & state1, const raft_serverpb::MergeState & state2)
{
    return state1.min_index() == state2.min_index() && state1.commit() == state2.commit()
        && state1.target() == state2.target();
}

bool operator==(const raft_serverpb::RegionLocalState & state1, const raft_serverpb::RegionLocalState & state2)
{
    return state1.region() == state2.region() && state1.state() == state2.state()
        && state1.merge_state() == state2.merge_state();
}
// NOLINTEND
} // namespace DB
