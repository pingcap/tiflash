// Copyright 2024 PingCAP, Inc.
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

#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/Utils/AsyncTasks.h>

namespace DB
{
struct CheckpointInfo;
using CheckpointInfoPtr = std::shared_ptr<CheckpointInfo>;
class Region;
using RegionPtr = std::shared_ptr<Region>;
using CheckpointRegionInfoAndData
    = std::tuple<CheckpointInfoPtr, RegionPtr, raft_serverpb::RaftApplyState, raft_serverpb::RegionLocalState>;
FastAddPeerRes genFastAddPeerRes(
    FastAddPeerStatus status,
    std::string && apply_str,
    std::string && region_str,
    uint64_t shard_ver,
    std::string && inner_key_str,
    std::string && enc_key_str,
    std::string && txn_file_ref_str);
FastAddPeerRes genFastAddPeerResFail(FastAddPeerStatus status);
std::variant<CheckpointRegionInfoAndData, FastAddPeerRes> FastAddPeerImplSelect(
    TMTContext & tmt,
    const TiFlashRaftProxyHelper * proxy_helper,
    uint64_t region_id,
    uint64_t new_peer_id);
FastAddPeerRes FastAddPeerImplWrite(
    TMTContext & tmt,
    const TiFlashRaftProxyHelper * proxy_helper,
    UInt64 region_id,
    UInt64 new_peer_id,
    CheckpointRegionInfoAndData && checkpoint,
    UInt64 start_time);

FastAddPeerRes FastAddPeerImpl(
    FastAddPeerContextPtr fap_ctx,
    TMTContext & tmt,
    const TiFlashRaftProxyHelper * proxy_helper,
    UInt64 region_id,
    UInt64 new_peer_id,
    UInt64 start_time);
uint8_t ApplyFapSnapshotImpl(
    TMTContext & tmt,
    TiFlashRaftProxyHelper * proxy_helper,
    UInt64 region_id,
    UInt64 peer_id,
    bool assert_exist,
    UInt64 index,
    UInt64 term);
} // namespace DB
