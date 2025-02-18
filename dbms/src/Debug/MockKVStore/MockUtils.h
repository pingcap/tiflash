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

#include <Storages/KVStore/MultiRaft/RegionMeta.h>
#include <Storages/KVStore/MultiRaft/RegionPersister.h>
#include <Storages/KVStore/TiKVHelpers/TiKVRecordFormat.h>

namespace DB::RegionBench
{
metapb::Peer createPeer(UInt64 id, bool);

metapb::Region createMetaRegion( //
    RegionID region_id,
    TableID table_id,
    HandleID start,
    HandleID end,
    std::optional<metapb::RegionEpoch> maybe_epoch = std::nullopt,
    std::optional<std::vector<metapb::Peer>> maybe_peers = std::nullopt);

metapb::Region createMetaRegionCommonHandle( //
    RegionID region_id,
    const std::string & start_key,
    const std::string & end_key,
    std::optional<metapb::RegionEpoch> maybe_epoch = std::nullopt,
    std::optional<std::vector<metapb::Peer>> maybe_peers = std::nullopt);

RegionPtr makeRegionForTable(
    UInt64 region_id,
    TableID table_id,
    HandleID start,
    HandleID end,
    const TiFlashRaftProxyHelper * proxy_helper = nullptr);

RegionPtr makeRegionForRange(
    UInt64 id,
    std::string start_key,
    std::string end_key,
    const TiFlashRaftProxyHelper * proxy_helper = nullptr);

RegionPtr makeRegion(RegionMeta && meta);

// Generates a lock value which fills all fields, only for test use.
TiKVValue encodeFullLockCfValue(
    UInt8 lock_type,
    const String & primary,
    Timestamp ts,
    UInt64 ttl,
    const String * short_value,
    Timestamp min_commit_ts,
    Timestamp for_update_ts,
    uint64_t txn_size,
    const std::vector<std::string> & async_commit,
    const std::vector<uint64_t> & rollback,
    UInt64 generation = 0);

} // namespace DB::RegionBench
