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


inline metapb::Peer createPeer(UInt64 id, bool)
{
    metapb::Peer peer;
    peer.set_id(id);
    return peer;
}

inline metapb::Region createRegionInfo(
    UInt64 id,
    const std::string start_key,
    const std::string end_key,
    std::optional<metapb::RegionEpoch> maybe_epoch = std::nullopt,
    std::optional<std::vector<metapb::Peer>> maybe_peers = std::nullopt)
{
    metapb::Region region_info;
    region_info.set_id(id);
    region_info.set_start_key(start_key);
    region_info.set_end_key(end_key);
    if (maybe_epoch)
    {
        *region_info.mutable_region_epoch() = (maybe_epoch.value());
    }
    else
    {
        region_info.mutable_region_epoch()->set_version(5);
        region_info.mutable_region_epoch()->set_version(6);
    }
    if (maybe_peers)
    {
        const auto & peers = maybe_peers.value();
        for (const auto & peer : peers)
        {
            *(region_info.mutable_peers()->Add()) = peer;
        }
    }
    else
    {
        *(region_info.mutable_peers()->Add()) = createPeer(1, true);
        *(region_info.mutable_peers()->Add()) = createPeer(2, false);
    }

    return region_info;
}

inline RegionMeta createRegionMeta(
    UInt64 id,
    DB::TableID table_id,
    std::optional<raft_serverpb::RaftApplyState> apply_state = std::nullopt)
{
    return RegionMeta(
        /*peer=*/createPeer(31, true),
        /*region=*/createRegionInfo(id, RecordKVFormat::genKey(table_id, 0), RecordKVFormat::genKey(table_id, 300)),
        /*apply_state_=*/apply_state.value_or(initialApplyState()));
}

inline RegionPtr makeRegion(
    UInt64 id,
    const std::string start_key,
    const std::string end_key,
    const TiFlashRaftProxyHelper * proxy_helper = nullptr)
{
    return std::make_shared<Region>(
        RegionMeta(
            createPeer(2, true),
            createRegionInfo(id, std::move(start_key), std::move(end_key)),
            initialApplyState()),
        proxy_helper);
}

inline RegionPtr makeRegion(RegionMeta && meta)
{
    return std::make_shared<Region>(std::move(meta), nullptr);
}

// Generates a lock value which fills all fields, only for test use.
inline TiKVValue encodeFullLockCfValue(
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
    UInt64 generation = 0)
{
    auto lock_value = RecordKVFormat::encodeLockCfValue(lock_type, primary, ts, ttl, short_value, min_commit_ts);
    WriteBufferFromOwnString res;
    res.write(lock_value.getStr().data(), lock_value.getStr().size());
    {
        res.write(RecordKVFormat::MIN_COMMIT_TS_PREFIX);
        RecordKVFormat::encodeUInt64(min_commit_ts, res);
    }
    {
        res.write(RecordKVFormat::FOR_UPDATE_TS_PREFIX);
        RecordKVFormat::encodeUInt64(for_update_ts, res);
    }
    {
        res.write(RecordKVFormat::TXN_SIZE_PREFIX);
        RecordKVFormat::encodeUInt64(txn_size, res);
    }
    {
        res.write(RecordKVFormat::ROLLBACK_TS_PREFIX);
        TiKV::writeVarUInt(rollback.size(), res);
        for (auto ts : rollback)
        {
            RecordKVFormat::encodeUInt64(ts, res);
        }
    }
    {
        res.write(RecordKVFormat::ASYNC_COMMIT_PREFIX);
        TiKV::writeVarUInt(async_commit.size(), res);
        for (const auto & s : async_commit)
        {
            writeVarInt(s.size(), res);
            res.write(s.data(), s.size());
        }
    }
    {
        res.write(RecordKVFormat::LAST_CHANGE_PREFIX);
        RecordKVFormat::encodeUInt64(12345678, res);
        TiKV::writeVarUInt(87654321, res);
    }
    {
        res.write(RecordKVFormat::TXN_SOURCE_PREFIX_FOR_LOCK);
        TiKV::writeVarUInt(876543, res);
    }
    {
        res.write(RecordKVFormat::PESSIMISTIC_LOCK_WITH_CONFLICT_PREFIX);
    }
    if (generation > 0)
    {
        res.write(RecordKVFormat::GENERATION_PREFIX);
        RecordKVFormat::encodeUInt64(generation, res);
    }
    return TiKVValue(res.releaseStr());
}

} // namespace DB::RegionBench
