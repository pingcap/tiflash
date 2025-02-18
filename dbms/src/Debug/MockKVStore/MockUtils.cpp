// Copyright 2025 PingCAP, Inc.
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


#include <Debug/MockKVStore/MockUtils.h>
#include <Storages/KVStore/Region.h>


namespace DB::RegionBench
{
metapb::Peer createPeer(UInt64 id, bool)
{
    metapb::Peer peer;
    peer.set_id(id);
    return peer;
}

metapb::Region createMetaRegion(
    RegionID region_id,
    TableID table_id,
    HandleID start,
    HandleID end,
    std::optional<metapb::RegionEpoch> maybe_epoch,
    std::optional<std::vector<metapb::Peer>> maybe_peers)
{
    TiKVKey start_key = RecordKVFormat::genKey(table_id, start);
    TiKVKey end_key = RecordKVFormat::genKey(table_id, end);

    return createMetaRegionCommonHandle(region_id, start_key, end_key, maybe_epoch, maybe_peers);
}

metapb::Region createMetaRegionCommonHandle(
    RegionID region_id,
    const std::string & start_key,
    const std::string & end_key,
    std::optional<metapb::RegionEpoch> maybe_epoch,
    std::optional<std::vector<metapb::Peer>> maybe_peers)
{
    metapb::Region meta;
    meta.set_id(region_id);

    meta.set_start_key(start_key);
    meta.set_end_key(end_key);

    if (maybe_epoch)
    {
        *meta.mutable_region_epoch() = maybe_epoch.value();
    }
    else
    {
        meta.mutable_region_epoch()->set_version(5);
        meta.mutable_region_epoch()->set_conf_ver(6);
    }

    if (maybe_peers)
    {
        const auto & peers = maybe_peers.value();
        for (const auto & peer : peers)
        {
            *(meta.mutable_peers()->Add()) = peer;
        }
    }
    else
    {
        *(meta.mutable_peers()->Add()) = createPeer(1, true);
        *(meta.mutable_peers()->Add()) = createPeer(2, false);
    }

    return meta;
}


RegionPtr makeRegion(RegionMeta && meta)
{
    return std::make_shared<Region>(std::move(meta), nullptr);
}

RegionPtr makeRegionForRange(
    UInt64 id,
    std::string start_key,
    std::string end_key,
    const TiFlashRaftProxyHelper * proxy_helper)
{
    return std::make_shared<Region>(
        RegionMeta(
            createPeer(2, true),
            createMetaRegionCommonHandle(id, std::move(start_key), std::move(end_key)),
            initialApplyState()),
        proxy_helper);
}

RegionPtr makeRegionForTable(
    UInt64 region_id,
    TableID table_id,
    HandleID start,
    HandleID end,
    const TiFlashRaftProxyHelper * proxy_helper)
{
    return makeRegionForRange(
        region_id,
        RecordKVFormat::genKey(table_id, start).toString(),
        RecordKVFormat::genKey(table_id, end).toString(),
        proxy_helper);
}

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
    UInt64 generation)
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
