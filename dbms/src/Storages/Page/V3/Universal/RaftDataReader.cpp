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

#include <Storages/Page/V3/Universal/RaftDataReader.h>
#include <Storages/Page/V3/Universal/UniversalPageId.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormatImpl.h>

namespace DB
{
// Proxy may try to scan data in range [start, end), and kv engine may specify an empty `end` which means infinite end.(Raft engine will always specify a non-empty `end` key)
// But we don't want to scan data unrelated to it.
// We notice that we prepend keys written by kv engine with the byte `0x02`
// so we will manually set an end value `0x03` when proxy pass an empty end key for range scan.
char RaftDataReader::raft_data_end_key[1] = {0x03};

Page RaftDataReader::read(const UniversalPageId & page_id) const
{
    auto snapshot = uni_ps.getSnapshot(fmt::format("read_r_{}", page_id));
    return uni_ps.read(page_id, nullptr, snapshot, /*throw_on_not_exist*/ false);
}

std::optional<raft_serverpb::RaftApplyState> RaftDataReader::readRegionApplyState(RegionID region_id) const
{
    UniversalPageIds keys{
        UniversalPageIdFormat::toRaftApplyStateKeyInRaftEngine(region_id), // raft-engine
        UniversalPageIdFormat::toRaftApplyStateKeyInKVEngine(region_id), // kv-engine
    };
    assert(keys.size() == 2);

    auto snap = uni_ps.getSnapshot(fmt::format("region_apply_state_{}", region_id));
    const auto page_map = uni_ps.read(keys, nullptr, snap, /*throw_on_not_exist*/ false);

    raft_serverpb::RaftApplyState region_apply_state;
    if (auto iter = page_map.find(keys[0]); iter != page_map.end() && iter->second.isValid())
    {
        // try to read by raft key
        if (!region_apply_state.ParseFromArray(iter->second.data.data(), iter->second.data.size()))
            return std::nullopt;
        return region_apply_state;
    }
    else if (auto iter = page_map.find(keys[1]); iter != page_map.end() && iter->second.isValid())
    {
        // try to read by kv key
        if (!region_apply_state.ParseFromArray(iter->second.data.data(), iter->second.data.size()))
            return std::nullopt;
        return region_apply_state;
    }
    return std::nullopt;
}

void RaftDataReader::traverse(
    const UniversalPageId & start,
    const UniversalPageId & end,
    const std::function<void(const UniversalPageId & page_id, DB::Page page)> & acceptor)
{
    auto transformed_end = end.empty() ? UniversalPageId(raft_data_end_key, 1) : end;
    auto snapshot = uni_ps.getSnapshot(fmt::format("scan_r_{}_{}", start, transformed_end));
    const auto page_ids = uni_ps.page_directory->getAllPageIdsInRange(start, transformed_end, snapshot);
    for (const auto & page_id : page_ids)
    {
        const auto page_id_and_entry = uni_ps.page_directory->getByID(page_id, snapshot);
        const auto & checkpoint_info = page_id_and_entry.second.checkpoint_info;
        if (checkpoint_info.has_value() && checkpoint_info.is_local_data_reclaimed)
        {
            acceptor(page_id_and_entry.first, uni_ps.remote_reader->read(page_id_and_entry));
        }
        else
        {
            acceptor(page_id_and_entry.first, uni_ps.blob_store->read(page_id_and_entry));
        }
    }
}

void RaftDataReader::traverseRemoteRaftLogForRegion(
    UInt64 region_id,
    const std::function<bool(size_t)> precheck,
    const std::function<
        void(const UniversalPageId & page_id, PageSize size, const PS::V3::CheckpointLocation & location)> & acceptor)
{
    auto start = UniversalPageIdFormat::toFullRaftLogPrefix(region_id);
    auto end = UniversalPageIdFormat::toFullRaftLogScanEnd(region_id);
    auto snapshot = uni_ps.getSnapshot(fmt::format("scan_r_{}_{}", start, end));
    const auto page_ids = uni_ps.page_directory->getAllPageIdsInRange(start, end, snapshot);
    if (!precheck(page_ids.size()))
    {
        return;
    }
    for (const auto & page_id : page_ids)
    {
        // TODO: change it when support key space
        // 20 = 1(RAFT_PREFIX) + 1(LOCAL_PREFIX) + 1(REGION_RAFT_PREFIX) + 8(region id) + 1(RAFT_LOG_SUFFIX) + 8(raft log index)
        RUNTIME_CHECK(page_id.size() == 20, page_id.size());
        auto maybe_location = uni_ps.getCheckpointLocation(page_id, snapshot);
        auto entry = uni_ps.getEntry(page_id, snapshot);
        acceptor(page_id, entry.size, maybe_location.value_or(PS::V3::CheckpointLocation()));
    }
}

std::optional<UniversalPageId> RaftDataReader::getLowerBound(const UniversalPageId & page_id)
{
    auto snapshot = uni_ps.getSnapshot(fmt::format("lower_bound_r_{}", page_id));
    return uni_ps.page_directory->getLowerBound(page_id, snapshot);
}

std::optional<UInt64> RaftDataReader::tryParseRegionId(const UniversalPageId & page_id)
{
    auto page_id_type = UniversalPageIdFormat::getUniversalPageIdType(page_id);
    if (page_id_type != StorageType::KVEngine && page_id_type != StorageType::RaftEngine)
    {
        return {};
    }
    // 11 = 1(RAFT_PREFIX/KV_PREIFIX) + 2(RAFT_DATA_PREFIX) + 8(region id)
    if (page_id.size() < 11)
    {
        return {};
    }
    auto v = *(reinterpret_cast<const UInt64 *>(page_id.data() + 3));
    return toBigEndian(v);
}

std::optional<UInt64> RaftDataReader::tryParseRaftLogIndex(const UniversalPageId & page_id)
{
    auto page_id_type = UniversalPageIdFormat::getUniversalPageIdType(page_id);
    if (page_id_type != StorageType::KVEngine && page_id_type != StorageType::RaftEngine)
    {
        return {};
    }
    // 20 = 1(RAFT_PREFIX/KV_PREIFIX) + 2(RAFT_DATA_PREFIX) + 8(region id) + 1(RAFT_LOG_SUFFIX) + 8(raft log index)
    if (page_id.size() < 20)
    {
        return {};
    }
    return UniversalPageIdFormat::getU64ID(page_id);
}
} // namespace DB
