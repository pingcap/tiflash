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

#include <Debug/MockKVStore/MockTiKV.h>
#include <Debug/MockKVStore/MockUtils.h>
#include <Storages/KVStore/TiKVHelpers/TiKVRecordFormat.h>
#include <TiDB/Schema/TiDB.h>

namespace DB
{
RegionPtr MockTiKV::createRegion(TableID table_id, RegionID region_id, const HandleID & start, const HandleID & end)
{
    // peer_id is a fake number here
    auto meta_region = RegionBench::createMetaRegion(region_id, table_id, start, end);
    metapb::Peer peer;
    RegionMeta region_meta(std::move(peer), std::move(meta_region), initialApplyState());
    UInt64 index = getNextRaftIndex(region_id);
    region_meta.setApplied(index, RAFT_INIT_LOG_TERM);
    return RegionBench::makeRegion(std::move(region_meta));
}

RegionPtr MockTiKV::createRegionCommonHandle(
    const TiDB::TableInfo & table_info,
    RegionID region_id,
    std::vector<Field> & start_keys,
    std::vector<Field> & end_keys)
{
    metapb::Region region = RegionBench::createMetaRegionCommonHandle(
        region_id,
        RecordKVFormat::genKey(table_info, start_keys),
        RecordKVFormat::genKey(table_info, end_keys));

    metapb::Peer peer;
    RegionMeta region_meta(std::move(peer), std::move(region), initialApplyState());
    auto index = getNextRaftIndex(region_id);
    region_meta.setApplied(index, RAFT_INIT_LOG_TERM);
    return RegionBench::makeRegion(std::move(region_meta));
}

Regions MockTiKV::createRegions(
    TableID table_id,
    size_t region_num,
    size_t key_num_each_region,
    HandleID handle_begin,
    RegionID new_region_id_begin)
{
    Regions regions;
    for (RegionID region_id = new_region_id_begin; region_id < static_cast<RegionID>(new_region_id_begin + region_num);
         ++region_id, handle_begin += key_num_each_region)
    {
        auto ptr = createRegion(table_id, region_id, handle_begin, handle_begin + key_num_each_region);
        regions.push_back(ptr);
    }
    return regions;
}
} // namespace DB
