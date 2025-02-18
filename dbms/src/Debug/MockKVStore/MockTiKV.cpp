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
} // namespace DB
