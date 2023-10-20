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

#include <Common/FailPoint.h>
#include <Flash/Coprocessor/TablesRegionsInfo.h>
#include <Flash/CoprocessorHandler.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/TMTContext.h>

namespace DB
{
namespace FailPoints
{
extern const char force_no_local_region_for_mpp_task[];
} // namespace FailPoints

SingleTableRegions & TablesRegionsInfo::getOrCreateTableRegionInfoByTableID(Int64 table_id)
{
    if (is_single_table)
        return table_regions_info_map.begin()->second;
    if (table_regions_info_map.find(table_id) == table_regions_info_map.end())
    {
        table_regions_info_map[table_id] = SingleTableRegions();
    }
    return table_regions_info_map.find(table_id)->second;
}
const SingleTableRegions & TablesRegionsInfo::getTableRegionInfoByTableID(Int64 table_id) const
{
    if (is_single_table)
        return table_regions_info_map.begin()->second;
    if (table_regions_info_map.find(table_id) != table_regions_info_map.end())
        return table_regions_info_map.find(table_id)->second;
    throw TiFlashException(Errors::Coprocessor::BadRequest, "Can't find region info for table id: {}", table_id);
}

static bool needRemoteRead(const RegionInfo & region_info, const TMTContext & tmt_context)
{
    fiu_do_on(FailPoints::force_no_local_region_for_mpp_task, { return true; });
    // For tiflash_compute node, all regions will be fetched from tiflash_storage node.
    // So treat all regions as remote regions.
    if (tmt_context.getContext().getSharedContextDisagg()->isDisaggregatedComputeMode())
        return true;
    RegionPtr current_region = tmt_context.getKVStore()->getRegion(region_info.region_id);
    if (current_region == nullptr || current_region->peerState() != raft_serverpb::PeerState::Normal)
        return true;
    auto meta_snap = current_region->dumpRegionMetaSnapshot();
    return meta_snap.ver != region_info.region_version;
}

/**
  * Build local and remote regions info into `tables_region_infos` according to `regions`
  * and `table_id`. It will also record the region_id into local_region_id_set` so that
  * we can find the duplicated region_id among multiple partitions.
  **/
static void insertRegionInfoToTablesRegionInfo(
    const google::protobuf::RepeatedPtrField<coprocessor::RegionInfo> & regions,
    Int64 table_id,
    TablesRegionsInfo & tables_region_infos,
    std::unordered_set<RegionID> & local_region_id_set,
    const TMTContext & tmt_context)
{
    auto & table_region_info = tables_region_infos.getOrCreateTableRegionInfoByTableID(table_id);
    for (const auto & r : regions)
    {
        RegionInfo region_info(
            r.region_id(),
            r.region_epoch().version(),
            r.region_epoch().conf_ver(),
            genCopKeyRange(r.ranges()),
            nullptr);
        if (region_info.key_ranges.empty())
        {
            throw TiFlashException(
                Errors::Coprocessor::BadRequest,
                "Income key ranges is empty for region_id={}",
                region_info.region_id);
        }
        /// TiFlash does not support regions with duplicated region id, so for regions with duplicated
        /// region id, only the first region will be treated as local region
        ///
        /// 1. Currently TiDB can't provide a consistent snapshot of the region cache and it may be updated during the
        ///    planning stage of a query. The planner may see multiple versions of one region (on one TiFlash node).
        /// 2. Two regions with same region id won't have overlapping key ranges.
        /// 3. TiFlash will pick the right version of region for local read and others for remote read.
        /// 4. The remote read will fetch the newest region info via key ranges. So it is possible to find the region
        ///    is served by the same node (but still read from remote).
        bool duplicated_region = local_region_id_set.contains(region_info.region_id);

        if (duplicated_region || needRemoteRead(region_info, tmt_context))
            table_region_info.remote_regions.push_back(region_info);
        else
        {
            table_region_info.local_regions.insert(std::make_pair(region_info.region_id, region_info));
            local_region_id_set.emplace(region_info.region_id);
        }
    }
}

TablesRegionsInfo TablesRegionsInfo::create(
    const google::protobuf::RepeatedPtrField<coprocessor::RegionInfo> & regions,
    const google::protobuf::RepeatedPtrField<coprocessor::TableRegions> & table_regions,
    const TMTContext & tmt_context)
{
    assert(regions.empty() || table_regions.empty());
    TablesRegionsInfo tables_regions_info(!regions.empty());
    std::unordered_set<RegionID> local_region_id_set;
    if (!regions.empty())
        insertRegionInfoToTablesRegionInfo(
            regions,
            InvalidTableID,
            tables_regions_info,
            local_region_id_set,
            tmt_context);
    else
    {
        for (const auto & table_region : table_regions)
        {
            assert(table_region.physical_table_id() != InvalidTableID);
            insertRegionInfoToTablesRegionInfo(
                table_region.regions(),
                table_region.physical_table_id(),
                tables_regions_info,
                local_region_id_set,
                tmt_context);
        }
        assert(static_cast<UInt64>(table_regions.size()) == tables_regions_info.tableCount());
    }
    return tables_regions_info;
}

} // namespace DB
