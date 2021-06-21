#pragma once

#include <Common/TiFlashException.h>
#include <Storages/Transaction/RegionException.h>

namespace DB
{

namespace ErrorCodes
{
extern const int COP_BAD_DAG_REQUEST;
} // namespace ErrorCodes

RegionException::RegionReadStatus GetRegionReadStatus(
    const RegionInfo & check_info, const RegionPtr & current_region, ImutRegionRangePtr & region_range)
{
    if (!current_region)
        return RegionException::RegionReadStatus::NOT_FOUND;
    auto meta_snap = current_region->dumpRegionMetaSnapshot();
    if (meta_snap.ver != check_info.region_ver_id.ver)
        return RegionException::RegionReadStatus::EPOCH_NOT_MATCH;
    // No need to check conf_version if its peer state is normal
    if (current_region->peerState() != raft_serverpb::PeerState::Normal)
        return RegionException::RegionReadStatus::NOT_FOUND;

    region_range = std::move(meta_snap.range);
    return RegionException::RegionReadStatus::OK;
}

std::tuple<std::optional<std::unordered_map<RegionVerID, const RegionInfo &>>, RegionException::RegionReadStatus> //
MakeRegionQueryInfos(const std::unordered_map<RegionVerID, RegionInfo> & dag_region_infos,
    const std::unordered_set<RegionVerID> & region_force_retry, TMTContext & tmt, MvccQueryInfo & mvcc_info, TableID table_id,
    Poco::Logger * log)
{
    mvcc_info.regions_query_info.clear();
    std::unordered_map<RegionVerID, const RegionInfo &> region_need_retry;
    RegionException::RegionReadStatus status_res = RegionException::RegionReadStatus::OK;
    std::unordered_set<RegionID> local_region_ids;
    for (auto & [id, r] : dag_region_infos)
    {
        if (r.key_ranges.empty())
        {
            throw TiFlashException("Income key ranges is empty for region: " + r.region_ver_id.toString(), Errors::Coprocessor::BadRequest);
        }
        if (region_force_retry.count(id))
        {
            region_need_retry.emplace(id, r);
            status_res = RegionException::RegionReadStatus::NOT_FOUND;
            continue;
        }
        /// this is a double check: in GetRegionReadStatus, it only checks the region id and region version, and region conf version
        /// is ignored, so if there are two regions with the same region id and region version, but different region conf version,
        /// GetRegionReadStatus will return OK for both regions, however, in learner read, it use region id as the key, so we need
        /// to make sure that if there are two regions with the same region id, at most one region will be treated as local region.
        if (local_region_ids.count(id.id))
        {
            LOG_WARNING(
                log, "Found duplicated region id in DAGRequest for region id: " << std::to_string(id.id) << ", will read it from remote");
            region_need_retry.emplace(id, r);
            status_res = RegionException::RegionReadStatus::EPOCH_NOT_MATCH;
            continue;
        }
        ImutRegionRangePtr region_range{nullptr};
        if (auto status = GetRegionReadStatus(r, tmt.getKVStore()->getRegion(id.id), region_range);
            status != RegionException::RegionReadStatus::OK)
        {
            region_need_retry.emplace(id, r);
            status_res = status;
            continue;
        }
        RegionQueryInfo info;
        {
            info.region_ver_id = id;
            info.range_in_table = region_range->rawKeys();
            for (const auto & p : r.key_ranges)
            {
                TableID table_id_in_range = -1;
                if (!computeMappedTableID(*p.first, table_id_in_range) || table_id_in_range != table_id)
                {
                    throw TiFlashException("Income key ranges is illegal for region: " + r.region_ver_id.toString()
                            + ", table id in key range is " + std::to_string(table_id_in_range) + ", table id in region is "
                            + std::to_string(table_id),
                        Errors::Coprocessor::BadRequest);
                }
                if (p.first->compare(*info.range_in_table.first) < 0 || p.second->compare(*info.range_in_table.second) > 0)
                    throw TiFlashException(
                        "Income key ranges is illegal for region: " + r.region_ver_id.toString(), Errors::Coprocessor::BadRequest);
            }
            info.required_handle_ranges = r.key_ranges;
            info.bypass_lock_ts = r.bypass_lock_ts;
        }
        mvcc_info.regions_query_info.emplace_back(std::move(info));
        local_region_ids.insert(id.id);
    }
    mvcc_info.concurrent = mvcc_info.regions_query_info.size() > 1 ? 1.0 : 0.0;

    if (region_need_retry.empty())
        return std::make_tuple(std::nullopt, RegionException::RegionReadStatus::OK);
    else
        return std::make_tuple(std::move(region_need_retry), status_res);
}

} // namespace DB
