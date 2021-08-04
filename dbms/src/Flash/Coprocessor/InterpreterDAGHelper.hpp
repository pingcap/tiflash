#pragma once

#include <Common/TiFlashException.h>
#include <Storages/Transaction/KVStore.h>
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
    if (meta_snap.ver != check_info.region_version)
        return RegionException::RegionReadStatus::EPOCH_NOT_MATCH;
    // No need to check conf_version if its peer state is normal
    if (current_region->peerState() != raft_serverpb::PeerState::Normal)
        return RegionException::RegionReadStatus::NOT_FOUND;

    region_range = std::move(meta_snap.range);
    return RegionException::RegionReadStatus::OK;
}

std::tuple<std::optional<RegionRetryList>, RegionException::RegionReadStatus> //
MakeRegionQueryInfos(const RegionInfoMap & dag_region_infos, const std::unordered_set<RegionID> & region_force_retry, TMTContext & tmt,
    MvccQueryInfo & mvcc_info, TableID table_id)
{
    mvcc_info.regions_query_info.clear();
    RegionRetryList region_need_retry;
    RegionException::RegionReadStatus status_res = RegionException::RegionReadStatus::OK;
    for (auto & [id, r] : dag_region_infos)
    {
        if (r.key_ranges.empty())
        {
            throw TiFlashException(
                "Income key ranges is empty for region: " + std::to_string(r.region_id), Errors::Coprocessor::BadRequest);
        }
        if (region_force_retry.count(id))
        {
            region_need_retry.emplace_back(r);
            status_res = RegionException::RegionReadStatus::NOT_FOUND;
            continue;
        }
        ImutRegionRangePtr region_range{nullptr};
        if (auto status = GetRegionReadStatus(r, tmt.getKVStore()->getRegion(id), region_range);
            status != RegionException::RegionReadStatus::OK)
        {
            region_need_retry.emplace_back(r);
            status_res = status;
            continue;
        }
        RegionQueryInfo info;
        {
            info.region_id = id;
            info.version = r.region_version;
            info.conf_version = r.region_conf_version;
            info.range_in_table = region_range->rawKeys();
            for (const auto & p : r.key_ranges)
            {
                TableID table_id_in_range = -1;
                if (!computeMappedTableID(*p.first, table_id_in_range) || table_id_in_range != table_id)
                {
                    throw TiFlashException("Income key ranges is illegal for region: " + std::to_string(r.region_id)
                            + ", table id in key range is " + std::to_string(table_id_in_range) + ", table id in region is "
                            + std::to_string(table_id),
                        Errors::Coprocessor::BadRequest);
                }
                if (p.first->compare(*info.range_in_table.first) < 0 || p.second->compare(*info.range_in_table.second) > 0)
                    throw TiFlashException(
                        "Income key ranges is illegal for region: " + std::to_string(r.region_id), Errors::Coprocessor::BadRequest);
            }
            info.required_handle_ranges = r.key_ranges;
            info.bypass_lock_ts = r.bypass_lock_ts;
        }
        mvcc_info.regions_query_info.emplace_back(std::move(info));
    }
    mvcc_info.concurrent = mvcc_info.regions_query_info.size() > 1 ? 1.0 : 0.0;

    if (region_need_retry.empty())
        return std::make_tuple(std::nullopt, RegionException::RegionReadStatus::OK);
    else
        return std::make_tuple(std::move(region_need_retry), status_res);
}

} // namespace DB
