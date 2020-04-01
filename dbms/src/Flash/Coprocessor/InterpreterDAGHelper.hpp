#pragma once

namespace DB
{

namespace ErrorCodes
{
extern const int COP_BAD_DAG_REQUEST;
} // namespace ErrorCodes


RegionException::RegionReadStatus GetRegionReadStatus(const RegionPtr & current_region, UInt64 region_version, UInt64 region_conf_version)
{
    if (!current_region)
        return RegionException::NOT_FOUND;
    if (current_region->version() != region_version || current_region->confVer() != region_conf_version)
        return RegionException::VERSION_ERROR;
    if (current_region->isPendingRemove())
        return RegionException::PENDING_REMOVE;
    return RegionException::OK;
}

std::tuple<std::optional<std::unordered_map<RegionID, const RegionInfo &>>, RegionException::RegionReadStatus> MakeRegionQueryInfos(
    const std::unordered_map<RegionID, RegionInfo> & dag_region_infos, const std::unordered_set<RegionID> & region_force_retry,
    TMTContext & tmt, MvccQueryInfo & mvcc_info, TableID table_id)
{
    mvcc_info.regions_query_info.clear();
    std::unordered_map<RegionID, const RegionInfo &> region_need_retry;
    RegionException::RegionReadStatus status_res = RegionException::RegionReadStatus::OK;
    for (auto & [id, r] : dag_region_infos)
    {
        if (r.key_ranges.empty())
        {
            throw Exception("Income key ranges is empty for region: " + std::to_string(r.region_id), ErrorCodes::COP_BAD_DAG_REQUEST);
        }
        auto current_region = tmt.getKVStore()->getRegion(id);
        auto status = GetRegionReadStatus(current_region, r.region_version, r.region_conf_version);
        if (region_force_retry.count(id) || status != RegionException::OK)
        {
            region_need_retry.emplace(id, r);
            status_res = status;
            continue;
        }
        RegionQueryInfo info;
        {
            info.region_id = id;
            info.version = r.region_version;
            info.conf_version = r.region_conf_version;
            for (const auto & p : r.key_ranges)
            {
                TiKVRange::Handle start = TiKVRange::getRangeHandle<true>(p.first, table_id);
                TiKVRange::Handle end = TiKVRange::getRangeHandle<false>(p.second, table_id);
                info.required_handle_ranges.emplace_back(std::make_pair(start, end));
            }
            info.range_in_table = current_region->getHandleRangeByTable(table_id);
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
