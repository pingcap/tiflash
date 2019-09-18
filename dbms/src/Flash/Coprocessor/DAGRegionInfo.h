#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Storages/Transaction/RegionState.h>
#include <Storages/Transaction/TiDB.h>

namespace DB
{

class DAGRegionInfo
{
public:
    DAGRegionInfo(RegionID region_id_, UInt64 region_version_, UInt64 region_conf_version_,
        std::vector<std::pair<DecodedTiKVKey, DecodedTiKVKey>> & scan_ranges_)
        : region_id(region_id_), region_version(region_version_), region_conf_version(region_conf_version_), scan_ranges(scan_ranges_){};
    RegionID region_id;
    UInt64 region_version;
    UInt64 region_conf_version;
    std::vector<std::pair<DecodedTiKVKey, DecodedTiKVKey>> & scan_ranges;
};

} // namespace DB
