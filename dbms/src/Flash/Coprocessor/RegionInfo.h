#pragma once

#include <Storages/RegionQueryInfo.h>
#include <Storages/Transaction/TiKVKeyValue.h>
#include <Storages/Transaction/Types.h>

namespace DB
{
struct RegionInfo
{
    RegionID region_id;
    UInt64 region_version;
    UInt64 region_conf_version;

    using RegionReadKeyRanges = std::vector<std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr>>;
    RegionReadKeyRanges key_ranges;
    const std::unordered_set<UInt64> * bypass_lock_ts;

    RegionInfo(
        RegionID id,
        UInt64 ver,
        UInt64 conf_ver,
        RegionReadKeyRanges && key_ranges_,
        const std::unordered_set<UInt64> * bypass_lock_ts_)
        : region_id(id)
        , region_version(ver)
        , region_conf_version(conf_ver)
        , key_ranges(std::move(key_ranges_))
        , bypass_lock_ts(bypass_lock_ts_)
    {}
};

using RegionInfoMap = std::unordered_map<RegionID, RegionInfo>;
using RegionInfoList = std::vector<RegionInfo>;
} // namespace DB
