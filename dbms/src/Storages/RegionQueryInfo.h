#pragma once

#include <Storages/Transaction/TiKVHandle.h>
#include <Storages/Transaction/TiKVKeyValue.h>

namespace DB
{

using DecodedTiKVKeyPtr = std::shared_ptr<DecodedTiKVKey>;

struct RegionQueryInfo
{
    RegionID region_id;
    UInt64 version;
    UInt64 conf_version;
    std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr> range_in_table;
    // required handle ranges is the handle range specified in DAG request
    std::vector<std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr>> required_handle_ranges;
    const std::unordered_set<UInt64> * bypass_lock_ts{nullptr};

    bool operator<(const RegionQueryInfo & o) const
    {
        int first_result = range_in_table.first->compare(*o.range_in_table.first);
        if (likely(first_result != 0))
            return first_result < 0;
        return range_in_table.second->compare(*o.range_in_table.second) < 0;
    }
};

struct MvccQueryInfo
{
    bool resolve_locks = false;

    UInt64 read_tso = 0;

    Float32 concurrent = 1.0;

    using RegionsQueryInfo = std::vector<RegionQueryInfo>;
    RegionsQueryInfo regions_query_info;
};

} // namespace DB
