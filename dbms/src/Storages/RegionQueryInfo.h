#pragma once

#include <Storages/Transaction/TiKVHandle.h>

namespace DB
{

struct RegionQueryInfo
{
    RegionID region_id;
    UInt64 version;
    UInt64 conf_version;
    HandleRange<HandleID> range_in_table;

    bool operator<(const RegionQueryInfo & o) const { return range_in_table < o.range_in_table; }
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
