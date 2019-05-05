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

} // namespace DB
