#pragma once

#include <Storages/Transaction/TiKVHandle.h>
#include <memory>
#include <unordered_map>

namespace DB
{

class IAST;
using ASTPtr = std::shared_ptr<IAST>;

class Set;
using SetPtr = std::shared_ptr<Set>;

/// Information about calculated sets in right hand side of IN.
using PreparedSets = std::unordered_map<IAST *, SetPtr>;

struct RegionQueryInfo
{
    RegionID region_id;
    UInt64 version;
    UInt64 conf_version;
    HandleRange<HandleID> range_in_table;

    bool operator<(const RegionQueryInfo & o) const { return range_in_table < o.range_in_table; }
};

/** Query along with some additional data,
  *  that can be used during query processing
  *  inside storage engines.
  */
struct SelectQueryInfo
{
    ASTPtr query;

    /// Prepared sets are used for indices by storage engine.
    /// Example: x IN (1, 2, 3)
    PreparedSets sets;

    bool resolve_locks = false;

    UInt64 read_tso;

    std::vector<RegionQueryInfo> regions_query_info;
};

} // namespace DB
