#pragma once

#include <chrono>
#include <unordered_set>

#include <Core/Types.h>

namespace DB
{

using TableID = Int64;
using TableIDSet = std::unordered_set<TableID>;

enum : TableID
{
    InvalidTableID = 0,
    MaxSystemTableID = 29
};

inline bool isTiDBSystemTable(TableID table_id) { return table_id <= MaxSystemTableID; }

using DatabaseID = Int64;

using ColumnID = Int64;

enum : ColumnID
{
    // Prevent conflict with TiDB.
    TiDBPkColumnID = -1,
    VersionColumnID = -1024,
    DelMarkColumnID = -1025,
};

using HandleID = Int64;
using Timestamp = UInt64;

using RegionID = UInt64;

enum : RegionID
{
    InvalidRegionID = 0
};

using RegionVersion = UInt64;

enum : RegionVersion
{
    InvalidRegionVersion = std::numeric_limits<RegionVersion>::max()
};

using Clock = std::chrono::system_clock;
using Timepoint = Clock::time_point;
using Duration = Clock::duration;
using Seconds = std::chrono::seconds;

} // namespace DB
