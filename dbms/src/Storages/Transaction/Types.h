#pragma once

#include <set>

#include <Core/Types.h>

namespace DB
{

using TableID = Int64;

enum : TableID
{
    InvalidTableID = 0,
    MaxSystemTableID = 29
};

inline bool isTiDBSystemTable(TableID table_id)
{
    return table_id <= MaxSystemTableID;
}

using DatabaseID = Int64;

using ColumnID = Int64;
using HandleID = Int64;
using Timestamp = UInt64;

using PartitionID = UInt64;
using RegionKey = Int64;

using RegionID = UInt64;

enum : RegionID
{
    InvalidRegionID = 0
};

} // namespace DB
