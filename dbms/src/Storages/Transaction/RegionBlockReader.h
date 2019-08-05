#pragma once

#include <Core/Names.h>
#include <Storages/Transaction/RegionDataRead.h>

namespace TiDB
{
struct TableInfo;
};

namespace DB
{

struct ColumnsDescription;
class Block;

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

/// Read the region data in data_list, decode based on the given table_info and columns, as a block.
///
/// Data with commit_ts > start_ts will be ignored. This is for the sake of decode safety on read,
/// i.e. as data keeps being synced to region cache while the schema for a specific read is fixed,
/// we'll always have newer data than schema, only ignoring them can guarantee the decode safety.
///
/// On decode error, i.e. column number/type mismatch, will do force apply schema,
/// i.e. add/remove/cast unknown/missing/type-mismatch column if force_decode is true, otherwise return empty block and false.
/// Moreover, exception will be thrown if we see fatal decode error meanwhile force_decode is true.
///
/// This is the common routine used by both 'flush' and 'read' processes of TXN engine,
/// each of which will use carefully adjusted 'start_ts' and 'force_decode' with appropriate error handling/retry to get what they want.
std::tuple<Block, bool> readRegionBlock(const TiDB::TableInfo & table_info,
    const ColumnsDescription & columns,
    const Names & column_names_to_read,
    RegionDataReadInfoList & data_list,
    Timestamp start_ts,
    bool force_decode);

} // namespace DB
