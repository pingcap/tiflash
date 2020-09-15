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

class RegionScanFilter
{
    bool is_full_range_scan;
    std::vector<HandleRange<Int64>> int64_ranges;
    std::vector<HandleRange<UInt64>> uint64_ranges;

    bool isValidHandle(UInt64 handle)
    {
        for (const auto & range : uint64_ranges)
        {
            if (handle >= range.first && handle < range.second)
            {
                return true;
            }
        }
        return false;
    }
    bool isValidHandle(Int64 handle)
    {
        for (const auto & range : int64_ranges)
        {
            if (handle >= range.first && handle < range.second)
            {
                return true;
            }
        }
        return false;
    }

public:
    RegionScanFilter(
        bool is_full_range_scan_, std::vector<HandleRange<Int64>> int64_ranges_, std::vector<HandleRange<UInt64>> uint64_ranges_)
        : is_full_range_scan(is_full_range_scan_), int64_ranges(std::move(int64_ranges_)), uint64_ranges(std::move(uint64_ranges_))
    {}
    bool filter(UInt64 handle) { return !is_full_range_scan && !isValidHandle(handle); }
    bool filter(Int64 handle) { return !is_full_range_scan && !isValidHandle(handle); }
    bool isFullRangeScan() { return is_full_range_scan; }
    const std::vector<HandleRange<UInt64>> & getUInt64Ranges() { return uint64_ranges; }
    const std::vector<HandleRange<Int64>> & getInt64Ranges() { return int64_ranges; }
};

using RegionScanFilterPtr = std::shared_ptr<RegionScanFilter>;

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
    bool force_decode,
    RegionScanFilterPtr scan_filter);

} // namespace DB
