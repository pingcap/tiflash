#pragma once

#include <Core/Names.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/Transaction/RegionDataRead.h>

namespace TiDB
{
struct TableInfo;
};

namespace DB
{

class IManageableStorage;
using ManageableStoragePtr = std::shared_ptr<IManageableStorage>;

struct ColumnsDescription;
class Block;

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

/// The Reader to read the region data in `data_list` and decode based on the given table_info and columns, as a block.
class RegionBlockReader : private boost::noncopyable
{
    /// The schema to decode rows
    const TiDB::TableInfo & table_info;
    const ColumnsDescription & columns;

    RegionScanFilterPtr scan_filter;
    Timestamp start_ts = std::numeric_limits<Timestamp>::max();

    // Whether to reorder the rows when pk is uint64.
    // For Delta-Tree, we don't need to reorder rows to be sorted by uint64 pk
    bool do_reorder_for_uint64_pk = true;

public:
    // Decode and read columns from `storage`
    RegionBlockReader(const ManageableStoragePtr & storage);

    RegionBlockReader(const TiDB::TableInfo & table_info_, const ColumnsDescription & columns_);

    inline RegionBlockReader & setFilter(RegionScanFilterPtr filter)
    {
        scan_filter = std::move(filter);
        return *this;
    }

    /// Set the `start_ts` for reading data. The `start_ts` is `Timestamp::max` if not set.
    ///
    /// Data with commit_ts > start_ts will be ignored. This is for the sake of decode safety on read,
    /// i.e. as data keeps being synced to region cache while the schema for a specific read is fixed,
    /// we'll always have newer data than schema, only ignoring them can guarantee the decode safety.
    inline RegionBlockReader & setStartTs(Timestamp tso)
    {
        start_ts = tso;
        return *this;
    }

    /// Set whether to reorder rows when the type of primary key is UInt64.
    /// It is false if this reader is created by `RegionBlockReader(const ManageableStoragePtr &)` and the
    /// storage engine is Delta-Tree.
    /// Otherwise it is true by default.
    inline RegionBlockReader & setReorderUInt64PK(bool flag)
    {
        do_reorder_for_uint64_pk = flag;
        return *this;
    }

    /// Read `data_list` as a block.
    ///
    /// On decode error, i.e. column number/type mismatch, will do force apply schema,
    /// i.e. add/remove/cast unknown/missing/type-mismatch column if force_decode is true, otherwise return empty block and false.
    /// Moreover, exception will be thrown if we see fatal decode error meanwhile `force_decode` is true.
    ///
    /// `RegionBlockReader::read` is the common routine used by both 'flush' and 'read' processes of TXN engine (Delta-Tree, TXN-MergeTree),
    /// each of which will use carefully adjusted 'start_ts' and 'force_decode' with appropriate error handling/retry to get what they want.
    std::tuple<Block, bool> read(const Names & column_names_to_read, RegionDataReadInfoList & data_list, bool force_decode);

    ///  Read all columns from `data_list` as a block.
    inline std::tuple<Block, bool> read(RegionDataReadInfoList & data_list, bool force_decode)
    {
        return read(columns.getNamesOfPhysical(), data_list, force_decode);
    }
};

} // namespace DB
