// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Storages/Transaction/DecodingStorageSchemaSnapshot.h>
#include <Storages/Transaction/RegionDataRead.h>

namespace DB
{
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
        bool is_full_range_scan_,
        std::vector<HandleRange<Int64>> int64_ranges_,
        std::vector<HandleRange<UInt64>> uint64_ranges_)
        : is_full_range_scan(is_full_range_scan_)
        , int64_ranges(std::move(int64_ranges_))
        , uint64_ranges(std::move(uint64_ranges_))
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
    RegionScanFilterPtr scan_filter;
    Timestamp start_ts = std::numeric_limits<Timestamp>::max();

public:
    RegionBlockReader(DecodingStorageSchemaSnapshotConstPtr schema_snapshot_);

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

    /// Read `data_list` as a block.
    ///
    /// On decode error, i.e. column number/type mismatch, caller should trigger a schema-sync and retry with `force_decode=True`,
    /// i.e. add/remove/cast unknown/missing/type-mismatch column if force_decode is true, otherwise return empty block and false.
    /// Moreover, exception will be thrown if we see fatal decode error meanwhile `force_decode` is true.
    ///
    /// `RegionBlockReader::read` is the common routine used by both 'flush' and 'read' processes of Delta-Tree engine,
    /// which will use carefully adjusted 'force_decode' with appropriate error handling/retry to get what they want.
    bool read(Block & block, const RegionDataReadInfoList & data_list, bool force_decode);

private:
    template <TMTPKType pk_type>
    bool readImpl(Block & block, const RegionDataReadInfoList & data_list, bool force_decode);

private:
    DecodingStorageSchemaSnapshotConstPtr schema_snapshot;
};

} // namespace DB
