// Copyright 2023 PingCAP, Inc.
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

#include <Storages/KVStore/Decode/DecodedTiKVKeyValue.h>
#include <Storages/KVStore/Decode/TiKVHandle.h>
#include <Storages/RegionQueryInfo_fwd.h>

namespace DB
{

namespace DM
{
class ScanContext;
using ScanContextPtr = std::shared_ptr<ScanContext>;
} // namespace DM

struct RegionQueryInfo
{
    RegionQueryInfo(
        RegionID region_id_,
        UInt64 version_,
        UInt64 conf_version_,
        TableID physical_table_id_,
        const std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr> & range_in_table_ = {},
        const std::vector<std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr>> & required_handle_ranges_ = {})
        : region_id(region_id_)
        , version(version_)
        , conf_version(conf_version_)
        , physical_table_id(physical_table_id_)
        , range_in_table(range_in_table_)
        , required_handle_ranges(required_handle_ranges_)
    {}
    RegionID region_id;
    UInt64 version;
    UInt64 conf_version;
    TableID physical_table_id;
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
    const UInt64 start_ts;

    const bool resolve_locks;

    using RegionsQueryInfo = std::vector<RegionQueryInfo>;
    RegionsQueryInfo regions_query_info;

    // A cache for Region -> read index result between retries
    using ReadIndexRes = std::unordered_map<RegionID, UInt64>;

    DM::ScanContextPtr scan_context;

private:
    ReadIndexRes read_index_res_cache;

public:
    explicit MvccQueryInfo(bool resolve_locks_ = false, UInt64 start_ts_ = 0, DM::ScanContextPtr scan_ctx = nullptr);

    void addReadIndexResToCache(RegionID region_id, UInt64 read_index)
    {
        LOG_DEBUG(
            DB::Logger::get(),
            "addReadIndexResToCache region_id={} read_index={} start_ts={}",
            region_id,
            read_index,
            start_ts);
        read_index_res_cache[region_id] = read_index;
    }

    UInt64 getReadIndexRes(RegionID region_id) const
    {
        if (auto it = read_index_res_cache.find(region_id); it != read_index_res_cache.end())
            return it->second;
        return 0;
    }
};

} // namespace DB
