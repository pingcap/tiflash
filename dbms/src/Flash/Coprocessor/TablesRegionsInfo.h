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
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/RegionInfo.h>


#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/mpp.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

namespace DB
{
class TMTContext;
struct SingleTableRegions
{
    RegionInfoMap local_regions;
    RegionInfoList remote_regions;
    size_t regionCount() const { return local_regions.size() + remote_regions.size(); }
};

/// this class contains all the region info for all physical table scans in a dag request,
/// currently, one dag request only contain one logical table scan, if the target table
/// is non-partition table, then is_single_table is true, otherwise, is_single_table is false
/// since one partition table may contain multiple physical tables
class TablesRegionsInfo
{
public:
    static TablesRegionsInfo create(
        const google::protobuf::RepeatedPtrField<coprocessor::RegionInfo> & regions,
        const google::protobuf::RepeatedPtrField<coprocessor::TableRegions> & table_regions,
        const TMTContext & tmt_context);
    TablesRegionsInfo()
        : is_single_table(false)
    {}
    explicit TablesRegionsInfo(bool is_single_table_)
        : is_single_table(is_single_table_)
    {
        if (is_single_table)
            table_regions_info_map[InvalidTableID] = SingleTableRegions();
    }
    SingleTableRegions & getSingleTableRegions()
    {
        assert(is_single_table);
        return table_regions_info_map.begin()->second;
    }
    SingleTableRegions & getOrCreateTableRegionInfoByTableID(TableID table_id);
    const SingleTableRegions & getTableRegionInfoByTableID(TableID table_id) const;
    const std::unordered_map<TableID, SingleTableRegions> & getTableRegionsInfoMap() const
    {
        return table_regions_info_map;
    }
    bool containsRegionsInfoForTable(TableID table_id) const
    {
        /// for single table, skip check the table_id since we use use InvalidTableID as a place holder
        if (is_single_table)
            return true;
        return table_regions_info_map.find(table_id) != table_regions_info_map.end();
    }
    UInt64 regionCount() const
    {
        UInt64 ret = 0;
        for (const auto & entry : table_regions_info_map)
            ret += entry.second.regionCount();
        return ret;
    }
    UInt64 tableCount() const { return table_regions_info_map.size(); }

private:
    bool is_single_table;
    std::unordered_map<TableID, SingleTableRegions> table_regions_info_map;
};

} // namespace DB
