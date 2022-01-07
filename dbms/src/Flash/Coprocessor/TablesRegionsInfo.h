#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/RegionInfo.h>
#include <Storages/Transaction/TMTContext.h>
#include <kvproto/mpp.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

namespace DB
{
struct TableRegionsInfo
{
    RegionInfoMap local_regions;
    RegionInfoList remote_regions;
    size_t regionCount() const { return local_regions.size() + remote_regions.size(); }
};

class TablesRegionsInfo
{
public:
    static TablesRegionsInfo createTablesRegionsInfo(const google::protobuf::RepeatedPtrField<coprocessor::RegionInfo> & regions,
                                                     const google::protobuf::RepeatedPtrField<coprocessor::TableRegions> & table_regions,
                                                     const TMTContext & tmt_context);
    TablesRegionsInfo()
        : is_single_table(false)
    {}
    explicit TablesRegionsInfo(bool is_single_table)
        : is_single_table(is_single_table)
    {
        if (is_single_table)
            table_regions_info_map[InvalidTableID] = TableRegionsInfo();
    }
    TableRegionsInfo & getTableRegionsInfo()
    {
        assert(is_single_table);
        return table_regions_info_map.begin()->second;
    }
    TableRegionsInfo & getOrCreateTableRegionInfoByTableID(TableID table_id);
    const TableRegionsInfo & getTableRegionInfoByTableID(TableID table_id) const;
    const std::unordered_map<TableID, TableRegionsInfo> & getTableRegionsInfoMap() const
    {
        return table_regions_info_map;
    }
    bool containsRegionsInfoForTable(TableID table_id) const
    {
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
    UInt64 tableCount() const
    {
        return table_regions_info_map.size();
    }

private:
    bool is_single_table;
    std::unordered_map<TableID, TableRegionsInfo> table_regions_info_map;
};

} // namespace DB
