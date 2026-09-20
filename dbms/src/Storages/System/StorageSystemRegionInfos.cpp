// Copyright 2025 PingCAP, Inc.
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

#include <Columns/ColumnString.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DatabaseTiFlash.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/Types.h>
#include <Storages/MutableSupport.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/System/StorageSystemRegionInfos.h>
#include <TiDB/Schema/SchemaNameMapper.h>

namespace DB
{

StorageSystemRegionInfos::StorageSystemRegionInfos(const std::string & name_)
    : name(name_)
{
    setColumns(ColumnsDescription({
        {"region_id", std::make_shared<DataTypeUInt64>()},
        {"keyspace_id", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>())},
        {"table_id", std::make_shared<DataTypeInt64>()},
        {"leader_safe_ts", std::make_shared<DataTypeUInt64>()},
        {"self_safe_ts", std::make_shared<DataTypeUInt64>()},
    }));
}

struct RegionInfoSummary
{
    RegionID region_id;
    KeyspaceTableID ks_table_id;
    SafeTS self_safe_ts;
    SafeTS leader_safe_ts;
};

BlockInputStreams StorageSystemRegionInfos::read(
    const Names & column_names,
    const SelectQueryInfo &,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    auto & tmt = context.getTMTContext();
    auto kvstore = tmt.getKVStore();
    std::map<RegionID, RegionInfoSummary> regions;
    kvstore->traverseRegions([&regions](RegionID region_id, const RegionPtr & region) {
        regions.emplace(
            region_id,
            RegionInfoSummary{
                .region_id = region_id,
                .ks_table_id = region->getKeyspaceTableID(),
            });
    });

    auto & region_tbl = tmt.getRegionTable();
    auto & safe_ts_mgr = region_tbl.safeTsMgr();
    for (auto & [region_id, region_summary] : regions)
    {
        auto safe_ts_pair = safe_ts_mgr.get(region_id);
        region_summary.leader_safe_ts = safe_ts_pair.leader_safe_ts;
        region_summary.self_safe_ts = safe_ts_pair.self_safe_ts;
    }

    MutableColumns res_columns = getSampleBlock().cloneEmptyColumns();
    for (const auto & [region_id, region_summary] : regions)
    {
        size_t j = 0;
        res_columns[j++]->insert(region_id);
        res_columns[j++]->insert(static_cast<UInt64>(region_summary.ks_table_id.first));
        res_columns[j++]->insert(region_summary.ks_table_id.second);
        res_columns[j++]->insert(region_summary.leader_safe_ts);
        res_columns[j++]->insert(region_summary.self_safe_ts);
    }

    return BlockInputStreams(
        1,
        std::make_shared<OneBlockInputStream>(getSampleBlock().cloneWithColumns(std::move(res_columns))));
}


} // namespace DB
