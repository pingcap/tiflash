#pragma once

namespace DB
{
class Context;
class Region;
using RegionPtr = std::shared_ptr<Region>;
using Regions = std::vector<RegionPtr>;
} // namespace DB

namespace DB::RegionBench
{

RegionPtr createRegion(TableID table_id, RegionID region_id, const RegionKey & start, const RegionKey & end);

Regions createRegions(TableID table_id, size_t region_num, size_t key_num_each_region, HandleID handle_begin, RegionID new_region_id_begin);

void insert(const TiDB::TableInfo & table_info, RegionID region_id, HandleID handle_id, ASTs::const_iterator begin,
    ASTs::const_iterator end, Context & context);

void concurrentBatchInsert(const TiDB::TableInfo & table_info, Int64 concurrent_num, Int64 flush_num, Int64 batch_num, UInt64 min_strlen,
    UInt64 max_strlen, Context & context);

void remove(const TiDB::TableInfo & table_info, RegionID region_id, HandleID handle_id, Context & context);

Int64 concurrentRangeOperate(
    const TiDB::TableInfo & table_info, HandleID start_handle, HandleID end_handle, Context & context, Int64 magic_num, bool del);

} // namespace DB::RegionBench
