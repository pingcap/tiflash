#include <Storages/MergeTree/TxnMergeTreeBlockOutputStream.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionBlockReader.h>
#include <Storages/Transaction/RegionTable.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiKVRange.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_TABLE;
} // namespace ErrorCodes

RegionTable::Table & RegionTable::getOrCreateTable(const TableID table_id)
{
    auto it = tables.find(table_id);
    if (it == tables.end())
    {
        // Load persisted info.
        if (getOrCreateStorage(table_id) == nullptr)
            throw Exception("Get or create storage fail", ErrorCodes::LOGICAL_ERROR);

        std::tie(it, std::ignore) = tables.emplace(table_id, Table(table_id));

        Poco::File dir(parent_path + "tables/" + DB::toString(table_id));

        if (dir.exists())
            LOG_INFO(log, "[getOrCreateTable] table " << table_id << " exists");
        else
        {
            LOG_INFO(log, "[getOrCreateTable] start to create table " << table_id);
            if (!dir.createFile())
                throw Exception("[RegionTable::getOrCreateTable] create file fail", ErrorCodes::LOGICAL_ERROR);
            LOG_INFO(log, "[getOrCreateTable] create table done");
        }
    }
    return it->second;
}

StoragePtr RegionTable::getOrCreateStorage(TableID table_id)
{
    auto & tmt_ctx = context.getTMTContext();
    auto storage = tmt_ctx.getStorages().get(table_id);
    if (storage == nullptr)
    {
        tmt_ctx.getSchemaSyncer()->syncSchema(table_id, context, false);
        storage = tmt_ctx.getStorages().get(table_id);
    }
    if (storage == nullptr)
    {
        LOG_WARNING(log, __PRETTY_FUNCTION__ << ": Table " << table_id << " not found in TMT context.");
    }
    return storage;
}

RegionTable::InternalRegion & RegionTable::insertRegion(Table & table, const Region & region)
{
    const auto range = region.getRange();
    return insertRegion(table, range.first, range.second, region.id());
}

RegionTable::InternalRegion & RegionTable::insertRegion(Table & table, const TiKVKey & start, const TiKVKey & end, const RegionID region_id)
{
    auto & table_regions = table.regions;
    // Insert table mapping.
    auto [it, ok]
        = table_regions.emplace(region_id, InternalRegion(region_id, TiKVRange::getHandleRangeByTable(start, end, table.table_id)));
    if (!ok)
        throw Exception(
            "[RegionTable::insertRegion] insert duplicate internal region " + DB::toString(region_id), ErrorCodes::LOGICAL_ERROR);

    // Insert region mapping.
    RegionInfo & region_info = regions[region_id];
    region_info.emplace(table.table_id);

    return it->second;
}

RegionTable::InternalRegion & RegionTable::getOrInsertRegion(const TableID table_id, const Region & region)
{
    auto & table = getOrCreateTable(table_id);
    auto & table_regions = table.regions;
    if (auto it = table_regions.find(region.id()); it != table_regions.end())
        return it->second;

    return insertRegion(table, region);
}

void RegionTable::shrinkRegionRange(const Region & region)
{
    std::lock_guard<std::mutex> lock(mutex);
    doShrinkRegionRange(region);
}

void RegionTable::doShrinkRegionRange(const Region & region)
{
    auto region_id = region.id();
    const auto range = region.getRange();

    auto it = regions.find(region_id);
    // if this region does not exist already, then nothing to shrink.
    if (it == regions.end())
        return;

    RegionInfo & region_info = it->second;
    auto region_table_it = region_info.begin();
    while (region_table_it != region_info.end())
    {
        auto table_id = *region_table_it;

        const auto handle_range = TiKVRange::getHandleRangeByTable(range, table_id);

        auto table_it = tables.find(table_id);
        if (table_it == tables.end())
            throw Exception("Table " + DB::toString(table_id) + " not found in table map", ErrorCodes::LOGICAL_ERROR);

        Table & table = table_it->second;
        if (handle_range.first < handle_range.second)
        {
            if (auto region_it = table.regions.find(region_id); region_it != table.regions.end())
                region_it->second.range_in_table = handle_range;
            else
                throw Exception("InternalRegion " + DB::toString(region_id) + " not found in table " + DB::toString(table_id),
                    ErrorCodes::LOGICAL_ERROR);
            ++region_table_it;
        }
        else
        {
            // remove from table mapping
            table.regions.erase(region_id);
            region_table_it = region_info.erase(region_table_it);
        }
    }
}

bool RegionTable::shouldFlush(const InternalRegion & region) const
{
    if (region.pause_flush)
        return false;
    if (region.must_flush)
        return true;
    if (!region.updated || !region.cache_bytes)
        return false;
    auto period_time = Clock::now() - region.last_flush_time;
    return flush_thresholds.traverse<bool>([&](const FlushThresholds::FlushThresholdsData & data) -> bool {
        for (const auto & [th_bytes, th_duration] : data)
        {
            if (region.cache_bytes >= th_bytes && period_time >= th_duration)
                return true;
        }
        return false;
    });
}

void RegionTable::flushRegion(TableID table_id, RegionID region_id, size_t & cache_size, const bool try_persist)
{
    StoragePtr storage = nullptr;
    {
        std::lock_guard<std::mutex> lock(mutex);
        storage = getOrCreateStorage(table_id);
    }

    TMTContext & tmt = context.getTMTContext();

    // store region ptr first.
    RegionPtr region = tmt.getKVStore()->getRegion(region_id);
    if (!region)
    {
        LOG_WARNING(log, "[flushRegion] region " << region_id << " is not found");
        return;
    }

    LOG_DEBUG(log, "[flushRegion] table " << table_id << ", [region " << region_id << "] original " << region->dataSize() << " bytes");

    UInt64 mem_read_cost = -1, write_part_cost = -1;

    RegionDataReadInfoList data_list;
    if (storage == nullptr)
    {
        // If storage still not existing after syncing schema, meaning this table is dropped and the data is to be GC-ed.
        // Ignore such data.
        LOG_WARNING(log,
            __PRETTY_FUNCTION__ << ": Not flushing table_id: " << table_id << ", region_id: " << region_id << " as storage doesn't exist.");
    }
    else
    {
        auto merge_tree = std::dynamic_pointer_cast<StorageMergeTree>(storage);

        auto table_lock = merge_tree->lockStructure(true, __PRETTY_FUNCTION__);

        const auto & table_info = merge_tree->getTableInfo();
        const auto & columns = merge_tree->getColumns();
        // TODO: confirm names is right
        Names names = columns.getNamesOfPhysical();
        if (names.size() < 3)
            throw Exception("[flushRegion] size of merge tree columns < 3, should not happen", ErrorCodes::LOGICAL_ERROR);

        auto start_time = Clock::now();

        auto block = getBlockInputStreamByRegion(table_id, region, table_info, columns, names, data_list);
        if (!block)
        {
            // no data in region for table. update cache size.
            cache_size = region->dataSize();
            return;
        }

        mem_read_cost = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start_time).count();
        start_time = Clock::now();

        TxnMergeTreeBlockOutputStream output(*merge_tree);
        output.write(std::move(*block));

        write_part_cost = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start_time).count();
    }

    // remove data in region
    {
        // avoid ABA problem.
        if (region != tmt.getKVStore()->getRegion(region_id))
        {
            LOG_DEBUG(log, "[flushRegion] region is moved out and back, ignore removing data.");
            return;
        }

        {
            auto remover = region->createCommittedRemover(table_id);
            for (const auto & [handle, write_type, commit_ts, value] : data_list)
            {
                std::ignore = write_type;
                std::ignore = value;

                remover->remove({handle, commit_ts});
            }
        }

        cache_size = region->dataSize();

        if (cache_size == 0)
        {
            if (try_persist)
                tmt.getKVStore()->tryPersist(region_id);
            else
                region->incDirtyFlag();
        }

        LOG_DEBUG(log,
            "[flushRegion] table " << table_id << ", [region " << region_id << "] after flush " << cache_size << " bytes, cost [mem read "
                                   << mem_read_cost << ", write part " << write_part_cost << "] ms");
    }
}

static const Int64 FTH_BYTES_1 = 1024;             // 1 KB
static const Int64 FTH_BYTES_2 = 1024 * 1024;      // 1 MB
static const Int64 FTH_BYTES_3 = 1024 * 1024 * 10; // 10 MBs
static const Int64 FTH_BYTES_4 = 1024 * 1024 * 50; // 50 MBs

static const Seconds FTH_PERIOD_1(60 * 60); // 1 hour
static const Seconds FTH_PERIOD_2(60 * 5);  // 5 minutes
static const Seconds FTH_PERIOD_3(60);      // 1 minute
static const Seconds FTH_PERIOD_4(5);       // 5 seconds

RegionTable::RegionTable(Context & context_, const std::string & parent_path_)
    : parent_path(parent_path_),
      flush_thresholds(RegionTable::FlushThresholds::FlushThresholdsData{
          {FTH_BYTES_1, FTH_PERIOD_1}, {FTH_BYTES_2, FTH_PERIOD_2}, {FTH_BYTES_3, FTH_PERIOD_3}, {FTH_BYTES_4, FTH_PERIOD_4}}),
      context(context_),
      log(&Logger::get("RegionTable"))
{}

void RegionTable::restore()
{
    std::lock_guard<std::mutex> lock(mutex);

    Poco::File dir(parent_path + "tables/");
    if (!dir.exists())
        dir.createDirectories();

    std::vector<std::string> file_names;
    dir.list(file_names);

    // Restore all table mappings and region mappings.
    for (auto & name : file_names)
    {
        TableID table_id = std::stoull(name);
        getOrCreateTable(table_id);
    }

    {
        std::stringstream ss;
        ss << "(";
        for (const auto & e : tables)
            ss << e.first << ",";
        ss << ")";

        LOG_INFO(log, "Restore " << tables.size() << " tables " << ss.str());
    }
}

void RegionTable::updateRegion(const Region & region, const TableIDSet & relative_table_ids)
{
    size_t cache_bytes = region.dataSize();

    std::lock_guard<std::mutex> lock(mutex);

    for (auto table_id : relative_table_ids)
    {
        auto & internal_region = getOrInsertRegion(table_id, region);
        internal_region.updated = true;
        internal_region.cache_bytes = cache_bytes;
    }
}

void RegionTable::applySnapshotRegion(const Region & region)
{
    // make operation about snapshot can only add mapping relations rather than delete.
    auto table_ids = region.getAllWriteCFTables();
    updateRegion(region, table_ids);
}

void RegionTable::applySnapshotRegions(const RegionMap & region_map)
{
    std::lock_guard<std::mutex> lock(mutex);

    for (const auto & [id, region] : region_map)
    {
        std::ignore = id;
        size_t cache_bytes = region->dataSize();
        auto table_ids = region->getAllWriteCFTables();
        for (const auto & e : tables)
            table_ids.insert(e.first);
        for (const auto table_id : table_ids)
        {
            auto handle_range = region->getHandleRangeByTable(table_id);
            if (handle_range.first >= handle_range.second)
                continue;

            auto & internal_region = getOrInsertRegion(table_id, *region);
            internal_region.cache_bytes = cache_bytes;
            if (cache_bytes)
                internal_region.updated = true;
        }
    }
}

void RegionTable::updateRegionForSplit(const Region & split_region, const RegionID source_region)
{
    std::lock_guard<std::mutex> lock(mutex);

    auto it = regions.find(source_region);

    if (it == regions.end())
    {
        // If source_region doesn't exist, usually means it does not contain any data we interested. Just ignore it.
        return;
    }

    for (const auto table_id : it->second)
    {
        const auto handle_range = split_region.getHandleRangeByTable(table_id);

        if (handle_range.first >= handle_range.second)
            continue;

        auto & internal_region = getOrInsertRegion(table_id, split_region);
        internal_region.must_flush = true;
        internal_region.cache_bytes = split_region.dataSize();
    }
}

void RegionTable::removeRegion(const RegionID region_id)
{
    std::unordered_set<TableID> tables;
    {
        std::lock_guard<std::mutex> lock(mutex);

        auto it = regions.find(region_id);
        if (it == regions.end())
        {
            LOG_WARNING(log, "[removeRegion] region " << region_id << " does not exist.");
            return;
        }
        RegionInfo & region_info = it->second;
        tables.swap(region_info);

        regions.erase(region_id);

        for (const auto table_id : tables)
        {
            auto & table = getOrCreateTable(table_id);
            table.regions.erase(region_id);
        }
    }
}

void RegionTable::tryFlushRegion(RegionID region_id)
{
    TableID table_id;
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (auto it = regions.find(region_id); it != regions.end())
        {
            if (it->second.empty())
            {
                LOG_DEBUG(log, "[tryFlushRegion] region " << region_id << " fail, no table for mapping");
                return;
            }
            // maybe this region contains more than one table, just flush the first one.
            table_id = *it->second.begin();
        }
        else
        {
            LOG_DEBUG(log, "[tryFlushRegion] region " << region_id << " fail, internal region not exist");
            return;
        }
    }

    const auto func_update_region = [&](std::function<bool(InternalRegion &)> && callback) -> bool {
        std::lock_guard<std::mutex> lock(mutex);
        if (auto table_it = tables.find(table_id); table_it != tables.end())
        {
            auto & internal_region_map = table_it->second.regions;
            if (auto region_it = internal_region_map.find(region_id); region_it != internal_region_map.end())
            {
                InternalRegion & region = region_it->second;
                return callback(region);
            }
            else
            {
                LOG_DEBUG(log, "[tryFlushRegion] region " << region_id << " fail, internal region might be removed");
                return false;
            }
        }
        else
        {
            LOG_DEBUG(log, "[tryFlushRegion] region " << region_id << " fail, table not exist");
            return false;
        }
    };

    size_t cache_bytes;
    bool status = func_update_region([&](InternalRegion & region) -> bool {
        if (region.pause_flush)
        {
            LOG_INFO(log, "[tryFlushRegion] internal region " << region_id << " pause flush, may be being flushed");
            return false;
        }
        region.pause_flush = true;
        cache_bytes = region.cache_bytes;
        return true;
    });

    if (!status)
        return;

    flushRegion(table_id, region_id, cache_bytes, false);

    func_update_region([&](InternalRegion & region) -> bool {
        region.pause_flush = false;
        region.must_flush = false;
        region.updated = false;
        region.cache_bytes = cache_bytes;
        region.last_flush_time = Clock::now();
        return true;
    });
}

bool RegionTable::tryFlushRegions()
{
    std::map<std::pair<TableID, RegionID>, size_t> to_flush;
    { // judge choose region to flush
        traverseInternalRegions([&](TableID table_id, InternalRegion & region) {
            if (shouldFlush(region))
            {
                to_flush.insert_or_assign({table_id, region.region_id}, region.cache_bytes);
                // Stop other flush threads.
                region.pause_flush = true;
            }
        });
    }

    for (auto & [id, cache_bytes] : to_flush)
        flushRegion(id.first, id.second, cache_bytes);

    { // Now reset status information.
        Timepoint now = Clock::now();
        traverseInternalRegions([&](TableID table_id, InternalRegion & region) {
            if (auto it = to_flush.find({table_id, region.region_id}); it != to_flush.end())
            {
                region.pause_flush = false;
                region.must_flush = false;
                region.updated = false;
                region.cache_bytes = it->second;
                region.last_flush_time = now;
            }
        });
    }

    return !to_flush.empty();
}

void RegionTable::traverseInternalRegions(std::function<void(TableID, InternalRegion &)> && callback)
{
    std::lock_guard<std::mutex> lock(mutex);
    for (auto && [table_id, table] : tables)
    {
        for (auto & region_info : table.regions)
        {
            callback(table_id, region_info.second);
        }
    }
}

void RegionTable::traverseInternalRegionsByTable(const TableID table_id, std::function<void(const InternalRegion &)> && callback)
{
    std::lock_guard<std::mutex> lock(mutex);

    auto & table = getOrCreateTable(table_id);
    for (const auto & region_info : table.regions)
        callback(region_info.second);
}

std::vector<std::pair<RegionID, RegionPtr>> RegionTable::getRegionsByTable(const TableID table_id)
{
    auto & kvstore = context.getTMTContext().getKVStore();
    std::vector<std::pair<RegionID, RegionPtr>> regions;
    {
        std::lock_guard<std::mutex> lock(mutex);
        auto & table = getOrCreateTable(table_id);

        for (const auto & region_info : table.regions)
        {
            auto region = kvstore->getRegion(region_info.second.region_id);
            regions.emplace_back(region_info.second.region_id, region);
        }
    }
    return regions;
}

void RegionTable::mockDropRegionsInTable(TableID table_id)
{
    std::lock_guard<std::mutex> lock(mutex);
    tables.erase(table_id);
    Poco::File dir(parent_path + "tables/" + DB::toString(table_id));
    if (dir.exists())
    {
        LOG_INFO(log, "[mockDropRegionsInTable] remove table " << table_id);
        dir.remove(true);
    }
}

void RegionTable::setFlushThresholds(const FlushThresholds::FlushThresholdsData & flush_thresholds_)
{
    flush_thresholds.setFlushThresholds(flush_thresholds_);
}

TableIDSet RegionTable::getAllMappedTables(const RegionID region_id) const
{
    std::lock_guard<std::mutex> lock(mutex);

    if (auto it = regions.find(region_id); it != regions.end())
        return it->second;

    return {};
}

} // namespace DB
