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

RegionTable::Table & RegionTable::getOrCreateTable(TableID table_id)
{
    auto it = tables.find(table_id);
    if (it == tables.end())
    {
        // Load persisted info.
        getOrCreateStorage(table_id);

        std::tie(it, std::ignore) = tables.try_emplace(table_id, parent_path + "tables/", table_id);

        auto & table = it->second;
        table.persist();
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

RegionTable::InternalRegion & RegionTable::insertRegion(Table & table, const RegionPtr & region)
{
    auto region_id = region->id();
    auto & table_regions = table.regions.get();
    // Insert table mapping.
    table_regions.emplace(region_id, InternalRegion(region_id, region->getHandleRangeByTable(table.table_id)));

    // Insert region mapping.
    auto r_it = regions.find(region_id);
    if (r_it == regions.end())
        std::tie(r_it, std::ignore) = regions.try_emplace(region_id);
    RegionInfo & region_info = r_it->second;
    region_info.tables.emplace(table.table_id);

    return table_regions[region_id];
}

RegionTable::InternalRegion & RegionTable::getOrInsertRegion(TableID table_id, const RegionPtr & region, TableIDSet & table_to_persist)
{
    auto & table = getOrCreateTable(table_id);
    auto & table_regions = table.regions.get();
    if (auto it = table_regions.find(region->id()); it != table_regions.end())
        return it->second;

    table_to_persist.emplace(table_id);
    return insertRegion(table, region);
}

void RegionTable::updateRegionRange(const RegionPtr & region, TableIDSet & table_to_persist)
{
    auto region_id = region->id();
    const auto range = region->getRange();

    auto it = regions.find(region_id);
    // if this region does not exist already, then nothing to shrink.
    if (it == regions.end())
        return;

    RegionInfo & region_info = it->second;
    auto t_it = region_info.tables.begin();
    while (t_it != region_info.tables.end())
    {
        auto table_id = *t_it;

        const auto handle_range = TiKVRange::getHandleRangeByTable(range, table_id);

        auto table_it = tables.find(table_id);
        if (table_it == tables.end())
            throw Exception("Table " + DB::toString(table_id) + " not found in table map", ErrorCodes::LOGICAL_ERROR);

        table_to_persist.emplace(table_id);

        Table & table = table_it->second;
        if (handle_range.first < handle_range.second)
        {
            if (auto region_it = table.regions.get().find(region_id); region_it != table.regions.get().end())
                region_it->second.range_in_table = handle_range;
            else
                throw Exception("InternalRegion " + DB::toString(region_id) + " not found in table " + DB::toString(table_id),
                    ErrorCodes::LOGICAL_ERROR);
            ++t_it;
        }
        else
        {
            // remove from table mapping
            table.regions.get().erase(region_id);
            t_it = region_info.tables.erase(t_it);
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

void RegionTable::flushRegion(TableID table_id, RegionID region_id, size_t & cache_size)
{
    StoragePtr storage = nullptr;
    {
        std::lock_guard<std::mutex> lock(mutex);
        storage = getOrCreateStorage(table_id);
    }

    LOG_DEBUG(log, "Flush region - table_id: " << table_id << ", region_id: " << region_id << ", original " << cache_size << " bytes");

    TMTContext & tmt = context.getTMTContext();

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
        auto [block, status] = getBlockInputStreamByRegion(tmt, table_id, region_id, table_info, columns, names, data_list);
        if (!block)
            return;

        std::ignore = status;

        TxnMergeTreeBlockOutputStream output(*merge_tree);
        output.write(*block);
    }

    // remove data in region
    {
        auto region = tmt.getKVStore()->getRegion(region_id);
        if (!region)
            return;
        auto remover = region->createCommittedRemover(table_id);
        for (const auto & [handle, write_type, commit_ts, value] : data_list)
        {
            std::ignore = write_type;
            std::ignore = value;

            remover->remove({handle, commit_ts});
        }
        cache_size = region->dataSize();

        if (cache_size == 0)
            region->incDirtyFlag();

        LOG_DEBUG(
            log, "Flush region - table_id: " << table_id << ", region_id: " << region_id << ", after flush " << cache_size << " bytes");
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

void RegionTable::restore(std::function<RegionPtr(RegionID)> region_fetcher)
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
        auto p = tables.try_emplace(table_id, parent_path + "tables/", table_id);
        Table & table = p.first->second;

        auto & table_regions = table.regions.get();
        for (auto it = table_regions.begin(); it != table_regions.end();)
        {
            auto region_id = it->first;
            auto region_ptr = region_fetcher(region_id);
            if (region_ptr == nullptr)
            {
                // It could happen that process crash after region split or apply snapshot,
                // and region has not been persisted, but region <-> table mapping does.
                it = table_regions.erase(it);
                LOG_WARNING(log, "Region " << region_id << " not found from KVStore, dropped.");
                continue;
            }
            else
                ++it;

            // Update region_id -> table_id
            {
                auto [it, ok] = regions.emplace(region_id, RegionInfo{});
                std::ignore = ok;
                it->second.tables.emplace(table_id);
            }
        }

        table.persist();
    }
}

void RegionTable::updateRegion(const RegionPtr & region, const TableIDSet & relative_table_ids)
{
    TableIDSet table_to_persist;
    size_t cache_bytes = region->dataSize();

    std::lock_guard<std::mutex> lock(mutex);

    for (auto table_id : relative_table_ids)
    {
        auto & internal_region = getOrInsertRegion(table_id, region, table_to_persist);
        internal_region.updated = true;
        internal_region.cache_bytes = cache_bytes;
    }

    for (auto table_id : table_to_persist)
        tables.find(table_id)->second.persist();
}

void RegionTable::applySnapshotRegion(const RegionPtr & region)
{
    // make operation about snapshot can only add mapping relations rather than delete.
    auto table_ids = region->getCommittedRecordTableID();
    updateRegion(region, table_ids);
}

void RegionTable::applySnapshotRegions(const RegionMap & region_map)
{
    std::lock_guard<std::mutex> lock(mutex);

    TableIDSet table_to_persist;
    for (const auto & [id, region] : region_map)
    {
        std::ignore = id;
        size_t cache_bytes = region->dataSize();
        auto table_ids = region->getCommittedRecordTableID();
        for (auto table_id : table_ids)
        {
            auto & internal_region = getOrInsertRegion(table_id, region, table_to_persist);
            internal_region.cache_bytes = cache_bytes;
            if (cache_bytes)
                internal_region.updated = true;
        }
        updateRegionRange(region, table_to_persist);
    }
    for (auto table_id : table_to_persist)
        tables.find(table_id)->second.persist();
}

void RegionTable::splitRegion(const RegionPtr & kvstore_region, const std::vector<RegionPtr> & split_regions)
{
    std::lock_guard<std::mutex> lock(mutex);

    auto region_id = kvstore_region->id();
    auto it = regions.find(region_id);

    if (it == regions.end())
    {
        // If kvstore_region doesn't exist, usually means it does not contain any data we interested. Just ignore it.
        return;
    }

    TableIDSet table_to_persist;
    RegionInfo & region_info = it->second;
    for (auto table_id : region_info.tables)
    {
        auto & table = getOrCreateTable(table_id);

        for (const RegionPtr & split_region : split_regions)
        {
            const auto handle_range = split_region->getHandleRangeByTable(table_id);

            if (handle_range.first >= handle_range.second)
                continue;

            table_to_persist.emplace(table_id);
            auto & region = insertRegion(table, split_region);
            region.must_flush = true;
            region.cache_bytes = split_region->dataSize();
        }
    }

    updateRegionRange(kvstore_region, table_to_persist);
    for (auto table_id : table_to_persist)
        tables.find(table_id)->second.persist();
}

void RegionTable::removeRegion(const RegionPtr & region)
{
    std::unordered_set<TableID> tables;
    {
        auto region_id = region->id();

        std::lock_guard<std::mutex> lock(mutex);

        auto r_it = regions.find(region_id);
        if (r_it == regions.end())
        {
            LOG_WARNING(log, "RegionTable::removeRegion: region " << region_id << " does not exist.");
            return;
        }
        RegionInfo & region_info = r_it->second;
        tables.swap(region_info.tables);

        regions.erase(region_id);

        for (auto table_id : tables)
        {
            auto & table = getOrCreateTable(table_id);
            table.regions.get().erase(region_id);
            table.persist();
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
            if (it->second.tables.empty())
            {
                LOG_DEBUG(log, "[tryFlushRegion] region " << region_id << " fail, no table for mapping");
                return;
            }
            table_id = *it->second.tables.begin();
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
            auto & internal_region_map = table_it->second.regions.get();
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
            LOG_INFO(log, "[tryFlushRegion] internal region " << region_id << " pause flush, might be flushing");
            return false;
        }
        region.pause_flush = true;
        cache_bytes = region.cache_bytes;
        return true;
    });

    if (!status)
        return;

    flushRegion(table_id, region_id, cache_bytes);

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

    for (auto && [id, cache_bytes] : to_flush)
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
        for (auto & region_info : table.regions.get())
        {
            callback(table_id, region_info.second);
        }
    }
}

void RegionTable::traverseInternalRegionsByTable(const TableID table_id, std::function<void(const InternalRegion &)> && callback)
{
    std::lock_guard<std::mutex> lock(mutex);

    auto & table = getOrCreateTable(table_id);
    for (const auto & region_info : table.regions.get())
        callback(region_info.second);
}

void RegionTable::traverseRegionsByTable(
    const TableID table_id, std::function<void(std::vector<std::pair<RegionID, RegionPtr>> &)> && callback)
{
    auto & kvstore = context.getTMTContext().getKVStore();
    std::vector<std::pair<RegionID, RegionPtr>> regions;
    {
        std::lock_guard<std::mutex> lock(mutex);
        auto & table = getOrCreateTable(table_id);

        for (const auto & region_info : table.regions.get())
        {
            auto region = kvstore->getRegion(region_info.second.region_id);
            regions.emplace_back(region_info.second.region_id, region);
        }
    }
    callback(regions);
}

void RegionTable::mockDropRegionsInTable(TableID table_id)
{
    auto & kvstore = context.getTMTContext().getKVStore();
    traverseRegionsByTable(table_id, [&](std::vector<std::pair<RegionID, RegionPtr>> & regions) {
        for (auto && [region_id, _] : regions)
        {
            std::ignore = _;
            kvstore->removeRegion(region_id, this);
        }
    });

    std::lock_guard<std::mutex> lock(mutex);
    tables.erase(table_id);
}

void RegionTable::setFlushThresholds(const FlushThresholds::FlushThresholdsData & flush_thresholds_)
{
    flush_thresholds.setFlushThresholds(flush_thresholds_);
}

} // namespace DB
