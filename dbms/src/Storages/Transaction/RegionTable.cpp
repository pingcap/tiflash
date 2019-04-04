#include <Raft/RaftService.h>

#include <Storages/MergeTree/TxnMergeTreeBlockOutputStream.h>
#include <Storages/Transaction/RegionBlockReader.h>
#include <Storages/Transaction/RegionTable.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_TABLE;
} // namespace ErrorCodes

// =============================================================
// Static methods.
// =============================================================

TableIDSet RegionTable::getRegionTableIds(const RegionPtr & region)
{
    TableIDSet table_ids;
    {
        auto scanner = region->createCommittedScanner(InvalidTableID);
        while (true)
        {
            TableID table_id = scanner->hasNext();
            if (table_id == InvalidTableID)
                break;
            table_ids.emplace(table_id);
            scanner->next();
        }
    }
    return table_ids;
}

// =============================================================
// Private member functions.
// =============================================================

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
    auto storage = tmt_ctx.storages.get(table_id);
    if (storage == nullptr)
    {
        tmt_ctx.getSchemaSyncer()->syncSchema(table_id, context);
        storage = tmt_ctx.storages.get(table_id);
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

    table_to_persist.insert(table_id);
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

        const auto handle_range = getHandleRangeByTable(range, table_id);

        auto table_it = tables.find(table_id);
        if (table_it == tables.end())
            throw Exception("Table " + DB::toString(table_id) + " not found in table map", ErrorCodes::LOGICAL_ERROR);

        table_to_persist.insert(table_id);

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

bool RegionTable::shouldFlush(const InternalRegion & region)
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

    RegionWriteCFDataTrait::Keys keys_to_remove;
    {
        auto merge_tree = std::dynamic_pointer_cast<StorageMergeTree>(storage);

        auto table_lock = merge_tree->lockStructure(true, __PRETTY_FUNCTION__);

        const auto & table_info = merge_tree->getTableInfo();
        const auto & columns = merge_tree->getColumns();
        // TODO: confirm names is right
        Names names = columns.getNamesOfPhysical();
        auto [input, status, tol] = getBlockInputStreamByRegion(tmt, table_id, region_id, table_info, columns, names, &keys_to_remove);
        if (input == nullptr)
            return;

        std::ignore = status;
        std::ignore = tol;

        TxnMergeTreeBlockOutputStream output(*merge_tree);
        input->readPrefix();
        output.writePrefix();
        while (true)
        {
            Block block = input->read();
            if (!block || block.rows() == 0)
                break;
            output.write(block);
        }
        input->readSuffix();
        output.writeSuffix();
    }

    // remove data in region
    {
        auto region = tmt.kvstore->getRegion(region_id);
        if (!region)
            return;
        auto remover = region->createCommittedRemover(table_id);
        for (const auto & key : keys_to_remove)
            remover->remove(key);
        cache_size = region->dataSize();

        if (cache_size == 0)
            region->incPersistParm();

        LOG_DEBUG(
            log, "Flush region - table_id: " << table_id << ", region_id: " << region_id << ", after flush " << cache_size << " bytes");
    }
}

// =============================================================
// Public member functions.
// =============================================================

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
    auto table_ids = getRegionTableIds(region);
    return applySnapshotRegion(region, table_ids);
}

void RegionTable::applySnapshotRegions(const ::DB::RegionMap & region_map)
{
    std::lock_guard<std::mutex> lock(mutex);

    TableIDSet table_to_persist;
    for (const auto & [id, region] : region_map)
    {
        std::ignore = id;
        size_t cache_bytes = region->dataSize();
        auto table_ids = getRegionTableIds(region);
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

void RegionTable::applySnapshotRegion(const RegionPtr & region, const TableIDSet & table_ids)
{
    TableIDSet table_to_persist;
    size_t cache_bytes = region->dataSize();

    std::lock_guard<std::mutex> lock(mutex);

    for (auto table_id : table_ids)
    {
        auto & internal_region = getOrInsertRegion(table_id, region, table_to_persist);
        internal_region.updated = true;
        internal_region.cache_bytes = cache_bytes;
    }

    updateRegionRange(region, table_to_persist);

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

            table_to_persist.insert(table_id);
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

    for (auto && [id, data] : to_flush)
        flushRegion(id.first, id.second, data);

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
    auto & kvstore = context.getTMTContext().kvstore;
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

void RegionTable::dumpRegionMap(RegionTable::RegionMap & res)
{
    std::lock_guard<std::mutex> lock(mutex);
    res = regions;
}

void RegionTable::dropRegionsInTable(TableID /*table_id*/)
{
    // TODO: impl
}

void RegionTable::setFlushThresholds(const FlushThresholds::FlushThresholdsData & flush_thresholds_)
{
    flush_thresholds.setFlushThresholds(flush_thresholds_);
}

} // namespace DB
