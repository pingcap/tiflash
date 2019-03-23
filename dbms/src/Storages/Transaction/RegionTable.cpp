#include <Raft/RaftService.h>

#include <Storages/MergeTree/TxnMergeTreeBlockOutputStream.h>
#include <Storages/Transaction/PartitionDataMover.h>
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

auto getRegionTableIds(const RegionPtr & region)
{
    std::unordered_set<TableID> table_ids;
    {
        auto scanner = region->createCommittedScanRemover(InvalidTableID);
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
        table.regions.persist();
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

RegionTable::InternalRegion & RegionTable::insertRegion(Table & table, RegionID region_id)
{
    auto & table_regions = table.regions.get();
    // Insert table mapping.
    table.regions.get().emplace(region_id, InternalRegion(region_id));
    table.regions.persist();

    // Insert region mapping.
    auto r_it = regions.find(region_id);
    if (r_it == regions.end())
        std::tie(r_it, std::ignore) = regions.try_emplace(region_id);
    RegionInfo & region_info = r_it->second;
    region_info.tables.emplace(table.table_id);

    return table_regions[region_id];
}

RegionTable::InternalRegion & RegionTable::getOrInsertRegion(TableID table_id, RegionID region_id)
{
    auto & table = getOrCreateTable(table_id);
    auto & table_regions = table.regions.get();
    if (auto it = table_regions.find(region_id); it != table_regions.end())
        return it->second;

    return insertRegion(table, region_id);
}

void RegionTable::updateRegionRange(const RegionPtr & region)
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
        if (TiKVRange::checkTableInvolveRange(table_id, range))
        {
            ++t_it;
            continue;
        }

        // remove from table mapping
        auto table_it = tables.find(table_id);
        if (table_it == tables.end())
        {
            throw Exception("Table " + DB::toString(table_id) + " not found in table map", ErrorCodes::LOGICAL_ERROR);
        }

        Table & table = table_it->second;
        table.regions.get().erase(region_id);
        table.regions.persist();

        // remove from region mapping
        t_it = region_info.tables.erase(t_it);
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

void RegionTable::flushRegion(TableID table_id, RegionID region_id, size_t & rest_cache_size)
{
    {
        auto & table = getOrCreateTable(table_id);
        auto & region = table.regions.get()[region_id];
        LOG_DEBUG(log,
            "Flush region - table_id: " << table_id << ", region_id: " << region_id << ", original " << region.cache_bytes << " bytes");
    }

    std::vector<TiKVKey> keys_to_remove;
    {
        StoragePtr storage = getOrCreateStorage(table_id);

        auto merge_tree = std::dynamic_pointer_cast<StorageMergeTree>(storage);

        auto table_lock = merge_tree->lockStructure(true, __PRETTY_FUNCTION__);

        const auto & table_info = merge_tree->getTableInfo();
        const auto & columns = merge_tree->getColumns();
        // TODO: confirm names is right
        Names names = columns.getNamesOfPhysical();
        auto [input, status, tol] = getBlockInputStreamByRegion(
            table_id, region_id, InvalidRegionVersion, table_info, columns, names, false, false, 0, &keys_to_remove);
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
        auto & kvstore = context.getTMTContext().kvstore;
        auto region = kvstore->getRegion(region_id);
        if (!region)
            return;
        auto scanner = region->createCommittedScanRemover(table_id);
        for (const auto & key : keys_to_remove)
            scanner->remove(key);
        rest_cache_size = region->dataSize();

        if (rest_cache_size == 0)
            region->incPersistParm();

        LOG_DEBUG(log,
            "Flush region - table_id: " << table_id << ", region_id: " << region_id << ", after flush " << rest_cache_size << " bytes");
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
{
}

void RegionTable::restore(std::function<RegionPtr(RegionID)> region_fetcher)
{
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

        for (auto it = table.regions.get().begin(); it != table.regions.get().end();)
        {
            auto region_id = it->first;
            auto & region = it->second;
            auto region_ptr = region_fetcher(region_id);
            if (!region_ptr)
            {
                // It could happen that process crash after region split or region snapshot apply,
                // and region has not been persisted, but region <-> partition mapping does.
                it = table.regions.get().erase(it);
                LOG_WARNING(log, "Region " << region_id << " not found from KVStore, dropped.");
                continue;
            }
            else
                ++it;

            region.cache_bytes = region_ptr->dataSize();
            if (region.cache_bytes)
                region.updated = true;

            // Update region_id -> table_id
            {
                auto it = regions.find(region_id);
                if (it == regions.end())
                    std::tie(it, std::ignore) = regions.try_emplace(region_id);
                RegionInfo & region_info = it->second;
                region_info.tables.emplace(table_id);
            }
        }

        table.regions.persist();
    }
}

void RegionTable::updateRegion(const RegionPtr & region, const TableIDSet & relative_table_ids)
{
    std::lock_guard<std::mutex> lock(mutex);

    auto region_id = region->id();
    size_t cache_bytes = region->dataSize();
    for (auto table_id : relative_table_ids)
    {
        auto & region = getOrInsertRegion(table_id, region_id);
        region.updated = true;
        region.cache_bytes = cache_bytes;
    }
}

void RegionTable::applySnapshotRegion(const RegionPtr & region)
{
    std::lock_guard<std::mutex> lock(mutex);

    auto region_id = region->id();
    auto table_ids = getRegionTableIds(region);
    for (auto table_id : table_ids)
    {
        auto & internal_region = getOrInsertRegion(table_id, region_id);
        internal_region.must_flush = true;
        internal_region.cache_bytes = region->dataSize();
    }
}

void RegionTable::splitRegion(const RegionPtr & region, const std::vector<RegionPtr> & split_regions)
{
    std::lock_guard<std::mutex> lock(mutex);

    auto region_id = region->id();
    auto it = regions.find(region_id);

    if (it == regions.end())
    {
        // If region doesn't exist, usually means it does not contain any data we interested. Just ignore it.
        return;
    }

    RegionInfo & region_info = it->second;
    for (auto table_id : region_info.tables)
    {
        auto & table = getOrCreateTable(table_id);

        for (const RegionPtr & split_region : split_regions)
        {
            const auto range = split_region->getRange();
            if (!TiKVRange::checkTableInvolveRange(table_id, range))
                continue;

            auto split_region_id = split_region->id();

            auto & region = insertRegion(table, split_region_id);
            region.must_flush = true;
        }
    }

    updateRegionRange(region);
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
            table.regions.persist();
        }
    }

    for (auto table_id : tables)
    {
        auto storage = getOrCreateStorage(table_id);
        if (storage == nullptr)
        {
            LOG_WARNING(log, "RegionTable::removeRegion: table " << table_id << " does not exist.");
            continue;
        }
        auto * merge_tree = dynamic_cast<StorageMergeTree *>(storage.get());
        auto [start_handle, end_handle] = region->getHandleRangeByTable(table_id);
        deleteRange(context, merge_tree, start_handle, end_handle);
    }
}

bool RegionTable::tryFlushRegions()
{
    std::map<std::pair<TableID, RegionID>, size_t> to_flush;
    {
        traverseRegions([&](TableID table_id, InternalRegion & region) {
            if (shouldFlush(region))
            {
                to_flush.insert_or_assign({table_id, region.region_id}, region.cache_bytes);
                // Stop other flush threads.
                region.pause_flush = true;
            }
        });
    }

    for (auto && [id, data] : to_flush)
    {
        flushRegion(id.first, id.second, data);
    }

    {
        // Now reset status infomations.
        Timepoint now = Clock::now();
        traverseRegions([&](TableID table_id, InternalRegion & region) {
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

void RegionTable::traverseRegions(std::function<void(TableID, InternalRegion &)> && callback)
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

void RegionTable::traverseRegionsByTable(const TableID table_id, std::function<void(Regions)> && callback)
{
    auto & kvstore = context.getTMTContext().kvstore;
    Regions regions;
    {
        std::lock_guard<std::mutex> lock(mutex);
        auto & table = getOrCreateTable(table_id);

        for (const auto & region_info : table.regions.get())
        {
            auto region = kvstore->getRegion(region_info.second.region_id);
            if (region == nullptr)
                continue;
            regions.push_back(region);
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
