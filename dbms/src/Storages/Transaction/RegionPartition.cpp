#include <Raft/RaftService.h>

#include <Storages/MergeTree/TxnMergeTreeBlockOutputStream.h>
#include <Storages/Transaction/PartitionDataMover.h>
#include <Storages/Transaction/RegionBlockReader.h>
#include <Storages/Transaction/RegionPartition.h>
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

Int64 calculatePartitionCacheBytes(KVStore & kvstore, RegionID region_id)
{
    return kvstore.getRegion(region_id)->dataSize();
}

// =============================================================
// Private member functions.
// =============================================================

RegionPartition::Table & RegionPartition::getOrCreateTable(TableID table_id)
{
    auto it = tables.find(table_id);
    if (it == tables.end())
    {
        // Load persisted info.
        auto & tmt_ctx = context.getTMTContext();
        auto storage = tmt_ctx.storages.get(table_id);
        if (!storage)
        {
            tmt_ctx.getSchemaSyncer()->syncSchema(table_id, context);
            storage = tmt_ctx.storages.get(table_id);
        }

        std::tie(it, std::ignore) = tables.try_emplace(table_id, parent_path + "tables/", table_id);

        auto & table = it->second;
        table.regions.persist();
    }
    return it->second;
}

RegionPartition::InternalRegion & RegionPartition::insertRegion(Table & table, RegionID region_id)
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

RegionPartition::InternalRegion & RegionPartition::getOrInsertRegion(TableID table_id, RegionID region_id)
{
    auto & table = getOrCreateTable(table_id);
    auto & table_regions = table.regions.get();
    if (auto it = table_regions.find(region_id); it != table_regions.end())
        return it->second;

    return insertRegion(table, region_id);
}

void RegionPartition::updateRegionRange(const RegionPtr & region)
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

bool RegionPartition::shouldFlush(const InternalRegion & region)
{
    if (region.pause_flush)
        return false;
    if (region.must_flush)
        return true;
    if (!region.updated || !region.cache_bytes)
        return false;
    auto period_time = Clock::now() - region.last_flush_time;
    for (auto && [th_bytes, th_duration] : flush_thresholds)
    {
        if (region.cache_bytes >= th_bytes && period_time >= th_duration)
            return true;
    }
    return false;
}

void RegionPartition::flushRegion(TableID table_id, RegionID region_id)
{
    if (log->debug())
    {
        auto & table = getOrCreateTable(table_id);
        auto & region = table.regions.get()[region_id];
        LOG_DEBUG(log,
            "Flush regions - table_id: " + DB::toString(table_id) + ", ~ "
                + DB::toString(region.cache_bytes) + " bytes, containing region_ids: " + DB::toString(region_id));
    }

    TMTContext & tmt = context.getTMTContext();
    tmt.getSchemaSyncer()->syncSchema(table_id, context);

    StoragePtr storage = tmt.storages.get(table_id);

    // TODO: handle if storage is nullptr
    // drop table and create another with same name, but the previous one will still flush
    if (storage == nullptr)
    {
        LOG_ERROR(log, "table " << table_id << " flush region " << region_id << " , but storage is not found");
        return;
    }

    auto * merge_tree = dynamic_cast<StorageMergeTree *>(storage.get());

    const auto & table_info = merge_tree->getTableInfo();
    const auto & columns = merge_tree->getColumns();
    // TODO: confirm names is right
    Names names = columns.getNamesOfPhysical();
    std::vector<TiKVKey> keys;
    auto [input, status] =
        getBlockInputStreamByRegion(table_id, region_id, -1, table_info, columns, names, false, false, 0, &keys);
    if (!input)
        return;

    std::ignore = status;

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
    output.writeSuffix();
    input->readSuffix();

    // remove data in region
    {
        auto & kvstore = context.getTMTContext().kvstore;
        auto region = kvstore->getRegion(region_id);
        if (!region)
            return;
        auto scanner = region->createCommittedScanRemover(table_id);
        for (const auto & key : keys)
            scanner->remove(key);
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

RegionPartition::RegionPartition(Context & context_, const std::string & parent_path_, std::function<RegionPtr(RegionID)> region_fetcher)
    : parent_path(parent_path_),
      flush_thresholds
      {
          {FTH_BYTES_1, FTH_PERIOD_1},
          {FTH_BYTES_2, FTH_PERIOD_2},
          {FTH_BYTES_3, FTH_PERIOD_3},
          {FTH_BYTES_4, FTH_PERIOD_4},
      },
      context(context_),
      log(&Logger::get("RegionPartition"))
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
            {
                ++it;
            }
            region.cache_bytes += region_ptr->dataSize();

            // Update region_id -> table_id & partition_id
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

void RegionPartition::updateRegion(const RegionPtr & region, const TableIDSet & relative_table_ids)
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

void RegionPartition::applySnapshotRegion(const RegionPtr & region)
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

void RegionPartition::splitRegion(const RegionPtr & region, std::vector<RegionPtr> split_regions)
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
    auto & tmt_ctx = context.getTMTContext();
    for (auto table_id : region_info.tables)
    {
        auto storage = tmt_ctx.storages.get(table_id);
        if (storage == nullptr)
        {
            throw Exception("Table " + DB::toString(table_id) + " not found", ErrorCodes::UNKNOWN_TABLE);
        }

        auto & table = getOrCreateTable(table_id);

        for (const RegionPtr & split_region : split_regions)
        {
            const auto range = split_region->getRange();
            if (!TiKVRange::checkTableInvolveRange(table_id, range))
                continue;

            auto split_region_id = split_region->id();

            insertRegion(table, split_region_id);
        }
    }

    updateRegionRange(region);
}

void RegionPartition::removeRegion(const RegionPtr & region)
{
    std::unordered_set<TableID> tables;
    {
        auto region_id = region->id();

        std::lock_guard<std::mutex> lock(mutex);

        auto r_it = regions.find(region_id);
        if (r_it == regions.end())
        {
            LOG_WARNING(log, "Being removed region " << region_id << " does not exist.");
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

    auto & tmt_ctx = context.getTMTContext();
    for (auto table_id : tables)
    {
        auto storage = tmt_ctx.storages.get(table_id);
        if (storage == nullptr)
        {
            LOG_WARNING(log, "RegionPartition::removeRegion: " << table_id << " does not exist.");
            continue;
        }
        auto * merge_tree = dynamic_cast<StorageMergeTree *>(storage.get());
        auto [start_key, end_key] = region->getRange();
        auto [start_handle, end_handle] = getRegionRangeField(start_key, end_key, table_id);
        deleteRange(context, merge_tree, start_handle, end_handle);
    }
}

bool RegionPartition::tryFlushRegions()
{
    KVStore & kvstore = *context.getTMTContext().kvstore;
    std::set<std::pair<TableID, RegionID>> to_flush;
    {
        traverseRegions([&](TableID table_id, InternalRegion & region) {
            if (shouldFlush(region)) {
                to_flush.emplace(table_id, region.region_id);
                // Stop other flush threads.
                region.pause_flush = true;
            }
        });
    }

    for (auto [table_id, region_id] : to_flush)
    {
        flushRegion(table_id, region_id);
    }

    {
        // Now reset status infomations.
        Timepoint now = Clock::now();
        traverseRegions([&](TableID table_id, InternalRegion& region) {
            if (to_flush.count({table_id, region.region_id})) {
                region.pause_flush = false;
                region.must_flush = false;
                region.updated = false;
                region.cache_bytes = calculatePartitionCacheBytes(kvstore, region.region_id);
                region.last_flush_time = now;
            }
        });
    }

    return !to_flush.empty();
}

void RegionPartition::traverseRegions(std::function<void(TableID, InternalRegion&)> && callback)
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

void RegionPartition::traverseRegionsByTable(
    const TableID table_id, std::function<void(Regions)> && callback)
{
    auto & kvstore = context.getTMTContext().kvstore;
    Regions regions;
    {
        std::lock_guard<std::mutex> lock(mutex);
        auto & table = getOrCreateTable(table_id);

        for (const auto & region_info : table.regions.get())
        {
            auto region = kvstore->getRegion(region_info.second.region_id);
            if (!region)
                throw Exception("Region " + DB::toString(region_info.second.region_id) + " not found!", ErrorCodes::LOGICAL_ERROR);
            regions.push_back(region);
        }
    }
    callback(regions);
}

void RegionPartition::dumpRegionMap(RegionPartition::RegionMap & res)
{
    std::lock_guard<std::mutex> lock(mutex);
    res = regions;
}

void RegionPartition::dropRegionsInTable(TableID /*table_id*/)
{
    // TODO: impl
}

} // namespace DB
