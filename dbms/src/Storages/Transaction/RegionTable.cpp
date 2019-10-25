#include <Storages/MergeTree/TxnMergeTreeBlockOutputStream.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/Region.h>
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
        it = tables.emplace(table_id, Table(table_id)).first;
        LOG_INFO(log, "[getOrCreateTable] create new table " << table_id);
    }
    return it->second;
}

RegionTable::InternalRegion & RegionTable::insertRegion(Table & table, const Region & region)
{
    const auto range = region.getRange();
    return insertRegion(table, *range, region.id());
}

RegionTable::InternalRegion & RegionTable::insertRegion(Table & table, const RegionRangeKeys & region_range_keys, const RegionID region_id)
{
    auto & table_regions = table.regions;
    // Insert table mapping.
    auto [it, ok] = table_regions.emplace(region_id, InternalRegion(region_id, region_range_keys.getHandleRangeByTable(table.table_id)));
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

        const auto handle_range = range->getHandleRangeByTable(table_id);
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
    if (!region.cache_bytes)
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

void RegionTable::flushRegion(TableID table_id, RegionID region_id, const bool try_persist) const
{
    const auto & tmt = context->getTMTContext();

    /// Store region ptr first
    RegionPtr region = tmt.getKVStore()->getRegion(region_id);
    {
        if (!region)
        {
            LOG_WARNING(log, "[flushRegion] region " << region_id << " is not found");
            return;
        }

        LOG_DEBUG(log, "[flushRegion] table " << table_id << ", [region " << region_id << "] original " << region->dataSize() << " bytes");
    }

    /// Write region data into corresponding storage.
    RegionDataReadInfoList data_list_to_remove;
    {
        writeBlockByRegion(*context, table_id, region, data_list_to_remove, log);
    }

    /// Remove data in region.
    {
        {
            auto remover = region->createCommittedRemover(table_id);
            for (const auto & [handle, write_type, commit_ts, value] : data_list_to_remove)
            {
                std::ignore = write_type;
                std::ignore = value;

                remover.remove({handle, commit_ts});
            }
        }

        size_t cache_size = region->dataSize();

        if (cache_size == 0)
        {
            if (try_persist)
                tmt.getKVStore()->tryPersist(region_id);
            else
                region->incDirtyFlag();
        }

        LOG_DEBUG(log, "[flushRegion] table " << table_id << ", [region " << region_id << "] after flush " << cache_size << " bytes");
    }
}

static const Int64 FTH_BYTES_1 = 1;                // 1 B
static const Int64 FTH_BYTES_2 = 1024 * 1024;      // 1 MB
static const Int64 FTH_BYTES_3 = 1024 * 1024 * 10; // 10 MBs
static const Int64 FTH_BYTES_4 = 1024 * 1024 * 50; // 50 MBs

static const Seconds FTH_PERIOD_1(60 * 60); // 1 hour
static const Seconds FTH_PERIOD_2(60 * 5);  // 5 minutes
static const Seconds FTH_PERIOD_3(60);      // 1 minute
static const Seconds FTH_PERIOD_4(5);       // 5 seconds

RegionTable::RegionTable(Context & context_)
    : flush_thresholds(RegionTable::FlushThresholds::FlushThresholdsData{
          {FTH_BYTES_1, FTH_PERIOD_1}, {FTH_BYTES_2, FTH_PERIOD_2}, {FTH_BYTES_3, FTH_PERIOD_3}, {FTH_BYTES_4, FTH_PERIOD_4}}),
      context(&context_),
      log(&Logger::get("RegionTable"))
{}

void RegionTable::restore()
{
    LOG_INFO(log, "Start to restore");

    const auto & tmt = context->getTMTContext();

    // first get all table from storage.
    for (const auto & storage : tmt.getStorages().getAllStorage())
        getOrCreateTable(storage.first);

    LOG_INFO(log, "Get " << tables.size() << " tables from TMTStorages");

    // find region whose range may be overlapped with table.
    for (const auto & table : tables)
    {
        const auto table_id = table.first;

        const auto table_range = RegionRangeKeys::makeComparableKeys(RecordKVFormat::genKey(table_id, std::numeric_limits<HandleID>::min()),
            RecordKVFormat::genKey(table_id, std::numeric_limits<HandleID>::max()));

        tmt.getKVStore()->handleRegionsByRangeOverlap(table_range, [&](RegionMap region_map, const KVStoreTaskLock &) {
            for (const auto & region : region_map)
                doUpdateRegion(*region.second, table_id);
        });
    }

    // traverse regions and update because there may be table id not mapped in RegionTable.
    tmt.getKVStore()->traverseRegions([this](const RegionID, const RegionPtr & region) { applySnapshotRegion(*region); });

    // if table schema exists but there is no relative region, remove it;
    for (auto it = tables.begin(); it != tables.end();)
    {
        if (it->second.regions.empty())
            it = tables.erase(it);
        else
            it++;
    }

    LOG_INFO(log, "Restore " << tables.size() << " tables");
}

void RegionTable::removeTable(TableID table_id)
{
    std::lock_guard<std::mutex> lock(mutex);

    auto it = tables.find(table_id);
    if (it == tables.end())
        return;
    auto & table = it->second;

    // Remove from region list.
    for (const auto & region_info : table.regions)
    {
        regions[region_info.first].erase(table.table_id);
    }

    // Remove from table map.
    tables.erase(it);

    LOG_INFO(log, "[removeTable] remove table " << table_id << " in RegionTable success");
}

void RegionTable::updateRegion(const Region & region, const TableIDSet & relative_table_ids)
{
    std::lock_guard<std::mutex> lock(mutex);

    for (const auto table_id : relative_table_ids)
        doUpdateRegion(region, table_id);
}

void RegionTable::doUpdateRegion(const Region & region, TableID table_id)
{
    auto & internal_region = getOrInsertRegion(table_id, region);
    internal_region.cache_bytes = region.dataSize();
    if (internal_region.cache_bytes)
        dirty_regions.insert(internal_region.region_id);
}

void RegionTable::applySnapshotRegion(const Region & region)
{
    // make operation about snapshot can only add mapping relations rather than delete.
    auto table_ids = region.getAllWriteCFTables();
    updateRegion(region, table_ids);
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

        doUpdateRegion(split_region, table_id);
    }
}

TableID RegionTable::popOneTableToOptimize()
{
    TableID res = InvalidTableID;
    std::lock_guard<std::mutex> lock(mutex);
    if (auto it = table_to_optimize.begin(); it != table_to_optimize.end())
    {
        res = *it;
        table_to_optimize.erase(it);
    }
    return res;
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
            if (table.regions.empty())
                table_to_optimize.emplace(table_id);
        }
    }
}

void RegionTable::tryFlushRegion(RegionID region_id)
{
    TableIDSet table_ids;
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
            table_ids = it->second;
        }
        else
        {
            LOG_DEBUG(log, "[tryFlushRegion] region " << region_id << " fail, internal region not exist");
            return;
        }
    }

    for (const auto table_id : table_ids)
        tryFlushRegion(region_id, table_id, false);
}

void RegionTable::tryFlushRegion(RegionID region_id, TableID table_id, const bool try_persist)
{
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

    bool status = func_update_region([&](InternalRegion & region) -> bool {
        if (region.pause_flush)
        {
            LOG_INFO(log, "[tryFlushRegion] internal region " << region_id << " pause flush, may be being flushed");
            return false;
        }
        region.pause_flush = true;
        return true;
    });

    if (!status)
        return;

    std::exception_ptr first_exception;

    try
    {
        flushRegion(table_id, region_id, try_persist);
    }
    catch (...)
    {
        first_exception = std::current_exception();
    }

    func_update_region([&](InternalRegion & internal_region) -> bool {
        internal_region.pause_flush = false;
        size_t cache_bytes = 0;
        if (auto region = context->getTMTContext().getKVStore()->getRegion(internal_region.region_id); region)
            cache_bytes = region->dataSize();

        internal_region.cache_bytes = cache_bytes;
        if (cache_bytes)
            dirty_regions.insert(region_id);
        else
            dirty_regions.erase(region_id);

        internal_region.last_flush_time = Clock::now();
        return true;
    });

    if (first_exception)
        std::rethrow_exception(first_exception);
}

bool RegionTable::tryFlushRegions()
{
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (dirty_regions.empty())
            return false;
    }

    struct DataToFlush
    {
        TableID table_id;
        RegionID region_id;
        bool got = false;
    };
    DataToFlush to_flush;
    { // judge choose region to flush
        const auto traverse = [this](std::function<bool(TableID, InternalRegion &)> && callback) {
            std::lock_guard<std::mutex> lock(mutex);

            for (const auto & region_id : dirty_regions)
            {
                if (auto it = regions.find(region_id); it != regions.end())
                {
                    for (auto & table_id : it->second)
                    {
                        if (callback(table_id, tables.find(table_id)->second.regions.find(region_id)->second))
                            return;
                    }
                }
            }
        };

        traverse([&](TableID table_id, InternalRegion & region) {
            if (shouldFlush(region))
            {
                to_flush = DataToFlush{table_id, region.region_id, true};
                dirty_regions.erase(region.region_id);
                return true;
            }
            return false;
        });

        if (!to_flush.got)
            return false;
    }

    tryFlushRegion(to_flush.region_id, to_flush.table_id, true);

    return true;
}

void RegionTable::handleInternalRegionsByTable(const TableID table_id, std::function<void(const InternalRegions &)> && callback) const
{
    std::lock_guard<std::mutex> lock(mutex);

    if (auto it = tables.find(table_id); it != tables.end())
        callback(it->second.regions);
}

std::vector<std::pair<RegionID, RegionPtr>> RegionTable::getRegionsByTable(const TableID table_id) const
{
    auto & kvstore = context->getTMTContext().getKVStore();
    std::vector<std::pair<RegionID, RegionPtr>> regions;
    {
        std::lock_guard<std::mutex> lock(mutex);

        if (auto it = tables.find(table_id); it != tables.end())
        {
            auto & table = it->second;
            for (const auto & region_info : table.regions)
            {
                auto region = kvstore->getRegion(region_info.second.region_id);
                regions.emplace_back(region_info.second.region_id, region);
            }
        }
    }
    return regions;
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

void RegionTable::dumpRegionsByTable(const TableID table_id, size_t & count, InternalRegions * regions) const
{
    std::lock_guard<std::mutex> lock(mutex);
    if (auto it = tables.find(table_id); it != tables.end())
    {
        count = it->second.regions.size();
        if (regions)
            *regions = it->second.regions;
    }
}

} // namespace DB
