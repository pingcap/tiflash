#include <Storages/IManageableStorage.h>
#include <Storages/MergeTree/TxnMergeTreeBlockOutputStream.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageDeltaMergeHelpers.h>
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
        it = tables.emplace(table_id, table_id).first;
        LOG_INFO(log, __FUNCTION__ << ": get new table " << table_id);
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
            std::string(__PRETTY_FUNCTION__) + ": insert duplicate internal region " + DB::toString(region_id), ErrorCodes::LOGICAL_ERROR);

    // Insert region mapping.
    regions[region_id] = table.table_id;

    return it->second;
}

RegionTable::InternalRegion & RegionTable::doGetInternalRegion(DB::TableID table_id, DB::RegionID region_id)
{
    return tables.find(table_id)->second.regions.find(region_id)->second;
}

RegionTable::InternalRegion & RegionTable::getOrInsertRegion(const Region & region)
{
    auto table_id = region.getMappedTableID();
    auto & table = getOrCreateTable(table_id);
    auto & table_regions = table.regions;
    if (auto it = table_regions.find(region.id()); it != table_regions.end())
        return it->second;

    return insertRegion(table, region);
}

void RegionTable::shrinkRegionRange(const Region & region)
{
    std::lock_guard<std::mutex> lock(mutex);
    auto & internal_region = getOrInsertRegion(region);
    internal_region.range_in_table = region.getHandleRangeByTable(region.getMappedTableID());
    internal_region.cache_bytes = region.dataSize();
    if (internal_region.cache_bytes)
        dirty_regions.insert(internal_region.region_id);
}

bool RegionTable::shouldFlush(const InternalRegion & region) const
{
    if (region.pause_flush)
        return false;
    if (!region.cache_bytes)
        return false;
    auto period_time = Clock::now() - region.last_flush_time;
    for (const auto & [th_bytes, th_duration] : *flush_thresholds.getData())
    {
        if (region.cache_bytes >= th_bytes && period_time >= th_duration)
        {
            LOG_INFO(log,
                __FUNCTION__ << ": region " << region.region_id << ", cache size " << region.cache_bytes << ", seconds since last "
                             << std::chrono::duration_cast<std::chrono::seconds>(period_time).count());

            return true;
        }
    }
    return false;
}

RegionDataReadInfoList RegionTable::flushRegion(const RegionPtr & region, bool try_persist) const
{
    auto & tmt = context->getTMTContext();

    if (tmt.isBgFlushDisabled())
    {
        LOG_TRACE(log,
            __FUNCTION__ << ": table " << region->getMappedTableID() << ", " << region->toString(false) << " original "
                         << region->dataSize() << " bytes");
    }
    else
    {
        LOG_INFO(log,
            __FUNCTION__ << ": table " << region->getMappedTableID() << ", " << region->toString(false) << " original "
                         << region->dataSize() << " bytes");
    }

    /// Write region data into corresponding storage.
    RegionDataReadInfoList data_list_to_remove;
    {
        writeBlockByRegion(*context, region, data_list_to_remove, log);
    }

    /// Remove data in region.
    {
        {
            auto remover = region->createCommittedRemover();
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
            {
                KVStore::tryFlushRegionCacheInStorage(tmt, *region, log);
                tmt.getKVStore()->tryPersist(region->id());
            }
        }

        if (tmt.isBgFlushDisabled())
        {
            LOG_TRACE(log,
                __FUNCTION__ << ": table " << region->getMappedTableID() << ", " << region->toString(false) << " after flush " << cache_size
                             << " bytes");
        }
        else
        {
            LOG_INFO(log,
                __FUNCTION__ << ": table " << region->getMappedTableID() << ", " << region->toString(false) << " after flush " << cache_size
                             << " bytes");
        }
    }

    return data_list_to_remove;
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

    tmt.getKVStore()->traverseRegions([this](const RegionID, const RegionPtr & region) { updateRegion(*region); });

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
        regions.erase(region_info.first);

    // Remove from table map.
    tables.erase(it);

    LOG_INFO(log, __FUNCTION__ << ": remove table " << table_id << " in RegionTable success");
}

void RegionTable::updateRegion(const Region & region)
{
    std::lock_guard<std::mutex> lock(mutex);
    auto & internal_region = getOrInsertRegion(region);
    internal_region.cache_bytes = region.dataSize();
    if (internal_region.cache_bytes)
        incrDirtyFlag(internal_region.region_id);
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
    std::lock_guard<std::mutex> lock(mutex);

    if (auto it = regions.find(region_id); it == regions.end())
    {
        LOG_WARNING(log, __FUNCTION__ << ": region " << region_id << " does not exist.");
        return;
    }
    else
    {
        TableID table_id = it->second;
        auto & table = tables.find(table_id)->second;
        regions.erase(it);
        table.regions.erase(region_id);
        if (table.regions.empty())
        {
            /// All regions of this table is removed, the storage maybe drop or pd
            /// move it to another node.
            table_to_optimize.insert(table_id);
            tables.erase(table_id);
        }
        else
        {
            /// Some region of this table is removed, if it is a DeltaTree, write deleteRange.

            /// Now we assume that StorageDeltaMerge::deleteRange do not block for long time and do it in sync mode.
            /// If this block for long time, consider to do this in background threads.
            TMTContext & tmt = context->getTMTContext();
            auto storage = tmt.getStorages().get(table_id);
            if (storage && storage->engineType() == TiDB::StorageEngine::DT)
            {
                // acquire lock so that no other threads can change storage's structure
                auto storage_lock = storage->lockStructure(true, __PRETTY_FUNCTION__);
                // Check if it dropped by other thread
                if (storage->is_dropped)
                    return;

                auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
                auto region_it = table.regions.find(region_id);
                if (region_it == table.regions.end())
                    return;
                HandleRange<HandleID> handle_range = region_it->second.range_in_table;

                auto dm_handle_range = toDMHandleRange(handle_range);
                dm_storage->deleteRange(dm_handle_range, context->getSettingsRef());
                dm_storage->flushCache(*context, dm_handle_range);
            }
        }
    }
}

RegionDataReadInfoList RegionTable::tryFlushRegion(RegionID region_id, bool try_persist)
{
    auto region = context->getTMTContext().getKVStore()->getRegion(region_id);
    if (!region)
    {
        LOG_WARNING(log, __FUNCTION__ << ": region " << region_id << " not found");
        return {};
    }

    return tryFlushRegion(region, try_persist);
}

RegionDataReadInfoList RegionTable::tryFlushRegion(const RegionPtr & region, bool try_persist)
{
    RegionID region_id = region->id();

    const auto func_update_region = [&](std::function<bool(InternalRegion &)> && callback) -> bool {
        std::lock_guard<std::mutex> lock(mutex);
        if (auto it = regions.find(region_id); it != regions.end())
        {
            auto & internal_region = doGetInternalRegion(it->second, region_id);
            return callback(internal_region);
        }
        else
        {
            LOG_WARNING(log, "Internal region " << region_id << " might be removed");
            return false;
        }
    };

    bool status = func_update_region([&](InternalRegion & internal_region) -> bool {
        if (internal_region.pause_flush)
        {
            LOG_INFO(log, "Internal region " << region_id << " pause flush, may be being flushed");
            return false;
        }
        internal_region.pause_flush = true;
        return true;
    });

    if (!status)
        return {};

    std::exception_ptr first_exception;
    RegionDataReadInfoList data_list_to_remove;
    try
    {
        data_list_to_remove = flushRegion(region, try_persist);
    }
    catch (...)
    {
        first_exception = std::current_exception();
    }

    func_update_region([&](InternalRegion & internal_region) -> bool {
        internal_region.pause_flush = false;
        internal_region.cache_bytes = region->dataSize();
        if (internal_region.cache_bytes)
            incrDirtyFlag(region_id);
        else
            clearDirtyFlag(region_id);

        internal_region.last_flush_time = Clock::now();
        return true;
    });

    if (first_exception)
        std::rethrow_exception(first_exception);

    return data_list_to_remove;
}

RegionID RegionTable::pickRegionToFlush()
{
    std::lock_guard<std::mutex> lock(mutex);
    std::lock_guard<std::mutex> dirty_regions_lock(dirty_regions_mutex);

    for (auto dirty_it = dirty_regions.begin(); dirty_it != dirty_regions.end();)
    {
        auto region_id = *dirty_it;
        if (auto it = regions.find(region_id); it != regions.end())
        {
            auto table_id = it->second;
            if (shouldFlush(doGetInternalRegion(table_id, region_id)))
            {
                // The dirty flag should only be removed after data is flush successfully.
                return region_id;
            }

            dirty_it++;
        }
        else
        {
            // Region{region_id} is removed, remove its dirty flag
            dirty_it = clearDirtyFlag(dirty_it, dirty_regions_lock);
        }
    }
    return InvalidRegionID;
}

bool RegionTable::tryFlushRegions()
{
    if (RegionID region_to_flush = pickRegionToFlush(); region_to_flush != InvalidRegionID)
    {
        tryFlushRegion(region_to_flush, true);
        return true;
    }

    return false;
}

void RegionTable::incrDirtyFlag(RegionID region_id)
{
    std::lock_guard lock(dirty_regions_mutex);
    dirty_regions.insert(region_id);
}

void RegionTable::clearDirtyFlag(RegionID region_id)
{
    std::lock_guard lock(dirty_regions_mutex);
    if (auto iter = dirty_regions.find(region_id); iter != dirty_regions.end())
        clearDirtyFlag(iter, lock);
}

RegionTable::DirtyRegions::iterator RegionTable::clearDirtyFlag(
    const RegionTable::DirtyRegions::iterator & region_iter, std::lock_guard<std::mutex> &)
{
    // Removing invalid iterator will cause segment fault.
    if (unlikely(region_iter == dirty_regions.end()))
        return region_iter;

    auto next_iter = dirty_regions.erase(region_iter);
    dirty_regions_cv.notify_all();
    return next_iter;
}

void RegionTable::waitTillRegionFlushed(const RegionID region_id)
{
    std::unique_lock lock(dirty_regions_mutex);
    dirty_regions_cv.wait(lock, [this, region_id] { return dirty_regions.count(region_id) == 0; });
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
    handleInternalRegionsByTable(table_id, [&](const InternalRegions & internal_regions) {
        for (const auto & region_info : internal_regions)
        {
            auto region = kvstore->getRegion(region_info.first);
            if (region)
                regions.emplace_back(region_info.first, std::move(region));
        }
    });
    return regions;
}

void RegionTable::setFlushThresholds(const FlushThresholds::FlushThresholdsData & flush_thresholds_)
{
    flush_thresholds.setFlushThresholds(flush_thresholds_);
}

void RegionTable::extendRegionRange(const RegionID region_id, const RegionRangeKeys & region_range_keys)
{
    std::lock_guard<std::mutex> lock(mutex);

    auto table_id = region_range_keys.getMappedTableID();
    auto new_handle_range = region_range_keys.getHandleRangeByTable(table_id);

    if (auto it = regions.find(region_id); it != regions.end())
    {
        if (table_id != it->second)
            throw Exception(std::string(__PRETTY_FUNCTION__) + ": table id " + std::to_string(table_id) + " not match previous one "
                    + std::to_string(it->second) + " in regions " + std::to_string(region_id),
                ErrorCodes::LOGICAL_ERROR);

        InternalRegion & internal_region = doGetInternalRegion(table_id, region_id);
        if (internal_region.range_in_table.first <= new_handle_range.first
            && internal_region.range_in_table.second >= new_handle_range.second)
        {
            LOG_INFO(log, __FUNCTION__ << ": table " << table_id << ", internal region " << region_id << " has larger range");
        }
        else
        {
            const auto ori_range = internal_region.range_in_table;
            internal_region.range_in_table.first = std::min(new_handle_range.first, internal_region.range_in_table.first);
            internal_region.range_in_table.second = std::max(new_handle_range.second, internal_region.range_in_table.second);

            LOG_INFO(log,
                __FUNCTION__ << ": table " << table_id << ", internal region " << region_id << " extend range from ["
                             << ori_range.first.toString() << "," << ori_range.second.toString() << ") to ["
                             << internal_region.range_in_table.first.toString() << "," << internal_region.range_in_table.second.toString()
                             << ")");
        }
    }
    else
    {
        auto & table = getOrCreateTable(table_id);
        insertRegion(table, region_range_keys, region_id);
        LOG_INFO(log, __FUNCTION__ << ": table " << table_id << " insert internal region " << region_id);
    }
}

} // namespace DB
