#include <Common/setThreadName.h>
#include <Storages/IManageableStorage.h>
#include <Storages/MergeTree/TxnMergeTreeBlockOutputStream.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageDeltaMergeHelpers.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionManager.h>
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
extern const int ILLFORMAT_RAFT_ROW;
extern const int TABLE_IS_DROPPED;
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
    // todo check if region_range_keys.mapped_table_id == table.table_id ??
    auto [it, ok] = table_regions.emplace(region_id, InternalRegion(region_id, region_range_keys.rawKeys()));
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
    internal_region.range_in_table = region.getRange()->rawKeys();
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

RegionDataReadInfoList RegionTable::flushRegion(const RegionPtrWithBlock & region, bool try_persist) const
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

    {
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
        dirty_regions.insert(internal_region.region_id);
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

namespace
{
/// Remove obsolete data for table after data of `handle_range` is removed from this TiFlash node.
/// Note that this function will try to acquire lock by `IStorage->lockForShare`
void removeObsoleteDataInStorage(
    Context * const context, const TableID table_id, const std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr> & handle_range)
{
    TMTContext & tmt = context->getTMTContext();
    auto storage = tmt.getStorages().get(table_id);
    // For DT only now
    if (!storage || storage->engineType() != TiDB::StorageEngine::DT)
        return;

    try
    {
        // Acquire a `drop_lock` so that no other threads can drop the `storage`
        auto storage_lock = storage->lockForShare(getThreadName());

        auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
        if (dm_storage == nullptr)
            return;

        /// Now we assume that these won't block for long time.
        auto rowkey_range
            = DM::RowKeyRange::fromRegionRange(handle_range, table_id, table_id, storage->isCommonHandle(), storage->getRowKeyColumnSize());
        dm_storage->deleteRange(rowkey_range, context->getSettingsRef());
        dm_storage->flushCache(*context, rowkey_range); // flush to disk
    }
    catch (DB::Exception & e)
    {
        // We can ignore if the storage is already dropped.
        if (e.code() != ErrorCodes::TABLE_IS_DROPPED)
            throw;
    }
}
} // namespace

void RegionTable::removeRegion(const RegionID region_id, bool remove_data, const RegionTaskLock &)
{
    TableID table_id = 0;
    std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr> handle_range;

    {
        /// We need to protect `regions` and `table` under mutex lock
        std::lock_guard<std::mutex> lock(mutex);

        auto it = regions.find(region_id);
        if (it == regions.end())
        {
            LOG_WARNING(log, __FUNCTION__ << ": region " << region_id << " does not exist.");
            return;
        }

        table_id = it->second;
        auto & table = tables.find(table_id)->second;
        auto internal_region_it = table.regions.find(region_id);
        handle_range = internal_region_it->second.range_in_table;

        regions.erase(it);
        table.regions.erase(internal_region_it);
        if (table.regions.empty())
        {
            /// All regions of this table is removed, the storage maybe drop or pd
            /// move it to another node, we can optimize outdated data.
            table_to_optimize.insert(table_id);
            tables.erase(table_id);
        }
        LOG_INFO(log, __FUNCTION__ << ": remove [region " << region_id << "] in RegionTable done");
    }

    // Sometime we don't need to remove data. e.g. remove region after region merge.
    if (remove_data)
    {
        // Try to remove obsolete data in storage

        // Note that we should do this without lock on RegionTable.
        // But caller(KVStore) should ensure that no new data write into this handle_range
        // before `removeObsoleteDataInStorage` is done. (by param `RegionTaskLock`)
        // And this is expected not to block for long time.
        removeObsoleteDataInStorage(context, table_id, handle_range);
        LOG_INFO(log, __FUNCTION__ << ": remove region [" << region_id << "] in storage done");
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

RegionDataReadInfoList RegionTable::tryFlushRegion(const RegionPtrWithBlock & region, bool try_persist)
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
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::ILLFORMAT_RAFT_ROW)
        {
            // br or lighting may write illegal data into tikv, skip flush.
            LOG_WARNING(&Logger::get(__PRETTY_FUNCTION__),
                "Got error while reading region committed cache: " << e.displayText() << ". Skip flush region and keep original cache.");
        }
        else
            first_exception = std::current_exception();
    }
    catch (...)
    {
        first_exception = std::current_exception();
    }

    func_update_region([&](InternalRegion & internal_region) -> bool {
        internal_region.pause_flush = false;
        internal_region.cache_bytes = region->dataSize();
        if (internal_region.cache_bytes)
            dirty_regions.insert(region_id);
        else
            dirty_regions.erase(region_id);

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
            dirty_it = dirty_regions.erase(dirty_it);
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
    auto new_handle_range = region_range_keys.rawKeys();

    if (auto it = regions.find(region_id); it != regions.end())
    {
        if (table_id != it->second)
            throw Exception(std::string(__PRETTY_FUNCTION__) + ": table id " + std::to_string(table_id) + " not match previous one "
                    + std::to_string(it->second) + " in regions " + std::to_string(region_id),
                ErrorCodes::LOGICAL_ERROR);

        InternalRegion & internal_region = doGetInternalRegion(table_id, region_id);
        if (*(internal_region.range_in_table.first) <= *(new_handle_range.first)
            && *(internal_region.range_in_table.second) >= *(new_handle_range.second))
        {
            LOG_INFO(log, __FUNCTION__ << ": table " << table_id << ", internal region " << region_id << " has larger range");
        }
        else
        {
            internal_region.range_in_table.first = *new_handle_range.first < *internal_region.range_in_table.first
                ? new_handle_range.first
                : internal_region.range_in_table.first;
            internal_region.range_in_table.second = *new_handle_range.second > *internal_region.range_in_table.second
                ? new_handle_range.second
                : internal_region.range_in_table.second;
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
