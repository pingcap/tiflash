// Copyright 2022 PingCAP, Ltd.
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

#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/ExternalDTFileInfo.h>
#include <Storages/IManageableStorage.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageDeltaMergeHelpers.h>
#include <Storages/Transaction/CheckpointInfo.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionManager.h>
#include <Storages/Transaction/RegionTable.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiKVRange.h>
#include <TiDB/Schema/SchemaSyncer.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_TABLE;
extern const int ILLFORMAT_RAFT_ROW;
extern const int TABLE_IS_DROPPED;
} // namespace ErrorCodes

RegionTable::Table & RegionTable::getOrCreateTable(const KeyspaceID keyspace_id, const TableID table_id)
{
    auto ks_tb_id = KeyspaceTableID{keyspace_id, table_id};
    auto it = tables.find(ks_tb_id);
    if (it == tables.end())
    {
        // Load persisted info.
        it = tables.emplace(ks_tb_id, table_id).first;
        LOG_INFO(log, "get new table {} of keyspace {}", table_id, keyspace_id);
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
    auto keyspace_id = region_range_keys.getKeyspaceID();
    auto & table_regions = table.regions;
    // Insert table mapping.
    // todo check if region_range_keys.mapped_table_id == table.table_id ??
    auto [it, ok] = table_regions.emplace(region_id, InternalRegion(region_id, region_range_keys.rawKeys()));
    if (!ok)
        throw Exception(
            std::string(__PRETTY_FUNCTION__) + ": insert duplicate internal region " + DB::toString(region_id),
            ErrorCodes::LOGICAL_ERROR);

    // Insert region mapping.
    regions[region_id] = KeyspaceTableID{keyspace_id, table.table_id};

    return it->second;
}

RegionTable::InternalRegion & RegionTable::doGetInternalRegion(KeyspaceTableID ks_tb_id, DB::RegionID region_id)
{
    return tables.find(ks_tb_id)->second.regions.find(region_id)->second;
}

RegionTable::InternalRegion & RegionTable::getOrInsertRegion(const Region & region)
{
    auto keyspace_id = region.getKeyspaceID();
    auto table_id = region.getMappedTableID();
    auto & table = getOrCreateTable(keyspace_id, table_id);
    auto & table_regions = table.regions;
    if (auto it = table_regions.find(region.id()); it != table_regions.end())
        return it->second;

    return insertRegion(table, region);
}

void RegionTable::shrinkRegionRange(const Region & region)
{
    std::lock_guard lock(mutex);
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
            LOG_INFO(log, "region {}, cache size {}, seconds since last {}", region.region_id, region.cache_bytes, std::chrono::duration_cast<std::chrono::seconds>(period_time).count());
            return true;
        }
    }
    return false;
}

RegionDataReadInfoList RegionTable::writeBlockByRegionAndFlush(const RegionPtrWithBlock & region, bool try_persist) const
{
    auto & tmt = context->getTMTContext();

    LOG_TRACE(log, "table {}, {} original {} bytes", region->getMappedTableID(), region->toString(false), region->dataSize());

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
                tmt.getKVStore()->tryPersistRegion(region->id());
            }
        }

        LOG_TRACE(log, "table {}, {} after flush {} bytes", region->getMappedTableID(), region->toString(false), cache_size);
    }

    return data_list_to_remove;
}

static const Int64 FTH_BYTES_1 = 1; // 1 B
static const Int64 FTH_BYTES_2 = 1024 * 1024; // 1 MB
static const Int64 FTH_BYTES_3 = 1024 * 1024 * 10; // 10 MBs
static const Int64 FTH_BYTES_4 = 1024 * 1024 * 50; // 50 MBs

static const Seconds FTH_PERIOD_1(60 * 60); // 1 hour
static const Seconds FTH_PERIOD_2(60 * 5); // 5 minutes
static const Seconds FTH_PERIOD_3(60); // 1 minute
static const Seconds FTH_PERIOD_4(5); // 5 seconds

RegionTable::RegionTable(Context & context_)
    : flush_thresholds(RegionTable::FlushThresholds::FlushThresholdsData{
        {FTH_BYTES_1, FTH_PERIOD_1},
        {FTH_BYTES_2, FTH_PERIOD_2},
        {FTH_BYTES_3, FTH_PERIOD_3},
        {FTH_BYTES_4, FTH_PERIOD_4}})
    , context(&context_)
    , log(Logger::get())
{}

void RegionTable::restore()
{
    LOG_INFO(log, "Start to restore");

    const auto & tmt = context->getTMTContext();

    tmt.getKVStore()->traverseRegions([this](const RegionID, const RegionPtr & region) { updateRegion(*region); });

    LOG_INFO(log, "Restore {} tables", tables.size());
}

void RegionTable::removeTable(KeyspaceID keyspace_id, TableID table_id)
{
    std::lock_guard lock(mutex);

    auto it = tables.find(KeyspaceTableID{keyspace_id, table_id});
    if (it == tables.end())
        return;
    auto & table = it->second;

    // Remove from region list.
    for (const auto & region_info : table.regions)
    {
        regions.erase(region_info.first);
        {
            std::unique_lock write_lock(rw_lock);
            safe_ts_map.erase(region_info.first);
        }
    }

    // Remove from table map.
    tables.erase(it);

    LOG_INFO(log, "remove table {} in RegionTable success", table_id);
}

void RegionTable::updateRegion(const Region & region)
{
    std::lock_guard lock(mutex);
    auto & internal_region = getOrInsertRegion(region);
    internal_region.cache_bytes = region.dataSize();
    if (internal_region.cache_bytes)
        dirty_regions.insert(internal_region.region_id);
}

namespace
{
/// Remove obsolete data for table after data of `handle_range` is removed from this TiFlash node.
/// Note that this function will try to acquire lock by `IStorage->lockForShare`
void removeObsoleteDataInStorage(
    Context * const context,
    const KeyspaceTableID ks_table_id,
    const std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr> & handle_range)
{
    TMTContext & tmt = context->getTMTContext();
    auto storage = tmt.getStorages().get(ks_table_id.first, ks_table_id.second);
    // For DT only now
    if (!storage || storage->engineType() != TiDB::StorageEngine::DT)
        return;

    try
    {
        // Acquire a `drop_lock` so that no other threads can drop the `storage`
        auto storage_lock = storage->lockForShare(getThreadNameAndID());

        auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
        if (dm_storage == nullptr)
            return;

        /// Now we assume that these won't block for long time.
        auto rowkey_range
            = DM::RowKeyRange::fromRegionRange(handle_range, ks_table_id.second, ks_table_id.second, storage->isCommonHandle(), storage->getRowKeyColumnSize());
        dm_storage->deleteRange(rowkey_range, context->getSettingsRef());
        dm_storage->flushCache(*context, rowkey_range, /*try_until_succeed*/ true); // flush to disk
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
    KeyspaceTableID ks_tb_id;
    std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr> handle_range;

    {
        /// We need to protect `regions` and `table` under mutex lock
        std::lock_guard lock(mutex);

        auto it = regions.find(region_id);
        if (it == regions.end())
        {
            LOG_WARNING(log, "region {} does not exist.", region_id);
            return;
        }

        ks_tb_id = it->second;
        auto & table = tables.find(ks_tb_id)->second;
        auto internal_region_it = table.regions.find(region_id);
        handle_range = internal_region_it->second.range_in_table;

        regions.erase(it);
        {
            std::unique_lock write_lock(rw_lock);
            safe_ts_map.erase(region_id);
        }
        table.regions.erase(internal_region_it);
        if (table.regions.empty())
        {
            tables.erase(ks_tb_id);
        }
        LOG_INFO(log, "remove [region {}] in RegionTable done", region_id);
    }

    // Sometime we don't need to remove data. e.g. remove region after region merge.
    if (remove_data)
    {
        // Try to remove obsolete data in storage

        // Note that we should do this without lock on RegionTable.
        // But caller(KVStore) should ensure that no new data write into this handle_range
        // before `removeObsoleteDataInStorage` is done. (by param `RegionTaskLock`)
        // And this is expected not to block for long time.
        removeObsoleteDataInStorage(context, ks_tb_id, handle_range);
        LOG_INFO(log, "remove region [{}] in storage done", region_id);
    }
}

RegionDataReadInfoList RegionTable::tryWriteBlockByRegionAndFlush(RegionID region_id, bool try_persist)
{
    auto region = context->getTMTContext().getKVStore()->getRegion(region_id);
    if (!region)
    {
        LOG_WARNING(log, "region {} not found", region_id);
        return {};
    }

    return tryWriteBlockByRegionAndFlush(region, try_persist);
}

RegionDataReadInfoList RegionTable::tryWriteBlockByRegionAndFlush(const RegionPtrWithBlock & region, bool try_persist)
{
    RegionID region_id = region->id();

    const auto func_update_region = [&](std::function<bool(InternalRegion &)> && callback) -> bool {
        std::lock_guard lock(mutex);
        if (auto it = regions.find(region_id); it != regions.end())
        {
            auto & internal_region = doGetInternalRegion(it->second, region_id);
            return callback(internal_region);
        }
        else
        {
            LOG_WARNING(log, "Internal region {} might be removed", region_id);
            return false;
        }
    };

    bool status = func_update_region([&](InternalRegion & internal_region) -> bool {
        if (internal_region.pause_flush)
        {
            LOG_INFO(log, "Internal region {} pause flush, may be being flushed", region_id);
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
        data_list_to_remove = writeBlockByRegionAndFlush(region, try_persist);
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::ILLFORMAT_RAFT_ROW)
        {
            // br or lighting may write illegal data into tikv, skip flush.
            LOG_WARNING(Logger::get(), "Got error while reading region committed cache: {}. Skip flush region and keep original cache.", e.displayText());
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
    std::lock_guard lock(mutex);

    for (auto dirty_it = dirty_regions.begin(); dirty_it != dirty_regions.end();)
    {
        auto region_id = *dirty_it;
        if (auto it = regions.find(region_id); it != regions.end())
        {
            if (shouldFlush(doGetInternalRegion(it->second, region_id)))
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
        tryWriteBlockByRegionAndFlush(region_to_flush, true);
        return true;
    }

    return false;
}

void RegionTable::handleInternalRegionsByTable(const KeyspaceID keyspace_id, const TableID table_id, std::function<void(const InternalRegions &)> && callback) const
{
    std::lock_guard lock(mutex);

    if (auto it = tables.find(KeyspaceTableID{keyspace_id, table_id}); it != tables.end())
    {
        callback(it->second.regions);
    }
}

std::vector<std::pair<RegionID, RegionPtr>> RegionTable::getRegionsByTable(const KeyspaceID keyspace_id, const TableID table_id) const
{
    auto & kvstore = context->getTMTContext().getKVStore();
    std::vector<std::pair<RegionID, RegionPtr>> regions;
    handleInternalRegionsByTable(keyspace_id, table_id, [&](const InternalRegions & internal_regions) {
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
    std::lock_guard lock(mutex);

    auto keyspace_id = region_range_keys.getKeyspaceID();
    auto table_id = region_range_keys.getMappedTableID();
    auto ks_tbl_id = KeyspaceTableID{keyspace_id, table_id};
    auto new_handle_range = region_range_keys.rawKeys();

    if (auto it = regions.find(region_id); it != regions.end())
    {
        RUNTIME_CHECK_MSG(
            ks_tbl_id == it->second,
            "{}: table id not match the previous one"
            ", region_id={} keyspace={} table_id={}, old_keyspace={} old_table_id={}",
            __PRETTY_FUNCTION__,
            region_id,
            keyspace_id,
            table_id,
            it->second.first,
            it->second.second);

        InternalRegion & internal_region = doGetInternalRegion(ks_tbl_id, region_id);
        if (*(internal_region.range_in_table.first) <= *(new_handle_range.first)
            && *(internal_region.range_in_table.second) >= *(new_handle_range.second))
        {
            LOG_INFO(log, "table {}, internal region {} has larger range", table_id, region_id);
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
        auto & table = getOrCreateTable(keyspace_id, table_id);
        insertRegion(table, region_range_keys, region_id);
        LOG_INFO(log, "table {} insert internal region {}", table_id, region_id);
    }
}

RegionPtrWithSnapshotFiles::RegionPtrWithSnapshotFiles(
    const Base & base_,
    std::vector<DM::ExternalDTFileInfo> && external_files_)
    : base(base_)
    , external_files(std::move(external_files_))
{}

RegionPtrWithCheckpointInfo::RegionPtrWithCheckpointInfo(const Base & base_, CheckpointInfoPtr checkpoint_info_)
    : base(base_)
    , checkpoint_info(std::move(checkpoint_info_))
{}

bool RegionTable::isSafeTSLag(UInt64 region_id, UInt64 * leader_safe_ts, UInt64 * self_safe_ts)
{
    {
        std::shared_lock lock(rw_lock);
        auto it = safe_ts_map.find(region_id);
        if (it == safe_ts_map.end())
        {
            return false;
        }
        *leader_safe_ts = it->second->leader_safe_ts.load(std::memory_order_relaxed);
        *self_safe_ts = it->second->self_safe_ts.load(std::memory_order_relaxed);
    }
    LOG_TRACE(log, "region_id={}, table_id={}, leader_safe_ts={}, self_safe_ts={}", region_id, regions[region_id], *leader_safe_ts, *self_safe_ts);
    return (*leader_safe_ts > *self_safe_ts) && ((*leader_safe_ts >> TsoPhysicalShiftBits) - (*self_safe_ts >> TsoPhysicalShiftBits) > SafeTsDiffThreshold);
}

UInt64 RegionTable::getSelfSafeTS(UInt64 region_id)
{
    std::shared_lock lock(rw_lock);
    auto it = safe_ts_map.find(region_id);
    if (it == safe_ts_map.end())
    {
        return 0;
    }
    return it->second->self_safe_ts.load(std::memory_order_relaxed);
}

void RegionTable::updateSafeTS(UInt64 region_id, UInt64 leader_safe_ts, UInt64 self_safe_ts)
{
    {
        std::shared_lock lock(rw_lock);
        auto it = safe_ts_map.find(region_id);
        if (it == safe_ts_map.end() && (leader_safe_ts == InvalidSafeTS || self_safe_ts == InvalidSafeTS))
        {
            LOG_TRACE(log, "safe_ts_map empty but safe ts invalid, region_id:{}, leader_safe_ts:{}, self_safe_ts:{}", region_id, leader_safe_ts, self_safe_ts);
            return;
        }
        if (it != safe_ts_map.end())
        {
            if (leader_safe_ts != InvalidSafeTS)
            {
                it->second->leader_safe_ts.store(leader_safe_ts, std::memory_order_relaxed);
            }
            if (self_safe_ts != InvalidSafeTS)
            {
                it->second->self_safe_ts.store(self_safe_ts, std::memory_order_relaxed);
            }
            return;
        }
    }
    std::unique_lock lock(rw_lock);
    safe_ts_map.emplace(region_id, std::make_unique<SafeTsEntry>(leader_safe_ts, self_safe_ts));
}

} // namespace DB
