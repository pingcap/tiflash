// Copyright 2023 PingCAP, Inc.
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
#include <Storages/KVStore/Decode/RegionTable.h>
#include <Storages/KVStore/Decode/TiKVRange.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/MultiRaft/Disagg/CheckpointInfo.h>
#include <Storages/KVStore/MultiRaft/RegionManager.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/Types.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageDeltaMergeHelpers.h>
#include <TiDB/Schema/SchemaSyncer.h>
#include <fiu.h>

#include <any>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_TABLE;
extern const int ILLFORMAT_RAFT_ROW;
extern const int TABLE_IS_DROPPED;
} // namespace ErrorCodes
namespace FailPoints
{
extern const char force_set_num_regions_for_table[];
} // namespace FailPoints

RegionTable::Table & RegionTable::getOrCreateTable(const KeyspaceID keyspace_id, const TableID table_id)
{
    auto ks_table_id = KeyspaceTableID{keyspace_id, table_id};
    auto it = tables.find(ks_table_id);
    if (it == tables.end())
    {
        // Load persisted info.
        it = tables.emplace(ks_table_id, table_id).first;
        addTableToIndex(keyspace_id, table_id);
        LOG_INFO(log, "get new table, keyspace={} table_id={}", keyspace_id, table_id);
    }
    return it->second;
}

RegionTable::InternalRegion & RegionTable::insertRegion(
    Table & table,
    const RegionRangeKeys & region_range_keys,
    const Region & region)
{
    auto region_id = region.id();
    region.setRegionTableCtx(table.ctx);
    auto keyspace_id = region_range_keys.getKeyspaceID();
    auto & table_regions = table.internal_regions;
    // Insert table mapping.
    // todo check if region_range_keys.mapped_table_id == table.table_id ??
    auto [it, ok] = table_regions.emplace(region_id, InternalRegion(region_id, region_range_keys.rawKeys()));
    if (!ok)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "{}: insert duplicate internal region, region_id={}",
            __PRETTY_FUNCTION__,
            region_id);

    // Insert region mapping.
    region_infos[region_id] = KeyspaceTableID{keyspace_id, table.table_id};

    return it->second;
}

RegionTable::InternalRegion & RegionTable::doGetInternalRegion(KeyspaceTableID ks_table_id, DB::RegionID region_id)
{
    return tables.find(ks_table_id)->second.internal_regions.find(region_id)->second;
}

RegionTable::InternalRegion & RegionTable::getOrInsertRegion(const Region & region)
{
    auto keyspace_id = region.getKeyspaceID();
    auto table_id = region.getMappedTableID();
    auto & table = getOrCreateTable(keyspace_id, table_id);
    auto & table_regions = table.internal_regions;
    if (auto it = table_regions.find(region.id()); it != table_regions.end())
        return it->second;

    return insertRegion(table, *region.getRange(), region);
}

RegionTable::RegionTable(Context & context_)
    : context(&context_)
    , log(Logger::get())
{}

void RegionTable::clear()
{
    region_infos.clear();
    tables.clear();
    safe_ts_mgr.clear();
}

void RegionTable::restore()
{
    LOG_INFO(log, "RegionTable restore start");

    const auto & tmt = context->getTMTContext();
    tmt.getKVStore()->traverseRegions([this](const RegionID, const RegionPtr & region) { addRegion(*region); });

    LOG_INFO(log, "RegionTable restore end, n_tables={}", tables.size());
}

void RegionTable::removeTable(KeyspaceID keyspace_id, TableID table_id)
{
    std::lock_guard lock(mutex);

    auto it = tables.find(KeyspaceTableID{keyspace_id, table_id});
    if (it == tables.end())
        return;
    auto & table = it->second;

    // Remove from region list.
    for (const auto & region_info : table.internal_regions)
    {
        region_infos.erase(region_info.first);
        safe_ts_mgr.remove(region_info.first);
    }

    // Remove from table map.
    tables.erase(it);
    removeTableFromIndex(keyspace_id, table_id);

    LOG_INFO(log, "remove table from RegionTable success, keyspace={} table_id={}", keyspace_id, table_id);
}

void RegionTable::addRegion(const Region & region)
{
    std::lock_guard lock(mutex);
    getOrInsertRegion(region);
}

void RegionTable::addPrehandlingRegion(const Region & region)
{
    std::lock_guard lock(mutex);
    auto keyspace_id = region.getKeyspaceID();
    auto table_id = region.getMappedTableID();
    auto & table = getOrCreateTable(keyspace_id, table_id);
    region.setRegionTableCtx(table.ctx);
}

size_t RegionTable::getTableRegionSize(KeyspaceID keyspace_id, TableID table_id) const
{
    std::scoped_lock lock(mutex);

    auto it = tables.find(KeyspaceTableID{keyspace_id, table_id});
    if (it == tables.end())
        return 0;
    const auto & table = it->second;
    if (table.ctx)
        return table.ctx->table_size;
    return 0;
}

void RegionTable::debugClearTableRegionSize(KeyspaceID keyspace_id, TableID table_id)
{
    std::scoped_lock lock(mutex);

    auto it = tables.find(KeyspaceTableID{keyspace_id, table_id});
    if (it == tables.end())
        return;
    const auto & table = it->second;
    if (table.ctx)
        table.ctx->table_size = 0;
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
        auto rowkey_range = DM::RowKeyRange::fromRegionRange(
            handle_range,
            ks_table_id.second,
            ks_table_id.second,
            storage->isCommonHandle(),
            storage->getRowKeyColumnSize());
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
    KeyspaceTableID ks_table_id;
    std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr> handle_range;

    {
        /// We need to protect `region_infos` and `table` under mutex lock
        std::lock_guard lock(mutex);

        auto it = region_infos.find(region_id);
        if (it == region_infos.end())
        {
            LOG_WARNING(log, "region does not exist, region_id={}", region_id);
            return;
        }

        ks_table_id = it->second;
        auto & table = tables.find(ks_table_id)->second;
        auto internal_region_it = table.internal_regions.find(region_id);
        handle_range = internal_region_it->second.range_in_table;

        region_infos.erase(it);
        safe_ts_mgr.remove(region_id);
        table.internal_regions.erase(internal_region_it);
        if (table.internal_regions.empty())
        {
            tables.erase(ks_table_id);
        }
    }
    LOG_INFO(log, "remove region in RegionTable done, region_id={}", region_id);

    // Sometime we don't need to remove data. e.g. remove region after region merge.
    if (remove_data)
    {
        // Try to remove obsolete data in storage

        // Note that we should do this without lock on RegionTable.
        // But caller(KVStore) should ensure that no new data write into this handle_range
        // before `removeObsoleteDataInStorage` is done. (by param `RegionTaskLock`)
        // And this is expected not to block for long time.
        removeObsoleteDataInStorage(context, ks_table_id, handle_range);
        LOG_INFO(log, "remove region in storage done, region_id={}", region_id);
    }
}

// RaftCommands will directly call `writeCommittedByRegion`.
RegionDataReadInfoList RegionTable::tryWriteBlockByRegion(const RegionPtr & region)
{
    const RegionID region_id = region->id();

    const auto func_update_region = [&](std::function<bool(InternalRegion &)> && callback) -> bool {
        std::lock_guard lock(mutex);
        if (auto it = region_infos.find(region_id); it != region_infos.end())
        {
            auto & internal_region = doGetInternalRegion(it->second, region_id);
            return callback(internal_region);
        }
        else
        {
            LOG_WARNING(log, "Internal region might be removed, region_id={}", region_id);
            return false;
        }
    };

    bool status = func_update_region([&](InternalRegion & internal_region) -> bool {
        if (internal_region.pause_flush)
        {
            LOG_INFO(log, "Internal region pause flush, may be being flushed, region_id={}", region_id);
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
        /// Write region data into corresponding storage.
        writeCommittedByRegion(*context, region, data_list_to_remove, log);
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::ILLFORMAT_RAFT_ROW)
        {
            // br or lighting may write illegal data into tikv, skip flush.
            LOG_WARNING(
                Logger::get(),
                "Got error while reading region committed cache: {}. Skip flush region and keep original cache.",
                e.displayText());
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
        return true;
    });

    if (first_exception)
        std::rethrow_exception(first_exception);

    return data_list_to_remove;
}

void RegionTable::handleInternalRegionsByTable(
    const KeyspaceID keyspace_id,
    const TableID table_id,
    std::function<void(const InternalRegions &)> && callback) const
{
    std::lock_guard lock(mutex);

    if (auto it = tables.find(KeyspaceTableID{keyspace_id, table_id}); it != tables.end())
    {
        callback(it->second.internal_regions);
    }
}

void RegionTable::handleInternalRegionsByKeyspace(
    KeyspaceID keyspace_id,
    std::function<void(const TableID table_id, const InternalRegions &)> && callback) const
{
    std::lock_guard lock(mutex);
    auto table_set = keyspace_index.find(keyspace_id);
    if (table_set != keyspace_index.end())
    {
        for (auto table_id : table_set->second)
        {
            if (auto it = tables.find(KeyspaceTableID{keyspace_id, table_id}); it != tables.end())
                callback(table_id, it->second.internal_regions);
        }
    }
}

std::vector<RegionID> RegionTable::getRegionIdsByTable(KeyspaceID keyspace_id, TableID table_id) const
{
    fiu_do_on(FailPoints::force_set_num_regions_for_table, {
        if (auto v = FailPointHelper::getFailPointVal(FailPoints::force_set_num_regions_for_table); v)
        {
            auto num_regions = std::any_cast<std::vector<RegionID>>(v.value());
            return num_regions;
        }
    });

    std::lock_guard lock(mutex);
    if (auto iter = tables.find(KeyspaceTableID{keyspace_id, table_id}); //
        unlikely(iter != tables.end()))
    {
        std::vector<RegionID> ret_regions;
        ret_regions.reserve(iter->second.internal_regions.size());
        for (const auto & r : iter->second.internal_regions)
        {
            ret_regions.emplace_back(r.first);
        }
        return ret_regions;
    }
    return {};
}

std::vector<std::pair<RegionID, RegionPtr>> RegionTable::getRegionsByTable(
    const KeyspaceID keyspace_id,
    const TableID table_id) const
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

void RegionTable::shrinkRegionRange(const Region & region)
{
    std::lock_guard lock(mutex);
    auto & internal_region = getOrInsertRegion(region);
    internal_region.range_in_table = region.getRange()->rawKeys();
}

void RegionTable::replaceRegion(const RegionPtr & old_region, const RegionPtr & new_region)
{
    const auto region_range_keys = new_region->getRange();
    // Extend region range to make sure data won't be removed.
    extendRegionRange(*new_region, *region_range_keys);
    if (old_region)
    {
        std::scoped_lock lock(mutex);
        // `old_region` will no longer contribute to the memory of the table.
        auto keyspace_id = region_range_keys->getKeyspaceID();
        auto table_id = region_range_keys->getMappedTableID();
        auto & table = getOrCreateTable(keyspace_id, table_id);
        old_region->resetRegionTableCtx();
        if unlikely (!new_region->getRegionTableCtx())
        {
            // For most of the cases, the region is prehandled, so the ctx is set at that moment.
            new_region->setRegionTableCtx(table.ctx);
        }
    }
}

void RegionTable::extendRegionRange(const Region & region, const RegionRangeKeys & region_range_keys)
{
    std::lock_guard lock(mutex);
    const RegionID region_id = region.id();
    auto keyspace_id = region_range_keys.getKeyspaceID();
    auto table_id = region_range_keys.getMappedTableID();
    auto new_handle_range = region_range_keys.rawKeys();

    if (auto it = region_infos.find(region_id); it != region_infos.end())
    {
        auto ks_tbl_id = KeyspaceTableID{keyspace_id, table_id};
        RUNTIME_CHECK_MSG(
            ks_tbl_id == it->second,
            "{}: table id not match the previous one"
            ", region_id={} keyspace={} table_id={} old_keyspace={} old_table_id={}",
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
            LOG_INFO(
                log,
                "internal region has larger range, keyspace={} table_id={} region_id={}",
                keyspace_id,
                table_id,
                region_id);
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
        insertRegion(table, region_range_keys, region);
        LOG_INFO(log, "insert internal region, keyspace={} table_id={} region_id={}", keyspace_id, table_id, region_id);
    }
}

void RegionTable::addTableToIndex(KeyspaceID keyspace_id, TableID table_id)
{
    auto it = keyspace_index.find(keyspace_id);
    if (it == keyspace_index.end())
    {
        keyspace_index.emplace(keyspace_id, std::unordered_set<TableID>{table_id});
    }
    else
    {
        it->second.insert(table_id);
    }
}
void RegionTable::removeTableFromIndex(KeyspaceID keyspace_id, TableID table_id)
{
    auto it = keyspace_index.find(keyspace_id);
    if (it != keyspace_index.end())
    {
        it->second.erase(table_id);
        if (it->second.empty())
            keyspace_index.erase(it);
    }
}

} // namespace DB
