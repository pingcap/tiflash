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

#include <Common/FailPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Common/setThreadName.h>
#include <Interpreters/Context.h>
#include <Storages/KVStore/Decode/RegionTable.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageDeltaMergeHelpers.h>

#include <ext/scope_guard.h>

namespace DB
{
namespace FailPoints
{
extern const char pause_until_apply_raft_snapshot[];
} // namespace FailPoints

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int TABLE_IS_DROPPED;
} // namespace ErrorCodes

template <typename RegionPtrWrap>
void KVStore::checkAndApplyPreHandledSnapshot(const RegionPtrWrap & new_region, TMTContext & tmt)
{
    auto region_id = new_region->id();
    auto old_region = getRegion(region_id);
    UInt64 old_applied_index = 0;

    /**
     * When applying snapshot of a region, its range must not be overlapped with any other(different id) region's.
     */
    if (old_region)
    {
        old_applied_index = old_region->appliedIndex();
        if (auto new_index = new_region->appliedIndex(); old_applied_index > new_index)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "try to apply with older index, region_id={} applied_index={} new_index={}",
                region_id,
                old_applied_index,
                new_index);
        }
        else if (old_applied_index == new_index)
        {
            LOG_WARNING(
                log,
                "{} already has same applied index, just ignore next process. Please check log whether server crashed "
                "after successfully applied snapshot.",
                old_region->getDebugString());
            return;
        }

        {
            LOG_INFO(log, "{} set state to `Applying`", old_region->toString());
            // Set original region state to `Applying` and any read request toward this region should be rejected because
            // engine may delete data unsafely.
            auto region_lock = region_manager.genRegionTaskLock(old_region->id());
            old_region->setStateApplying();
            tmt.getRegionTable().tryWriteBlockByRegion(old_region);
            tryFlushRegionCacheInStorage(tmt, *old_region, log);
            persistRegion(*old_region, &region_lock, PersistRegionReason::ApplySnapshotPrevRegion, "");
        }
    }

    {
        const auto & new_range = new_region->getRange();
        auto task_lock = genTaskLock();
        auto region_map = getRegionsByRangeOverlap(new_range->comparableKeys());
        for (const auto & overlapped_region : region_map)
        {
            if (overlapped_region.first != region_id)
            {
                auto state = getProxyHelper()->getRegionLocalState(overlapped_region.first);
                if (state.state() != raft_serverpb::PeerState::Tombstone)
                {
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "range of region_id={} is overlapped with region_id={}, state: {}",
                        region_id,
                        overlapped_region.first,
                        state.ShortDebugString());
                }
                else
                {
                    LOG_INFO(
                        log,
                        "range of region_id={} is overlapped with `Tombstone` region_id={}",
                        region_id,
                        overlapped_region.first);
                    handleDestroy(overlapped_region.first, tmt, task_lock);
                }
            }
        }
    }

    onSnapshot(new_region, old_region, old_applied_index, tmt);
}

// This function get tiflash replica count from local schema.
std::pair<UInt64, bool> getTiFlashReplicaSyncInfo(StorageDeltaMergePtr & dm_storage)
{
    auto struct_lock = dm_storage->lockStructureForShare(getThreadNameAndID());
    const auto & replica_info = dm_storage->getTableInfo().replica_info;
    auto is_syncing = replica_info.count > 0 && replica_info.available.has_value() && !(*replica_info.available);
    return {replica_info.count, is_syncing};
}

template <typename RegionPtrWrap>
void KVStore::onSnapshot(
    const RegionPtrWrap & new_region_wrap,
    RegionPtr old_region,
    UInt64 old_region_index,
    TMTContext & tmt)
{
    RegionID region_id = new_region_wrap->id();

    {
        auto keyspace_id = new_region_wrap->getKeyspaceID();
        auto table_id = new_region_wrap->getMappedTableID();
        if (auto storage = tmt.getStorages().get(keyspace_id, table_id);
            storage && storage->engineType() == TiDB::StorageEngine::DT)
        {
            try
            {
                auto & context = tmt.getContext();
                // Acquire `drop_lock` so that no other threads can drop the storage. `alter_lock` is not required.
                auto table_lock = storage->lockForShare(getThreadNameAndID());
                auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
                auto new_key_range = DM::RowKeyRange::fromRegionRange(
                    new_region_wrap->getRange(),
                    table_id,
                    storage->isCommonHandle(),
                    storage->getRowKeyColumnSize());
                if (old_region)
                {
                    auto old_key_range = DM::RowKeyRange::fromRegionRange(
                        old_region->getRange(),
                        table_id,
                        storage->isCommonHandle(),
                        storage->getRowKeyColumnSize());
                    if (old_key_range != new_key_range)
                    {
                        LOG_INFO(
                            log,
                            "clear old range before apply snapshot, region_id={} old_range={} new_range={} "
                            "keyspace_id={} table_id={}",
                            region_id,
                            old_key_range.toDebugString(),
                            new_key_range.toDebugString(),
                            keyspace_id,
                            table_id);
                        dm_storage->deleteRange(old_key_range, context.getSettingsRef());
                        // We must flush the deletion to the disk here, because we only flush new range when persisting this region later.
                        dm_storage->flushCache(context, old_key_range, /*try_until_succeed*/ true);
                    }
                }
                if constexpr (std::is_same_v<RegionPtrWrap, RegionPtrWithSnapshotFiles>)
                {
                    // Call `ingestFiles` to delete data for range and ingest external DTFiles.
                    auto ingested_bytes = dm_storage->ingestFiles(
                        new_key_range,
                        new_region_wrap.external_files,
                        /*clear_data_in_range=*/true,
                        context.getSettingsRef());
                    if (auto [count, is_syncing] = getTiFlashReplicaSyncInfo(dm_storage); is_syncing)
                    {
                        // For write, 1 RU per KB. Reference: https://docs.pingcap.com/tidb/v7.0/tidb-resource-control
                        // Only calculate RU of one replica. So each replica reports 1/count consumptions.
                        TiFlashMetrics::instance().addReplicaSyncRU(
                            keyspace_id,
                            std::ceil(static_cast<double>(ingested_bytes) / 1024.0 / count));
                    }
                }
                else if constexpr (std::is_same_v<RegionPtrWrap, RegionPtrWithCheckpointInfo>)
                {
                    dm_storage->ingestSegmentsFromCheckpointInfo(
                        new_key_range,
                        new_region_wrap.checkpoint_info,
                        context.getSettingsRef());
                }
                else
                {
                    // It is only for debug usage now.
                    static_assert(std::is_same_v<RegionPtrWrap, RegionPtrWithBlock>);
                    // Call `deleteRange` to delete data for range
                    dm_storage->deleteRange(new_key_range, context.getSettingsRef());
                }
            }
            catch (DB::Exception & e)
            {
                // We can ignore if storage is dropped.
                if (e.code() != ErrorCodes::TABLE_IS_DROPPED)
                    throw;
            }
        }
    }

    {
        const auto range = new_region_wrap->getRange();
        auto & region_table = tmt.getRegionTable();
        // extend region to make sure data won't be removed.
        region_table.extendRegionRange(region_id, *range);
        // For `RegionPtrWithBlock`, try to flush data into storage first.
        if constexpr (std::is_same_v<RegionPtrWrap, RegionPtrWithBlock>)
        {
            try
            {
                auto tmp = region_table.tryWriteBlockByRegion(new_region_wrap);
                {
                    std::lock_guard lock(bg_gc_region_data_mutex);
                    bg_gc_region_data.push_back(std::move(tmp));
                }
                tryFlushRegionCacheInStorage(tmt, *new_region_wrap, log);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
        // For `RegionPtrWithSnapshotFiles`, don't need to flush cache.
    }

    RegionPtr new_region = new_region_wrap.base;
    {
        auto task_lock = genTaskLock();
        auto region_lock = region_manager.genRegionTaskLock(region_id);

        // check that old_region is not changed and no new applied raft-log during applying snapshot.
        if (getRegion(region_id) != old_region || (old_region && old_region_index != old_region->appliedIndex()))
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "{}: region_id={} instance changed, should not happen",
                __PRETTY_FUNCTION__,
                region_id);
        }

        if (old_region != nullptr)
        {
            LOG_DEBUG(log, "previous {}, new {}", old_region->getDebugString(), new_region->getDebugString());
            {
                // remove index for key_range -> region_id first
                const auto & range = old_region->makeRaftCommandDelegate(task_lock).getRange().comparableKeys();
                {
                    auto manage_lock = genRegionMgrWriteLock(task_lock);
                    manage_lock.index.remove(range, region_id);
                }
            }
            // Reuse the old region for non-region-related data.
            old_region->assignRegion(std::move(*new_region));
            new_region = old_region;
            {
                // add index for new_region
                auto manage_lock = genRegionMgrWriteLock(task_lock);
                manage_lock.index.add(new_region);
            }
        }
        else
        {
            auto manage_lock = genRegionMgrWriteLock(task_lock);
            manage_lock.regions.emplace(region_id, new_region);
            manage_lock.index.add(new_region);
        }

        persistRegion(*new_region, &region_lock, PersistRegionReason::ApplySnapshotCurRegion, "");

        tmt.getRegionTable().shrinkRegionRange(*new_region);
    }

    prehandling_trace.deregisterTask(new_region->id());
}

template <typename RegionPtrWrap>
void KVStore::applyPreHandledSnapshot(const RegionPtrWrap & new_region, TMTContext & tmt)
{
    LOG_INFO(log, "Begin apply snapshot, new_region={}", new_region->toString(true));

    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(tiflash_raft_command_duration_seconds, type_apply_snapshot_flush).Observe(watch.elapsedSeconds());
    });

    checkAndApplyPreHandledSnapshot(new_region, tmt);

    FAIL_POINT_PAUSE(FailPoints::pause_until_apply_raft_snapshot);

    // `new_region` may change in the previous function, just log the region_id down
    LOG_INFO(log, "Finish apply snapshot, cost={:.3f}s region_id={}", watch.elapsedSeconds(), new_region->id());
}

template void KVStore::applyPreHandledSnapshot<RegionPtrWithSnapshotFiles>(
    const RegionPtrWithSnapshotFiles &,
    TMTContext &);

template void KVStore::checkAndApplyPreHandledSnapshot<RegionPtrWithBlock>(const RegionPtrWithBlock &, TMTContext &);
template void KVStore::checkAndApplyPreHandledSnapshot<RegionPtrWithSnapshotFiles>(
    const RegionPtrWithSnapshotFiles &,
    TMTContext &);
template void KVStore::onSnapshot<RegionPtrWithBlock>(const RegionPtrWithBlock &, RegionPtr, UInt64, TMTContext &);
template void KVStore::onSnapshot<RegionPtrWithSnapshotFiles>(
    const RegionPtrWithSnapshotFiles &,
    RegionPtr,
    UInt64,
    TMTContext &);

void KVStore::handleIngestCheckpoint(RegionPtr region, CheckpointInfoPtr checkpoint_info, TMTContext & tmt)
{
    applyPreHandledSnapshot(RegionPtrWithCheckpointInfo{region, checkpoint_info}, tmt);
}

} // namespace DB
