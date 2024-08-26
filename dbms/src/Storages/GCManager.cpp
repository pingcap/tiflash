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

#include <Common/Logger.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/GCOptions.h>
#include <Storages/GCManager.h>
#include <Storages/IManageableStorage.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/TMTContext.h>
#include <TiDB/Schema/TiDB.h>

namespace DB
{
namespace ErrorCodes
{
extern const int TABLE_IS_DROPPED;
} // namespace ErrorCodes

GCManager::GCManager(Context & context)
    : global_context{context.getGlobalContext()}
    , log(Logger::get())
{}

bool GCManager::work()
{
    auto & global_settings = global_context.getSettingsRef();
    if (gc_check_stop_watch.elapsedSeconds() < global_settings.dt_bg_gc_check_interval)
        return false;
    Int64 gc_segments_limit = global_settings.dt_bg_gc_max_segments_to_check_every_round;
    // limit less than or equal to 0 means no gc
    if (gc_segments_limit <= 0)
    {
        gc_check_stop_watch.restart();
        return false;
    }
    if (global_context.getSharedContextDisagg()->isDisaggregatedComputeMode())
    {
        // We have disabled the background service for running this, just add a sanitize check
        assert(false);
        return false;
    }
    else if (global_context.getSharedContextDisagg()->isDisaggregatedStorageMode())
    {
        // For disagg enabled, we must wait before the store meta inited before doing compaction
        // on segments. Or it will upload new data with incorrect remote path.
        auto & kvstore = global_context.getTMTContext().getKVStore();
        auto store_info = kvstore->clonedStoreMeta();
        if (store_info.id() == InvalidStoreID)
        {
            LOG_INFO(log, "Skip GC because store meta is not initialized");
            return false;
        }
        // else we can continue the background segments GC
    }

    LOG_DEBUG(
        log,
        "Start GC with keyspace={}, table_id={}",
        next_keyspace_table_id.first,
        next_keyspace_table_id.second);
    // Get a storage snapshot with weak_ptrs first
    // TODO: avoid gc on storage which have no data?
    std::map<KeyspaceTableID, std::weak_ptr<IManageableStorage>> storages;
    for (const auto & [ks_tbl_id, storage] : global_context.getTMTContext().getStorages().getAllStorage())
        storages.emplace(ks_tbl_id, storage);
    auto iter = storages.begin();
    if (next_keyspace_table_id != KeyspaceTableID{NullspaceID, InvalidTableID})
        iter = storages.lower_bound(next_keyspace_table_id);

    UInt64 checked_storage_num = 0;
    while (true)
    {
        // The TiFlash process receive a signal to terminate.
        if (global_context.getTMTContext().checkShuttingDown())
            break;
        // All storages have been checked, stop here
        if (checked_storage_num >= storages.size())
            break;
        if (iter == storages.end())
            iter = storages.begin();
        checked_storage_num++;
        auto storage = iter->second.lock();
        iter++;
        // The storage has been free
        if (!storage)
            continue;

        try
        {
            auto keyspace_id = storage->getTableInfo().keyspace_id;
            auto ks_log = log->getChild(fmt::format("keyspace={}", keyspace_id));
            TableLockHolder table_read_lock = storage->lockForShare(RWLock::NO_QUERY);
            // Block this thread and do GC on the storage
            // It is OK if any schema changes is applied to the storage while doing GC, so we
            // do not acquire structure lock on the storage.
            auto gc_segments_num = storage->onSyncGc(gc_segments_limit, DM::GCOptions::newAll());
            gc_segments_limit = gc_segments_limit - gc_segments_num;
            LOG_TRACE(ks_log, "GCManager gc {} segments of table_id={}", gc_segments_num, storage->getTableInfo().id);
            // Reach the limit on the number of segments to be gc, stop here
            if (gc_segments_limit <= 0)
                break;
        }
        catch (DB::Exception & e)
        {
            // If the storage is physical dropped, just ignore and continue
            if (e.code() != ErrorCodes::TABLE_IS_DROPPED)
                tryLogCurrentException(__PRETTY_FUNCTION__);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
    if (iter == storages.end())
        iter = storages.begin();

    if (iter != storages.end())
        next_keyspace_table_id = iter->first;
    LOG_DEBUG(
        log,
        "End GC and next gc will start with keyspace={}, table_id={}",
        next_keyspace_table_id.first,
        next_keyspace_table_id.second);
    gc_check_stop_watch.restart();
    // Always return false
    return false;
}

} // namespace DB
