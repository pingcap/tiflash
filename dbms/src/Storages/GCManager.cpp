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

#include <Storages/GCManager.h>
#include <Storages/IManageableStorage.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{
namespace ErrorCodes
{
extern const int TABLE_IS_DROPPED;
} // namespace ErrorCodes

GCManager::GCManager(Context & context)
    : global_context{context.getGlobalContext()}
    , log(&Poco::Logger::get("GCManager"))
{
}

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

    LOG_FMT_INFO(log, "Start GC with table id: {}", next_table_id);
    // Get a storage snapshot with weak_ptrs first
    // TODO: avoid gc on storage which have no data?
    std::map<TableID, std::weak_ptr<IManageableStorage>> storages;
    for (const auto & [table_id, storage] : global_context.getTMTContext().getStorages().getAllStorage())
        storages.emplace(table_id, storage);
    auto iter = storages.begin();
    if (next_table_id != InvalidTableID)
        iter = storages.lower_bound(next_table_id);

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
            TableLockHolder table_read_lock = storage->lockForShare(RWLock::NO_QUERY);
            // Block this thread and do GC on the storage
            // It is OK if any schema changes is apply to the storage while doing GC, so we
            // do not acquire structure lock on the storage.
            auto gc_segments_num = storage->onSyncGc(gc_segments_limit);
            gc_segments_limit = gc_segments_limit - gc_segments_num;
            LOG_FMT_TRACE(log, "GCManager gc {} segments of table {}", gc_segments_num, storage->getTableInfo().id);
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
    next_table_id = iter->first;
    LOG_FMT_INFO(log, "End GC and next gc will start with table id: {}", next_table_id);
    gc_check_stop_watch.restart();
    // Always return false
    return false;
}

} // namespace DB
