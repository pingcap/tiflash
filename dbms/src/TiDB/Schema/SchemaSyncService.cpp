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

#include <Common/TiFlashMetrics.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Storages/BackgroundProcessingPool.h>
#include <Storages/IManageableStorage.h>
#include <Storages/Transaction/TMTContext.h>
#include <TiDB/Schema/SchemaNameMapper.h>
#include <TiDB/Schema/SchemaSyncService.h>
#include <TiDB/Schema/SchemaSyncer.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int DEADLOCK_AVOIDED;
} // namespace ErrorCodes

// TODO: make this interval configurable
constexpr size_t interval_seconds = 60;

SchemaSyncService::SchemaSyncService(DB::Context & context_)
    : context(context_)
    , background_pool(context_.getBackgroundPool())
    , log(Logger::get())
{
    // Add task for adding and removing keyspace sync schema tasks.
    handle = background_pool.addTask(
        [&, this] {
            addKeyspaceGCTasks();
            removeKeyspaceGCTasks();

            return false;
        },
        false,
        interval_seconds * 1000);
}

void SchemaSyncService::addKeyspaceGCTasks()
{
    auto keyspaces = context.getTMTContext().getStorages().getAllKeyspaces();

    UInt64 num_add_tasks = 0;
    // Add new sync schema task for new keyspace.
    std::unique_lock<std::shared_mutex> lock(ks_map_mutex);
    for (auto const iter : keyspaces)
    {
        auto ks = iter.first;
        if (!ks_handle_map.count(ks))
        {
            auto ks_log = log->getChild(fmt::format("keyspace={}", ks));
            LOG_INFO(ks_log, "add sync schema task");
            auto task_handle = background_pool.addTask(
                [&, this, ks, ks_log]() noexcept {
                    String stage;
                    bool done_anything = false;
                    try
                    {
                        /// Do sync schema first, then gc.
                        /// They must be performed synchronously,
                        /// otherwise table may get mis-GC-ed if RECOVER was not properly synced caused by schema sync pause but GC runs too aggressively.
                        // GC safe point must be obtained ahead of syncing schema.
                        stage = "Sync schemas";
                        done_anything = syncSchemas(ks);
                        if (done_anything)
                            GET_METRIC(tiflash_schema_trigger_count, type_timer).Increment();

                        stage = "GC";
                        auto gc_safe_point = PDClientHelper::getGCSafePointWithRetry(context.getTMTContext().getPDClient());
                        done_anything = gc(gc_safe_point, ks);

                        return done_anything;
                    }
                    catch (const Exception & e)
                    {
                        LOG_ERROR(ks_log, "{} failed by {} \n stack : {}", stage, e.displayText(), e.getStackTrace().toString());
                    }
                    catch (const Poco::Exception & e)
                    {
                        LOG_ERROR(ks_log, "{} failed by {}", stage, e.displayText());
                    }
                    catch (const std::exception & e)
                    {
                        LOG_ERROR(ks_log, "{} failed by {}", stage, e.what());
                    }
                    return false;
                },
                false,
                interval_seconds * 1000);

            ks_handle_map.emplace(ks, task_handle);
            num_add_tasks += 1;
        }
    }

    auto log_level = num_add_tasks > 0 ? Poco::Message::PRIO_INFORMATION : Poco::Message::PRIO_DEBUG;
    LOG_IMPL(log, log_level, "add sync schema task for keyspaces done, num_add_tasks={}", num_add_tasks);
}

void SchemaSyncService::removeKeyspaceGCTasks()
{
    auto keyspaces = context.getTMTContext().getStorages().getAllKeyspaces();
    std::unique_lock<std::shared_mutex> lock(ks_map_mutex);

    UInt64 num_remove_tasks = 0;
    // Remove stale sync schema task.
    for (auto ks_handle_iter = ks_handle_map.begin(); ks_handle_iter != ks_handle_map.end(); /*empty*/)
    {
        const auto & ks = ks_handle_iter->first;
        if (keyspaces.count(ks))
        {
            ++ks_handle_iter;
            continue;
        }
        auto keyspace_log = log->getChild(fmt::format("keyspace={}", ks));
        LOG_INFO(keyspace_log, "remove sync schema task");
        background_pool.removeTask(ks_handle_iter->second);
        ks_handle_iter = ks_handle_map.erase(ks_handle_iter);
        num_remove_tasks += 1;
        // remove schema version for this keyspace
        removeCurrentVersion(ks);
        keyspace_gc_context.erase(ks); // clear the last gc safepoint
    }

    auto log_level = num_remove_tasks > 0 ? Poco::Message::PRIO_INFORMATION : Poco::Message::PRIO_DEBUG;
    LOG_IMPL(log, log_level, "remove sync schema task for keyspaces done, num_remove_tasks={}", num_remove_tasks);
}

void SchemaSyncService::shutdown()
{
    background_pool.removeTask(handle);
    for (auto const & iter : ks_handle_map)
    {
        auto task_handle = iter.second;
        background_pool.removeTask(task_handle);
    }
    LOG_INFO(log, "SchemaSyncService stopped");
}

SchemaSyncService::~SchemaSyncService()
{
    shutdown();
}

bool SchemaSyncService::syncSchemas(KeyspaceID keyspace_id)
{
    return context.getTMTContext().getSchemaSyncer()->syncSchemas(context, keyspace_id);
}


void SchemaSyncService::removeCurrentVersion(KeyspaceID keyspace_id)
{
    context.getTMTContext().getSchemaSyncer()->removeCurrentVersion(keyspace_id);
}

template <typename DatabaseOrTablePtr>
inline std::tuple<bool, Timestamp> isSafeForGC(const DatabaseOrTablePtr & ptr, Timestamp gc_safepoint)
{
    const auto tombstone_ts = ptr->getTombstone();
    return {tombstone_ts != 0 && tombstone_ts < gc_safepoint, tombstone_ts};
}

std::optional<Timestamp> SchemaSyncService::lastGcSafePoint(KeyspaceID keyspace_id) const
{
    std::shared_lock lock(ks_map_mutex);
    auto iter = keyspace_gc_context.find(keyspace_id);
    if (iter == keyspace_gc_context.end())
        return std::nullopt;
    return iter->second.last_gc_safepoint;
}

void SchemaSyncService::updateLastGcSafepoint(KeyspaceID keyspace_id, Timestamp gc_safepoint)
{
    std::unique_lock lock(ks_map_mutex);
    keyspace_gc_context[keyspace_id].last_gc_safepoint = gc_safepoint;
}

bool SchemaSyncService::gc(Timestamp gc_safepoint, KeyspaceID keyspace_id)
{
    return gcImpl(gc_safepoint, keyspace_id, /*ignore_remain_regions*/ false);
}

bool SchemaSyncService::gcImpl(Timestamp gc_safepoint, KeyspaceID keyspace_id, bool ignore_remain_regions)
{
    const std::optional<Timestamp> last_gc_safepoint = lastGcSafePoint(keyspace_id);
    // for new deploy cluster, there is an interval that gc_safepoint return 0, skip it
    if (gc_safepoint == 0)
        return false;
    // the gc safepoint is not changed since last schema gc run, skip it
    if (last_gc_safepoint.has_value() && gc_safepoint == *last_gc_safepoint)
        return false;

    String last_gc_safepoint_str = "none";
    if (last_gc_safepoint.has_value())
        last_gc_safepoint_str = fmt::format("{}", *last_gc_safepoint);
    auto keyspace_log = log->getChild(fmt::format("keyspace={}", keyspace_id));
    LOG_INFO(keyspace_log, "Schema GC begin, last_safepoint={} safepoint={}", last_gc_safepoint_str, gc_safepoint);

    size_t num_tables_removed = 0;
    size_t num_databases_removed = 0;

    // The storages that are ready for gc
    std::vector<std::weak_ptr<IManageableStorage>> storages_to_gc;
    // Get a snapshot of database
    auto dbs = context.getDatabases();
    for (const auto & iter : dbs)
    {
        auto db_ks_id = SchemaNameMapper::getMappedNameKeyspaceID(iter.first);
        if (db_ks_id != keyspace_id)
            continue;
        const auto & db = iter.second;
        for (auto table_iter = db->getIterator(context); table_iter->isValid(); table_iter->next())
        {
            auto & storage = table_iter->table();
            auto managed_storage = std::dynamic_pointer_cast<IManageableStorage>(storage);
            if (!managed_storage)
                continue;

            const auto & [database_is_stale, db_tombstone] = isSafeForGC(db, gc_safepoint);
            const auto & [table_is_stale, table_tombstone] = isSafeForGC(managed_storage, gc_safepoint);
            if (database_is_stale || table_is_stale)
            {
                // Only keep a weak_ptr on storage so that the memory can be free as soon as
                // it is dropped.
                storages_to_gc.emplace_back(std::weak_ptr<IManageableStorage>(managed_storage));
                LOG_INFO(
                    keyspace_log,
                    "Detect stale table, database_name={} table_name={} database_tombstone={} table_tombstone={} "
                    "safepoint={}",
                    managed_storage->getDatabaseName(),
                    managed_storage->getTableName(),
                    db_tombstone,
                    table_tombstone,
                    gc_safepoint);
            }
        }
    }

    auto & tmt_context = context.getTMTContext();
    // Physically drop tables
    bool succeeded = true;
    for (auto & storage_ptr : storages_to_gc)
    {
        // Get a shared_ptr from weak_ptr, it should always success.
        auto storage = storage_ptr.lock();
        if (unlikely(!storage))
            continue;

        String database_name = storage->getDatabaseName();
        String table_name = storage->getTableName();
        const auto & table_info = storage->getTableInfo();
        auto canonical_name = [&]() {
            auto database_id = SchemaNameMapper::tryGetDatabaseID(database_name);
            if (!database_id.has_value())
            {
                return fmt::format("{}.{} table_id={}", database_name, table_name, table_info.id);
            }
            return fmt::format(
                "{}.{} database_id={} table_id={}",
                database_name,
                table_name,
                *database_id,
                table_info.id);
        }();

        auto & region_table = tmt_context.getRegionTable();
        if (auto remain_regions = region_table.getRegionIdsByTable(keyspace_id, table_info.id); //
            !remain_regions.empty())
        {
            if (likely(!ignore_remain_regions))
            {
                LOG_WARNING(
                    keyspace_log,
                    "Physically drop table is skip, regions are not totally removed from TiFlash, remain_region_ids={}"
                    " table_tombstone={} safepoint={} {}",
                    remain_regions,
                    storage->getTombstone(),
                    gc_safepoint,
                    canonical_name);
                succeeded = false; // dropping this table is skipped, do not succee the `last_gc_safepoint`
                continue;
            }
            else
            {
                LOG_WARNING(
                    keyspace_log,
                    "Physically drop table is executed while regions are not totally removed from TiFlash,"
                    " remain_region_ids={} ignore_remain_regions={} table_tombstone={} safepoint={} {} ",
                    remain_regions,
                    ignore_remain_regions,
                    storage->getTombstone(),
                    gc_safepoint,
                    canonical_name);
            }
        }

        LOG_INFO(
            keyspace_log,
            "Physically drop table begin, table_tombstone={} safepoint={} {}",
            storage->getTombstone(),
            gc_safepoint,
            canonical_name);
        auto drop_query = std::make_shared<ASTDropQuery>();
        drop_query->database = std::move(database_name);
        drop_query->table = std::move(table_name);
        drop_query->if_exists = true;
        drop_query->lock_timeout = std::chrono::milliseconds(1 * 1000); // timeout for acquring table drop lock
        ASTPtr ast_drop_query = drop_query;
        try
        {
            InterpreterDropQuery drop_interpreter(ast_drop_query, context);
            drop_interpreter.execute();
            LOG_INFO(keyspace_log, "Physically drop table {} end", canonical_name);
            ++num_tables_removed;
        }
        catch (DB::Exception & e)
        {
            succeeded = false; // dropping this table is skipped, do not succee the `last_gc_safepoint`
            String err_msg;
            // Maybe a read lock of a table is held for a long time, just ignore it this round.
            if (e.code() == ErrorCodes::DEADLOCK_AVOIDED)
                err_msg = "locking attempt has timed out!"; // ignore verbose stack for this error
            else
                err_msg = getCurrentExceptionMessage(true);
            LOG_INFO(keyspace_log, "Physically drop table {} is skipped, reason: {}", canonical_name, err_msg);
        }
    }
    storages_to_gc.clear();

    // Physically drop database
    for (const auto & iter : dbs)
    {
        const auto & db = iter.second;
        auto ks_db_id = SchemaNameMapper::getMappedNameKeyspaceID(iter.first);
        if (ks_db_id != keyspace_id)
            continue;
        const auto & [db_is_stale, db_tombstone] = isSafeForGC(db, gc_safepoint);
        if (!db_is_stale)
            continue;

        const auto & db_name = iter.first;
        size_t num_tables = 0;
        for (auto table_iter = db->getIterator(context); table_iter->isValid(); table_iter->next())
            ++num_tables;
        if (num_tables > 0)
        {
            // There should be something wrong, maybe a read lock of a table is held for a long time.
            // Just ignore and try to collect this database next time.
            LOG_INFO(keyspace_log, "Physically drop database {} is skipped, reason: {} tables left", db_name, num_tables);
            continue;
        }

        LOG_INFO(keyspace_log, "Physically drop database begin, database_tombstone={} {}", db->getTombstone(), db_name);
        auto drop_query = std::make_shared<ASTDropQuery>();
        drop_query->database = db_name;
        drop_query->if_exists = true;
        drop_query->lock_timeout = std::chrono::milliseconds(1 * 1000); // timeout for acquring table drop lock
        ASTPtr ast_drop_query = drop_query;
        try
        {
            InterpreterDropQuery drop_interpreter(ast_drop_query, context);
            drop_interpreter.execute();
            LOG_INFO(keyspace_log, "Physically drop database {} end, safepoint={}", db_name, gc_safepoint);
            ++num_databases_removed;
        }
        catch (DB::Exception & e)
        {
            succeeded = false; // dropping this database is skipped, do not succee the `last_gc_safepoint`
            String err_msg;
            if (e.code() == ErrorCodes::DEADLOCK_AVOIDED)
                err_msg = "locking attempt has timed out!"; // ignore verbose stack for this error
            else
                err_msg = getCurrentExceptionMessage(true);
            LOG_INFO(keyspace_log, "Physically drop database {} is skipped, reason: {}", db_name, err_msg);
        }
    }

    // TODO: Optimize it after `BackgroundProcessingPool` can the task return how many seconds to sleep
    //       before next round.
    if (succeeded)
    {
        updateLastGcSafepoint(keyspace_id, gc_safepoint);
        LOG_INFO(
            keyspace_log,
            "Schema GC done, tables_removed={} databases_removed={} safepoint={}",
            num_tables_removed,
            num_databases_removed,
            gc_safepoint);
        // This round of GC could run for a long time. Run immediately to check whether
        // the latest gc_safepoint has been updated in PD.
        // - gc_safepoint is not updated, it will be skipped because gc_safepoint == last_gc_safepoint
        // - gc_safepoint is updated, run again immediately to cleanup other dropped data
        return true;
    }
    else
    {
        // Don't update last_gc_safe_point and retry later
        LOG_INFO(
            keyspace_log,
            "Schema GC meet error, will try again later, last_safepoint={} safepoint={}",
            last_gc_safepoint_str,
            gc_safepoint);
        // Return false to let it run again after `ddl_sync_interval_seconds` even if the gc_safepoint
        // on PD is not updated.
        return false;
    }
}

} // namespace DB
