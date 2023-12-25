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

SchemaSyncService::SchemaSyncService(DB::Context & context_)
    : context(context_)
    , background_pool(context_.getBackgroundPool())
    , log(Logger::get())
{
    handle = background_pool.addTask(
        [&, this] {
            String stage;
            bool done_anything = false;
            try
            {
                /// Do sync schema first, then gc.
                /// They must be performed synchronously,
                /// otherwise table may get mis-GC-ed if RECOVER was not properly synced caused by schema sync pause but GC runs too aggressively.
                // GC safe point must be obtained ahead of syncing schema.
                auto gc_safepoint = PDClientHelper::getGCSafePointWithRetry(context.getTMTContext().getPDClient());
                stage = "Sync schemas";
                done_anything = syncSchemas();
                if (done_anything)
                    GET_METRIC(tiflash_schema_trigger_count, type_timer).Increment();

                stage = "GC";
                done_anything = gc(gc_safepoint);

                return done_anything;
            }
            catch (const Exception & e)
            {
                LOG_ERROR(log, "{} failed by {} \n stack : {}", stage, e.displayText(), e.getStackTrace().toString());
            }
            catch (const Poco::Exception & e)
            {
                LOG_ERROR(log, "{} failed by {}", stage, e.displayText());
            }
            catch (const std::exception & e)
            {
                LOG_ERROR(log, "{} failed by {}", stage, e.what());
            }
            return false;
        },
        false);
}

SchemaSyncService::~SchemaSyncService()
{
    background_pool.removeTask(handle);
    LOG_INFO(log, "SchemaSyncService stopped");
}

bool SchemaSyncService::syncSchemas()
{
    return context.getTMTContext().getSchemaSyncer()->syncSchemas(context);
}

template <typename DatabaseOrTablePtr>
inline std::tuple<bool, Timestamp> isSafeForGC(const DatabaseOrTablePtr & ptr, Timestamp gc_safepoint)
{
    const auto tombstone_ts = ptr->getTombstone();
    return {tombstone_ts != 0 && tombstone_ts < gc_safepoint, tombstone_ts};
}

bool SchemaSyncService::gc(Timestamp gc_safepoint)
{
    auto & tmt_context = context.getTMTContext();
    // for new deploy cluster, there is an interval that gc_safepoint return 0, skip it
    if (gc_safepoint == 0)
        return false;
    if (gc_safepoint == gc_context.last_gc_safepoint)
        return false;

    LOG_INFO(log, "Schema GC begin, last_safepoint={} safepoint={}", gc_context.last_gc_safepoint, gc_safepoint);

    size_t num_tables_removed = 0;
    size_t num_databases_removed = 0;

    // The storages that are ready for gc
    std::vector<std::weak_ptr<IManageableStorage>> storages_to_gc;
    // Get a snapshot of database
    auto dbs = context.getDatabases();
    for (const auto & iter : dbs)
    {
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
                    log,
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
            // DB info maintenance is parallel with GC logic so we can't always assume one specific DB info's existence, thus checking its validity.
            auto db_info = tmt_context.getSchemaSyncer()->getDBInfoByMappedName(database_name);
            return db_info ? SchemaNameMapper().debugCanonicalName(*db_info, table_info)
                           : "(" + database_name + ")." + SchemaNameMapper().debugTableName(table_info);
        }();
        LOG_INFO(
            log,
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
            LOG_INFO(log, "Physically drop table {} end", canonical_name);
        }
        catch (DB::Exception & e)
        {
            succeeded = false;
            String err_msg;
            // Maybe a read lock of a table is held for a long time, just ignore it this round.
            if (e.code() == ErrorCodes::DEADLOCK_AVOIDED)
                err_msg = "locking attempt has timed out!"; // ignore verbose stack for this error
            else
                err_msg = getCurrentExceptionMessage(true);
            LOG_INFO(log, "Physically drop table {} is skipped, reason: {}", canonical_name, err_msg);
        }
    }
    storages_to_gc.clear();

    // Physically drop database
    for (const auto & iter : dbs)
    {
        const auto & db = iter.second;
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
            LOG_INFO(
                log,
                "Physically drop database {} is skipped, reason: {} tables left",
                db_name,
                num_tables);
            continue;
        }

        LOG_INFO(log, "Physically drop database begin, database_tombstone={} {}", db->getTombstone(), db_name);
        auto drop_query = std::make_shared<ASTDropQuery>();
        drop_query->database = db_name;
        drop_query->if_exists = true;
        drop_query->lock_timeout = std::chrono::milliseconds(1 * 1000); // timeout for acquring table drop lock
        ASTPtr ast_drop_query = drop_query;
        try
        {
            InterpreterDropQuery drop_interpreter(ast_drop_query, context);
            drop_interpreter.execute();
            LOG_INFO(log, "Physically drop database {} end, safepoint={}", db_name, gc_safepoint);
        }
        catch (DB::Exception & e)
        {
            succeeded = false;
            String err_msg;
            if (e.code() == ErrorCodes::DEADLOCK_AVOIDED)
                err_msg = "locking attempt has timed out!"; // ignore verbose stack for this error
            else
                err_msg = getCurrentExceptionMessage(true);
            LOG_INFO(log, "Physically drop database {} is skipped, reason: {}", db_name, err_msg);
        }
    }

    if (succeeded)
    {
        gc_context.last_gc_safepoint = gc_safepoint;
        LOG_INFO(
            log,
            "Schema GC done, tables_removed={} databases_removed={} safepoint={}",
            num_tables_removed,
            num_databases_removed,
            gc_safepoint);
    }
    else
    {
        // Don't update last_gc_safepoint and retry later
        LOG_INFO(
            log,
            "Schema GC meet error, will try again later, last_safepoint={} safepoint={}",
            gc_context.last_gc_safepoint,
            gc_safepoint);
    }

    return true;
}

} // namespace DB
