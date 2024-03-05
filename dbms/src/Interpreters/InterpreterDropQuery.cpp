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
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <Databases/IDatabase.h>
#include <IO/FileProvider/FileProvider.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Poco/File.h>
#include <Storages/IManageableStorage.h>
#include <Storages/IStorage.h>
#include <common/logger_useful.h>

#include <ext/scope_guard.h>


namespace DB
{
namespace ErrorCodes
{
extern const int TABLE_WAS_NOT_DROPPED;
extern const int DATABASE_NOT_EMPTY;
extern const int UNKNOWN_DATABASE;
extern const int READONLY;
extern const int FAIL_POINT_ERROR;
} // namespace ErrorCodes

namespace FailPoints
{
extern const char exception_between_drop_meta_and_data[];
}

InterpreterDropQuery::InterpreterDropQuery(const ASTPtr & query_ptr_, Context & context_)
    : query_ptr(query_ptr_)
    , context(context_)
{}


BlockIO InterpreterDropQuery::execute()
{
    ASTDropQuery & drop = typeid_cast<ASTDropQuery &>(*query_ptr);

    checkAccess(drop);

    String path = context.getPath();
    String current_database = context.getCurrentDatabase();

    bool drop_database = drop.table.empty() && !drop.database.empty();

    if (drop_database && drop.detach)
    {
        auto database = context.detachDatabase(drop.database);
        database->shutdown();
        return {};
    }

    /// Drop temporary table.
    if (drop.database.empty() || drop.temporary)
    {
        StoragePtr table
            = (context.hasSessionContext() ? context.getSessionContext() : context).tryRemoveExternalTable(drop.table);
        if (table)
        {
            if (drop.database.empty() && !drop.temporary)
            {
                LOG_WARNING(
                    (&Poco::Logger::get("InterpreterDropQuery")),
                    "It is recommended to use `DROP TEMPORARY TABLE` to delete temporary tables");
            }
            table->shutdown();
            /// If table was already dropped by anyone, an exception will be thrown
            auto table_lock = table->lockExclusively(context.getCurrentQueryId(), drop.lock_timeout);
            /// Delete table data
            table->drop();
            table->is_dropped = true;
            return {};
        }
    }

    String database_name = drop.database.empty() ? current_database : drop.database;
    String database_name_escaped = escapeForFileName(database_name);

    auto database = context.tryGetDatabase(database_name);
    if (!database && !drop.if_exists)
        throw Exception("Database " + database_name + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);

    std::vector<std::pair<StoragePtr, std::unique_ptr<DDLGuard>>> tables_to_drop;

    if (!drop_database)
    {
        StoragePtr table;

        if (drop.if_exists)
            table = context.tryGetTable(database_name, drop.table);
        else
            table = context.getTable(database_name, drop.table);

        if (table)
            tables_to_drop.emplace_back(
                table,
                context.getDDLGuard(
                    drop.table,
                    fmt::format("Table {}.{} is dropping or detaching right now", database_name, drop.table)));
        else
            return {};
    }
    else
    {
        if (!database)
        {
            if (!drop.if_exists)
                throw Exception("Database " + database_name + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);
            return {};
        }

        for (auto iterator = database->getIterator(context); iterator->isValid(); iterator->next())
            tables_to_drop.emplace_back(
                iterator->table(),
                context.getDDLGuard(
                    iterator->name(),
                    fmt::format("Table {}.{} is dropping or detaching right now", database_name, iterator->name())));
    }

    for (auto & table : tables_to_drop)
    {
        if (!drop.detach)
        {
            if (!table.first->checkTableCanBeDropped())
                throw Exception(
                    "Table " + database_name + "." + table.first->getTableName()
                        + " couldn't be dropped due to failed pre-drop check",
                    ErrorCodes::TABLE_WAS_NOT_DROPPED);
        }

        /// If table was already dropped by anyone, an exception will be thrown;
        /// If can not acquire the drop lock on table within `drop.lock_timeout`,
        /// an exception will be thrown;
        auto table_lock = table.first->lockExclusively(context.getCurrentQueryId(), drop.lock_timeout);

        table.first->shutdown();

        String current_table_name = table.first->getTableName();

        if (drop.detach)
        {
            /// Drop table from memory, don't touch data and metadata
            database->detachTable(current_table_name);
        }
        else
        {
            /// Clear storage data first, and if tiflash crash in the middle of `clearData`,
            /// this table can still be restored, and can call `clearData` again.
            table.first->clearData();

            /// Delete table metdata and table itself from memory
            database->removeTable(context, current_table_name);

            SCOPE_EXIT({
                // Once the storage's metadata removed from disk, we should ensure that it is removed from TMTStorages
                if (auto storage = std::dynamic_pointer_cast<IManageableStorage>(table.first); storage)
                    storage->removeFromTMTContext();
            });

            FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_between_drop_meta_and_data);

            /// Delete table data
            table.first->drop();

            table.first->is_dropped = true;

            /// If it is not virtual database like Dictionary then drop remaining data dir
            const String database_data_path = database->getDataPath();
            if (!database_data_path.empty())
            {
                String table_data_path = database_data_path + (endsWith(database_data_path, "/") ? "" : "/")
                    + escapeForFileName(current_table_name);

                if (Poco::File(table_data_path).exists())
                {
                    context.getFileProvider()->deleteDirectory(table_data_path, false, true);
                }
            }
        }
    }

    if (drop_database)
    {
        /// Delete the database. The tables in it have already been deleted.

        auto lock = context.getLock();

        /// Someone could have time to delete the database before us.
        context.assertDatabaseExists(database_name);

        /// Someone could have time to create a table in the database to be deleted while we deleted the tables without the context lock.
        if (!context.getDatabase(database_name)->empty(context))
            throw Exception(
                "New table appeared in database being dropped. Try dropping it again.",
                ErrorCodes::DATABASE_NOT_EMPTY);

        /// Delete database information from the RAM
        auto database = context.detachDatabase(database_name);

        /// Delete the database and remove its data / meta directory if need.
        database->drop(context);
    }

    return {};
}


void InterpreterDropQuery::checkAccess(const ASTDropQuery & drop)
{
    const Settings & settings = context.getSettingsRef();
    auto readonly = settings.readonly;

    /// It's allowed to drop temporary tables.
    if (!readonly || (drop.database.empty() && context.tryGetExternalTable(drop.table) && readonly >= 2))
    {
        return;
    }

    throw Exception("Cannot drop table in readonly mode", ErrorCodes::READONLY);
}

} // namespace DB
