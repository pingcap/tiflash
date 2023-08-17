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

#include <Common/typeid_cast.h>
#include <Databases/DatabaseTiFlash.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Storages/IStorage.h>

#include <optional>


namespace DB
{
InterpreterRenameQuery::InterpreterRenameQuery(
    const ASTPtr & query_ptr_,
    Context & context_,
    const String executor_name_)
    : query_ptr(query_ptr_)
    , context(context_)
    , executor_name(std::move(executor_name_))
{}


struct RenameDescription
{
    RenameDescription(const ASTRenameQuery::Element & elem, const String & current_database)
        : from_database_name(elem.from.database.empty() ? current_database : elem.from.database)
        , from_table_name(elem.from.table)
        , to_database_name(elem.to.database.empty() ? current_database : elem.to.database)
        , to_table_name(elem.to.table)
        , tidb_display_database_name(
              elem.tidb_display.has_value() ? std::make_optional(elem.tidb_display->database) : std::nullopt)
        , tidb_display_table_name(
              elem.tidb_display.has_value() ? std::make_optional(elem.tidb_display->table) : std::nullopt)
    {
        if (tidb_display_database_name.has_value() && tidb_display_database_name->empty())
            throw Exception("Display database name is empty, should not happed. " + toString());

        if (tidb_display_table_name.has_value() && tidb_display_table_name->empty())
            throw Exception("Display table name is empty, should not happed. " + toString());
    }

    String from_database_name;
    String from_table_name;

    String to_database_name;
    String to_table_name;

    String toString() const
    {
        return from_database_name + "." + from_table_name + " -> " + to_database_name + "." + to_table_name;
    }

    bool hasTidbDisplayName() const
    {
        return tidb_display_database_name.has_value() && tidb_display_table_name.has_value();
    }

    std::optional<String> tidb_display_database_name;
    std::optional<String> tidb_display_table_name;
};


BlockIO InterpreterRenameQuery::execute()
{
    ASTRenameQuery & rename = typeid_cast<ASTRenameQuery &>(*query_ptr);

    String current_database = context.getCurrentDatabase();

    /** In case of error while renaming, it is possible that only part of tables was renamed
      *  or we will be in inconsistent state. (It is worth to be fixed.)
      */

    std::vector<RenameDescription> descriptions;
    descriptions.reserve(rename.elements.size());

    /// To avoid deadlocks, we must acquire locks for tables in same order in any different RENAMES.
    struct UniqueTableName
    {
        String database_name;
        String table_name;

        UniqueTableName(const String & database_name_, const String & table_name_) //
            : database_name(database_name_)
            , table_name(table_name_)
        {}

        bool operator<(const UniqueTableName & rhs) const
        {
            return std::tie(database_name, table_name) < std::tie(rhs.database_name, rhs.table_name);
        }
    };

    std::set<UniqueTableName> unique_tables_from;

    /// Don't allow to drop tables (that we are renaming); do't allow to create tables in places where tables will be renamed.
    std::map<UniqueTableName, std::unique_ptr<DDLGuard>> table_guards;

    for (const auto & elem : rename.elements)
    {
        descriptions.emplace_back(elem, current_database);

        UniqueTableName from(descriptions.back().from_database_name, descriptions.back().from_table_name);
        UniqueTableName to(descriptions.back().to_database_name, descriptions.back().to_table_name);

        unique_tables_from.emplace(from);

        if (!table_guards.count(to))
            table_guards.emplace(
                to,
                context.getDDLGuard(
                    to.table_name,
                    fmt::format("Some table right now is being renamed to {}.{}", to.database_name, to.table_name)));

        // Don't need any lock on "tidb_display" names, because we don't identify any table by that name in TiFlash
    }

    std::vector<TableLockHolder> alter_locks;
    alter_locks.reserve(unique_tables_from.size());

    for (const auto & names : unique_tables_from)
        if (auto table = context.tryGetTable(names.database_name, names.table_name))
            alter_locks.emplace_back(table->lockForAlter(executor_name));

    /** All tables are locked. If there are more than one rename in chain,
      *  we need to hold global lock while doing all renames. Order matters to avoid deadlocks.
      * It provides atomicity of all RENAME chain as a whole, from the point of view of DBMS client,
      *  but only in cases when there was no exceptions during this process and server does not fall.
      */

    decltype(context.getLock()) lock;

    if (descriptions.size() > 1)
        lock = context.getLock();

    for (const auto & elem : descriptions)
    {
        // For rename query synced from TiDB, we may do "rename db_1.t2 to db_1.t2" with only modifying
        // table's display name, we need to avoid exception for that.
        if (elem.from_database_name != elem.to_database_name || elem.from_table_name != elem.to_table_name)
            context.assertTableDoesntExist(elem.to_database_name, elem.to_table_name);

        auto database = context.getDatabase(elem.from_database_name);
        if (database->getEngineName() == "TiFlash")
        {
            auto * from_database_concrete = typeid_cast<DatabaseTiFlash *>(database.get());
            if (likely(from_database_concrete))
            {
                // Keep for rename actions executed through ch-client.
                const String & display_db
                    = elem.hasTidbDisplayName() ? *elem.tidb_display_database_name : elem.to_database_name;
                const String & display_tbl
                    = elem.hasTidbDisplayName() ? *elem.tidb_display_table_name : elem.to_table_name;
                from_database_concrete->renameTable(
                    context,
                    elem.from_table_name,
                    *context.getDatabase(elem.to_database_name),
                    elem.to_table_name,
                    display_db,
                    display_tbl);
            }
            else
                throw Exception(
                    "Failed to cast from database: " + elem.from_database_name
                        + " as DatabaseTiFlash in renaming: " + elem.toString(),
                    ErrorCodes::LOGICAL_ERROR);
        }
        else
        {
            // Keep for DatabaseOrdinary and others engines.
            database->renameTable(
                context,
                elem.from_table_name,
                *context.getDatabase(elem.to_database_name),
                elem.to_table_name);
        }
    }

    return {};
}


} // namespace DB
