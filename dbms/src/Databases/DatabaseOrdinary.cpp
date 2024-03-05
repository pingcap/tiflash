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
#include <Common/Stopwatch.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/UniThreadPool.h>
#include <Common/escapeForFileName.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/DatabaseOrdinary.h>
#include <Databases/DatabasesCommon.h>
#include <IO/FileProvider/FileProvider.h>
#include <IO/FileProvider/ReadBufferFromRandomAccessFileBuilder.h>
#include <IO/FileProvider/WriteBufferFromWritableFileBuilder.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/Settings.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Poco/DirectoryIterator.h>
#include <common/logger_useful.h>
#include <fmt/core.h>

namespace DB
{
namespace ErrorCodes
{
extern const int TABLE_ALREADY_EXISTS;
extern const int UNKNOWN_TABLE;
extern const int CANNOT_CREATE_TABLE_FROM_METADATA;
extern const int INCORRECT_FILE_NAME;
extern const int FILE_DOESNT_EXIST;
extern const int LOGICAL_ERROR;
extern const int CANNOT_GET_CREATE_TABLE_QUERY;
extern const int SYNTAX_ERROR;
extern const int TIDB_TABLE_ALREADY_EXISTS;
} // namespace ErrorCodes

namespace FailPoints
{
extern const char exception_drop_table_during_remove_meta[];
extern const char exception_between_rename_table_data_and_metadata[];
} // namespace FailPoints

static constexpr size_t PRINT_MESSAGE_EACH_N_TABLES = 256;
static constexpr size_t PRINT_MESSAGE_EACH_N_SECONDS = 5;
static constexpr size_t METADATA_FILE_BUFFER_SIZE = 32768;
static constexpr size_t TABLES_PARALLEL_LOAD_BUNCH_SIZE = 100;

namespace detail
{
String getTableMetadataPath(const String & base_path, const String & table_name)
{
    return base_path + (endsWith(base_path, "/") ? "" : "/") + escapeForFileName(table_name) + ".sql";
}

String getDatabaseMetadataPath(const String & base_path)
{
    return (endsWith(base_path, "/") ? base_path.substr(0, base_path.size() - 1) : base_path) + ".sql";
}

} // namespace detail

DatabaseOrdinary::DatabaseOrdinary(String name_, const String & metadata_path_, const Context & context)
    : DatabaseWithOwnTablesBase(std::move(name_))
    , metadata_path(metadata_path_)
    , data_path(context.getPath() + "data/" + escapeForFileName(name) + "/")
    , log(&Poco::Logger::get("DatabaseOrdinary (" + name + ")"))
{
    Poco::File(data_path).createDirectories();
}


void DatabaseOrdinary::loadTables(Context & context, ThreadPool * thread_pool, bool has_force_restore_data_flag)
{
    using FileNames = std::vector<std::string>;
    FileNames file_names = DatabaseLoading::listSQLFilenames(metadata_path, log);

    /** Tables load faster if they are loaded in sorted (by name) order.
      * Otherwise (for the ext4 filesystem), `DirectoryIterator` iterates through them in some order,
      *  which does not correspond to order tables creation and does not correspond to order of their location on disk.
      */
    std::sort(file_names.begin(), file_names.end());

    size_t total_tables = file_names.size();
    LOG_INFO(log, "Total {} tables.", total_tables);

    String data_path = context.getPath() + "data/" + escapeForFileName(name) + "/";

    AtomicStopwatch watch;
    std::atomic<size_t> tables_processed{0};

    auto wait_group = thread_pool ? thread_pool->waitGroup() : nullptr;

    std::mutex failed_tables_mutex;
    Tables tables_failed_to_startup;

    auto task_function = [&](FileNames::const_iterator begin, FileNames::const_iterator end) {
        for (auto it = begin; it != end; ++it)
        {
            const String & table_file = *it;

            /// Messages, so that it's not boring to wait for the server to load for a long time.
            if ((++tables_processed) % PRINT_MESSAGE_EACH_N_TABLES == 0
                || watch.compareAndRestart(PRINT_MESSAGE_EACH_N_SECONDS))
            {
                LOG_INFO(log, "{:.2f}%", tables_processed * 100.0 / total_tables);
                watch.restart();
            }

            auto [table_name, table] = DatabaseLoading::loadTable(
                context,
                *this,
                metadata_path,
                name,
                data_path,
                getEngineName(),
                table_file,
                has_force_restore_data_flag);

            /// After table was basically initialized, startup it.
            if (table)
            {
                try
                {
                    table->startup();
                }
                catch (DB::Exception & e)
                {
                    if (e.code() == ErrorCodes::TIDB_TABLE_ALREADY_EXISTS)
                    {
                        // While doing IStorage::startup, Exception thorwn with TIDB_TABLE_ALREADY_EXISTS,
                        // means that we may crashed in the middle of renaming tables. We clean the meta file
                        // for those storages by `cleanupTables`.
                        // - If the storage is the outdated one after renaming, remove it is right.
                        // - If the storage should be the target table, remove it means we "rollback" the
                        //   rename action. And the table will be renamed by TiDBSchemaSyncer later.
                        std::lock_guard lock(failed_tables_mutex);
                        tables_failed_to_startup.emplace(table_name, table);
                    }
                    else
                        throw;
                }
            }
        }
    };

    const size_t bunch_size = TABLES_PARALLEL_LOAD_BUNCH_SIZE;
    size_t num_bunches = (total_tables + bunch_size - 1) / bunch_size;

    for (size_t i = 0; i < num_bunches; ++i)
    {
        auto begin = file_names.begin() + i * bunch_size;
        auto end = (i + 1 == num_bunches) ? file_names.end() : (file_names.begin() + (i + 1) * bunch_size);

        auto task = [&task_function, begin, end] {
            task_function(begin, end);
        };

        if (thread_pool)
            wait_group->schedule(task);
        else
            task();
    }

    if (thread_pool)
        wait_group->wait();

    DatabaseLoading::cleanupTables(*this, name, tables_failed_to_startup, log);
}

void DatabaseOrdinary::createTable(const Context & context, const String & table_name, const ASTPtr & query)
{
    const auto & settings = context.getSettingsRef();

    /// Create a file with metadata if necessary - if the query is not ATTACH.
    /// Write the query of `ATTACH table` to it.

    /** The code is based on the assumption that all threads share the same order of operations
      * - creating the .sql.tmp file;
      * - adding a table to `tables`;
      * - rename .sql.tmp to .sql.
      */

    /// A race condition would be possible if a table with the same name is simultaneously created using CREATE and using ATTACH.
    /// But there is protection from it - see using DDLGuard in InterpreterCreateQuery.

    {
        std::lock_guard lock(mutex);
        if (tables.find(table_name) != tables.end())
            throw Exception(
                fmt::format("Table {}.{} already exists.", name, table_name),
                ErrorCodes::TABLE_ALREADY_EXISTS);
    }

    String table_metadata_path = getTableMetadataPath(table_name);
    String table_metadata_tmp_path = table_metadata_path + ".tmp";
    String statement;

    {
        statement = getTableDefinitionFromCreateQuery(query);

        /// Exclusive flags guarantees, that table is not created right now in another thread. Otherwise, exception will be thrown.
        auto out = WriteBufferFromWritableFileBuilder::build(
            context.getFileProvider(),
            table_metadata_tmp_path,
            EncryptionPath(table_metadata_tmp_path, ""),
            true,
            nullptr,
            statement.size(),
            O_WRONLY | O_CREAT | O_EXCL);
        writeString(statement, out);
        out.next();
        if (settings.fsync_metadata)
            out.sync();
        out.close();
    }

    try
    {
        context.getFileProvider()->renameFile(
            table_metadata_tmp_path,
            EncryptionPath(table_metadata_tmp_path, ""),
            table_metadata_path,
            EncryptionPath(table_metadata_path, ""),
            true);
    }
    catch (...)
    {
        context.getFileProvider()->deleteRegularFile(
            table_metadata_tmp_path,
            EncryptionPath(table_metadata_tmp_path, ""));
        throw;
    }
}


void DatabaseOrdinary::removeTable(const Context & /*context*/, const String & table_name)
{
    StoragePtr res = detachTable(table_name);

    String table_metadata_path = getTableMetadataPath(table_name);

    try
    {
        // If tiflash crash before remove metadata, next time it restart, will
        // full apply schema from TiDB. And the old table's metadata and data
        // will be removed.
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_drop_table_during_remove_meta);
        Poco::File(table_metadata_path).remove();
    }
    catch (...)
    {
        attachTable(table_name, res);
        throw;
    }
}

void DatabaseOrdinary::renameTable(
    const Context & context,
    const String & table_name,
    IDatabase & to_database,
    const String & to_table_name)
{
    auto * to_database_concrete = typeid_cast<DatabaseOrdinary *>(&to_database);

    if (!to_database_concrete)
        throw Exception(
            "Moving tables between databases of different engines is not supported",
            ErrorCodes::NOT_IMPLEMENTED);

    StoragePtr table = tryGetTable(context, table_name);

    if (!table)
        throw Exception(fmt::format("Table {}.{} doesn't exist.", name, table_name), ErrorCodes::UNKNOWN_TABLE);

    /// Notify the table that it is renamed. If the table does not support renaming, exception is thrown.
    try
    {
        table->rename(
            fmt::format("{}/data/{}/", context.getPath(), escapeForFileName(to_database_concrete->name)),
            to_database_concrete->name,
            to_table_name);
    }
    catch (const Exception & e)
    {
        throw;
    }
    catch (const Poco::Exception & e)
    {
        /// Better diagnostics.
        throw Exception{e};
    }

    // TODO: Atomic rename table is not fixed.
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_between_rename_table_data_and_metadata);

    ASTPtr ast
        = DatabaseLoading::getQueryFromMetadata(context, detail::getTableMetadataPath(metadata_path, table_name));
    if (!ast)
        throw Exception(
            fmt::format("There is no metadata file for table {}", table_name),
            ErrorCodes::FILE_DOESNT_EXIST);
    ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
    ast_create_query.table = to_table_name;

    /// NOTE Non-atomic.
    // Create new metadata and remove old metadata.
    to_database_concrete->createTable(context, to_table_name, ast);
    to_database_concrete->attachTable(to_table_name, table);
    removeTable(context, table_name);
}


time_t DatabaseOrdinary::getTableMetadataModificationTime(const Context & /*context*/, const String & table_name)
{
    String table_metadata_path = getTableMetadataPath(table_name);
    Poco::File meta_file(table_metadata_path);

    if (meta_file.exists())
    {
        return meta_file.getLastModified().epochTime();
    }
    else
    {
        return static_cast<time_t>(0);
    }
}

ASTPtr DatabaseOrdinary::getCreateTableQueryImpl(
    const Context & context,
    const String & table_name,
    bool throw_on_error) const
{
    ASTPtr ast;

    auto table_metadata_path = detail::getTableMetadataPath(metadata_path, table_name);
    ast = DatabaseLoading::getCreateQueryFromMetadata(context, table_metadata_path, name, throw_on_error);
    if (!ast && throw_on_error)
    {
        /// Handle system.* tables for which there are no table.sql files.
        bool has_table = tryGetTable(context, table_name) != nullptr;

        const auto * msg
            = has_table ? "There is no CREATE TABLE query for table " : "There is no metadata file for table ";

        throw Exception(fmt::format("{}{}", msg, table_name), ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY);
    }

    return ast;
}

ASTPtr DatabaseOrdinary::getCreateTableQuery(const Context & context, const String & table_name) const
{
    return getCreateTableQueryImpl(context, table_name, true);
}

ASTPtr DatabaseOrdinary::tryGetCreateTableQuery(const Context & context, const String & table_name) const
{
    return getCreateTableQueryImpl(context, table_name, false);
}

ASTPtr DatabaseOrdinary::getCreateDatabaseQuery(const Context & context) const
{
    ASTPtr ast;

    auto database_metadata_path = detail::getDatabaseMetadataPath(metadata_path);
    ast = DatabaseLoading::getCreateQueryFromMetadata(context, database_metadata_path, name, true);
    if (!ast)
    {
        /// Handle databases (such as default) for which there are no database.sql files.
        String query = "CREATE DATABASE " + backQuoteIfNeed(name) + " ENGINE = Ordinary";
        ParserCreateQuery parser;
        ast = parseQuery(parser, query.data(), query.data() + query.size(), "", 0);
    }

    return ast;
}


void DatabaseOrdinary::shutdown()
{
    /// You can not hold a lock during shutdown.
    /// Because inside `shutdown` function the tables can work with database, and mutex is not recursive.

    Tables tables_snapshot;
    {
        std::lock_guard lock(mutex);
        tables_snapshot = tables;
    }

    for (const auto & kv : tables_snapshot)
    {
        kv.second->shutdown();
    }

    std::lock_guard lock(mutex);
    tables.clear();
}


void DatabaseOrdinary::drop(const Context & context)
{
    if (context.getFileProvider()->isEncryptionEnabled())
    {
        LOG_WARNING(log, "Encryption is enabled. There may be some encryption info left.");
    }
    // Remove data dir for this database
    const String database_data_path = getDataPath();
    if (!database_data_path.empty())
        context.getFileProvider()->deleteDirectory(database_data_path, false, false);
    // Remove metadata dir for this database
    if (auto dir = Poco::File(getMetadataPath()); dir.exists())
    {
        context.getFileProvider()->deleteDirectory(getMetadataPath(), false, false);
    }

    /// Old ClickHouse versions did not store database.sql files
    if (auto meta_file = Poco::File(detail::getDatabaseMetadataPath(getMetadataPath())); meta_file.exists())
    {
        context.getFileProvider()->deleteRegularFile(
            detail::getDatabaseMetadataPath(getMetadataPath()),
            EncryptionPath(detail::getDatabaseMetadataPath(getMetadataPath()), ""));
    }
}

void DatabaseOrdinary::alterTable(
    const Context & context,
    const String & name,
    const ColumnsDescription & columns,
    const ASTModifier & storage_modifier)
{
    /// Read the definition of the table and replace the necessary parts with new ones.

    String table_name_escaped = escapeForFileName(name);
    String table_metadata_tmp_path = metadata_path + "/" + table_name_escaped + ".sql.tmp";
    String table_metadata_path = metadata_path + "/" + table_name_escaped + ".sql";
    String statement;

    {
        char in_buf[METADATA_FILE_BUFFER_SIZE];
        auto in = ReadBufferFromRandomAccessFileBuilder::build(
            context.getFileProvider(),
            table_metadata_path,
            EncryptionPath(table_metadata_path, ""),
            METADATA_FILE_BUFFER_SIZE,
            /*read_limiter*/ nullptr,
            -1,
            in_buf);
        readStringUntilEOF(statement, in);
    }

    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(
        parser,
        statement.data(),
        statement.data() + statement.size(),
        "in file " + table_metadata_path,
        0);

    ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);

    ASTPtr new_columns = InterpreterCreateQuery::formatColumns(columns);
    ast_create_query.replace(ast_create_query.columns, new_columns);

    if (storage_modifier)
        storage_modifier(*ast_create_query.storage);

    statement = getTableDefinitionFromCreateQuery(ast);

    bool use_target_encrypt_info = context.getFileProvider()->isFileEncrypted(EncryptionPath(table_metadata_path, ""));
    EncryptionPath encryption_path = use_target_encrypt_info ? EncryptionPath(table_metadata_path, "")
                                                             : EncryptionPath(table_metadata_tmp_path, "");
    {
        bool create_new_encryption_info = !use_target_encrypt_info && !statement.empty();
        auto out = WriteBufferFromWritableFileBuilder::build(
            context.getFileProvider(),
            table_metadata_tmp_path,
            encryption_path,
            create_new_encryption_info,
            nullptr,
            statement.size(),
            O_WRONLY | O_CREAT | O_EXCL);
        writeString(statement, out);
        out.next();
        if (context.getSettingsRef().fsync_metadata)
            out.sync();
        out.close();
    }

    try
    {
        /// rename atomically replaces the old file with the new one.
        context.getFileProvider()->renameFile(
            table_metadata_tmp_path,
            encryption_path,
            table_metadata_path,
            EncryptionPath(table_metadata_path, ""),
            !use_target_encrypt_info);
    }
    catch (...)
    {
        context.getFileProvider()->deleteRegularFile(table_metadata_tmp_path, encryption_path);
        throw;
    }
}

String DatabaseOrdinary::getDataPath() const
{
    return data_path;
}

String DatabaseOrdinary::getMetadataPath() const
{
    return metadata_path;
}

String DatabaseOrdinary::getTableMetadataPath(const String & table_name) const
{
    return detail::getTableMetadataPath(metadata_path, table_name);
}

} // namespace DB
