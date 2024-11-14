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

#include <Common/Stopwatch.h>
#include <Databases/DatabasesCommon.h>
#include <IO/FileProvider/ReadBufferFromRandomAccessFileBuilder.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Poco/DirectoryIterator.h>
#include <Storages/PrimaryKeyNotMatchException.h>
#include <Storages/StorageFactory.h>
#include <common/logger_useful.h>
#include <fmt/core.h>

#include <sstream>

namespace DB
{
namespace ErrorCodes
{
extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
extern const int TABLE_ALREADY_EXISTS;
extern const int UNKNOWN_TABLE;
extern const int LOGICAL_ERROR;
extern const int INCORRECT_FILE_NAME;
extern const int CANNOT_CREATE_TABLE_FROM_METADATA;
extern const int SYNTAX_ERROR;
} // namespace ErrorCodes


String getTableDefinitionFromCreateQuery(const ASTPtr & query)
{
    ASTPtr query_clone = query->clone();
    ASTCreateQuery & create = typeid_cast<ASTCreateQuery &>(*query_clone.get());

    /// We remove everything that is not needed for ATTACH from the query.
    create.attach = true;
    create.database.clear();
    create.if_not_exists = false;
    create.is_populate = false;

    create.format = nullptr;
    create.out_file = nullptr;

    std::ostringstream statement_stream;
    formatAST(create, statement_stream, false);
    statement_stream << '\n';
    return statement_stream.str();
}

String getDatabaseDefinitionFromCreateQuery(const ASTPtr & query)
{
    ASTPtr query_clone = query->clone();
    ASTCreateQuery & create = typeid_cast<ASTCreateQuery &>(*query_clone.get());

    /// We remove everything that is not needed for ATTACH from the query
    create.attach = true;
    create.table.clear();
    create.if_not_exists = false;
    create.is_populate = false;

    create.format = nullptr;
    create.out_file = nullptr;

    std::ostringstream statement_stream;
    formatAST(create, statement_stream, false);
    statement_stream << '\n';
    return statement_stream.str();
}


std::pair<String, StoragePtr> createTableFromDefinition(
    const String & definition,
    const String & database_name,
    const String & database_data_path,
    const String & database_engine,
    Context & context,
    bool has_force_restore_data_flag,
    const String & description_for_error_message)
{
    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(
        parser,
        definition.data(),
        definition.data() + definition.size(),
        description_for_error_message,
        0);

    ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
    ast_create_query.attach = true;
    ast_create_query.database = database_name;

    /// We do not directly use `InterpreterCreateQuery::execute`, because
    /// - the database has not been created yet;
    /// - the code is simpler, since the query is already brought to a suitable form.
    if (!ast_create_query.columns)
        throw Exception("Missing definition of columns.", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);

    ColumnsDescription columns = InterpreterCreateQuery::getColumnsDescription(*ast_create_query.columns, context);

    return {
        ast_create_query.table,
        StorageFactory::instance().get(
            ast_create_query,
            database_data_path,
            ast_create_query.table,
            database_name,
            database_engine,
            context,
            context.getGlobalContext(),
            columns,
            true,
            has_force_restore_data_flag)};
}


bool DatabaseWithOwnTablesBase::isTableExist(const Context & /*context*/, const String & table_name) const
{
    std::lock_guard lock(mutex);
    return tables.find(table_name) != tables.end();
}

StoragePtr DatabaseWithOwnTablesBase::tryGetTable(const Context & /*context*/, const String & table_name) const
{
    std::lock_guard lock(mutex);
    auto it = tables.find(table_name);
    if (it == tables.end())
        return {};
    return it->second;
}

DatabaseIteratorPtr DatabaseWithOwnTablesBase::getIterator(const Context & /*context*/)
{
    std::lock_guard lock(mutex);
    return std::make_unique<DatabaseSnapshotIterator>(tables);
}

bool DatabaseWithOwnTablesBase::empty(const Context & /*context*/) const
{
    std::lock_guard lock(mutex);
    return tables.empty();
}

StoragePtr DatabaseWithOwnTablesBase::detachTable(const String & table_name)
{
    StoragePtr res;
    {
        std::lock_guard lock(mutex);
        auto it = tables.find(table_name);
        if (it == tables.end())
            throw Exception(fmt::format("Table {}.{} dosen't exist.", name, table_name), ErrorCodes::UNKNOWN_TABLE);
        res = it->second;
        tables.erase(it);
    }

    return res;
}

void DatabaseWithOwnTablesBase::attachTable(const String & table_name, const StoragePtr & table)
{
    std::lock_guard lock(mutex);
    if (!tables.emplace(table_name, table).second)
        throw Exception(fmt::format("Table {}.{} already exists.", name, table_name), ErrorCodes::TABLE_ALREADY_EXISTS);
}

void DatabaseWithOwnTablesBase::shutdown()
{
    /// You can not hold a lock during shutdown.
    /// Because inside `shutdown` function tables can work with database, and mutex is not recursive.

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

DatabaseWithOwnTablesBase::~DatabaseWithOwnTablesBase()
{
    try
    {
        DatabaseWithOwnTablesBase::shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

namespace DatabaseLoading
{
ASTPtr getQueryFromMetadata(const Context & context, const String & metadata_path, bool throw_on_error)
{
    if (!Poco::File(metadata_path).exists())
        return nullptr;

    String query;
    {
        auto in = ReadBufferFromRandomAccessFileBuilder::build(
            context.getFileProvider(),
            metadata_path,
            EncryptionPath(metadata_path, ""),
            4096);
        readStringUntilEOF(query, in);
    }

    ParserCreateQuery parser;
    const char * pos = query.data();
    std::string error_message;
    auto ast = tryParseQuery(
        parser,
        pos,
        pos + query.size(),
        error_message,
        /* hilite = */ false,
        "in file " + metadata_path,
        /* allow_multi_statements = */ false,
        0);

    if (!ast && throw_on_error)
        throw Exception(error_message, ErrorCodes::SYNTAX_ERROR);

    return ast;
}

ASTPtr getCreateQueryFromMetadata(
    const Context & context,
    const String & metadata_path,
    const String & database,
    bool throw_on_error)
{
    ASTPtr ast = DatabaseLoading::getQueryFromMetadata(context, metadata_path, throw_on_error);
    if (ast)
    {
        ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
        ast_create_query.attach = false;
        ast_create_query.database = database;
    }
    return ast;
}


std::vector<String> listSQLFilenames(const String & meta_dir, Poco::Logger * log)
{
    std::vector<String> filenames;
    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator dir_it(meta_dir); dir_it != dir_end; ++dir_it)
    {
        // Ignore directories
        if (!dir_it->isFile())
            continue;

        /// For '.svn', '.gitignore' directory and similar.
        if (dir_it.name().at(0) == '.')
            continue;

        /// There are .sql.bak files - skip them.
        if (endsWith(dir_it.name(), ".sql.bak"))
            continue;

        /// There are files .sql.tmp - delete.
        if (endsWith(dir_it.name(), ".sql.tmp"))
        {
            LOG_INFO(log, "Removing file {}", dir_it->path());
            Poco::File(dir_it->path()).remove();
            continue;
        }

        /// The required files have names like `table_name.sql`
        if (endsWith(dir_it.name(), ".sql"))
            filenames.push_back(dir_it.name());
        else
            throw Exception(
                fmt::format("Incorrect file extension: {} in metadata directory {}", dir_it.name(), meta_dir),
                ErrorCodes::INCORRECT_FILE_NAME);
    }
    return filenames;
}

std::tuple<String, StoragePtr> loadTable(
    Context & context,
    IDatabase & database,
    const String & database_metadata_path,
    const String & database_name,
    const String & database_data_path,
    const String & database_engine,
    const String & file_name,
    bool has_force_restore_data_flag)
{
    Poco::Logger * log = &Poco::Logger::get("loadTable");
    const String table_metadata_path
        = database_metadata_path + (endsWith(database_metadata_path, "/") ? "" : "/") + file_name;

    String s;
    {
        auto in = ReadBufferFromRandomAccessFileBuilder::build(
            context.getFileProvider(),
            table_metadata_path,
            EncryptionPath(table_metadata_path, ""),
            1024);
        readStringUntilEOF(s, in);
    }

    /** Empty files with metadata are generated after a rough restart of the server.
      * Remove these files to slightly reduce the work of the admins on startup.
      */
    if (s.empty())
    {
        LOG_ERROR(log, "File {} is empty. Removing.", table_metadata_path);
        Poco::File(table_metadata_path).remove();
        return std::make_tuple("", nullptr);
    }

    try
    {
        String table_name;
        StoragePtr table;
        try
        {
            std::tie(table_name, table) = createTableFromDefinition(
                s,
                database_name,
                database_data_path,
                database_engine,
                context,
                has_force_restore_data_flag,
                "in file " + table_metadata_path);
        }
        catch (const PrimaryKeyNotMatchException & pri_key_ex)
        {
            // Replace the primary key and update statement in `table_metadata_path`. The correct statement will be return.
            const String statement
                = fixCreateStatementWithPriKeyNotMatchException(context, s, table_metadata_path, pri_key_ex, log);
            // Then try to load with correct statement.
            std::tie(table_name, table) = createTableFromDefinition(
                statement,
                database_name,
                database_data_path,
                database_engine,
                context,
                has_force_restore_data_flag,
                "in file " + table_metadata_path);
        }
        database.attachTable(table_name, table);
        return std::make_tuple(table_name, table);
    }
    catch (const Exception & e)
    {
        throw Exception(
            fmt::format(
                "Cannot create table from metadata file {}, error: {}, stack trace:\n{}",
                table_metadata_path,
                e.displayText(),
                e.getStackTrace().toString()),
            ErrorCodes::CANNOT_CREATE_TABLE_FROM_METADATA);
    }

    return std::make_tuple("", nullptr);
}

void cleanupTables(IDatabase & database, const String & db_name, const Tables & tables, Poco::Logger * log)
{
    if (tables.empty())
        return;

    for (const auto & table : tables)
    {
        const String & table_name = table.first;
        LOG_WARNING(log, "Detected startup failed table {}.{}, removing it from TiFlash", db_name, table_name);
        const String table_meta_path = database.getTableMetadataPath(table_name);
        if (!table_meta_path.empty())
        {
            Poco::File{table_meta_path}.remove();
        }
        // detach from this database
        database.detachTable(table_name);
    }
}
} // namespace DatabaseLoading

} // namespace DB
