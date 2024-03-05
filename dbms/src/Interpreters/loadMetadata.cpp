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
#include <Common/escapeForFileName.h>
#include <Databases/DatabaseOrdinary.h>
#include <Databases/DatabaseTiFlash.h>
#include <Databases/DatabasesCommon.h>
#include <IO/FileProvider/ReadBufferFromRandomAccessFileBuilder.h>
#include <IO/IOThreadPools.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/loadMetadata.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/FileStream.h>
#include <Storages/KVStore/TMTContext.h>
#include <TiDB/Schema/SchemaNameMapper.h>
#include <TiDB/Schema/SchemaSyncer.h>
#include <common/ThreadPool.h>

#include <thread>


namespace DB
{
static void executeCreateQuery(
    const String & query,
    Context & context,
    const String & database,
    const String & file_name,
    ThreadPool * pool,
    bool has_force_restore_data_flag)
{
    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "in file " + file_name, 0);

    ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
    ast_create_query.attach = true;
    ast_create_query.database = database;

    InterpreterCreateQuery interpreter(ast, context);
    interpreter.setInternal(true);
    if (pool)
        interpreter.setDatabaseLoadingThreadpool(*pool);
    interpreter.setForceRestoreData(has_force_restore_data_flag);
    interpreter.execute();
}

#define SYSTEM_DATABASE "system"

static void loadDatabase(
    Context & context,
    const String & database,
    const String & database_metadata_file,
    ThreadPool * thread_pool,
    bool force_restore_data)
{
    /// There may exist .sql file with database creation statement.
    /// Or, if it is absent, then database with default engine is created.
    String database_attach_query;

    if (Poco::File(database_metadata_file).exists())
    {
        auto in = ReadBufferFromRandomAccessFileBuilder::build(
            context.getFileProvider(),
            database_metadata_file,
            EncryptionPath(database_metadata_file, ""),
            1024);
        readStringUntilEOF(database_attach_query, in);
    }
    else
    {
        // Old fashioned way, keep engine as "Ordinary"
        database_attach_query = "ATTACH DATABASE " + backQuoteIfNeed(database) + " ENGINE=Ordinary";
    }

    executeCreateQuery(
        database_attach_query,
        context,
        database,
        database_metadata_file,
        thread_pool,
        force_restore_data);
}

void loadMetadata(Context & context)
{
    const String path = context.getPath() + "metadata/";

    /** There may exist 'force_restore_data' file, that means,
      *  skip safety threshold on difference of data parts while initializing tables.
      * This file is deleted after successful loading of tables.
      * (flag is "one-shot")
      */
    Poco::File force_restore_data_flag_file(context.getFlagsPath() + "force_restore_data");
    bool has_force_restore_data_flag = force_restore_data_flag_file.exists();

    Poco::Logger * log = &Poco::Logger::get("loadMetadata");

    /// Loop over databases sql files. This ensure filename ends with ".sql".
    std::map<String, String> databases;
    auto sql_files = DatabaseLoading::listSQLFilenames(path, log);
    for (const auto & file : sql_files)
    {
        const auto db_name = unescapeForFileName(file.substr(0, file.size() - strlen(".sql")));
        // Ignore "system" database.
        if (db_name == SYSTEM_DATABASE)
            continue;

        databases.emplace(db_name, path + file);
    }

    {
        // Sanity check if we miss some directories that have no related sql file.
        Poco::DirectoryIterator dir_end;
        for (Poco::DirectoryIterator it(path); it != dir_end; ++it)
        {
            if (!it->isDirectory())
                continue;
            /// For '.svn', '.gitignore' directory and similar. Ignore "system" database.
            if (it.name().at(0) == '.' || it.name() == SYSTEM_DATABASE)
                continue;
            const auto db_name = unescapeForFileName(it.name());
            if (databases.find(db_name) != databases.end()) // already detected
                continue;
            LOG_WARNING(
                log,
                "Directory \"" + it.path().toString()
                    + "\" is ignored while loading metadata since we can't find its .sql file.");
        }
    }


    auto load_database = [&](Context & context,
                             const String & database,
                             const String & database_metadata_file,
                             ThreadPool * thread_pool,
                             bool force_restore_data) {
        /// There may exist .sql file with database creation statement.
        /// Or, if it is absent, then database with default engine is created.
        String database_attach_query;
        if (Poco::File(database_metadata_file).exists())
        {
            auto in = ReadBufferFromRandomAccessFileBuilder::build(
                context.getFileProvider(),
                database_metadata_file,
                EncryptionPath(database_metadata_file, ""),
                1024);
            readStringUntilEOF(database_attach_query, in);
        }
        else
        {
            // Old fashioned way, keep engine as "Ordinary"
            database_attach_query = "ATTACH DATABASE " + backQuoteIfNeed(database) + " ENGINE=Ordinary";
        }

        executeCreateQuery(
            database_attach_query,
            context,
            database,
            database_metadata_file,
            thread_pool,
            force_restore_data);
    };

    size_t default_num_threads
        = std::max(4UL, std::thread::hardware_concurrency()) * context.getSettingsRef().init_thread_count_scale;
    auto load_database_thread_num = std::min(default_num_threads, databases.size());

    auto load_databases_thread_pool
        = ThreadPool(load_database_thread_num, load_database_thread_num / 2, load_database_thread_num * 2);
    auto load_databases_wait_group = load_databases_thread_pool.waitGroup();

    auto load_tables_thread_pool = ThreadPool(default_num_threads, default_num_threads / 2, default_num_threads * 2);

    for (const auto & database : databases)
    {
        const auto & db_name = database.first;
        const auto & meta_file = database.second;

        auto task
            = [&load_database, &context, &db_name, &meta_file, has_force_restore_data_flag, &load_tables_thread_pool] {
                  load_database(context, db_name, meta_file, &load_tables_thread_pool, has_force_restore_data_flag);
              };

        load_databases_wait_group->schedule(task);
    }

    load_databases_wait_group->wait();

    if (has_force_restore_data_flag)
        force_restore_data_flag_file.remove();
}

void loadMetadataSystem(Context & context)
{
    const String path = context.getPath() + "metadata/" SYSTEM_DATABASE;
    if (Poco::File(path).exists())
    {
        /// 'has_force_restore_data_flag' is true, to not fail on loading query_log table, if it is corrupted.
        loadDatabase(context, SYSTEM_DATABASE, path + ".sql", nullptr, true);
    }
    else
    {
        /// Initialize system database manually
        const String global_path = context.getPath();
        Poco::File(global_path + "data/" SYSTEM_DATABASE).createDirectories();
        Poco::File(global_path + "metadata/" SYSTEM_DATABASE).createDirectories();

        // Keep DatabaseOrdinary for database "system". Storages in this database is not IManageableStorage.
        auto system_database
            = std::make_shared<DatabaseOrdinary>(SYSTEM_DATABASE, global_path + "metadata/" SYSTEM_DATABASE, context);
        context.addDatabase(SYSTEM_DATABASE, system_database);
    }
}

} // namespace DB
