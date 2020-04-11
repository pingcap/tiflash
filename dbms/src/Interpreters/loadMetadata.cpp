#include <Common/Stopwatch.h>
#include <Common/escapeForFileName.h>
#include <Databases/DatabaseOrdinary.h>
#include <Databases/DatabaseTiFlash.h>
#include <Databases/DatabasesCommon.h>
#include <IO/ReadBufferFromFile.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/loadMetadata.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/FileStream.h>
#include <Storages/Transaction/SchemaNameMapper.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>
#include <common/ThreadPool.h>

#include <future>
#include <iomanip>
#include <thread>


namespace DB
{

namespace ErrorCodes
{
extern const int FILE_DOESNT_EXIST;
}

static void executeCreateQuery(const String & query,
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
    Context & context, const String & database, const String & database_metadata_file, ThreadPool * thread_pool, bool force_restore_data)
{
    /// There may exist .sql file with database creation statement.
    /// Or, if it is absent, then database with default engine is created.

    String database_attach_query;

    if (Poco::File(database_metadata_file).exists())
    {
        ReadBufferFromFile in(database_metadata_file, 1024);
        readStringUntilEOF(database_attach_query, in);
    }
    else if (database == SYSTEM_DATABASE)
    {
        database_attach_query = "ATTACH DATABASE " + backQuoteIfNeed(database) + " ENGINE=Ordinary";
    }
    else
    {
        throw Exception("File is not exist: " + database_metadata_file, ErrorCodes::FILE_DOESNT_EXIST);
    }

    executeCreateQuery(database_attach_query, context, database, database_metadata_file, thread_pool, force_restore_data);
}


void loadMetadata(Context & context)
{
    String path = context.getPath() + "metadata";

    /** There may exist 'force_restore_data' file, that means,
      *  skip safety threshold on difference of data parts while initializing tables.
      * This file is deleted after successful loading of tables.
      * (flag is "one-shot")
      */
    Poco::File force_restore_data_flag_file(context.getFlagsPath() + "force_restore_data");
    bool has_force_restore_data_flag = force_restore_data_flag_file.exists();

    /// For parallel tables loading.
    ThreadPool thread_pool(SettingMaxThreads().getAutoValue());

    /// Loop over databases.
    std::map<String, String> databases;
    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator it(path); it != dir_end; ++it)
    {
        if (!it->isDirectory())
            continue;

        /// For '.svn', '.gitignore' directory and similar.
        if (it.name().at(0) == '.')
            continue;

        if (it.name() == SYSTEM_DATABASE)
            continue;

        databases.emplace(unescapeForFileName(it.name()), it.path().toString());
    }

    for (const auto & elem : databases)
        loadDatabase(context, elem.first, elem.second + ".sql", &thread_pool, has_force_restore_data_flag);

    thread_pool.wait();

    if (has_force_restore_data_flag)
        force_restore_data_flag_file.remove();
}


void loadTiFlashMetadata(Context & context)
{
    Logger * log = &Logger::get("loadTiFlashMetadata");

    const String meta_root = context.getPath() + "/metadata/";

    /** There may exist 'force_restore_data' file, that means,
      *  skip safety threshold on difference of data parts while initializing tables.
      * This file is deleted after successful loading of tables.
      * (flag is "one-shot")
      */
    Poco::File force_restore_data_flag_file(context.getFlagsPath() + "force_restore_data");
    bool has_force_restore_data_flag = force_restore_data_flag_file.exists();

    /// For parallel tables loading.
    ThreadPool thread_pool(SettingMaxThreads().getAutoValue());

    // Get all filename endsWith ".sql", and cleanup temporary file with ".sql.tmp" suffix
    std::set<String> table_files;
    std::vector<String> database_files;
    auto filenames = DatabaseLoading::listSQLFilenames(meta_root, log);
    for (const auto & filename : filenames)
    {
        if (startsWith(filename, SchemaNameMapper::DATABASE_PREFIX))
            database_files.emplace_back(filename);
        else if (startsWith(filename, SchemaNameMapper::TABLE_PREFIX))
            table_files.emplace(filename);
        else
            LOG_WARNING(log, "Unkonw sql file: " << meta_root << "/" << filename); // just ignore with warning
    }

    SchemaNameMapper mapper;

    // Fetch relationships from TiDB between databases and tables.
    auto schema_syncer = context.getTMTContext().getSchemaSyncer();
    auto tidb_databases_info = schema_syncer->fetchAllDBs();
    // Load all database. (files with "db_" prefix)
    for (const auto & db_file : database_files)
    {
        String db_name = unescapeForFileName(db_file.c_str() + strlen(SchemaNameMapper::DATABASE_PREFIX));
        db_name = db_name.substr(0, db_name.length() - strlen(".sql")); // remove suffix
        const String db_file_path = meta_root + db_file;
        loadDatabase(context, db_name, db_file_path, &thread_pool, has_force_restore_data_flag);
        auto database = context.getDatabase(db_name);
        if (database->getEngineName() == "TiFlash")
        {
            DatabaseTiFlash * database_concrete = typeid_cast<DatabaseTiFlash *>(database.get());
            if (!database_concrete)
                throw Exception("Cast failed!", ErrorCodes::LOGICAL_ERROR);

            auto db_info = std::find_if( //
                tidb_databases_info.begin(),
                tidb_databases_info.end(),
                [&mapper, &db_name](const TiDB::DBInfoPtr & db) { return mapper.mapDatabaseName(*db) == db_name; });
            if (db_info == tidb_databases_info.end())
            {
                LOG_WARNING(log, "Database " << db_name << " not exists in TiDB, going to drop it");
                // FIXME: This database is not exists in TiDB, drop it?
                continue;
            }

            // If databse is "DatabaseTiFlash", we need to attach tables to database according to relationships from TiDB
            std::vector<std::pair<TableID, TiDB::DatabaseID>> //
                tidb_tables = schema_syncer->fetchAllTables(*db_info);

            std::vector<String> table_files_for_this_db;
            for (const auto & [t_id, db_id] : tidb_tables)
            {
                (void)db_id;
                const String expected_tbl_file = mapper.mapTableName(t_id) + ".sql";
                // TiFlash schema may lag behind TiDB, some table may not exist in this time.
                if (auto iter = table_files.find(expected_tbl_file); iter != table_files.end())
                {
                    table_files.erase(iter);
                    table_files_for_this_db.emplace_back(meta_root + expected_tbl_file);
                }
            }

            // Then load all storages (files with "t_" prefix) and attach them to databases.
            database_concrete->loadTables(context, table_files_for_this_db, &thread_pool, has_force_restore_data_flag, log);
        }
    }

    // After all, TiFlash schema may lag behind TiDB, some tables may not be dropped
    if (!table_files.empty())
    {
        LOG_WARNING(log, "There are " << table_files.size() << " tables not found in TiDB schema");
        for (const auto & table_file : table_files)
        {
            // FIXME: drop schema file, data, multi-disk
            LOG_WARNING(log, "Drop table " << table_file);
        }
    }
}


void loadMetadataSystem(Context & context)
{
    const String path = context.getPath() + "metadata/" SYSTEM_DATABASE;
    if (Poco::File(path).exists())
    {
        /// 'has_force_restore_data_flag' is true, to not fail on loading query_log table, if it is corrupted.
        loadDatabase(context, SYSTEM_DATABASE, path, nullptr, true);
    }
    else
    {
        /// Initialize system database manually
        const String global_path = context.getPath();
        Poco::File(global_path + "data/" SYSTEM_DATABASE).createDirectories();
        Poco::File(global_path + "metadata/" SYSTEM_DATABASE).createDirectories();

        // Keep DatabaseOrdinary for database "system". Storages in this database is not IManageableStorage.
        auto system_database = std::make_shared<DatabaseOrdinary>(SYSTEM_DATABASE, global_path + "metadata/" SYSTEM_DATABASE, context);
        context.addDatabase(SYSTEM_DATABASE, system_database);
    }
}

} // namespace DB
