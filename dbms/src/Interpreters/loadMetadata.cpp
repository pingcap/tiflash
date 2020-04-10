#include <Common/Stopwatch.h>
#include <Common/escapeForFileName.h>
#include <Databases/DatabaseOrdinary.h>
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


static constexpr size_t PRINT_MESSAGE_EACH_N_TABLES = 256;
static constexpr size_t PRINT_MESSAGE_EACH_N_SECONDS = 5;
static constexpr size_t TABLES_PARALLEL_LOAD_BUNCH_SIZE = 100;

void loadTiFlashTables(Context & context,
    const std::vector<String> & table_files,
    ThreadPool * thread_pool,
    bool has_force_restore_data_flag,
    Poco::Logger * log)
{
    const auto total_tables = table_files.size();
    LOG_INFO(log, "Total " << total_tables << " tables.");

    // table name -> table
    Tables tables;

    AtomicStopwatch watch;
    std::atomic<size_t> tables_processed{0};

    auto task_function = [&](std::vector<String>::const_iterator begin, std::vector<String>::const_iterator end) {
        for (auto it = begin; it != end; ++it)
        {
            /// Messages, so that it's not boring to wait for the server to load for a long time.
            if ((++tables_processed) % PRINT_MESSAGE_EACH_N_TABLES == 0 || watch.compareAndRestart(PRINT_MESSAGE_EACH_N_SECONDS))
            {
                LOG_INFO(log, DB::toString(tables_processed * 100.0 / total_tables, 2) << "%");
                watch.restart();
            }

            const String & table_file = *it;
            (void)table_file;
            (void)has_force_restore_data_flag;
            (void)context;

            // DatabaseLoading::loadTable(context, metadata_path, database, database_name, data_path, table_file, has_force_restore_data_flag);
        }
    };

    const size_t bunch_size = TABLES_PARALLEL_LOAD_BUNCH_SIZE;
    size_t num_bunches = (total_tables + bunch_size - 1) / bunch_size;
    for (size_t i = 0; i < num_bunches; ++i)
    {
        auto begin = table_files.begin() + i * bunch_size;
        auto end = (i + 1 == num_bunches) ? table_files.end() : (table_files.begin() + (i + 1) * bunch_size);

        auto task = std::bind(task_function, begin, end);
        if (thread_pool)
            thread_pool->schedule(task);
        else
            task();
    }

    if (thread_pool)
        thread_pool->wait();

    // After all tables was basically initialized, startup them.
    DatabaseLoading::startupTables(tables, thread_pool, log);
}

void loadTiFlashMetadata(Context & context)
{
    Logger * log = &Logger::get("loadTiFlashMetadata");

    const String meta_path = context.getPath() + "/metadata/";

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
    std::vector<String> table_files;
    std::vector<String> database_files;
    auto filenames = DatabaseLoading::listSQLFilenames(meta_path, log);
    for (const auto & filename : filenames)
    {
        if (startsWith(filename, SchemaNameMapper::DATABASE_PREFIX))
            database_files.emplace_back(filename);
        else if (startsWith(filename, SchemaNameMapper::TABLE_PREFIX))
            table_files.emplace_back(filename);
        else
            LOG_WARNING(log, "Unkonw sql file: " << meta_path << "/" << filename); // just ignore with warning
    }

    SchemaNameMapper mapper;

    // Fetch relationships from TiDB between databases and tables.
    auto schema_syncer = context.getTMTContext().getSchemaSyncer();
    auto tidb_databases_info = schema_syncer->fetchAllDBs();
    // Load all database. (files with "db_" prefix)
    for (const auto & db_file : database_files)
    {
        const String db_name = unescapeForFileName(db_file.c_str() + strlen(SchemaNameMapper::DATABASE_PREFIX));
        const String db_file_path = meta_path + db_file;
        loadDatabase(context, db_name, db_file_path, &thread_pool, has_force_restore_data_flag);
        auto database = context.getDatabase(db_name);
        if (database->getEngineName() == "TiFlash")
        {
            auto db_info = std::find_if( //
                tidb_databases_info.begin(),
                tidb_databases_info.end(),
                [&mapper, &db_name](const TiDB::DBInfoPtr & db) { return mapper.mapDatabaseName(*db) == db_name; });
            // If databse is "DatabaseTiFlash", we need to attach tables to database according to relationships from TiDB
            std::vector<std::pair<TableID, TiDB::DatabaseID>> //
                tidb_tables = schema_syncer->fetchAllTables(*db_info);
        }
    }
    // Then load all storages (files with "t_" prefix) and attach them to databases.
    loadTiFlashTables(context, table_files, &thread_pool, has_force_restore_data_flag, log);
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
