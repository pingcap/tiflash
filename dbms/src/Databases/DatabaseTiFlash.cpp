#include <Common/FailPoint.h>
#include <Common/Stopwatch.h>
#include <Common/escapeForFileName.h>
#include <Databases/DatabaseTiFlash.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Poco/File.h>
#include <Storages/IManageableStorage.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TMTStorages.h>
#include <common/ThreadPool.h>
#include <common/logger_useful.h>

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
} // namespace ErrorCodes

static constexpr size_t METADATA_FILE_BUFFER_SIZE = 32768;

DatabaseTiFlash::DatabaseTiFlash(String name_, const String & metadata_path_, const Context & context)
    : DatabaseWithOwnTablesBase(std::move(name_)),
      metadata_path(metadata_path_),
      data_path(context.getPath() + "data/"),
      log(&Logger::get("DatabaseTiFlash (" + name + ")"))
{
    Poco::File(data_path).createDirectories();
}

// metadata/${db_name}.sql
String getDatabaseMetadataPath(const String & base_path)
{
    return (endsWith(base_path, "/") ? base_path.substr(0, base_path.size() - 1) : base_path) + ".sql";
}

// metadata/${db_name}/
String DatabaseTiFlash::getMetadataPath() const { return metadata_path; }

// metadata/${db_name}/${tbl_name}.sql
String DatabaseTiFlash::getTableMetadataPath(const String & table_name) const
{
    return metadata_path + (endsWith(metadata_path, "/") ? "" : "/") + escapeForFileName(table_name) + ".sql";
}

// data/
// Note that data path of all databases are flatten.
String DatabaseTiFlash::getDataPath() const { return data_path; }


static constexpr size_t PRINT_MESSAGE_EACH_N_TABLES = 256;
static constexpr size_t PRINT_MESSAGE_EACH_N_SECONDS = 5;
static constexpr size_t TABLES_PARALLEL_LOAD_BUNCH_SIZE = 100;

void DatabaseTiFlash::loadTables(Context & context, ThreadPool * thread_pool, bool has_force_restore_data_flag)
{
    using FileNames = std::vector<std::string>;
    FileNames table_files = DatabaseLoading::listSQLFilenames(getMetadataPath(), log);

    /** Tables load faster if they are loaded in sorted (by name) order.
      * Otherwise (for the ext4 filesystem), `DirectoryIterator` iterates through them in some order,
      *  which does not correspond to order tables creation and does not correspond to order of their location on disk.
      */
    std::sort(table_files.begin(), table_files.end());

    const auto total_tables = table_files.size();
    LOG_INFO(log, "Total " << total_tables << " tables in database " << name);

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
            DatabaseLoading::loadTable(context, *this, metadata_path, name, data_path, table_file, has_force_restore_data_flag);
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


void DatabaseTiFlash::createTable(const Context & context, const String & table_name, const StoragePtr & table, const ASTPtr & query)
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
        std::lock_guard<std::mutex> lock(mutex);
        if (tables.find(table_name) != tables.end())
            throw Exception("Table " + name + "." + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
    }

    const String table_metadata_path = getTableMetadataPath(table_name);
    const String table_metadata_tmp_path = table_metadata_path + ".tmp";

    {
        const String statement = getTableDefinitionFromCreateQuery(query);

        /// Exclusive flags guarantees, that table is not created right now in another thread. Otherwise, exception will be thrown.
        WriteBufferFromFile out(table_metadata_tmp_path, statement.size(), O_WRONLY | O_CREAT | O_EXCL);
        writeString(statement, out);
        out.next();
        if (settings.fsync_metadata)
            out.sync();
        out.close();
    }

    try
    {
        /// Add a table to the map of known tables.
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (!tables.emplace(table_name, table).second)
                throw Exception("Table " + name + "." + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
        }

        /// If it was ATTACH query and file with table metadata already exist
        /// (so, ATTACH is done after DETACH), then rename atomically replaces old file with new one.
        Poco::File(table_metadata_tmp_path).renameTo(table_metadata_path);
    }
    catch (...)
    {
        Poco::File(table_metadata_tmp_path).remove();
        throw;
    }
}

void DatabaseTiFlash::removeTable(const Context & /*context*/, const String & table_name)
{
    StoragePtr res = detachTable(table_name);

    try
    {
        // If tiflash crash before remove metadata, next time it restart, will
        // full apply schema from TiDB. And the old table's metadata and data
        // will be removed.
        String table_metadata_path = getTableMetadataPath(table_name);
        FAIL_POINT_TRIGGER_EXCEPTION(exception_drop_table_during_remove_meta);
        Poco::File(table_metadata_path).remove();
    }
    catch (...)
    {
        attachTable(table_name, res);
        throw;
    }
}

void DatabaseTiFlash::renameTable(const Context & context, const String & table_name, IDatabase & to_database, const String & to_table_name)
{
    DatabaseTiFlash * to_database_concrete = typeid_cast<DatabaseTiFlash *>(&to_database);
    if (!to_database_concrete)
        throw Exception("Moving tables between databases of different engines is not supported", ErrorCodes::NOT_IMPLEMENTED);

    // DatabaseTiFlash should only manage tables in TMTContext.
    ManageableStoragePtr table;
    {
        StoragePtr table_ = tryGetTable(context, table_name);
        if (!table_)
            throw Exception("Table " + name + "." + table_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
        table = std::dynamic_pointer_cast<IManageableStorage>(table_);
        if (!table)
            throw Exception("Table " + name + "." + table_name + " is not manageable storage.", ErrorCodes::UNKNOWN_TABLE);
    }

    /// Notify the table that it is renamed. If the table does not support renaming, exception is thrown.
    try
    {
        if (likely(name != to_database_concrete->name))
        {
            // First move table meta file to new database directory.
            const String old_tbl_meta_file = getTableMetadataPath(table_name);
            const String new_tbl_meta_file = to_database_concrete->getTableMetadataPath(to_table_name);
            Poco::File{old_tbl_meta_file}.renameTo(new_tbl_meta_file);

            // Detach from this database and attach to new database
            // Not atomic between two databases in memory, but not big deal.
            const String table_name = table->getTableName();
            StoragePtr detach_storage = detachTable(table_name);
            to_database_concrete->attachTable(table_name, detach_storage);
        }

        // Update `table->getDatabase()` for IManageableStorage
        table->rename(/*new_path_to_db=*/context.getPath() + "/data/", // DeltaTree just ignored this param
            /*new_database_name=*/to_database_concrete->name, to_table_name);

        /// Nothing changed in table meta file, we can remove these comments later.
        // auto updated_table_info = table->getTableInfo();
        // updated_table_info.name = to_table_name;
        // storage->alterFromTiDB(AlterCommands{}, to_database_concrete->name, name_mapper, context);
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
}

void DatabaseTiFlash::alterTable(
    const Context & context, const String & name, const ColumnsDescription & columns, const ASTModifier & storage_modifier)
{
    /// Read the definition of the table and replace the necessary parts with new ones.

    const String table_name_escaped = escapeForFileName(name);
    const String table_metadata_tmp_path = metadata_path + "/" + table_name_escaped + ".sql.tmp";
    const String table_metadata_path = metadata_path + "/" + table_name_escaped + ".sql";
    String statement;

    {
        char in_buf[METADATA_FILE_BUFFER_SIZE];
        ReadBufferFromFile in(table_metadata_path, METADATA_FILE_BUFFER_SIZE, -1, in_buf);
        readStringUntilEOF(statement, in);
    }

    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, statement.data(), statement.data() + statement.size(), "in file " + table_metadata_path, 0);

    ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);

    ASTPtr new_columns = InterpreterCreateQuery::formatColumns(columns);
    ast_create_query.replace(ast_create_query.columns, new_columns);

    if (storage_modifier)
        storage_modifier(*ast_create_query.storage);

    statement = getTableDefinitionFromCreateQuery(ast);

    {
        WriteBufferFromFile out(table_metadata_tmp_path, statement.size(), O_WRONLY | O_CREAT | O_EXCL);
        writeString(statement, out);
        out.next();
        if (context.getSettingsRef().fsync_metadata)
            out.sync();
        out.close();
    }

    try
    {
        /// rename atomically replaces the old file with the new one.
        Poco::File(table_metadata_tmp_path).renameTo(table_metadata_path);
    }
    catch (...)
    {
        Poco::File(table_metadata_tmp_path).remove();
        throw;
    }
}


time_t DatabaseTiFlash::getTableMetadataModificationTime(const Context & /*context*/, const String & table_name)
{
    const String table_metadata_path = getTableMetadataPath(table_name);
    if (Poco::File meta_file(table_metadata_path); meta_file.exists())
    {
        return meta_file.getLastModified().epochTime();
    }
    else
    {
        return static_cast<time_t>(0);
    }
}

ASTPtr DatabaseTiFlash::getCreateTableQueryImpl(const Context & context, const String & table_name, bool throw_on_error) const
{
    const auto table_metadata_path = getTableMetadataPath(table_name);
    ASTPtr ast = DatabaseLoading::getCreateQueryFromMetadata(table_metadata_path, name, throw_on_error);
    if (!ast && throw_on_error)
    {
        /// Handle system.* tables for which there are no table.sql files.
        bool has_table = tryGetTable(context, table_name) != nullptr;

        auto msg = has_table ? "There is no CREATE TABLE query for table " : "There is no metadata file for table ";

        throw Exception(msg + table_name, ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY);
    }

    return ast;
}

ASTPtr DatabaseTiFlash::getCreateTableQuery(const Context & context, const String & table_name) const
{
    return getCreateTableQueryImpl(context, table_name, true);
}

ASTPtr DatabaseTiFlash::tryGetCreateTableQuery(const Context & context, const String & table_name) const
{
    return getCreateTableQueryImpl(context, table_name, false);
}

ASTPtr DatabaseTiFlash::getCreateDatabaseQuery(const Context & /*context*/) const
{
    const auto database_metadata_path = getDatabaseMetadataPath(metadata_path);
    ASTPtr ast = DatabaseLoading::getCreateQueryFromMetadata(database_metadata_path, name, true);
    if (!ast)
    {
        /// Handle databases (such as default) for which there are no database.sql files.
        String query = "CREATE DATABASE " + backQuoteIfNeed(name) + " ENGINE = " + getEngineName();
        ParserCreateQuery parser;
        ast = parseQuery(parser, query.data(), query.data() + query.size(), "", 0);
    }

    return ast;
}

void DatabaseTiFlash::shutdown()
{
    /// You can not hold a lock during shutdown.
    /// Because inside `shutdown` function the tables can work with database, and mutex is not recursive.

    Tables tables_snapshot;
    {
        std::lock_guard<std::mutex> lock(mutex);
        tables_snapshot = tables;
    }

    for (const auto & [name, storage] : tables_snapshot)
    {
        (void)name;
        storage->shutdown();
    }

    std::lock_guard<std::mutex> lock(mutex);
    tables.clear();
}


void DatabaseTiFlash::drop()
{
    // Remove metadata dir for this database
    if (auto dir = Poco::File(getMetadataPath()); dir.exists())
    {
        dir.remove(false);
    }
    // Removd meta file for this database
    if (auto meta_file = Poco::File(getDatabaseMetadataPath(getMetadataPath())); meta_file.exists())
    {
        meta_file.remove();
    }
}


} // namespace DB
