#include <Common/FailPoint.h>
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

String getDatabaseMetadataPath(const String & base_path)
{
    return (endsWith(base_path, "/") ? base_path.substr(0, base_path.size() - 1) : base_path) + ".sql";
}

String DatabaseTiFlash::getDataPath() const { return data_path; }

String DatabaseTiFlash::getMetadataPath() const { return metadata_path; }

String DatabaseTiFlash::getTableMetadataPath(const String & table_name) const
{
    return metadata_path + (endsWith(metadata_path, "/") ? "" : "/") + escapeForFileName(table_name) + ".sql";
}

void DatabaseTiFlash::loadTables(Context & context, ThreadPool * thread_pool, bool has_force_restore_data_flag)
{
    (void)context;
    (void)thread_pool;
    (void)has_force_restore_data_flag;
// TODO: FIX this load
#if 0
    using FileNames = std::vector<std::string>;
    FileNames file_names;

    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator dir_it(metadata_path); dir_it != dir_end; ++dir_it)
    {
        /// For '.svn', '.gitignore' directory and similar.
        if (dir_it.name().at(0) == '.')
            continue;

        /// There are .sql.bak files - skip them.
        if (endsWith(dir_it.name(), ".sql.bak"))
            continue;

        /// There are files .sql.tmp - delete.
        if (endsWith(dir_it.name(), ".sql.tmp"))
        {
            LOG_INFO(log, "Removing file " << dir_it->path());
            Poco::File(dir_it->path()).remove();
            continue;
        }

        /// The required files have names like `table_name.sql`
        if (endsWith(dir_it.name(), ".sql"))
            file_names.push_back(dir_it.name());
        else
            throw Exception(
                "Incorrect file extension: " + dir_it.name() + " in metadata directory " + metadata_path, ErrorCodes::INCORRECT_FILE_NAME);
    }

    /** Tables load faster if they are loaded in sorted (by name) order.
      * Otherwise (for the ext4 filesystem), `DirectoryIterator` iterates through them in some order,
      *  which does not correspond to order tables creation and does not correspond to order of their location on disk.
      */
    std::sort(file_names.begin(), file_names.end());

    size_t total_tables = file_names.size();
    LOG_INFO(log, "Total " << total_tables << " tables.");

    String data_path = context.getPath() + "data/" + escapeForFileName(name) + "/";

    AtomicStopwatch watch;
    std::atomic<size_t> tables_processed{0};

    auto task_function = [&](FileNames::const_iterator begin, FileNames::const_iterator end) {
        for (auto it = begin; it != end; ++it)
        {
            const String & table = *it;

            /// Messages, so that it's not boring to wait for the server to load for a long time.
            if ((++tables_processed) % PRINT_MESSAGE_EACH_N_TABLES == 0 || watch.compareAndRestart(PRINT_MESSAGE_EACH_N_SECONDS))
            {
                LOG_INFO(log, std::fixed << std::setprecision(2) << tables_processed * 100.0 / total_tables << "%");
                watch.restart();
            }

            loadTable(context, metadata_path, *this, name, data_path, table, has_force_restore_data_flag);
        }
    };

    const size_t bunch_size = TABLES_PARALLEL_LOAD_BUNCH_SIZE;
    size_t num_bunches = (total_tables + bunch_size - 1) / bunch_size;

    for (size_t i = 0; i < num_bunches; ++i)
    {
        auto begin = file_names.begin() + i * bunch_size;
        auto end = (i + 1 == num_bunches) ? file_names.end() : (file_names.begin() + (i + 1) * bunch_size);

        auto task = std::bind(task_function, begin, end);

        if (thread_pool)
            thread_pool->schedule(task);
        else
            task();
    }

    if (thread_pool)
        thread_pool->wait();

    /// After all tables was basically initialized, startup them.
    startupTables(thread_pool);
#endif
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
        // Update `table->getDatabase()` for IManageableStorage
        table->rename(/*new_path_to_db=*/context.getPath() + "/data/", // DeltaTree will ignored it
            /*new_database_name=*/to_database_concrete->name, to_table_name);

        if (name != to_database_concrete->name)
        {
            // Detach from this database and attach to new database
            // Not atomic between two databases, but not big deal.
            const String table_name = table->getTableName();
            StoragePtr detach_storage = detachTable(table_name);
            to_database_concrete->attachTable(table_name, detach_storage);
        }

        /// FIXME: we have no name_mapper in DatabaseTiFlash,
        /// should we update metadata for table inside this method?

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

static ASTPtr getQueryFromMetadata(const String & metadata_path, bool throw_on_error = true)
{
    if (!Poco::File(metadata_path).exists())
        return nullptr;

    String query;

    {
        ReadBufferFromFile in(metadata_path, 4096);
        readStringUntilEOF(query, in);
    }

    ParserCreateQuery parser;
    const char * pos = query.data();
    std::string error_message;
    auto ast = tryParseQuery(parser, pos, pos + query.size(), error_message, /* hilite = */ false, "in file " + metadata_path,
        /* allow_multi_statements = */ false, 0);

    if (!ast && throw_on_error)
        throw Exception(error_message, ErrorCodes::SYNTAX_ERROR);

    return ast;
}

static ASTPtr getCreateQueryFromMetadata(const String & metadata_path, const String & database, bool throw_on_error)
{
    ASTPtr ast = getQueryFromMetadata(metadata_path, throw_on_error);
    if (ast)
    {
        ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
        ast_create_query.attach = false;
        ast_create_query.database = database;
    }
    return ast;
}

ASTPtr DatabaseTiFlash::getCreateTableQueryImpl(const Context & context, const String & table_name, bool throw_on_error) const
{
    const auto table_metadata_path = getTableMetadataPath(table_name);
    ASTPtr ast = getCreateQueryFromMetadata(table_metadata_path, name, throw_on_error);
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
    ASTPtr ast = getCreateQueryFromMetadata(database_metadata_path, name, true);
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
    /// No additional removal actions are required.
}


} // namespace DB
