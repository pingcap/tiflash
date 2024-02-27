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
#include <Common/UniThreadPool.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <Databases/DatabaseTiFlash.h>
#include <IO/FileProvider/FileProvider.h>
#include <IO/FileProvider/ReadBufferFromRandomAccessFileBuilder.h>
#include <IO/FileProvider/WriteBufferFromWritableFileBuilder.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Storages/IManageableStorage.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/TMTStorages.h>
#include <TiDB/Schema/TiDB.h>
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
extern const int TIDB_TABLE_ALREADY_EXISTS;
} // namespace ErrorCodes

namespace FailPoints
{
extern const char exception_drop_table_during_remove_meta[];
extern const char exception_before_rename_table_old_meta_removed[];
} // namespace FailPoints

static constexpr size_t METADATA_FILE_BUFFER_SIZE = 32768;

DatabaseTiFlash::DatabaseTiFlash(
    String name_,
    const String & metadata_path_,
    const TiDB::DBInfo & db_info_,
    DatabaseTiFlash::Version version_,
    Timestamp tombstone_,
    const Context & context)
    : DatabaseWithOwnTablesBase(std::move(name_))
    , metadata_path(metadata_path_)
    , data_path(context.getPath() + "data/")
    , db_info(std::make_shared<TiDB::DBInfo>(db_info_))
    , tombstone(tombstone_)
    , log(&Poco::Logger::get("DatabaseTiFlash (" + name + ")"))
{
    if (unlikely(version_ != DatabaseTiFlash::CURRENT_VERSION))
        throw Exception(
            "Can not create database TiFlash with unknown version: " + DB::toString(version_),
            ErrorCodes::LOGICAL_ERROR);

    Poco::File(data_path).createDirectories();
}

TiDB::DBInfo & DatabaseTiFlash::getDatabaseInfo() const
{
    return *db_info;
}

// metadata/${db_name}.sql
String getDatabaseMetadataPath(const String & base_path)
{
    return (endsWith(base_path, "/") ? base_path.substr(0, base_path.size() - 1) : base_path) + ".sql";
}

// metadata/${db_name}/
String DatabaseTiFlash::getMetadataPath() const
{
    return metadata_path;
}

// metadata/${db_name}/${tbl_name}.sql
String DatabaseTiFlash::getTableMetadataPath(const String & table_name) const
{
    return metadata_path + (endsWith(metadata_path, "/") ? "" : "/") + escapeForFileName(table_name) + ".sql";
}

// data/
// Note that data path of all databases are flatten.
String DatabaseTiFlash::getDataPath() const
{
    return data_path;
}


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
    LOG_INFO(log, "Total {} tables in database {}", total_tables, name);

    AtomicStopwatch watch;
    std::atomic<size_t> tables_processed{0};

    auto wait_group = thread_pool ? thread_pool->waitGroup() : nullptr;

    std::mutex failed_tables_mutex;
    Tables tables_failed_to_startup;

    auto task_function = [&](std::vector<String>::const_iterator begin, std::vector<String>::const_iterator end) {
        for (auto it = begin; it != end; ++it)
        {
            /// Messages, so that it's not boring to wait for the server to load for a long time.
            if ((++tables_processed) % PRINT_MESSAGE_EACH_N_TABLES == 0
                || watch.compareAndRestart(PRINT_MESSAGE_EACH_N_SECONDS))
            {
                LOG_INFO(log, "{:.2f}%", tables_processed * 100.0 / total_tables);
                watch.restart();
            }

            const String & table_file = *it;
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
        auto begin = table_files.begin() + i * bunch_size;
        auto end = (i + 1 == num_bunches) ? table_files.end() : (table_files.begin() + (i + 1) * bunch_size);

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


void DatabaseTiFlash::createTable(const Context & context, const String & table_name, const ASTPtr & query)
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
            throw Exception("Table " + name + "." + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
    }

    const String table_metadata_path = getTableMetadataPath(table_name);
    const String table_metadata_tmp_path = table_metadata_path + ".tmp";

    {
        const String statement = getTableDefinitionFromCreateQuery(query);

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
        /// If it was ATTACH query and file with table metadata already exist
        /// (so, ATTACH is done after DETACH), then rename atomically replaces old file with new one.
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

void DatabaseTiFlash::removeTable(const Context & context, const String & table_name)
{
    StoragePtr res = detachTable(table_name);

    try
    {
        // If tiflash crash before remove metadata, next time it restart, will
        // full apply schema from TiDB. And the old table's metadata and data
        // will be removed.
        String table_metadata_path = getTableMetadataPath(table_name);
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_drop_table_during_remove_meta);
        context.getFileProvider()->deleteRegularFile(table_metadata_path, EncryptionPath(table_metadata_path, ""));
    }
    catch (...)
    {
        attachTable(table_name, res);
        throw;
    }
}

void DatabaseTiFlash::renameTable(
    const Context & /*context*/,
    const String & /*table_name*/,
    IDatabase & /*to_database*/,
    const String & /*to_table_name*/)
{
    throw Exception(DB::toString(__PRETTY_FUNCTION__) + " should never called!");
}

// This function will tidy up path and compare if them are the same one.
// For example "/tmp/data/a.sql" is equal to "/tmp//data//a.sql"
static inline bool isSamePath(const String & lhs, const String & rhs)
{
    return Poco::Path{lhs}.toString() == Poco::Path{rhs}.toString();
}

void DatabaseTiFlash::renameTable(
    const Context & context,
    const String & table_name,
    IDatabase & to_database,
    const String & to_table_name,
    const String & /* display_database */,
    const String & display_table)
{
    auto * to_database_concrete = typeid_cast<DatabaseTiFlash *>(&to_database);
    if (!to_database_concrete)
        throw Exception(
            "Moving tables between databases of different engines is not supported",
            ErrorCodes::NOT_IMPLEMENTED);

    // DatabaseTiFlash should only manage tables in TMTContext.
    ManageableStoragePtr table;
    {
        StoragePtr tmp = tryGetTable(context, table_name);
        if (!tmp)
            throw Exception("Table " + name + "." + table_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
        table = std::dynamic_pointer_cast<IManageableStorage>(tmp);
        if (!table)
            throw Exception(
                "Table " + name + "." + table_name + " is not manageable storage.",
                ErrorCodes::UNKNOWN_TABLE);
    }

    // First move table meta file to new database directory.
    {
        const String old_tbl_meta_file = getTableMetadataPath(table_name);
        // Generate new meta file according to ast in old meta file and to_table_name
        const String new_tbl_meta_file = to_database_concrete->getTableMetadataPath(to_table_name);
        const String new_tbl_meta_file_tmp = new_tbl_meta_file + ".tmp";

        ASTPtr ast;
        String statement;
        {
            {
                char in_buf[METADATA_FILE_BUFFER_SIZE];
                auto in = ReadBufferFromRandomAccessFileBuilder::build(
                    context.getFileProvider(),
                    old_tbl_meta_file,
                    EncryptionPath(old_tbl_meta_file, ""),
                    METADATA_FILE_BUFFER_SIZE,
                    /*read_limiter*/ nullptr,
                    -1,
                    in_buf);
                readStringUntilEOF(statement, in);
            }
            ParserCreateQuery parser;
            ast = parseQuery(
                parser,
                statement.data(),
                statement.data() + statement.size(),
                "in file " + old_tbl_meta_file,
                0);
        }

        ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
        {
            ast_create_query.table = to_table_name;
            ASTStorage * storage_ast = ast_create_query.storage;
            auto updated_table_info = table->getTableInfo();
            updated_table_info.name = display_table;
            table->modifyASTStorage(storage_ast, updated_table_info);
        }
        statement = getTableDefinitionFromCreateQuery(ast);

        // 1. Assume the case that we need to rename the file `t_31.sql.tmp` to `t_31.sql`,
        // and t_31.sql already exists and is a encrypted file.
        // 2. The implementation in this function assume that the rename operation is atomic.
        // 3. If we create new encryption info for `t_31.sql.tmp`,
        // then we cannot rename the encryption info and the file in an atomic operation.
        bool use_target_encrypt_info
            = context.getFileProvider()->isFileEncrypted(EncryptionPath(new_tbl_meta_file, ""));
        EncryptionPath encryption_path = use_target_encrypt_info ? EncryptionPath(new_tbl_meta_file, "")
                                                                 : EncryptionPath(new_tbl_meta_file_tmp, "");
        {
            bool create_new_encryption_info = !use_target_encrypt_info && !statement.empty();
            auto out = WriteBufferFromWritableFileBuilder::build(
                context.getFileProvider(),
                new_tbl_meta_file_tmp,
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
                new_tbl_meta_file_tmp,
                encryption_path,
                new_tbl_meta_file,
                EncryptionPath(new_tbl_meta_file, ""),
                !use_target_encrypt_info);
        }
        catch (...)
        {
            context.getFileProvider()->deleteRegularFile(
                new_tbl_meta_file_tmp,
                EncryptionPath(new_tbl_meta_file_tmp, ""));
            throw;
        }

        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_rename_table_old_meta_removed);

        // If only display name updated, don't remove `old_tbl_meta_file`.
        if (!isSamePath(old_tbl_meta_file, new_tbl_meta_file))
        {
            // If process crash before removing old table meta file, we will continue or rollback this
            // rename command next time `loadTables` is called. See `loadTables` and
            // `DatabaseLoading::startupTables` for more details.
            context.getFileProvider()->deleteRegularFile(
                old_tbl_meta_file,
                EncryptionPath(old_tbl_meta_file, "")); // Then remove old meta file
        }
    }

    //// Note that remains codes should only change variables in memory if this rename command
    //// is synced from TiDB, aka table_name always equal to to_table_name.

    // Detach from this database and attach to new database
    // Not atomic between two databases in memory, but not big deal,
    // because of alter_locks in InterpreterRenameQuery
    StoragePtr detach_storage = detachTable(table_name);
    to_database_concrete->attachTable(to_table_name, detach_storage);

    // Update database and table name in TiDB table info for IManageableStorage
    table->rename(
        /*new_path_to_db=*/context.getPath() + "/data/", // DeltaTree just ignored this param
        /*new_database_name=*/to_database_concrete->name,
        to_table_name,
        display_table);
}

void DatabaseTiFlash::alterTable(
    const Context & context,
    const String & name,
    const ColumnsDescription & columns,
    const ASTModifier & storage_modifier)
{
    /// Read the definition of the table and replace the necessary parts with new ones.

    const String table_name_escaped = escapeForFileName(name);
    const String table_metadata_tmp_path
        = metadata_path + (endsWith(metadata_path, "/") ? "" : "/") + table_name_escaped + ".sql.tmp";
    const String table_metadata_path
        = metadata_path + (endsWith(metadata_path, "/") ? "" : "/") + table_name_escaped + ".sql";
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

    // refer to the comment in `renameTable`
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
        context.getFileProvider()->deleteRegularFile(
            table_metadata_tmp_path,
            EncryptionPath(table_metadata_tmp_path, ""));
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

ASTPtr DatabaseTiFlash::getCreateTableQueryImpl(const Context & context, const String & table_name, bool throw_on_error)
    const
{
    const auto table_metadata_path = getTableMetadataPath(table_name);
    ASTPtr ast = DatabaseLoading::getCreateQueryFromMetadata(context, table_metadata_path, name, throw_on_error);
    if (!ast && throw_on_error)
    {
        throw Exception("There is no metadata file for table " + table_name, ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY);
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

ASTPtr DatabaseTiFlash::getCreateDatabaseQuery(const Context & context) const
{
    const auto database_metadata_path = getDatabaseMetadataPath(metadata_path);
    ASTPtr ast = DatabaseLoading::getCreateQueryFromMetadata(context, database_metadata_path, name, true);
    if (!ast)
    {
        throw Exception("There is no metadata file for database " + name, ErrorCodes::LOGICAL_ERROR);
    }
    return ast;
}

void DatabaseTiFlash::shutdown()
{
    /// You can not hold a lock during shutdown.
    /// Because inside `shutdown` function the tables can work with database, and mutex is not recursive.

    Tables tables_snapshot;
    {
        std::lock_guard lock(mutex);
        tables_snapshot = tables;
    }

    for (const auto & [name, storage] : tables_snapshot)
    {
        (void)name;
        storage->shutdown();

        auto managed_storage = std::dynamic_pointer_cast<IManageableStorage>(storage);
        if (managed_storage)
            managed_storage->removeFromTMTContext();
    }

    std::lock_guard lock(mutex);
    tables.clear();
}

void DatabaseTiFlash::alterTombstone(const Context & context, Timestamp tombstone_, const TiDB::DBInfoPtr & new_db_info)
{
    const auto database_metadata_path = getDatabaseMetadataPath(metadata_path);
    const auto database_metadata_tmp_path = database_metadata_path + ".tmp";
    ASTPtr ast = DatabaseLoading::getCreateQueryFromMetadata(context, database_metadata_path, name, true);
    if (!ast)
    {
        throw Exception("There is no metadata file for database " + name, ErrorCodes::LOGICAL_ERROR);
    }

    {
        // Alter the attach statement in metadata.
        std::shared_ptr<ASTLiteral> dbinfo_literal = [&]() {
            String seri_info;
            if (new_db_info != nullptr)
            {
                seri_info = new_db_info->serialize();
            }
            else if (db_info != nullptr)
            {
                seri_info = db_info->serialize();
            }
            return std::make_shared<ASTLiteral>(Field(seri_info));
        }();
        Field format_version_field(static_cast<UInt64>(DatabaseTiFlash::CURRENT_VERSION));
        auto version_literal = std::make_shared<ASTLiteral>(format_version_field);
        auto tombstone_literal = std::make_shared<ASTLiteral>(Field(tombstone_));

        auto & create = typeid_cast<ASTCreateQuery &>(*ast);
        create.attach = true; // Should be an attach statement but not create
        auto & storage_ast = create.storage;
        if (!storage_ast->engine->arguments)
            storage_ast->engine->arguments = std::make_shared<ASTExpressionList>();

        auto & args = typeid_cast<ASTExpressionList &>(*storage_ast->engine->arguments);
        if (args.children.empty())
        {
            args.children.emplace_back(dbinfo_literal);
            args.children.emplace_back(version_literal);
            args.children.emplace_back(tombstone_literal);
        }
        else if (args.children.size() == 1)
        {
            args.children.emplace_back(version_literal);
            args.children.emplace_back(tombstone_literal);
        }
        else if (args.children.size() == 2)
        {
            args.children.emplace_back(tombstone_literal);
        }
        else
        {
            // update the seri dbinfo
            args.children[0] = dbinfo_literal;
            args.children[1] = version_literal;
            // udpate the tombstone mark
            args.children[2] = tombstone_literal;
        }
    }

    String statement;
    {
        std::ostringstream stream;
        formatAST(*ast, stream, false, false);
        stream << '\n';
        statement = stream.str();
    }

    {
        // Atomic replace database metadata file and its encryption info
        auto provider = context.getFileProvider();
        bool reuse_encrypt_info = provider->isFileEncrypted(EncryptionPath(database_metadata_path, ""));
        EncryptionPath encryption_path = reuse_encrypt_info ? EncryptionPath(database_metadata_path, "")
                                                            : EncryptionPath(database_metadata_tmp_path, "");
        {
            auto out = WriteBufferFromWritableFileBuilder::build(
                provider,
                database_metadata_tmp_path,
                encryption_path,
                !reuse_encrypt_info,
                nullptr,
                statement.size(),
                O_WRONLY | O_CREAT | O_TRUNC);
            writeString(statement, out);
            out.next();
            if (context.getSettingsRef().fsync_metadata)
                out.sync();
            out.close();
        }

        try
        {
            provider->renameFile(
                database_metadata_tmp_path,
                encryption_path,
                database_metadata_path,
                EncryptionPath(database_metadata_path, ""),
                !reuse_encrypt_info);
        }
        catch (...)
        {
            provider->deleteRegularFile(database_metadata_tmp_path, EncryptionPath(database_metadata_tmp_path, ""));
            throw;
        }
    }

    // After all done, set the tombstone
    tombstone = tombstone_;
    // Overwrite db_info if not null
    if (new_db_info)
    {
        db_info = new_db_info;
    }
}

void DatabaseTiFlash::drop(const Context & context)
{
    // Remove metadata dir for this database
    if (auto dir = Poco::File(getMetadataPath()); dir.exists())
    {
        context.getFileProvider()->deleteDirectory(getMetadataPath(), false, false);
    }
    // Remove meta file for this database
    if (auto meta_file = Poco::File(getDatabaseMetadataPath(getMetadataPath())); meta_file.exists())
    {
        context.getFileProvider()->deleteRegularFile(
            getDatabaseMetadataPath(getMetadataPath()),
            EncryptionPath(getDatabaseMetadataPath(getMetadataPath()), ""));
    }
}


} // namespace DB
