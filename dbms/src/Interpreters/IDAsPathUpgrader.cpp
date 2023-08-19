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

#include <Common/StringUtils/StringUtils.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <Databases/DatabaseOrdinary.h>
#include <Databases/DatabasesCommon.h>
#include <Debug/MockSchemaNameMapper.h>
#include <Encryption/ReadBufferFromFileProvider.h>
#include <Encryption/WriteBufferFromFileProvider.h>
#include <Interpreters/Context.h>
#include <Interpreters/IDAsPathUpgrader.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/File.h>
#include <Poco/FileStream.h>
#include <Storages/MutableSupport.h>
#include <Storages/PathPool.h>
#include <Storages/Transaction/SchemaNameMapper.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/TiDBSchemaSyncer.h>
#include <common/logger_useful.h>
#include <fmt/core.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
extern const int FILE_DOESNT_EXIST;
extern const int SYNTAX_ERROR;
} // namespace ErrorCodes

static constexpr auto SYSTEM_DATABASE = "system";

namespace
{
std::shared_ptr<ASTFunction> getDatabaseEngine(const FileProviderPtr & file_provider, const String & filename)
{
    String query;
    if (Poco::File(filename).exists())
    {
        ReadBufferFromFileProvider in(file_provider, filename, EncryptionPath(filename, ""), 1024);
        readStringUntilEOF(query, in);
    }
    else
    {
        // only directory exists, "default" database, return "Ordinary" engine by default.
        return std::static_pointer_cast<ASTFunction>(makeASTFunction("Ordinary"));
    }

    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "in file " + filename, 0);
    ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
    auto storage = ast_create_query.storage;
    if (storage == nullptr || storage->engine == nullptr || storage->engine->name.empty())
    {
        throw Exception("Can not get database engine for file: " + filename, ErrorCodes::LOGICAL_ERROR);
    }

    return std::static_pointer_cast<ASTFunction>(storage->engine->clone());
}

// Get <TableName, TableInfo> from `table_metadata_file`
std::pair<String, TiDB::TableInfo> getTableInfo(const FileProviderPtr & file_provider, const String & table_metadata_file)
{
    String definition;
    if (Poco::File(table_metadata_file).exists())
    {
        ReadBufferFromFileProvider in(file_provider, table_metadata_file, EncryptionPath(table_metadata_file, ""), 1024);
        readStringUntilEOF(definition, in);
    }
    else
    {
        throw Exception("Can not open table schema file: " + table_metadata_file, ErrorCodes::LOGICAL_ERROR);
    }

    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, definition.data(), definition.data() + definition.size(), "in file " + table_metadata_file, 0);
    ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
    auto storage = ast_create_query.storage;
    if (storage == nullptr || storage->engine == nullptr || storage->engine->name.empty())
    {
        throw Exception("Can not get table engine for file: " + table_metadata_file, ErrorCodes::LOGICAL_ERROR);
    }

    TiDB::TableInfo info;
    ASTFunction * engine = storage->engine;
    auto * args = typeid_cast<const ASTExpressionList *>(engine->arguments.get());
    if (args == nullptr)
        throw Exception("Can not cast table engine arguments", ErrorCodes::BAD_ARGUMENTS);

    const ASTLiteral * table_info_ast = nullptr;
    if (engine->name == MutableSupport::delta_tree_storage_name)
    {
        if (args->children.size() >= 2)
        {
            table_info_ast = typeid_cast<const ASTLiteral *>(args->children[1].get());
        }
    }
    else if (engine->name == MutableSupport::txn_storage_name)
    {
        if (args->children.size() >= 3)
        {
            table_info_ast = typeid_cast<const ASTLiteral *>(args->children[2].get());
        }
    }
    else
    {
        throw Exception("Unknown storage engine: " + engine->name, ErrorCodes::LOGICAL_ERROR);
    }

    if (table_info_ast && table_info_ast->value.getType() == Field::Types::String)
    {
        const auto table_info_json = safeGet<String>(table_info_ast->value);
        if (!table_info_json.empty())
        {
            info.deserialize(table_info_json);
            return {ast_create_query.table, info};
        }
    }

    throw Exception("Can not get TableInfo for file: " + table_metadata_file, ErrorCodes::BAD_ARGUMENTS);
}

void renamePath(const String & old_path, const String & new_path, Poco::Logger * log, bool must_success)
{
    if (auto file = Poco::File{old_path}; file.exists())
    {
        file.renameTo(new_path);
    }
    else
    {
        std::string err_msg = fmt::format(R"(Path "{}" is missing.)", old_path);
        if (must_success)
            throw Exception(err_msg);
        else
            LOG_WARNING(log, err_msg);
    }
}

void writeTableDefinitionToFile(
    const FileProviderPtr & file_provider,
    const String & table_meta_path,
    const ASTPtr & query,
    bool fsync_metadata)
{
    String table_meta_tmp_path = table_meta_path + ".tmp";
    {
        String statement = getTableDefinitionFromCreateQuery(query);

        /// Exclusive flags guarantees, that table is not created right now in another thread. Otherwise, exception will be thrown.
        WriteBufferFromFileProvider out(file_provider, table_meta_tmp_path, EncryptionPath(table_meta_tmp_path, ""), true, nullptr, statement.size(), O_WRONLY | O_CREAT | O_EXCL);
        writeString(statement, out);
        out.next();
        if (fsync_metadata)
            out.sync();
        out.close();
    }
    file_provider->renameFile(
        table_meta_tmp_path,
        EncryptionPath(table_meta_tmp_path, ""),
        table_meta_path,
        EncryptionPath(table_meta_path, ""),
        true);
}

void writeDatabaseDefinitionToFile(
    const FileProviderPtr & file_provider,
    const String & database_meta_path,
    const ASTPtr & query,
    bool fsync_metadata)
{
    String db_meta_tmp_path = database_meta_path + ".tmp";
    {
        String statement = getDatabaseDefinitionFromCreateQuery(query);

        /// Exclusive flags guarantees, that table is not created right now in another thread. Otherwise, exception will be thrown.
        WriteBufferFromFileProvider out(file_provider, db_meta_tmp_path, EncryptionPath(db_meta_tmp_path, ""), true, nullptr, statement.size(), O_WRONLY | O_CREAT | O_EXCL);
        writeString(statement, out);
        out.next();
        if (fsync_metadata)
            out.sync();
        out.close();
    }
    file_provider->renameFile(
        db_meta_tmp_path,
        EncryptionPath(db_meta_tmp_path, ""),
        database_meta_path,
        EncryptionPath(database_meta_path, ""),
        true);
}

ASTPtr parseCreateDatabaseAST(const String & statement)
{
    ParserCreateQuery parser;
    const char * pos = statement.data();
    std::string error_msg;
    auto ast = tryParseQuery(parser,
                             pos,
                             pos + statement.size(),
                             error_msg,
                             /*hilite=*/false,
                             String("in ") + __PRETTY_FUNCTION__,
                             /*allow_multi_statements=*/false,
                             0);
    if (!ast)
        throw Exception(error_msg, ErrorCodes::SYNTAX_ERROR);
    return ast;
}

// By default, only remove directory if it is empy
void tryRemoveDirectory(const String & directory, Poco::Logger * log, bool recursive = false)
{
    if (auto dir = Poco::File(directory); dir.exists() && dir.isDirectory())
    {
        try
        {
            dir.remove(/*recursive=*/recursive);
        }
        catch (Poco::DirectoryNotEmptyException &)
        {
            // just ignore and keep that directory if it is not empty
            LOG_FMT_WARNING(log, "Can not remove directory: {}, it is not empty", directory);
        }
    }
}

// This function will tidy up path and compare if them are the same one.
// For example "/tmp/data/a.sql" is equal to "/tmp//data//a.sql"
inline bool isSamePath(const String & lhs, const String & rhs)
{
    return Poco::Path{lhs}.absolute().toString() == Poco::Path{rhs}.absolute().toString();
}

} // namespace


// ================================================
//   TableDiskInfo
// ================================================

String IDAsPathUpgrader::TableDiskInfo::name() const
{
    // Name in table_info may not be updated, use the name in `ATTACH TABLE <name> ...`.
    // The name in table_info will be updated in later schema sync.
    return old_name;
}
String IDAsPathUpgrader::TableDiskInfo::newName() const
{
    return mapper->mapTableName(*tidb_table_info);
}
const TiDB::TableInfo & IDAsPathUpgrader::TableDiskInfo::getInfo() const
{
    return *tidb_table_info;
}

// "metadata/${db_name}/${tbl_name}.sql"
String IDAsPathUpgrader::TableDiskInfo::getMetaFilePath(const String & root_path, const DatabaseDiskInfo & db) const
{
    return db.getMetaDirectory(root_path) + escapeForFileName(name()) + ".sql";
}
// "data/${db_name}/${tbl_name}/"
String IDAsPathUpgrader::TableDiskInfo::getDataDirectory(
    const String & root_path,
    const DatabaseDiskInfo & db,
    bool escape_db,
    bool escape_tbl) const
{
    String res = db.getDataDirectory(root_path, escape_db);
    if (escape_tbl)
        res += escapeForFileName(name());
    else
        res += name();
    return res + "/";
}
// "extra_data/${db_name}/${tbl_name}/"
String IDAsPathUpgrader::TableDiskInfo::getExtraDirectory(
    const String & root_path,
    const DatabaseDiskInfo & db,
    bool escape_db,
    bool escape_tbl) const
{
    String res = db.getExtraDirectory(root_path, escape_db);
    if (escape_tbl)
        res += escapeForFileName(name());
    else
        res += name();
    return res + "/";
}

// "metadata/db_${db_id}/t_${id}.sql"
String IDAsPathUpgrader::TableDiskInfo::getNewMetaFilePath(const String & root_path, const DatabaseDiskInfo & db) const
{
    return db.getNewMetaDirectory(root_path) + escapeForFileName(newName()) + ".sql";
}
// "data/t_${id}/"
String IDAsPathUpgrader::TableDiskInfo::getNewDataDirectory(const String & root_path, const DatabaseDiskInfo & db) const
{
    return db.getNewDataDirectory(root_path) + escapeForFileName(newName()) + "/";
}
// "extra_data/t_${id}"
String IDAsPathUpgrader::TableDiskInfo::getNewExtraDirectory(const String & root_path, const DatabaseDiskInfo & db) const
{
    return db.getNewExtraDirectory(root_path) + escapeForFileName(newName()) + "/";
}

// ================================================
//   DatabaseDiskInfo
// ================================================

void IDAsPathUpgrader::DatabaseDiskInfo::setDBInfo(TiDB::DBInfoPtr info_)
{
    tidb_db_info = info_;
}

const TiDB::DBInfo & IDAsPathUpgrader::DatabaseDiskInfo::getInfo() const
{
    if (!hasValidTiDBInfo())
        throw Exception("Try to get database info of not inited database: " + name);
    return *tidb_db_info;
}

String IDAsPathUpgrader::DatabaseDiskInfo::newName() const
{
    return mapper->mapDatabaseName(getInfo());
}

String IDAsPathUpgrader::DatabaseDiskInfo::getTiDBSerializeInfo() const
{
    if (!hasValidTiDBInfo())
        throw Exception("Try to serialize database info of not inited database: " + name);
    return tidb_db_info->serialize();
}

// "metadata/${db_name}.sql"
String IDAsPathUpgrader::DatabaseDiskInfo::doGetMetaFilePath(const String & root_path, bool tmp) const
{
    String meta_dir = doGetMetaDirectory(root_path, tmp);
    return (endsWith(meta_dir, "/") ? meta_dir.substr(0, meta_dir.size() - 1) : meta_dir) + ".sql";
}
// "metadata/${db_name}/"
String IDAsPathUpgrader::DatabaseDiskInfo::doGetMetaDirectory(const String & root_path, bool tmp) const
{
    return root_path + (endsWith(root_path, "/") ? "" : "/") + "metadata/" + escapeForFileName(name + (tmp ? TMP_SUFFIX : "")) + "/";
}
// "data/${db_name}/"
String IDAsPathUpgrader::DatabaseDiskInfo::doGetDataDirectory(const String & root_path, bool escape, bool tmp) const
{
    // Old data path don't do escape for path
    if (escape)
        return root_path + (endsWith(root_path, "/") ? "" : "/") + "data/" + escapeForFileName(name + (tmp ? TMP_SUFFIX : "")) + "/";
    else
    {
        // Old extra data path (in PathPool) don't escape for path.
        return root_path + (endsWith(root_path, "/") ? "" : "/") + "data/" + name + (tmp ? TMP_SUFFIX : "") + "/";
    }
}
// "extra_data/${db_name}/"
String IDAsPathUpgrader::DatabaseDiskInfo::doGetExtraDirectory(const String & extra_root, bool escape, bool tmp) const
{
    if (escape)
        return extra_root + (endsWith(extra_root, "/") ? "" : "/") + escapeForFileName(name + (tmp ? TMP_SUFFIX : "")) + "/";
    else
    {
        // Old extra data path (in PathPool) don't escape for path.
        return extra_root + (endsWith(extra_root, "/") ? "" : "/") + name + (tmp ? TMP_SUFFIX : "") + "/";
    }
}

// "metadata/db_${id}.sql"
String IDAsPathUpgrader::DatabaseDiskInfo::getNewMetaFilePath(const String & root_path) const
{
    String meta_dir = getNewMetaDirectory(root_path);
    return (endsWith(meta_dir, "/") ? meta_dir.substr(0, meta_dir.size() - 1) : meta_dir) + ".sql";
}
// "metadata/db_${id}/"
String IDAsPathUpgrader::DatabaseDiskInfo::getNewMetaDirectory(const String & root_path) const
{
    return root_path + (endsWith(root_path, "/") ? "" : "/") + "/metadata/" + escapeForFileName(newName()) + "/";
}
// "data/"
String IDAsPathUpgrader::DatabaseDiskInfo::getNewDataDirectory(const String & root_path) const
{
    return root_path + "/data/";
}
// "extra_data/"
String IDAsPathUpgrader::DatabaseDiskInfo::getNewExtraDirectory(const String & extra_root) const
{
    return extra_root + "/";
}


void IDAsPathUpgrader::DatabaseDiskInfo::renameToTmpDirectories(const Context & ctx, Poco::Logger * log)
{
    if (moved_to_tmp)
        return;

    auto root_path = ctx.getPath();
    // Rename database meta file if exist
    renamePath(doGetMetaFilePath(root_path, false), doGetMetaFilePath(root_path, true), log, false);
    // Rename database meta dir
    renamePath(doGetMetaDirectory(root_path, false), doGetMetaDirectory(root_path, true), log, true);

    // Rename database data dir
    renamePath( //
        doGetDataDirectory(root_path, /*escape*/ true, /*tmp*/ false),
        doGetDataDirectory(root_path, /*escape*/ true, /*tmp*/ true),
        log,
        true);

    // Rename database data dir for multi-paths
    auto root_pool = ctx.getPathPool();
    for (const auto & extra_path : root_pool.listPaths())
        renamePath( //
            doGetExtraDirectory(extra_path, /*escape*/ true, /*tmp*/ false), //
            doGetExtraDirectory(extra_path, /*escape*/ true, /*tmp*/ true),
            log,
            false);

    moved_to_tmp = true;
}


// ================================================
//   IDAsPathUpgrader
// ================================================

IDAsPathUpgrader::IDAsPathUpgrader(Context & global_ctx_, bool is_mock_, std::unordered_set<std::string> reserved_databases_)
    : global_context(global_ctx_)
    , root_path{global_context.getPath()}
    , is_mock(is_mock_)
    , mapper(is_mock ? std::make_shared<MockSchemaNameMapper>() //
                     : std::make_shared<SchemaNameMapper>())
    , reserved_databases{std::move(reserved_databases_)}
    , log{&Poco::Logger::get("IDAsPathUpgrader")}
{}

bool IDAsPathUpgrader::needUpgrade()
{
    const auto metadataPath = global_context.getPath() + "/metadata";

    // For old version, we have database directories and its `.sql` file
    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator it(metadataPath); it != dir_end; ++it)
    {
        if (!it->isDirectory())
            continue;

        /// For '.svn', '.gitignore' directory and similar.
        if (it.name().at(0) == '.')
            continue;

        if (it.name() == SYSTEM_DATABASE)
            continue;

        String db_name = unescapeForFileName(it.name());
        databases.emplace(db_name, DatabaseDiskInfo{db_name, mapper});
    }

    bool has_old_db_engine = false;
    for (auto && [db_name, db_info] : databases)
    {
        (void)db_name;
        const String database_metadata_file = db_info.getMetaFilePath(root_path);
        auto engine = getDatabaseEngine(global_context.getFileProvider(), database_metadata_file);
        db_info.engine = engine->name;
        if (db_info.engine != "TiFlash")
        {
            has_old_db_engine = true;
        }
    }

    return has_old_db_engine;
}

std::vector<TiDB::DBInfoPtr> IDAsPathUpgrader::fetchInfosFromTiDB() const
{
    // Fetch DBs info from TiDB/TiKV
    // Note: Not get table info from TiDB, just rename according to TableID in persisted TableInfo
    for (size_t i = 0; i < 60; i++) // retry for 3 mins
    {
        try
        {
            auto schema_syncer = global_context.getTMTContext().getSchemaSyncer();
            return schema_syncer->fetchAllDBs();
        }
        catch (Poco::Exception & e)
        {
            const int wait_seconds = 3;
            LOG_FMT_ERROR(
                log,
                "Upgrade failed because fetch schema error: {}\nWe will sleep for {} seconds and try again.",
                e.displayText(),
                wait_seconds);
            ::sleep(wait_seconds);
        }
    }
    throw Exception("Upgrade failed because fetch schema error.");
}

static void dropAbsentDatabase(
    Context & context,
    const String & db_name,
    const IDAsPathUpgrader::DatabaseDiskInfo & db_info,
    Poco::Logger * log)
{
    if (db_info.hasValidTiDBInfo())
        throw Exception("Invalid call for dropAbsentDatabase for database " + db_name + " with info: " + db_info.getTiDBSerializeInfo());

    /// tryRemoveDirectory with recursive=true to clean up

    const auto root_path = context.getPath();
    // Remove old metadata dir
    const String old_meta_dir = db_info.getMetaDirectory(root_path);
    tryRemoveDirectory(old_meta_dir, log, true);
    // Remove old metadata file
    const String old_meta_file = db_info.getMetaFilePath(root_path);
    if (auto file = Poco::File(old_meta_file); file.exists())
        file.remove();
    else
        LOG_FMT_WARNING(log, "Can not remove database meta file: {}", old_meta_file);
    // Remove old data dir
    const String old_data_dir = db_info.getDataDirectory(root_path);
    tryRemoveDirectory(old_data_dir, log, true);
    // not escaped dir created by old PathPool
    const String old_data_dir_not_escaped = db_info.getDataDirectory(root_path, false);
    tryRemoveDirectory(old_data_dir_not_escaped, log, true);

    const auto & data_extra_paths = context.getPathPool();
    for (const auto & extra_root_path : data_extra_paths.listPaths())
    {
        tryRemoveDirectory(db_info.getExtraDirectory(extra_root_path), log, true);
        tryRemoveDirectory(db_info.getExtraDirectory(extra_root_path, false), log, true);
    }
}

void IDAsPathUpgrader::linkDatabaseTableInfos(const std::vector<TiDB::DBInfoPtr> & all_databases)
{
    for (const auto & db : all_databases)
    {
        if (auto iter = databases.find(db->name); iter != databases.end())
        {
            iter->second.setDBInfo(db);
        }
    }

    // list all table in old style.
    for (auto iter = databases.begin(); iter != databases.end(); /*empty*/)
    {
        const auto & db_name = iter->first;
        auto & db_info = iter->second;
        if (!db_info.hasValidTiDBInfo())
        {
            // If we can't find it in TiDB, maybe it already dropped.
            if (reserved_databases.count(db_name) > 0)
            {
                // For mock test or develop environment, we may reserve some database
                // for convenience. Keep them as what they are. Print warnings and
                // ignore it in later upgrade.
                LOG_FMT_WARNING(log, "Database {} is reserved, ignored in upgrade.", db_name);
            }
            else
            {
                // If we keep them as "Ordinary", when user actually create database with
                // same name, next time TiFlash restart and will try to do "upgrade" on
                // those legacy data, and it will mess everything up.
                // Drop them.
                dropAbsentDatabase(global_context, db_name, db_info, log);
            }
            iter = databases.erase(iter);
            continue;
        }

        if (db_info.engine == "TiFlash")
        {
            ++iter;
            continue;
        }

        const String db_meta_dir = db_info.getMetaDirectory(root_path);
        std::vector<std::string> file_names = DatabaseLoading::listSQLFilenames(db_meta_dir, log);
        for (const auto & table_filename : file_names)
        {
            String table_meta_file = db_meta_dir + "/" + table_filename;
            // Name in table_info may not be updated, use the name in `ATTACH TABLE <name> ...`.
            auto [old_name, table_info] = getTableInfo(global_context.getFileProvider(), table_meta_file);
            db_info.tables.emplace_back( //
                TableDiskInfo{old_name, std::make_shared<TiDB::TableInfo>(table_info), mapper});
        }
        ++iter;
    }
}

void IDAsPathUpgrader::fixNotEscapedDirectories()
{
    for (const auto & [db_name, db_info] : databases)
    {
        const auto db_name_escaped = escapeForFileName(db_name);

        // database's meta file, meta dir (created by old DatabaseOrdinary) is escaped.
        // only need to create data path
        if (db_name != db_name_escaped)
        {
            LOG_FMT_INFO(log, "database `{}` fixing name escape to `{}`", db_name, db_name_escaped);
            // Create directory for escaped database
            auto escaped_db_data_dir = db_info.getDataDirectory(root_path, /*escape=*/true);
            if (Poco::File dir(escaped_db_data_dir); !dir.exists())
                dir.createDirectory();

            const auto & data_extra_paths = global_context.getPathPool();
            for (const auto & extra_root_path : data_extra_paths.listPaths())
            {
                auto escaped_extra_dir = db_info.getExtraDirectory(extra_root_path, /*escape=*/true);
                if (Poco::File dir(escaped_extra_dir); !dir.exists())
                    dir.createDirectory();
            }
        }

        /// Fix not escaped name for table
        for (const auto & table : db_info.tables)
        {
            const auto table_name_escaped = escapeForFileName(table.name());
            if (db_name_escaped == db_name && table_name_escaped == table.name())
                continue;

            LOG_FMT_INFO(
                log,
                "table `{}`.`{}` fixing name escape to `{}`.`{}`",
                db_name,
                table.name(),
                db_name_escaped,
                table_name_escaped);
            // Table's metadata don't need to fix.

            // Fix data path. It was create by DatabaseOrdinary and StorageDeltaMerge,
            // database name is escaped but table name not.
            auto not_escaped_path = table.getDataDirectory(root_path, db_info, /*escape_db*/ true, /*escape_tbl*/ false);
            auto escaped_path = table.getDataDirectory(root_path, db_info, /*escape_db*/ true, /*escape_tbl*/ true);
            if (auto file = Poco::File{not_escaped_path}; file.exists())
            {
                if (auto escaped_dir = Poco::File{escaped_path}; !escaped_dir.exists())
                    escaped_dir.createDirectory();
                renamePath(not_escaped_path + "/meta", escaped_path + "/meta", log, true);
                renamePath(not_escaped_path + "/data", escaped_path + "/data", log, true);
                renamePath(not_escaped_path + "/log", escaped_path + "/log", log, true);
                // For the cases that database's name did not need to be escaped but table's name did.
                renamePath(not_escaped_path + "/stable", escaped_path + "/stable", log, false);
                tryRemoveDirectory(not_escaped_path, log);
            }
            auto db_tbl_not_escaped_path = not_escaped_path;
            if (db_name != db_name_escaped)
            {
                // For the cases that database's name need to be escaped.
                // Stable dir was created by old PathPool, database name and table name were not escaped.
                db_tbl_not_escaped_path = table.getDataDirectory(root_path, db_info, false, false);
                auto not_escaped_stable = db_tbl_not_escaped_path + "/stable";
                auto escaped_stable = table.getDataDirectory(root_path, db_info, true, true) + "/stable";
                if (auto file = Poco::File{not_escaped_stable}; file.exists())
                    renamePath(not_escaped_stable, escaped_stable, log, true);
            }

            // Fix extra path.
            const auto & data_extra_paths = global_context.getPathPool();
            for (const auto & extra_root_path : data_extra_paths.listPaths())
            {
                // It was created by old PathPool, both database name and table name are not escaped.
                auto not_escaped_extra_path = table.getExtraDirectory(extra_root_path, db_info, /*escape_db*/ false, /*escape_tbl*/ false);
                if (isSamePath(not_escaped_extra_path, db_tbl_not_escaped_path))
                    continue;
                auto escaped_extra_path = table.getExtraDirectory(extra_root_path, db_info, /*escape_db*/ true, /*escape_tbl*/ true);
                renamePath(not_escaped_extra_path, escaped_extra_path, log, false);
            }
            LOG_FMT_INFO(
                log,
                "table `{}`.`{}` fixing name escape to `{}`.`{}` done.",
                db_name,
                table.name(),
                db_name_escaped,
                table_name_escaped);
        }

        if (db_name != db_name_escaped)
        {
            // clean not escaped database dir created by old PathPool
            const String not_escaped_data_dir = db_info.getDataDirectory(root_path, /*escape*/ false);
            tryRemoveDirectory(not_escaped_data_dir, log, true);
            const auto & data_extra_paths = global_context.getPathPool();
            for (const auto & extra_root_path : data_extra_paths.listPaths())
            {
                auto not_escaped_extra_data_dir = db_info.getExtraDirectory(extra_root_path, /*escape*/ false);
                if (isSamePath(not_escaped_data_dir, not_escaped_extra_data_dir))
                    continue;
                tryRemoveDirectory(not_escaped_extra_data_dir, log);
            }
        }
        LOG_FMT_INFO(log, "database `{}` fixing name escape to `{}` done.", db_name, db_name_escaped);
    }
}

void IDAsPathUpgrader::resolveConflictDirectories()
{
    std::unordered_set<String> conflict_databases;
    for (const auto & [db_name, db_info] : databases)
    {
        // In theory, user can create database naming "t_xx" and there is cyclic renaming between table and database.
        // First detect if there is any database may have cyclic rename with table.
        for (const auto & table : db_info.tables)
        {
            const auto new_tbl_name = table.newName();
            if (auto iter = databases.find(new_tbl_name); iter != databases.end())
            {
                conflict_databases.insert(iter->first);
                LOG_FMT_INFO(
                    log,
                    "Detect cyclic renaming between table `{}`.`{}`(new name:{}) and database `{}`",
                    db_name,
                    table.name(),
                    new_tbl_name,
                    iter->first);
            }
        }

        // In theory, user can create two database naming "db_xx" and there is cyclic renaming.
        // We need to break that cyclic.
        const auto new_database_name = db_info.newName();
        if (auto iter = databases.find(new_database_name); iter != databases.end())
        {
            conflict_databases.insert(iter->first);
            LOG_FMT_INFO(
                log,
                "Detect cyclic renaming between database `{}`(new name:{}) and database `{}`",
                db_name,
                new_database_name,
                iter->first);
        }
    }
    LOG_FMT_INFO(log, "Detect {} cyclic renaming", conflict_databases.size());
    for (const auto & db_name : conflict_databases)
    {
        auto iter = databases.find(db_name);
        auto & db_info = iter->second;
        LOG_FMT_INFO(log, "Move {} to tmp directories..", db_name);
        db_info.renameToTmpDirectories(global_context, log);
    }
}

void IDAsPathUpgrader::doRename()
{
    for (const auto & [db_name, db_info] : databases)
    {
        renameDatabase(db_name, db_info);
    }
}

void IDAsPathUpgrader::renameDatabase(const String & db_name, const DatabaseDiskInfo & db_info)
{
    const auto mapped_db_name = db_info.newName();

    {
        // Create directory for target database
        auto new_db_meta_dir = db_info.getNewMetaDirectory(root_path);
        Poco::File(new_db_meta_dir).createDirectory();
    }

    // Rename all tables of this database
    for (const auto & table : db_info.tables)
    {
        renameTable(db_name, db_info, mapped_db_name, table);
    }

    // Then rename database
    LOG_FMT_INFO(log, "database `{}` to `{}` renaming", db_name, mapped_db_name);
    {
        // Recreate metadata file for database
        const String new_meta_file = db_info.getNewMetaFilePath(root_path);
        const String statement = "ATTACH DATABASE `" + mapped_db_name + "` ENGINE=TiFlash('" + db_info.getTiDBSerializeInfo() + "', 1)\n";
        auto ast = parseCreateDatabaseAST(statement);
        const auto & settings = global_context.getSettingsRef();
        writeDatabaseDefinitionToFile(global_context.getFileProvider(), new_meta_file, ast, settings.fsync_metadata);
    }

    {
        // Remove old metadata dir
        const String old_meta_dir = db_info.getMetaDirectory(root_path);
        tryRemoveDirectory(old_meta_dir, log);
        // Remove old metadata file
        const String old_meta_file = db_info.getMetaFilePath(root_path);
        if (auto file = Poco::File(old_meta_file); file.exists())
            file.remove();
        else
            LOG_FMT_WARNING(log, "Can not remove database meta file: {}", old_meta_file);
        // Remove old data dir
        const String old_data_dir = db_info.getDataDirectory(root_path);
        tryRemoveDirectory(old_data_dir, log);
        const auto & data_extra_paths = global_context.getPathPool();
        for (const auto & extra_root_path : data_extra_paths.listPaths())
        {
            tryRemoveDirectory(db_info.getExtraDirectory(extra_root_path), log);
        }
    }
    LOG_FMT_INFO(log, "database `{}` to `{}` rename done.", db_name, mapped_db_name);
}

void IDAsPathUpgrader::renameTable(
    const String & db_name,
    const DatabaseDiskInfo & db_info,
    const String & mapped_db_name,
    const TableDiskInfo & table)
{
    const auto mapped_table_name = table.newName();
    LOG_FMT_INFO(
        log,
        "table `{}`.`{}` to `{}`.`{}` renaming",
        db_name,
        table.name(),
        mapped_db_name,
        mapped_table_name);

    String old_tbl_data_path;
    {
        // Former data path use ${path}/data/${database}/${table}/ as data path.
        // Rename it to ${path}/data/${mapped_table_name}.
        old_tbl_data_path = table.getDataDirectory(root_path, db_info);
        renamePath(old_tbl_data_path, table.getNewDataDirectory(root_path, db_info), log, true);
    }

    {
        // Rename data path for multi disk
        auto data_extra_paths = global_context.getPathPool();
        for (const auto & extra_root_path : data_extra_paths.listPaths())
        {
            auto old_tbl_extra_data_path = table.getExtraDirectory(extra_root_path, db_info);
            if (isSamePath(old_tbl_extra_data_path, old_tbl_data_path))
                continue;
            renamePath(old_tbl_extra_data_path, table.getNewExtraDirectory(extra_root_path, db_info), log, false);
        }
    }

    // Recreate metadata file
    {
        auto old_tbl_meta_file = table.getMetaFilePath(root_path, db_info);
        auto ast = DatabaseLoading::getQueryFromMetadata(global_context, old_tbl_meta_file, /*throw_on_error=*/true);
        if (!ast)
            throw Exception("There is no metadata file for table " + table.name() + ", expected file: " + old_tbl_meta_file,
                            ErrorCodes::FILE_DOESNT_EXIST);

        ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
        ast_create_query.table = mapped_table_name;
        ASTStorage * storage_ast = ast_create_query.storage;
        TiDB::TableInfo table_info = table.getInfo(); // get a copy
        if (table_info.is_partition_table)
        {
            LOG_FMT_INFO(
                log,
                "partition table `{}`.`{}` to `{}`.`{}` update table info",
                db_name,
                table.name(),
                mapped_db_name,
                mapped_table_name);
            // Old partition name is "${table_name}_${physical_id}" while new name is "t_${physical_id}"
            // If it is a partition table, we need to update TiDB::TableInfo::name
            do
            {
                if (!storage_ast || !storage_ast->engine)
                    break;
                auto * args = typeid_cast<ASTExpressionList *>(storage_ast->engine->arguments.get());
                if (!args)
                    break;

                table_info.name = mapper->mapPartitionName(table_info);
                std::shared_ptr<ASTLiteral> literal = std::make_shared<ASTLiteral>(Field(table_info.serialize()));
                if (args->children.size() == 1)
                    args->children.emplace_back(literal);
                else if (args->children.size() >= 2)
                    args->children.at(1) = literal;
            } while (0);
        }

        const String new_tbl_meta_file = table.getNewMetaFilePath(root_path, db_info);
        const auto & settings = global_context.getSettingsRef();
        writeTableDefinitionToFile(global_context.getFileProvider(), new_tbl_meta_file, ast, settings.fsync_metadata);

        // Remove old metadata file
        if (auto file = Poco::File(old_tbl_meta_file); file.exists())
            file.remove();
    }

    LOG_FMT_INFO(
        log,
        "table `{}`.`{}` to `{}`.`{}` rename done.",
        db_name,
        table.name(),
        mapped_db_name,
        mapped_table_name);
}

void IDAsPathUpgrader::doUpgrade()
{
    auto all_databases = fetchInfosFromTiDB();
    linkDatabaseTableInfos(all_databases);
    fixNotEscapedDirectories();
    // Check if destination db / tbl file exists and resolve conflict
    resolveConflictDirectories();
    // Rename
    doRename();
}

} // namespace DB
