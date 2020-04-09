#include <Common/StringUtils/StringUtils.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <Databases/DatabaseOrdinary.h>
#include <Databases/DatabasesCommon.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
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
std::shared_ptr<ASTFunction> getDatabaseEngine(const String & db_name, const String & filename)
{
    String query;
    if (Poco::File(filename).exists())
    {
        ReadBufferFromFile in(filename, 1024);
        readStringUntilEOF(query, in);
    }
    else if (db_name == "default")
    {
        return std::static_pointer_cast<ASTFunction>(makeASTFunction("Ordinary"));
    }
    else
    {
        throw Exception("Can not open database schema file: " + filename, ErrorCodes::LOGICAL_ERROR);
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

std::optional<TiDB::TableInfo> getTableInfo(const String & table_metadata_file)
{
    String definition;
    if (Poco::File(table_metadata_file).exists())
    {
        ReadBufferFromFile in(table_metadata_file, 1024);
        readStringUntilEOF(definition, in);
    }
    else
    {
        throw Exception("Can not open database schema file: " + table_metadata_file, ErrorCodes::LOGICAL_ERROR);
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
    if (engine->name == MutableSupport::delta_tree_storage_name)
    {
        if (args->children.size() >= 2)
        {
            auto ast = typeid_cast<const ASTLiteral *>(args->children[1].get());
            if (ast && ast->value.getType() == Field::Types::String)
            {
                const auto table_info_json = safeGet<String>(ast->value);
                if (!table_info_json.empty())
                {
                    info.deserialize(table_info_json);
                    if (unlikely(info.columns.empty()))
                    {
                        throw Exception("", ErrorCodes::BAD_ARGUMENTS);
                    }
                    return std::make_optional(info);
                }
            }
        }
    }
    else if (engine->name == MutableSupport::txn_storage_name)
    {
        // TODO:
    }
    else
    {
        throw Exception("Unknown storage engine: " + engine->name, ErrorCodes::LOGICAL_ERROR);
    }

    return std::nullopt;
}

void renamePath(const String & old_path, const String & new_path, Poco::Logger * log, bool must_success)
{
    if (auto file = Poco::File{old_path}; file.exists())
    {
        file.renameTo(new_path);
    }
    else
    {
        if (must_success)
            throw Exception("Path \"" + old_path + "\" is missed.");
        else
            LOG_WARNING(log, "Path \"" << old_path << "\" is missed.");
    }
}

void writeTableDefinitionToFile(const String & table_meta_path, const ASTPtr & query, bool fsync_metadata)
{
    String table_meta_tmp_path = table_meta_path + ".tmp";
    {
        String statement = getTableDefinitionFromCreateQuery(query);

        /// Exclusive flags guarantees, that table is not created right now in another thread. Otherwise, exception will be thrown.
        WriteBufferFromFile out(table_meta_tmp_path, statement.size(), O_WRONLY | O_CREAT | O_EXCL);
        writeString(statement, out);
        out.next();
        if (fsync_metadata)
            out.sync();
        out.close();
    }
    Poco::File(table_meta_tmp_path).renameTo(table_meta_path);
}

void writeDatabaseDefinitionToFile(const String & database_meta_path, const ASTPtr & query, bool fsync_metadata)
{
    String db_meta_tmp_path = database_meta_path + ".tmp";
    {
        String statement = getDatabaseDefinitionFromCreateQuery(query);

        /// Exclusive flags guarantees, that table is not created right now in another thread. Otherwise, exception will be thrown.
        WriteBufferFromFile out(db_meta_tmp_path, statement.size(), O_WRONLY | O_CREAT | O_EXCL);
        writeString(statement, out);
        out.next();
        if (fsync_metadata)
            out.sync();
        out.close();
    }
    Poco::File(db_meta_tmp_path).renameTo(database_meta_path);
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

} // namespace

String IDAsPathUpgrader::DatabaseDiskInfo::getMetaFilePath() const
{
    return (endsWith(meta_dir_path, "/") ? meta_dir_path.substr(0, meta_dir_path.size() - 1) : meta_dir_path) + ".sql";
}

IDAsPathUpgrader::IDAsPathUpgrader(Context & global_ctx_) : global_context(global_ctx_), log(&Logger::get("IDAsPathUpgrader")) {}

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

        databases.emplace(unescapeForFileName(it.name()), DatabaseDiskInfo{it.path().toString()});
    }

    bool has_old_db_engine = false;
    for (auto && [db_name, db_info] : databases)
    {
        const String database_metadata_file = db_info.meta_dir_path + ".sql";
        auto engine = getDatabaseEngine(db_name, database_metadata_file);
        db_info.engine = engine->name;
        if (db_info.engine != "TiFlash")
        {
            has_old_db_engine = true;
        }
    }

    return has_old_db_engine;
}

std::tuple<std::vector<TiDB::DBInfoPtr>, std::vector<std::pair<TableID, DatabaseID>>> IDAsPathUpgrader::fetchInfosFromTiDB() const
{
    // Fetch DBs and tables info from TiDB/TiKV
    auto schema_syncer = global_context.getTMTContext().getSchemaSyncer();
    std::vector<TiDB::DBInfoPtr> all_databases = schema_syncer->fetchAllDBs();
    std::vector<std::pair<TableID, DatabaseID>> //
        all_tables_mapping = schema_syncer->fetchAllTablesMapping(all_databases);

    return {all_databases, all_tables_mapping};
}

void IDAsPathUpgrader::linkDatabaseTableInfos(
    const std::vector<TiDB::DBInfoPtr> & all_databases, const std::vector<std::pair<TableID, DatabaseID>> & /*all_tables_mapping*/)
{
    for (const auto & db : all_databases)
    {
        if (auto iter = databases.find(db->name); iter != databases.end())
        {
            iter->second.id = db->id;
        }
    }

    // list all table in former directories.
    for (auto && [db_name, db_info] : databases)
    {
        std::vector<std::string> file_names = DatabaseOrdinary::listTableFilenames(db_info.meta_dir_path, log);
        const String database_data_dir = global_context.getPath() + "data/" + escapeForFileName(db_name) + "/";
        for (const auto & table_filename : file_names)
        {
            String table_meta_file = db_info.meta_dir_path + "/" + table_filename;
            auto table_info = getTableInfo(table_meta_file);
            if (table_info.has_value())
            {
                TableDiskInfo disk_info;
                disk_info.meta_file_path = std::move(table_meta_file);
                disk_info.id = table_info->id;
                disk_info.name = table_info->name;
                db_info.tables.emplace_back(std::move(disk_info));
            }
        }
    }
}

void resolveConflictDirectories() {}

void IDAsPathUpgrader::doRename()
{
    for (const auto & [db_name, db_info] : databases)
    {
        renameDatabase(db_name, db_info);
    }
}

void IDAsPathUpgrader::renameDatabase(const String & db_name, const DatabaseDiskInfo & db_info)
{
    const auto mapped_db_name = mapper.mapDatabaseName(db_info.id);

    // First rename all tables of this database
    for (const auto & table : db_info.tables)
    {
        renameTable(db_name, mapped_db_name, table);
    }

    // Then rename database
    LOG_INFO(log, "Renaming database `" << db_name << "` to `" << mapped_db_name << "`");
    {
        // Recreate metadata file for database
        const String old_meta_file = db_info.getMetaFilePath();
        const String new_meta_file = global_context.getPath() + "/metadata/" + mapped_db_name + ".sql";
        const String statement = "ATTACH DATABASE `" + mapped_db_name + "` ENGINE=TiFlash";
        auto ast = parseCreateDatabaseAST(statement);
        const auto & settings = global_context.getSettingsRef();
        writeDatabaseDefinitionToFile(new_meta_file, ast, settings.fsync_metadata);
        // Remove old metadata file
        Poco::File(old_meta_file).remove();
    }
    LOG_INFO(log, "Rename database `" << db_name << "` to `" << mapped_db_name << "` done.");
}

void IDAsPathUpgrader::renameTable(const String & db_name, const String & mapped_db_name, const TableDiskInfo & table)
{
    const auto mapped_table_name = mapper.mapTableName(table.id);
    LOG_INFO(log,
        "Renaming table `" << db_name << "`.`" << table.name << "` to `" //
                           << mapped_db_name << "`.`" << mapped_table_name << "`");

    {
        // Former data path use ${path}/data/${database}/${table}/ as data path.
        // Rename it to ${path}/data/${mapped_table_name}.
        auto data_root_path = global_context.getPath() + "/data";
        auto old_tbl_data_path = data_root_path + "/" + db_name + "/" + table.name;
        auto new_tbl_data_path = data_root_path + "/" + mapped_table_name;
        renamePath(old_tbl_data_path, new_tbl_data_path, log, true);
    }

    {
        // Rename data path for multi disk
        auto data_extra_paths = global_context.getExtraPaths();
        for (const auto & extra_root_path : data_extra_paths.listPaths())
        {
            auto old_tbl_data_path = extra_root_path + "/" + db_name + "/" + table.name;
            auto new_tbl_data_path = extra_root_path + "/" + mapped_table_name;
            renamePath(old_tbl_data_path, new_tbl_data_path, log, false);
        }
    }

    // Recreate metadata path
    {
        auto ast = DatabaseOrdinary::getQueryFromMetadata(table.meta_file_path, /*throw_on_error=*/true);
        if (!ast)
            throw Exception("There is no metadata file for table " + table.name + ", expected file: " + table.meta_file_path,
                ErrorCodes::FILE_DOESNT_EXIST);
        ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
        ast_create_query.table = mapped_table_name;
        const String new_tbl_meta_file = global_context.getPath() + "/metadata/" + mapped_table_name + ".sql";
        const auto & settings = global_context.getSettingsRef();
        writeTableDefinitionToFile(new_tbl_meta_file, ast, settings.fsync_metadata);
    }

    // Remove old metadata path
    Poco::File(table.meta_file_path).remove();

    LOG_INFO(log,
        "Rename table `" << db_name << "`.`" << table.name << "` to `" //
                         << mapped_db_name << "`.`" << mapped_table_name << "` done.");
}

void IDAsPathUpgrader::doUpgrade()
{
    auto [all_databases, all_table_mapping] = fetchInfosFromTiDB();
    linkDatabaseTableInfos(all_databases, all_table_mapping);
    // Check if destination db / tbl file exists and resolve conflict
    // Rename
    doRename();
}

} // namespace DB
