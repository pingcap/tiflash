#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <Databases/DatabaseOrdinary.h>
#include <IO/ReadBufferFromFile.h>
#include <Interpreters/Context.h>
#include <Interpreters/IDAsPathUpgrader.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/FileStream.h>
#include <Storages/MutableSupport.h>
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
} // namespace

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

        databases.emplace(unescapeForFileName(it.name()), DatabaseDiskInfo{it.path().toString(), "", 0});
    }

    bool has_old_db_engine = false;
    for (auto && [db_name, db_info] : databases)
    {
        const String database_metadata_file = db_info.path + ".sql";
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
    for (const auto & [db_name, db_info] : databases)
    {
        std::vector<std::string> file_names = DatabaseOrdinary::listTableFilenames(db_info.path, log);
        const String database_data_dir = global_context.getPath() + "data/" + escapeForFileName(db_name) + "/";
        for (const auto & table_filename : file_names)
        {
            TableDiskInfo disk_info;
            disk_info.meta_file_path = db_info.path + "/" + table_filename;
            auto table_info = getTableInfo(disk_info.meta_file_path);
            if (table_info.has_value())
            {
                disk_info.id = table_info->id;
                disk_info.data_path = {}; // TODO:
                tables.emplace(table_info->name, disk_info);
            }
        }
    }
}

void resolveConflictDirectories() {}

void IDAsPathUpgrader::doUpgrade()
{
    auto [all_databases, all_table_mapping] = fetchInfosFromTiDB();
    linkDatabaseTableInfos(all_databases, all_table_mapping);
    // Check if destination db / tbl file exists and resolve conflict
    // Rename
}

} // namespace DB
