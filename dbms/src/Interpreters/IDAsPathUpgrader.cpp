#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <IO/ReadBufferFromFile.h>
#include <Interpreters/Context.h>
#include <Interpreters/IDAsPathUpgrader.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/FileStream.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/TiDBSchemaSyncer.h>

namespace DB
{

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
} // namespace

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

        databases.emplace(unescapeForFileName(it.name()), DatabaseDiskInfo{it.path().toString(), ""});
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

void IDAsPathUpgrader::prepare()
{
    // Fetch DBs and tables info from TiDB/TiKV
    auto schema_syncer = global_context.getTMTContext().getSchemaSyncer();
    std::vector<TiDB::DBInfoPtr> all_databases = schema_syncer->fetchAllDBs();
    for (const auto & db : all_databases)
    {
        if (auto iter = databases.find(db->name); iter != databases.end())
        {
            iter->second.id = db->id;
        }
    }

    std::vector<std::pair<TableInfoPtr, DBInfoPtr>> all_tables = schema_syncer->fetchAllTables(all_databases);
}

void resolveConflictDatabaseDirectories()
{

}

void resolveConflictTableDirectories()
{

}

void IDAsPathUpgrader::doUpgrade()
{
    prepare();
    // Check if destination db / tbl file exists and resolve conflict
    // Rename
}

} // namespace DB
