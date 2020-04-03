#include <Common/typeid_cast.h>
#include <DataStreams/StringStreamBlockInputStream.h>
#include <Debug/dbgFuncSchemaName.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/IManageableStorage.h>
#include <Storages/Transaction/SchemaNameMapper.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>

#include <boost/algorithm/string/replace.hpp>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
} // namespace ErrorCodes

using QualifiedName = std::pair<String, String>;

std::optional<String> reverseDatabase(Context & context, const String & database_name)
{
    TMTContext & tmt = context.getTMTContext();
    auto syncer = tmt.getSchemaSyncer();
    auto db_info = syncer->getDBInfoByName(database_name);
    if (db_info == nullptr)
        return std::nullopt;
    return SchemaNameMapper().mapDatabaseName(*db_info);
}

std::optional<QualifiedName> reverseTable(Context & context, const String & database_name, const String & table_name)
{
    auto reversed_db = reverseDatabase(context, database_name);
    if (reversed_db == std::nullopt)
        return std::nullopt;
    TMTContext & tmt = context.getTMTContext();
    auto storage = tmt.getStorages().getByName(reversed_db.value(), table_name, false);
    if (storage == nullptr)
        return std::nullopt;
    return std::make_pair(storage->getDatabaseName(), storage->getTableName());
}

void dbgFuncReverseDatabase(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 1)
        throw Exception("Args not matched, should be: database-name", ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;

    std::stringstream ss;
    auto reversed = reverseDatabase(context, database_name);
    if (reversed == std::nullopt)
        ss << "Database " << database_name << " not found.";
    else
        ss << reversed.value();
    output(ss.str());
}

void dbgFuncReverseTable(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 2)
        throw Exception("Args not matched, should be: database-name, table-name", ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;

    std::stringstream ss;
    auto reversed = reverseTable(context, database_name, table_name);
    if (reversed == std::nullopt)
        ss << "Table " << database_name << "." << table_name << " not found.";
    else
        ss << reversed->first << "." << reversed->second;
    output(ss.str());
}

BlockInputStreamPtr dbgFuncReverseQuery(Context & context, const ASTs & args)
{
    if (args.size() != 3)
        throw Exception("Args not matched, should be: query, database-name, table-name", ErrorCodes::BAD_ARGUMENTS);

    String query = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[2]).name;

    auto reversed = reverseTable(context, database_name, table_name);
    if (reversed == std::nullopt)
    {
        std::shared_ptr<StringStreamBlockInputStream> res = std::make_shared<StringStreamBlockInputStream>("Error");
        res->append("Table " + database_name + "." + table_name + " not found.");
        return res;
    }

    boost::algorithm::replace_all(query, "$d", reversed->first);
    boost::algorithm::replace_all(query, "$t", reversed->second);

    return executeQuery(query, context, true).in;
}

} // namespace DB
