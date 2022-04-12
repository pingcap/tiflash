// Copyright 2022 PingCAP, Ltd.
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
#include <fmt/core.h>

#include <boost/algorithm/string/replace.hpp>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
} // namespace ErrorCodes

using QualifiedName = std::pair<String, String>;

std::optional<String> mappedDatabase(Context & context, const String & database_name)
{
    TMTContext & tmt = context.getTMTContext();
    auto syncer = tmt.getSchemaSyncer();
    auto db_info = syncer->getDBInfoByName(database_name);
    if (db_info == nullptr)
        return std::nullopt;
    return SchemaNameMapper().mapDatabaseName(*db_info);
}

std::optional<QualifiedName> mappedTable(Context & context, const String & database_name, const String & table_name)
{
    auto mapped_db = mappedDatabase(context, database_name);
    if (mapped_db == std::nullopt)
        return std::nullopt;
    TMTContext & tmt = context.getTMTContext();
    auto storage = tmt.getStorages().getByName(mapped_db.value(), table_name, false);
    if (storage == nullptr)
        return std::nullopt;
    return std::make_pair(storage->getDatabaseName(), storage->getTableName());
}

void dbgFuncMappedDatabase(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 1)
        throw Exception("Args not matched, should be: database-name", ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;

    auto mapped = mappedDatabase(context, database_name);
    if (mapped == std::nullopt)
        output(fmt::format("Database {} not found.", database_name));
    else
        output(fmt::format(mapped.value()));
}

void dbgFuncMappedTable(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() < 2 || args.size() > 3)
        throw Exception("Args not matched, should be: database-name, table-name[, qualify = 'true']", ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    bool qualify = true;
    if (args.size() == 3)
        qualify = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[2]).value) == "true";

    auto mapped = mappedTable(context, database_name, table_name);
    if (mapped == std::nullopt)
        output(fmt::format("Table {}.{} not found.", database_name, table_name));
    else if (qualify)
        output(fmt::format("{}.{}", mapped->first, mapped->second));
    else
        output(fmt::format(mapped->second));
}

BlockInputStreamPtr dbgFuncQueryMapped(Context & context, const ASTs & args)
{
    if (args.size() < 2 || args.size() > 3)
        throw Exception("Args not matched, should be: query, database-name[, table-name]", ErrorCodes::BAD_ARGUMENTS);

    String query = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;

    if (args.size() == 3)
    {
        const String & table_name = typeid_cast<const ASTIdentifier &>(*args[2]).name;
        auto mapped = mappedTable(context, database_name, table_name);
        if (mapped == std::nullopt)
        {
            std::shared_ptr<StringStreamBlockInputStream> res = std::make_shared<StringStreamBlockInputStream>("Error");
            res->append("Table " + database_name + "." + table_name + " not found.");
            return res;
        }
        boost::algorithm::replace_all(query, "$d", mapped->first);
        boost::algorithm::replace_all(query, "$t", mapped->second);
    }
    else
    {
        auto mapped = mappedDatabase(context, database_name);
        if (mapped == std::nullopt)
        {
            std::shared_ptr<StringStreamBlockInputStream> res = std::make_shared<StringStreamBlockInputStream>("Error");
            res->append("Database " + database_name + " not found.");
            return res;
        }
        boost::algorithm::replace_all(query, "$d", mapped.value());
    }

    return executeQuery(query, context, true).in;
}

} // namespace DB
