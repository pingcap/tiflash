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

#include <Common/FmtUtils.h>
#include <Common/typeid_cast.h>
#include <Databases/DatabaseTiFlash.h>
#include <Debug/dbgFuncSchema.h>
#include <Debug/dbgTools.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateQuery.h>
#include <Storages/IManageableStorage.h>
#include <Storages/KVStore/TMTContext.h>
#include <TiDB/Schema/SchemaNameMapper.h>
#include <TiDB/Schema/SchemaSyncService.h>
#include <TiDB/Schema/SchemaSyncer.h>
#include <TiDB/Schema/TiDB.h>
#include <TiDB/Schema/TiDBSchemaManager.h>
#include <common/logger_useful.h>
#include <fmt/core.h>

#include <ext/singleton.h>

namespace DB
{
namespace ErrorCodes
{
extern const int FAIL_POINT_ERROR;
extern const int UNKNOWN_TABLE;
} // namespace ErrorCodes

void dbgFuncEnableSchemaSyncService(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 1)
        throw Exception("Args not matched, should be: enable (true/false)", ErrorCodes::BAD_ARGUMENTS);

    bool enable = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[0]).value) == "true";

    if (enable)
    {
        if (!context.getSchemaSyncService())
            context.initializeSchemaSyncService();
    }
    else
    {
        if (context.getSchemaSyncService())
            context.getSchemaSyncService().reset();
    }

    output(fmt::format("schema sync service {}", (enable ? "enabled" : "disabled")));
}

void dbgFuncRefreshSchemas(Context & context, const ASTs &, DBGInvoker::Printer output)
{
    TMTContext & tmt = context.getTMTContext();
    auto schema_syncer = tmt.getSchemaSyncerManager();
    try
    {
        schema_syncer->syncSchemas(context, NullspaceID);
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::FAIL_POINT_ERROR)
        {
            output(e.message());
            return;
        }
        else
        {
            throw;
        }
    }

    output("schemas refreshed");
}

void dbgFuncRefreshTableSchema(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 2)
        throw Exception("Args not matched, should be: database-name, table-name", ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;

    auto mapped_db = mappedDatabase(context, database_name);

    TMTContext & tmt = context.getTMTContext();
    auto storage = tmt.getStorages().getByName(mapped_db, table_name, false);
    if (storage == nullptr)
    {
        return;
    }

    auto schema_syncer = tmt.getSchemaSyncerManager();
    try
    {
        schema_syncer->syncTableSchema(context, storage->getTableInfo().keyspace_id, storage->getTableInfo().id);
        if (storage->getTableInfo().partition.num > 0)
        {
            for (const auto & def : storage->getTableInfo().partition.definitions)
            {
                schema_syncer->syncTableSchema(context, storage->getTableInfo().keyspace_id, def.id);
            }
        }
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::FAIL_POINT_ERROR)
        {
            output(e.message());
            return;
        }
        else
        {
            throw;
        }
    }

    output("table schema refreshed");
}

void dbgFuncRefreshMappedTableSchema(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 2)
        throw Exception("Args not matched, should be: database-name, table-name", ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;

    MockTiDB::TablePtr table = MockTiDB::instance().getTableByName(database_name, table_name);

    auto table_id = table->table_info.id;
    auto keyspace_id = table->table_info.keyspace_id;

    TMTContext & tmt = context.getTMTContext();
    auto schema_syncer = tmt.getSchemaSyncerManager();
    try
    {
        schema_syncer->syncTableSchema(context, keyspace_id, table_id);
        if (table->table_info.partition.num > 0)
        {
            for (const auto & def : table->table_info.partition.definitions)
            {
                schema_syncer->syncTableSchema(context, keyspace_id, def.id);
            }
        }
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::FAIL_POINT_ERROR)
        {
            output(e.message());
            return;
        }
        else
        {
            throw;
        }
    }

    output("table schema refreshed");
}

// Trigger gc on all databases / tables.
// Usage:
//   ./storage-client.sh "DBGInvoke gc_schemas([gc_safe_point, ignore_remain_regions])"
void dbgFuncGcSchemas(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    auto & service = context.getSchemaSyncService();
    Timestamp gc_safe_point = 0;
    bool ignore_remain_regions = false;
    if (args.empty())
        gc_safe_point = PDClientHelper::getGCSafePointWithRetry(context.getTMTContext().getPDClient(), NullspaceID);
    if (!args.empty())
        gc_safe_point = safeGet<Timestamp>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    if (args.size() >= 2)
        ignore_remain_regions = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[1]).value) == "true";
    // Note that only call it in tests, we need to ignore remain regions
    service->gcImpl(gc_safe_point, NullspaceID, ignore_remain_regions);

    output("schemas gc done");
}

void dbgFuncResetSchemas(Context & context, const ASTs &, DBGInvoker::Printer output)
{
    TMTContext & tmt = context.getTMTContext();
    auto schema_syncer = tmt.getSchemaSyncerManager();
    schema_syncer->reset(NullspaceID);

    output("reset schemas");
}

void dbgFuncIsTombstone(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.empty() || args.size() > 2)
        throw Exception("Args not matched, should be: database-name[, table-name]", ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    FmtBuffer fmt_buf;
    if (args.size() == 1)
    {
        auto mapped_database_name = mappedDatabase(context, database_name);
        auto db = context.getDatabase(mapped_database_name);
        auto tiflash_db = std::dynamic_pointer_cast<DatabaseTiFlash>(db);
        if (!tiflash_db)
            throw Exception(database_name + " is not DatabaseTiFlash", ErrorCodes::BAD_ARGUMENTS);

        fmt_buf.append((tiflash_db->isTombstone() ? "true" : "false"));
    }
    else if (args.size() == 2)
    {
        const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
        auto mapped_table_name = mappedTable(context, database_name, table_name, true).second;
        auto mapped_database_name = mappedDatabase(context, database_name);
        auto storage = context.getTable(mapped_database_name, mapped_table_name);
        auto managed_storage = std::dynamic_pointer_cast<IManageableStorage>(storage);
        if (!managed_storage)
            throw Exception(database_name + "." + table_name + " is not ManageableStorage", ErrorCodes::BAD_ARGUMENTS);

        fmt_buf.append((managed_storage->isTombstone() ? "true" : "false"));
    }
    output(fmt_buf.toString());
}

void dbgFuncSkipSchemaVersion(Context &, const ASTs &, DBGInvoker::Printer output)
{
    auto empty_schema_version = MockTiDB::instance().skipSchemaVersion();
    LOG_WARNING(Logger::get(), "Generate an empty schema diff with schema_version={}", empty_schema_version);
    output(fmt::format("Generate an empty schema diff with schema_version={}", empty_schema_version));
}

void dbgFuncRegrenationSchemaMap(Context &, const ASTs &, DBGInvoker::Printer output)
{
    auto regen_schema_version = MockTiDB::instance().regenerateSchemaMap();
    LOG_WARNING(
        Logger::get(),
        "Generate a schema diff with regenerate_schema_map == true, schema_version={}",
        regen_schema_version);
    output(fmt::format(
        "Generate a empty schema diff with regenerate_schema_map == true, schema_version={}",
        regen_schema_version));
}

} // namespace DB
