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

#include <Common/typeid_cast.h>
#include <Debug/MockKVStore/MockTiKV.h>
#include <Debug/MockKVStore/MockUtils.h>
#include <Debug/MockTiDB.h>
#include <Debug/dbgKVStore/dbgKVStore.h>
#include <Debug/dbgTools.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/IManageableStorage.h>
#include <Storages/KVStore/Decode/TiKVRange.h>
#include <Storages/KVStore/FFI/ColumnFamily.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/MultiRaft/ApplySnapshot.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TMTContext.h>
#include <TiDB/Decode/DatumCodec.h>
#include <TiDB/Decode/RowCodec.h>
#include <TiDB/Schema/TiDB.h>
#include <TiDB/Schema/TiDBSchemaManager.h>


namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_TABLE;
extern const int UNKNOWN_DATABASE;
} // namespace DB::ErrorCodes


namespace DB
{
String mappedDatabase(Context & context, const String & database_name)
{
    TMTContext & tmt = context.getTMTContext();
    auto syncer = tmt.getSchemaSyncerManager();
    auto db_info = syncer->getDBInfoByName(NullspaceID, database_name);
    if (db_info == nullptr)
        throw Exception("in mappedDatabase, Database " + database_name + " not found", ErrorCodes::UNKNOWN_DATABASE);
    return SchemaNameMapper().mapDatabaseName(*db_info);
}

std::optional<String> mappedDatabaseWithOptional(Context & context, const String & database_name)
{
    TMTContext & tmt = context.getTMTContext();
    auto syncer = tmt.getSchemaSyncerManager();
    auto db_info = syncer->getDBInfoByName(NullspaceID, database_name);
    if (db_info == nullptr)
        return std::nullopt;
    return SchemaNameMapper().mapDatabaseName(*db_info);
}

std::optional<QualifiedName> mappedTableWithOptional(
    Context & context,
    const String & database_name,
    const String & table_name)
{
    auto mapped_db = mappedDatabaseWithOptional(context, database_name);

    if (!mapped_db.has_value())
        return std::nullopt;
    TMTContext & tmt = context.getTMTContext();
    auto storage = tmt.getStorages().getByName(mapped_db.value(), table_name, false);
    if (storage == nullptr)
    {
        return std::nullopt;
    }

    return std::make_pair(storage->getDatabaseName(), storage->getTableName());
}

QualifiedName mappedTable(
    Context & context,
    const String & database_name,
    const String & table_name,
    bool include_tombstone)
{
    auto mapped_db = mappedDatabase(context, database_name);

    TMTContext & tmt = context.getTMTContext();
    auto storage = tmt.getStorages().getByName(mapped_db, table_name, include_tombstone);
    if (storage == nullptr)
    {
        throw Exception("Table " + table_name + " not found", ErrorCodes::UNKNOWN_TABLE);
    }

    return std::make_pair(storage->getDatabaseName(), storage->getTableName());
}
} // namespace DB
