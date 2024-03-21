// Copyright 2024 PingCAP, Inc.
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

#include <Core/Field.h>
#include <DataStreams/StringStreamBlockInputStream.h>
#include <Debug/DAGProperties.h>
#include <Debug/MockTiDB.h>
#include <Debug/dbgKVStore/dbgFuncInvestigator.h>
#include <Debug/dbgKVStore/dbgRegion.h>
#include <Debug/dbgTools.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/IManageableStorage.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/MultiRaft/RegionData.h>
#include <Storages/KVStore/MultiRaft/RegionRangeKeys.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TMTContext.h>
#include <TiDB/Schema/TiDBSchemaManager.h>

#include <unordered_map>
#include <unordered_set>

namespace DB
{

struct MatchResult
{
    String database_name;
    String table_name;
    std::vector<std::pair<UInt64, UInt64>> in_default;
    std::vector<std::pair<UInt64, UInt64>> in_write;
    std::vector<UInt64> in_lock;
    std::unordered_map<UInt64, RegionPtr> regions;

    std::string toString() const
    {
        FmtBuffer fmt_buf;
        FmtBuffer fmt_buf1;
        FmtBuffer fmt_buf2;
        FmtBuffer fmt_buf3;
        for (const auto & a : in_default)
        {
            fmt_buf1.fmtAppend("{}-{}:", a.first, a.second);
        }
        for (const auto & a : in_write)
        {
            fmt_buf2.fmtAppend("{}-{}:", a.first, a.second);
        }
        for (const auto & a : in_lock)
        {
            fmt_buf3.fmtAppend("{}:", a);
        }
        fmt_buf.fmtAppend(
            "default_cf {}, write_cf {}, lock_cf {}, ",
            fmt_buf1.toString(),
            fmt_buf2.toString(),
            fmt_buf3.toString());
        for (const auto & [region_id, region] : regions)
        {
            fmt_buf.fmtAppend(
                "region {} {}, tikv_range: {}",
                region_id,
                region->getDebugString(),
                region->getRange()->toDebugString());
        }
        return fmt_buf.toString();
    }
};

// tablePrefix_rowPrefix_tableID_rowID
static void findKeyInKVStore(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    MatchResult result;
    const String & database_name_raw = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;

    auto maybe_database_name = mappedDatabaseWithOptional(context, database_name_raw);
    if (maybe_database_name == std::nullopt)
    {
        output(fmt::format("Database {} not found.", database_name_raw));
        return;
    }

    result.database_name = maybe_database_name.value();
    auto database_name = result.database_name;
    result.table_name = table_name;

    auto & tmt = context.getTMTContext();
    auto & kvstore = *tmt.getKVStore();

    auto schema_syncer = tmt.getSchemaSyncerManager();
    auto storage = tmt.getStorages().getByName(database_name, table_name, false);
    std::string debug;

    if (storage == nullptr)
    {
        output(fmt::format("can't find table {} {}", database_name, table_name));
        return;
    }

    auto table_info = storage->getTableInfo();
    schema_syncer->syncTableSchema(context, table_info.keyspace_id, table_info.id);
    if (table_info.partition.num > 0)
    {
        for (const auto & def : table_info.partition.definitions)
        {
            schema_syncer->syncTableSchema(context, table_info.keyspace_id, def.id);
        }
    }

    auto table_id = table_info.id;

    TiKVKey start_key, end_key;
    HandleID start_handle, end_handle;
    size_t handle_column_size = table_info.is_common_handle ? table_info.getPrimaryIndexInfo().idx_cols.size() : 1;

    constexpr static size_t OFFSET = 2;
    size_t arg_size = args.size() - OFFSET;
    if ((arg_size & 1) != 0)
    {
        throw Exception(
            "Args not matched, should be: database-name, table-name, start1 [, start2, ...], end1, [, end2, ...]",
            ErrorCodes::BAD_ARGUMENTS);
    }
    auto key_size = arg_size / 2;

    if (table_info.is_common_handle)
    {
        std::vector<Field> start_field;
        std::vector<Field> end_field;

        for (size_t i = 0; i < handle_column_size; i++)
        {
            auto & column_info = table_info.columns[table_info.getPrimaryIndexInfo().idx_cols[i].offset];
            TiDB::DatumBumpy start_datum = TiDB::DatumBumpy(
                RegionBench::convertField(column_info, typeid_cast<const ASTLiteral &>(*args[OFFSET + i]).value),
                column_info.tp);
            start_field.emplace_back(start_datum.field());
            TiDB::DatumBumpy end_datum = TiDB::DatumBumpy(
                RegionBench::convertField(
                    column_info,
                    typeid_cast<const ASTLiteral &>(*args[OFFSET + key_size + i]).value),
                column_info.tp);
            end_field.emplace_back(end_datum.field());
        }

        start_key = RecordKVFormat::genKey(table_info, start_field);
        end_key = RecordKVFormat::genKey(table_info, end_field);
    }
    else
    {
        start_handle = static_cast<HandleID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[OFFSET]).value));
        start_key = RecordKVFormat::genKey(table_id, start_handle);
        auto start_key2 = RecordKVFormat::genKey(table_id, start_handle, 0);
        auto kkk2 = RecordKVFormat::decodeTiKVKey(start_key2);
        auto rawpk2 = RecordKVFormat::getRawTiDBPK(kkk2);
        end_handle = static_cast<HandleID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[OFFSET + 1]).value));
        end_key = RecordKVFormat::genKey(table_id, end_handle);
    }

    auto range = RegionRangeKeys(std::move(start_key), std::move(end_key));
    auto regions = kvstore.getRegionsByRangeOverlap(range.comparableKeys());

    for (const auto & [region_id, region] : regions)
    {
        auto r = RegionBench::DebugRegion(region);
        auto & data = r.debugData();

        for (const auto & [k, v] : data.defaultCF().getData())
        {
            if (k.first == start_handle)
            {
                result.in_default.emplace_back(std::make_pair(region_id, k.second));
            }
        }
        for (const auto & [k, v] : data.writeCF().getData())
        {
            if (k.first == start_handle)
            {
                result.in_write.emplace_back(std::make_pair(region_id, k.second));
            }
        }

        if (data.lockCF().getData().contains(RegionLockCFDataTrait::genKey(start_key)))
            result.in_lock.emplace_back(region_id);
    }
    result.regions = regions;
    output(fmt::format("find key result {}", result.toString()));
}

void dbgFuncFindKey(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() < 4)
        throw Exception(
            "Args not matched, should be: database-name, table-name, start1 [, start2, ...], end1, [, end2, ...]",
            ErrorCodes::BAD_ARGUMENTS);

    findKeyInKVStore(context, args, output);
}

BlockInputStreamPtr dbgFuncFindKeyDt(Context & context, const ASTs & args)
{
    if (args.size() < 4)
        throw Exception(
            "Args not matched, should be: database-name, table-name, start1 [, start2, ...], end1, [, end2, ...]",
            ErrorCodes::BAD_ARGUMENTS);

    const String & database_name_raw = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name_raw = typeid_cast<const ASTIdentifier &>(*args[1]).name;

    auto maybe_database_name = mappedDatabaseWithOptional(context, database_name_raw);
    if (maybe_database_name == std::nullopt)
    {
        LOG_INFO(DB::Logger::get(), "Can't find database {}", database_name_raw);
        return std::make_shared<StringStreamBlockInputStream>("Error");
    }
    auto database_name = maybe_database_name.value();

    auto mapped_table_name = mappedTable(context, database_name_raw, table_name_raw);
    auto table_name = mapped_table_name.second;
    auto & tmt = context.getTMTContext();

    auto schema_syncer = tmt.getSchemaSyncerManager();
    auto storage = tmt.getStorages().getByName(database_name, table_name_raw, false);

    if (storage == nullptr)
    {
        LOG_INFO(DB::Logger::get(), "Can't find database and table {}.{}", database_name, table_name);
        return std::make_shared<StringStreamBlockInputStream>("Error");
    }
    auto table_info = storage->getTableInfo();
    schema_syncer->syncTableSchema(context, table_info.keyspace_id, table_info.id);
    if (table_info.partition.num > 0)
    {
        for (const auto & def : table_info.partition.definitions)
        {
            schema_syncer->syncTableSchema(context, table_info.keyspace_id, def.id);
        }
    }

    String key = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    String value = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[3]).value);
    auto query
        = fmt::format("selraw *,_INTERNAL_VERSION from {}.{} where {} = {}", database_name, table_name, key, value);

    ParserSelectQuery parser;
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "dbgFuncFindKeyDt", 0);

    InterpreterSelectQuery interpreter(ast, context);
    auto res = interpreter.execute();
    return res.in;
}


} // namespace DB