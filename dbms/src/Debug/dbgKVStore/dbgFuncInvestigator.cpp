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
    String mapped_database_name;
    String table_name;
    std::vector<std::pair<UInt64, UInt64>> in_default;
    std::vector<std::pair<UInt64, UInt64>> in_write;
    std::vector<UInt64> in_lock;
    std::unordered_map<UInt64, RegionPtr> regions;

    String toString() const
    {
        FmtBuffer fmt_buf;
        fmt_buf.fmtAppend("default_cf ");
        fmt_buf.joinStr(
            in_default.begin(),
            in_default.end(),
            [](const auto & a, const auto &) { return fmt::format("{}-{}", a.first, a.second); },
            ":");
        fmt_buf.fmtAppend("; write_cf ");
        fmt_buf.joinStr(
            in_write.begin(),
            in_write.end(),
            [](const auto & a, const auto &) { return fmt::format("{}-{}", a.first, a.second); },
            ":");
        fmt_buf.fmtAppend("; lock_cf ");
        fmt_buf.joinStr(in_lock.begin(), in_lock.end(), ":");
        for (const auto & [region_id, region] : regions)
        {
            fmt_buf.fmtAppend(
                "; region {} {}, tikv_range: {}; ",
                region_id,
                region->getDebugString(),
                region->getRange()->toDebugString());
        }
        return fmt_buf.toString();
    }
};

/// 1. If the arg is [start1, end1], find all key-value pairs in this range;
/// 2. If the arg is [start1], make sure it is not a common handle, and we will only check the key-value pair by start1;
/// 3. If the arg is [start1, end1, start2, end2, ...], it must be a common handle, return all key-value pairs within the range.
void dbgFuncFindKey(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() < 3)
        throw Exception(
            "Args not matched, should be: database-name, table-name, start1 [, start2, ..., end1, end2, ...]",
            ErrorCodes::BAD_ARGUMENTS);

    auto & tmt = context.getTMTContext();
    auto & kvstore = *tmt.getKVStore();
    MatchResult result;
    const String & database_name_raw = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;

    auto maybe_database_name = mappedDatabaseWithOptional(context, database_name_raw);
    if (maybe_database_name == std::nullopt)
    {
        output(fmt::format("Database {} not found.", database_name_raw));
        return;
    }
    result.mapped_database_name = maybe_database_name.value();
    auto & mapped_database_name = result.mapped_database_name;
    result.table_name = table_name;

    auto schema_syncer = tmt.getSchemaSyncerManager();
    auto storage = tmt.getStorages().getByName(mapped_database_name, table_name, false);
    if (storage == nullptr)
    {
        output(fmt::format("can't find table {} {}", mapped_database_name, table_name));
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

    // tablePrefix_rowPrefix_tableID_rowID
    TiKVKey start_key, end_key;
    HandleID start_handle, end_handle;

    constexpr static size_t OFFSET = 2;
    if (table_info.is_common_handle)
    {
        size_t arg_size = args.size() - OFFSET;
        // The `start` and `end` argments should be provided in pair.
        if ((arg_size & 1) != 0)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Args not matched for common handle table, arg_size={}, should be: database-name, table-name, "
                "start_col, [, start_col2, ..., end_col1, end_col2, ...]",
                arg_size);
        }
        size_t handle_column_size = table_info.is_common_handle ? table_info.getPrimaryIndexInfo().idx_cols.size() : 1;
        auto key_size = arg_size / 2;
        std::vector<Field> start_field;
        start_field.reserve(key_size);
        std::vector<Field> end_field;
        end_field.reserve(key_size);

        for (size_t i = 0; i < handle_column_size; i++)
        {
            auto & column_info = table_info.columns[table_info.getPrimaryIndexInfo().idx_cols[i].offset];
            auto start_datum = TiDB::DatumBumpy(
                RegionBench::convertField(column_info, typeid_cast<const ASTLiteral &>(*args[OFFSET + i]).value),
                column_info.tp);
            start_field.emplace_back(start_datum.field());
            auto end_datum = TiDB::DatumBumpy(
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
        if (args.size() == 3)
        {
            end_handle = start_handle + 1;
        }
        else
        {
            end_handle
                = static_cast<HandleID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[OFFSET + 1]).value));
        }
        end_key = RecordKVFormat::genKey(table_id, end_handle);
    }

    auto range = RegionRangeKeys(TiKVKey::copyFrom(start_key), std::move(end_key));
    auto regions = kvstore.getRegionsByRangeOverlap(range.comparableKeys());

    for (const auto & [region_id, region] : regions)
    {
        auto r = RegionBench::DebugRegion(region);
        const auto & data = r.debugData();

        for (const auto & [k, v] : data.defaultCF().getData())
        {
            if (k.first == start_handle)
            {
                result.in_default.emplace_back(region_id, k.second);
            }
        }
        for (const auto & [k, v] : data.writeCF().getData())
        {
            if (k.first == start_handle)
            {
                result.in_write.emplace_back(region_id, k.second);
            }
        }

        auto lock_key = std::make_shared<const TiKVKey>(TiKVKey::copyFrom(start_key));
        if (data.lockCF().getData().contains(
                RegionLockCFDataTrait::Key{lock_key, std::string_view(lock_key->data(), lock_key->dataSize())}))
        {
            result.in_lock.emplace_back(region_id);
        }
    }
    result.regions = regions;
    output(fmt::format("find key result {}", result.toString()));
}

BlockInputStreamPtr dbgFuncFindKeyDt(Context & context, const ASTs & args)
{
    if (args.size() < 4)
        throw Exception(
            "Args not matched, should be: database-name, table-name, key, value, [key2, value2, ...]",
            ErrorCodes::BAD_ARGUMENTS);

    auto & tmt = context.getTMTContext();
    const String & database_name_raw = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name_raw = typeid_cast<const ASTIdentifier &>(*args[1]).name;

    auto maybe_database_name = mappedDatabaseWithOptional(context, database_name_raw);
    if (maybe_database_name == std::nullopt)
    {
        LOG_INFO(DB::Logger::get(), "Can't find database {}", database_name_raw);
        return std::make_shared<StringStreamBlockInputStream>("Error");
    }
    auto mapped_database_name = maybe_database_name.value();
    auto mapped_qualified_table_name = mappedTable(context, database_name_raw, table_name_raw);
    auto mapped_table_name = mapped_qualified_table_name.second;

    auto schema_syncer = tmt.getSchemaSyncerManager();
    auto storage = tmt.getStorages().getByName(mapped_database_name, table_name_raw, false);
    if (storage == nullptr)
    {
        LOG_INFO(DB::Logger::get(), "Can't find database and table {}.{}", mapped_database_name, mapped_table_name);
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

    auto key = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    auto value = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[3]).value);

    constexpr static size_t OFFSET = 4;
    FmtBuffer fmt_buf;
    auto key_size = args.size() - OFFSET;
    if (key_size & 1)
    {
        LOG_INFO(DB::Logger::get(), "Key-values should be in pair {}", database_name_raw, table_name_raw);
        return std::make_shared<StringStreamBlockInputStream>("Error");
    }
    for (size_t i = 0; i != key_size / 2; i++)
    {
        auto k = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[OFFSET + 2 * i]).value);
        auto v = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[OFFSET + 2 * i + 1]).value);
        fmt_buf.fmtAppend(" and {} = {}", k, v);
    }
    String query;
    if (table_info.is_common_handle && !table_info.pk_is_handle)
    {
        query = fmt::format(
            "selraw *,_INTERNAL_VERSION,_INTERNAL_DELMARK,_tidb_rowid from {}.{} where {} = {}{}",
            mapped_database_name,
            mapped_table_name,
            key,
            value,
            fmt_buf.toString());
    }
    else
    {
        query = fmt::format(
            "selraw *,_INTERNAL_VERSION,_INTERNAL_DELMARK from {}.{} where {} = {}{}",
            mapped_database_name,
            mapped_table_name,
            key,
            value,
            fmt_buf.toString());
    }
    LOG_INFO(DB::Logger::get(), "The query is {}", query);
    ParserSelectQuery parser;
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "dbgFuncFindKeyDt", 0);

    InterpreterSelectQuery interpreter(ast, context);
    auto res = interpreter.execute();
    return res.in;
}


} // namespace DB