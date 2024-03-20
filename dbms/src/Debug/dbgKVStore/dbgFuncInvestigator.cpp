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

#include <Debug/dbgKVStore/dbgFuncInvestigator.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/MultiRaft/RegionData.h>
#include <Debug/MockTiDB.h>
#include <Debug/dbgTools.h>
#include <Debug/dbgKVStore/dbgRegion.h>
#include <Parsers/ASTLiteral.h>
#include <Core/Field.h>
#include <Storages/KVStore/MultiRaft/RegionRangeKeys.h>
#include <Parsers/ASTIdentifier.h>
#include <Interpreters/Context.h>
#include <Storages/KVStore/TMTContext.h>
#include <TiDB/Schema/TiDBSchemaManager.h>
#include <Storages/IManageableStorage.h>

#include <unordered_set>
#include <unordered_map>

namespace DB
{

struct MatchResult {
    String database_name;
    String table_name;
    std::vector<std::pair<UInt64, UInt64>> in_default;
    std::vector<std::pair<UInt64, UInt64>> in_write;
    std::vector<UInt64> in_lock;
    std::unordered_map<UInt64, RegionPtr> regions;

    std::string toString() const {
        FmtBuffer fmt_buf;
        FmtBuffer fmt_buf1;
        FmtBuffer fmt_buf2;
        FmtBuffer fmt_buf3;
        for (const auto & a: in_default) {
            fmt_buf1.fmtAppend("{}-{}:", a.first, a.second);
        }
        for (const auto & a: in_write) {
            fmt_buf2.fmtAppend("{}-{}:", a.first, a.second);
        }
        for (const auto & a: in_lock) {
            fmt_buf3.fmtAppend("{}:", a);
        }
        fmt_buf.fmtAppend("default_cf {}, write_cf {}, lock_cf {}, ", fmt_buf1.toString(), fmt_buf2.toString(), fmt_buf3.toString());
        for (const auto & [region_id, region]: regions) {
            fmt_buf.fmtAppend("region {} {}, ", region_id, region->getDebugString());
        }
        return fmt_buf.toString();
    }
};

// tablePrefix_rowPrefix_tableID_rowID
static void findKeyInKVStore(Context & context, const ASTs & args, DBGInvoker::Printer output) {
    MatchResult result;
    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    result.database_name = database_name;
    result.table_name = table_name;

    auto & tmt = context.getTMTContext();
    auto & kvstore = *tmt.getKVStore();

    auto schema_syncer = tmt.getSchemaSyncerManager();
    auto storage = tmt.getStorages().getByName(database_name, table_name, false);
    std::string debug;
    LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore 1");
    for(const auto & it: tmt.getStorages().getAllStorage()) {
        debug += fmt::format("{}-{}-{}-{}", it.first.first, it.first.second, it.second->getDatabaseName(), it.second->getTableInfo().name);
        debug += ":";
    }
    if (storage == nullptr)
    {
        output(fmt::format("can't find table {} {} {}", database_name, table_name, debug));
        return;
    }

    LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore 2");
    auto table_info = storage->getTableInfo();
    schema_syncer->syncTableSchema(context, table_info.keyspace_id, table_info.id);
    if (table_info.partition.num > 0)
    {
        for (const auto & def : table_info.partition.definitions)
        {
            schema_syncer->syncTableSchema(context, table_info.keyspace_id, def.id);
        }
    }

    LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore 3");
    auto table_id = table_info.id;

    TiKVKey start_key, end_key;
    HandleID start_handle, end_handle;
    size_t handle_column_size = table_info.is_common_handle ? table_info.getPrimaryIndexInfo().idx_cols.size() : 1;


    LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore 4");
    constexpr static size_t OFFSET = 2;
    size_t arg_size = args.size() - OFFSET;
    if((arg_size & 1) != 0) {
        throw Exception("Args not matched, should be: database-name, table-name, start1 [, start2, ...], end1, [, end2, ...]", ErrorCodes::BAD_ARGUMENTS);
    }
    auto key_size = arg_size / 2;

    LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore 5");
    if (table_info.is_common_handle)
    {
        std::vector<Field> start_field;
        std::vector<Field> end_field;

        for (size_t i = 0; i < handle_column_size; i++)
        {
            LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore a {}", i);
            auto & column_info = table_info.columns[table_info.getPrimaryIndexInfo().idx_cols[i].offset];
            TiDB::DatumBumpy start_datum = TiDB::DatumBumpy(RegionBench::convertField(
                column_info,
                typeid_cast<const ASTLiteral &>(*args[OFFSET + i]).value), column_info.tp);
            start_field.emplace_back(start_datum.field());
            TiDB::DatumBumpy end_datum = TiDB::DatumBumpy(RegionBench::convertField(
                column_info,
                typeid_cast<const ASTLiteral &>(*args[OFFSET + key_size + i]).value), column_info.tp);
            end_field.emplace_back(end_datum.field());
        }

        start_key = RecordKVFormat::genKey(table_info, start_field);
        end_key = RecordKVFormat::genKey(table_info, end_field);
    }
    else
    {
        start_handle = static_cast<HandleID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[OFFSET]).value));
        LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore b start_handle {}", start_handle);
        start_key = RecordKVFormat::genKey(table_id, start_handle);
        auto start_key2 = RecordKVFormat::genKey(table_id, start_handle, 0);
        LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore b start_key2 {}", start_key2.toDebugString());
        auto kkk2 = RecordKVFormat::decodeTiKVKey(start_key2);
        LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore b kkk2 {}", kkk2.toDebugString());
        auto rawpk2 = RecordKVFormat::getRawTiDBPK(kkk2);
        LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore b rawpk2 {}", rawpk2.toDebugString());
        LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore b handle {}", (HandleID)rawpk2);
        end_handle = static_cast<HandleID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[OFFSET + 1]).value));
        LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore b end_handle {}", end_handle);
        end_key = RecordKVFormat::genKey(table_id, end_handle);
        LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore b2 {} {}", start_key.toDebugString(), end_key.toDebugString());
    }

    LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore 6");
    auto range = RegionRangeKeys(std::move(start_key), std::move(end_key));
    auto regions = kvstore.getRegionsByRangeOverlap(range.comparableKeys());

    for (const auto & [region_id, region]: regions) {
        auto r = RegionBench::DebugRegion(region);
        auto & data = r.debugData();

                LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore S1 {}", data.defaultCF().getSize());
        for(const auto & [k, v]: data.defaultCF().getData()) {
                LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore YYYYY {}", k.first.toDebugString());
            if (k.first.toDebugString() == std::to_string(start_handle)) {
                LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore find default {}", region_id);
                result.in_default.emplace_back(std::make_pair(region_id, k.second));
            }
        }
                LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore S2 {}", data.writeCF().getSize());
        for(const auto & [k, v]: data.writeCF().getData()) {
            LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore XXXXX {} {} {}", k.first.toDebugString(), start_handle, (HandleID)k.first);
            if (k.first.toDebugString() == std::to_string(start_handle)) {
                LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore find write {}", region_id);
                result.in_write.emplace_back(std::make_pair(region_id, k.second));
            }
        }
        
                LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore S3 {}", data.lockCF().getSize());
        LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore t1234 {}", region_id);
        for(const auto & [k, v]: data.lockCF().getData()) {
            LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore ZZZZ {}", k.key->toDebugString());
        }
        if(data.lockCF().getData().contains(RegionLockCFDataTrait::genKey(start_key)))
            result.in_lock.emplace_back(region_id);
    }
    result.regions = regions;
    output(fmt::format("find key result {}", result.toString()));
}

void dbgFuncFindKey(Context & context, const ASTs & args, DBGInvoker::Printer output) {
    if (args.size() < 4)
        throw Exception("Args not matched, should be: database-name, table-name, start1 [, start2, ...], end1, [, end2, ...]", ErrorCodes::BAD_ARGUMENTS);

    findKeyInKVStore(context, args, output);
}


} // namespace DB