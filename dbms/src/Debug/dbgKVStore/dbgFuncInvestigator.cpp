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
    std::unordered_set<UInt64> in_default;
    std::unordered_set<UInt64> in_write;
    std::unordered_set<UInt64> in_lock;
    std::unordered_map<UInt64, RegionPtr> regions;

    std::string toString() const {
        FmtBuffer fmt_buf;
        fmt_buf.fmtAppend("default_cf {}, ", fmt::join(in_default.begin(), in_default.end(), ":"));
        fmt_buf.fmtAppend("write_cf {}, ", fmt::join(in_write.begin(), in_write.end(), ":"));
        fmt_buf.fmtAppend("lock_cf {}, ", fmt::join(in_lock.begin(), in_lock.end(), ":"));
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
        auto start_handle = static_cast<HandleID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[OFFSET]).value));
        LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore b start_handle {}", start_handle);
        start_key = RecordKVFormat::genKey(table_id, start_handle);
        auto end_handle = static_cast<HandleID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[OFFSET + 1]).value));
        LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore b end_handle {}", end_handle);
        end_key = RecordKVFormat::genKey(table_id, end_handle);
        LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore b2 {} {}", start_key.toDebugString(), end_key.toDebugString());
    }

    LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore 6");
    auto range = RegionRangeKeys(std::move(start_key), std::move(end_key));
    auto regions = kvstore.getRegionsByRangeOverlap(range.comparableKeys());

    for (const auto & [region_id, region]: regions) {
        LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore t {}", region_id);
        auto r = RegionBench::DebugRegion(region);
        LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore t1 {}", region_id);
        auto & data = r.debugData();
        LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore t12 {}", region_id);
        auto c = RegionDefaultCFDataTrait::genKey(start_key);
        LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore 22211t {}", region_id);
        if(data.defaultCF().getData().contains(c)) {
            result.in_default.insert(region_id);
        }
        LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore t123 {}", region_id);
        if(data.writeCF().getData().contains(RegionWriteCFDataTrait::genKey(start_key)))
            result.in_write.insert(region_id);
        LOG_INFO(DB::Logger::get(), "!!!!! findKeyInKVStore t1234 {}", region_id);
        if(data.lockCF().getData().contains(RegionLockCFDataTrait::genKey(start_key)))
            result.in_lock.insert(region_id);
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