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
#include <Debug/MockKVStore/MockTiKV.h>
#include <Debug/MockTiDB.h>
<<<<<<< HEAD:dbms/src/Debug/dbgFuncRegion.cpp
#include <Debug/MockTiKV.h>
#include <Debug/dbgFuncRegion.h>
=======
#include <Debug/dbgKVStore/dbgFuncRegion.h>
#include <Debug/dbgKVStore/dbgKVStore.h>
>>>>>>> ce8ae39fb9 (Debug: Add find key debug invoker (#8853)):dbms/src/Debug/dbgKVStore/dbgFuncRegion.cpp
#include <Debug/dbgTools.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/ProxyFFI.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiKVRange.h>
#include <fmt/core.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int UNKNOWN_TABLE;
} // namespace ErrorCodes

// Inject a region and optionally map it to a table.
// put_region(region_id, start, end, database_name, table_name[, partition-name])
void dbgFuncPutRegion(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    auto region_id = static_cast<RegionID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value));
    bool has_partition_id = false;
    size_t args_size = args.size();
    if (dynamic_cast<ASTLiteral *>(args[args_size - 1].get()) != nullptr)
        has_partition_id = true;
    const String & partition_id
        = has_partition_id ? std::to_string(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[args_size - 1]).value)) : "";
    size_t offset = has_partition_id ? 1 : 0;
    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[args_size - 2 - offset]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[args_size - 1 - offset]).name;
    TableID table_id = RegionBench::getTableID(context, database_name, table_name, partition_id);
    const auto & table_info = RegionBench::getTableInfo(context, database_name, table_name);
    size_t handle_column_size = table_info.is_common_handle ? table_info.getPrimaryIndexInfo().idx_cols.size() : 1;
    if (args_size < 3 + 2 * handle_column_size || args_size > 3 + 2 * handle_column_size + 1)
        throw Exception("Args not matched, should be: region-id, start-key, end-key, database-name, table-name[, partition-id]",
                        ErrorCodes::BAD_ARGUMENTS);

    if (table_info.is_common_handle)
    {
        std::vector<Field> start_keys;
        std::vector<Field> end_keys;
        std::unordered_map<String, size_t> column_name_columns_index_map;
        for (size_t i = 0; i < table_info.columns.size(); i++)
        {
            column_name_columns_index_map.emplace(table_info.columns[i].name, i);
        }
        for (size_t i = 0; i < handle_column_size; i++)
        {
            auto idx = column_name_columns_index_map[table_info.getPrimaryIndexInfo().idx_cols[i].name];
            const auto & column_info = table_info.columns[idx];
            auto start_field = RegionBench::convertField(column_info, typeid_cast<const ASTLiteral &>(*args[1 + i]).value);
            TiDB::DatumBumpy start_datum = TiDB::DatumBumpy(start_field, column_info.tp);
            start_keys.emplace_back(start_datum.field());
            auto end_field
                = RegionBench::convertField(column_info, typeid_cast<const ASTLiteral &>(*args[1 + handle_column_size + i]).value);
            TiDB::DatumBumpy end_datum = TiDB::DatumBumpy(end_field, column_info.tp);
            end_keys.emplace_back(end_datum.field());
        }

        TMTContext & tmt = context.getTMTContext();
        RegionPtr region = RegionBench::createRegion(table_info, region_id, start_keys, end_keys);
        tmt.getKVStore()->onSnapshot<RegionPtrWithBlock>(region, nullptr, 0, tmt);

        output(fmt::format("put region #{}, range{} to table #{} with kvstore.onSnapshot", region_id, RecordKVFormat::DecodedTiKVKeyRangeToDebugString(region->getRange()->rawKeys()), table_id));
    }
    else
    {
        auto start = static_cast<HandleID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[1]).value));
        auto end = static_cast<HandleID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value));

        TMTContext & tmt = context.getTMTContext();
        RegionPtr region = RegionBench::createRegion(table_id, region_id, start, end);
        tmt.getKVStore()->onSnapshot<RegionPtrWithBlock>(region, nullptr, 0, tmt);

        output(fmt::format("put region #{}, range[{}, {}) to table #{} with kvstore.onSnapshot", region_id, start, end, table_id));
    }
}

void dbgFuncTryFlush(Context & context, const ASTs &, DBGInvoker::Printer output)
{
    TMTContext & tmt = context.getTMTContext();
    tmt.getRegionTable().tryFlushRegions();

    output("region_table try flush regions");
}

void dbgFuncTryFlushRegion(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 1)
    {
        throw Exception("Args not matched, should be: region-id", ErrorCodes::BAD_ARGUMENTS);
    }

    auto region_id = static_cast<RegionID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value));

    TMTContext & tmt = context.getTMTContext();
    tmt.getRegionTable().tryFlushRegion(region_id);

    output(fmt::format("region_table try flush region {}", region_id));
}

void dbgFuncDumpAllRegion(Context & context, TableID table_id, bool ignore_none, bool dump_status, DBGInvoker::Printer & output)
{
    size_t size = 0;
    context.getTMTContext().getKVStore()->traverseRegions([&](const RegionID region_id, const RegionPtr & region) {
        std::ignore = region_id;
        FmtBuffer fmt_buf;
        auto rawkeys = region->getRange()->rawKeys();
        auto table_info = MockTiDB::instance().getTableInfoByID(table_id);
        bool is_common_handle = false;
        if (table_info != nullptr)
            is_common_handle = table_info->is_common_handle;
        size += 1;
        if (!is_common_handle)
        {
            auto range = getHandleRangeByTable(rawkeys, table_id);

            if (range.first >= range.second && ignore_none)
                return;

            fmt_buf.append(region->toString(dump_status));
            if (range.first >= range.second)
                fmt_buf.append(" ranges: [none], ");
            else
                fmt_buf.fmtAppend(" ranges: [{}, {}), ", range.first.toString(), range.second.toString());
        }
        else
        {
            if (*rawkeys.first >= *rawkeys.second && ignore_none)
                return;

            fmt_buf.fmtAppend("{} ranges: {}, ", region->toString(dump_status), RecordKVFormat::DecodedTiKVKeyRangeToDebugString(rawkeys));
        }
        fmt_buf.fmtAppend("state: {}", raft_serverpb::PeerState_Name(region->peerState()));
        if (auto s = region->dataInfo(); s.size() > 2)
            fmt_buf.fmtAppend(", {}", s);
        output(fmt_buf.toString());
    });
    output("total size: " + toString(size));
}

void dbgFuncDumpAllRegion(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.empty())
        throw Exception("Args not matched, should be: table_id", ErrorCodes::BAD_ARGUMENTS);

    auto table_id = static_cast<TableID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value));

    bool ignore_none = false;
    if (args.size() > 1)
        ignore_none = (std::string(typeid_cast<const ASTIdentifier &>(*args[1]).name) == "true");

    bool dump_status = true;
    if (args.size() > 2)
        dump_status = (std::string(typeid_cast<const ASTIdentifier &>(*args[2]).name) == "true");

    output("table #" + toString(table_id));
    dbgFuncDumpAllRegion(context, table_id, ignore_none, dump_status, output);
}

void dbgFuncDumpAllMockRegion(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;

    auto table = MockTiDB::instance().getTableByName(database_name, table_name);
    auto table_id = table->id();

    dbgFuncDumpAllRegion(context, table_id, false, false, output);
}

void dbgFuncRemoveRegion(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.empty())
        throw Exception("Args not matched, should be: region_id", ErrorCodes::BAD_ARGUMENTS);

    auto region_id = static_cast<RegionID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value));

    TMTContext & tmt = context.getTMTContext();
    KVStorePtr & kvstore = tmt.getKVStore();
    RegionTable & region_table = tmt.getRegionTable();
    kvstore->mockRemoveRegion(region_id, region_table);

    output(fmt::format("remove region #{}", region_id));
}

void KVStore::mockRemoveRegion(DB::RegionID region_id, RegionTable & region_table)
{
    auto task_lock = genTaskLock();
    auto region_lock = region_manager.genRegionTaskLock(region_id);
    // mock remove region should remove data by default
    removeRegion(region_id, /* remove_data */ true, region_table, task_lock, region_lock);
}


inline std::string ToPdKey(const char * key, const size_t len)
{
    std::string res(len * 2, 0);
    size_t i = 0;
    for (size_t k = 0; k < len; ++k)
    {
        uint8_t o = key[k];
        res[i++] = o / 16;
        res[i++] = o % 16;
    }

    for (char & re : res)
    {
        if (re < 10)
            re = re + '0';
        else
            re = re - 10 + 'A';
    }
    return res;
}

inline std::string ToPdKey(const std::string & key)
{
    return ToPdKey(key.data(), key.size());
}

inline std::string FromPdKey(const char * key, const size_t len)
{
    std::string res(len / 2, 0);
    for (size_t k = 0; k < len; k += 2)
    {
        int s[2];

        for (size_t i = 0; i < 2; ++i)
        {
            char p = toupper(key[k + i]);
            if (p >= 'A')
                s[i] = p - 'A' + 10;
            else
                s[i] = p - '0';
        }

        res[k / 2] = s[0] * 16 + s[1];
    }
    return res;
}

void dbgFuncFindRegionByRange(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    enum Mode : UInt64
    {
        DEFAULT = 0,
        ID_LIST = 1,
    };

    if (args.size() < 2)
        throw Exception("Args not matched, should be: start-key, end-key", ErrorCodes::BAD_ARGUMENTS);

    Mode mode = DEFAULT;
    const auto start_key = safeGet<std::string>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    const auto end_key = safeGet<std::string>(typeid_cast<const ASTLiteral &>(*args[1]).value);
    if (args.size() > 2)
        mode = static_cast<Mode>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value));

    auto & tmt = context.getTMTContext();
    auto & kvstore = tmt.getKVStore();

    auto start = FromPdKey(start_key.data(), start_key.size());
    auto end = FromPdKey(end_key.data(), end_key.size());
    auto range = RegionRangeKeys::makeComparableKeys(std::move(start), std::move(end));
    RegionMap regions = kvstore->getRegionsByRangeOverlap(range);

    output(toString(regions.size()));
    if (mode == ID_LIST)
    {
        FmtBuffer fmt_buf;
        if (!regions.empty())
            fmt_buf.append("regions: ");
        for (const auto & region : regions)
            fmt_buf.fmtAppend("{} ", region.second->id());
        output(fmt_buf.toString());
    }
    else
    {
        if (!regions.empty())
        {
            for (const auto & region : regions)
            {
                auto str = fmt::format(
                    "{}, local state: {}, proxy internal state: {}",
                    region.second->toString(),
                    region.second->getMetaRegion().ShortDebugString(),
                    kvstore->getProxyHelper()->getRegionLocalState(region.first).ShortDebugString());
                output(str);
            }
        }
    }
}

} // namespace DB
