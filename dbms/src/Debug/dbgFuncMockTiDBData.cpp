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
#include <Debug/MockTiDB.h>
#include <Debug/dbgFuncMockTiDBData.h>
#include <Debug/dbgTools.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/KVStore/StorageEngineType.h>
#include <Storages/KVStore/TMTContext.h>
#include <fmt/core.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

void dbgInsertRow(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() < 4)
        throw Exception(
            "Args not matched, should be: database-name, table-name, region-id, handle-id, values",
            ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    auto region_id = static_cast<RegionID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value));
    const auto & handle_field = typeid_cast<const ASTLiteral &>(*args[3]).value;
    HandleID handle_id = 0;
    if (handle_field.getType() == Field::Types::Int64 || handle_field.getType() == Field::Types::UInt64)
        handle_id = static_cast<HandleID>(safeGet<UInt64>(handle_field));

    MockTiDB::TablePtr table = MockTiDB::instance().getTableByName(database_name, table_name);
    RegionBench::insert(table->table_info, region_id, handle_id, args.begin() + 4, args.end(), context);

    output(fmt::format("wrote one row to {}.{} region #{} with raft commands", database_name, table_name, region_id));
}

void dbgInsertRowFull(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() < 6)
        throw Exception(
            "Args not matched, should be: database-name, table-name, region-id, handle-id, tso, del, values",
            ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    auto region_id = static_cast<RegionID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value));
    const auto & handle_field = typeid_cast<const ASTLiteral &>(*args[3]).value;
    HandleID handle_id = 0;
    if (handle_field.getType() == Field::Types::Int64 || handle_field.getType() == Field::Types::UInt64)
        handle_id = static_cast<HandleID>(safeGet<UInt64>(handle_field));
    auto tso = static_cast<Timestamp>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[4]).value));
    auto del = static_cast<UInt8>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[5]).value));

    using TsoDel = std::tuple<Timestamp, UInt8>;
    std::optional<TsoDel> extra_data = TsoDel{tso, del};

    MockTiDB::TablePtr table = MockTiDB::instance().getTableByName(database_name, table_name);
    RegionBench::insert(table->table_info, region_id, handle_id, args.begin() + 6, args.end(), context, extra_data);

    output(fmt::format("wrote one row to {}.{} region #{} with raft commands", database_name, table_name, region_id));
}

void dbgFuncRaftInsertRow(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    dbgInsertRow(context, args, output);
}

void dbgFuncRaftInsertRowFull(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    dbgInsertRowFull(context, args, output);
}

void dbgFuncRaftDeleteRow(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() < 4)
        throw Exception(
            "Args not matched, should be: database-name, table-name, region-id, handle-id",
            ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    auto region_id = static_cast<RegionID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value));
    auto handle_id = static_cast<HandleID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[3]).value));

    MockTiDB::TablePtr table = MockTiDB::instance().getTableByName(database_name, table_name);
    RegionBench::remove(table->table_info, region_id, handle_id, context);

    output(fmt::format("delete one row in {}.{}, region #{}", database_name, table_name, region_id));
}

void dbgInsertRows(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    const Int64 concurrent_num = safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    const Int64 flush_num = safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[3]).value);
    const Int64 batch_num = safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[4]).value);
    const auto min_strlen = safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[5]).value);
    const auto max_strlen = safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[6]).value);

    if (min_strlen < 1)
    {
        output("min_strlen should be greater than 0");
        return;
    }

    if (max_strlen < min_strlen)
    {
        output("max_strlen should be equal or greater than min_strlen");
        return;
    }

    using TablePtr = MockTiDB::TablePtr;

    TablePtr table = MockTiDB::instance().getTableByName(database_name, table_name);
    RegionBench::concurrentBatchInsert(
        table->table_info,
        concurrent_num,
        flush_num,
        batch_num,
        min_strlen,
        max_strlen,
        context);

    output(
        "wrote " + std::to_string(concurrent_num * flush_num * batch_num) + " row to " + database_name + "."
        + table_name + (" with raft commands"));
}

void dbgFuncRaftInsertRows(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    dbgInsertRows(context, args, output);
}

void dbgFuncRaftUpdateRows(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    const HandleID start_handle = safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    const HandleID end_handle = safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[3]).value);
    const Int64 magic_num = safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[4]).value);

    if (end_handle < start_handle)
    {
        output("end_handle should be equal or greater than start_handle");
        return;
    }

    using TablePtr = MockTiDB::TablePtr;

    TablePtr table = MockTiDB::instance().getTableByName(database_name, table_name);
    Int64 tol
        = RegionBench::concurrentRangeOperate(table->table_info, start_handle, end_handle, context, magic_num, false);

    output("update " + std::to_string(tol) + " row in " + database_name + "." + table_name);
}

void dbgFuncRaftDelRows(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    const HandleID start_handle = safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    const HandleID end_handle = safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[3]).value);

    if (end_handle < start_handle)
    {
        output("end_handle should be equal or greater than start_handle");
        return;
    }

    using TablePtr = MockTiDB::TablePtr;

    TablePtr table = MockTiDB::instance().getTableByName(database_name, table_name);
    Int64 tol = RegionBench::concurrentRangeOperate(table->table_info, start_handle, end_handle, context, 0, true);

    output("delete " + std::to_string(tol) + " row in " + database_name + "." + table_name);
}

} // namespace DB
