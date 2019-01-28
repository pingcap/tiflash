#include <Common/typeid_cast.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>

#include <Storages/Transaction/TMTContext.h>

#include <Debug/dbgFuncMockTiDBData.h>
#include <Debug/dbgTools.h>
#include <Debug/MockTiDB.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void dbgFuncSetFlushRows(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 1)
        throw Exception("Args not matched, should be: threshold-rows", ErrorCodes::BAD_ARGUMENTS);

    auto rows = safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    TMTContext & tmt = context.getTMTContext();
    tmt.table_flushers.setFlushThresholdRows(rows);

    std::stringstream ss;
    ss << "set flush threshold to " << rows << " rows";
    output(ss.str());
}

void dbgFuncSetDeadlineSeconds(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 1)
        throw Exception("Args not matched, should be: second-uint", ErrorCodes::BAD_ARGUMENTS);

    const UInt64 second = safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value);

    TMTContext & tmt = context.getTMTContext();
    tmt.table_flushers.setDeadlineSeconds(second);

    std::stringstream ss;
    ss << "set deadline seconds to " << second << "s";
    output(ss.str());
}

void dbgInsertRow(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() < 4)
        throw Exception("Args not matched, should be: database-name, table-name, region-id, handle-id, values", ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    RegionID region_id = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    HandleID handle_id = (HandleID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[3]).value);

    MockTiDB::TablePtr table = MockTiDB::instance().getTableByName(database_name, table_name);
    RegionBench::insert(table->table_info, region_id, handle_id, args.begin() + 4, args.end(), context);

    std::stringstream ss;
    ss << "wrote one row to " << database_name << "." + table_name << " region #" << region_id;
    ss << " with raft commands";
    output(ss.str());
}

void dbgFuncRaftInsertRow(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    dbgInsertRow(context, args, output);
}

void dbgFuncRaftDeleteRow(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() < 4)
        throw Exception("Args not matched, should be: database-name, table-name, region-id, handle-id", ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    RegionID region_id = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    HandleID handle_id = (HandleID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[3]).value);

    MockTiDB::TablePtr table = MockTiDB::instance().getTableByName(database_name, table_name);
    RegionBench::remove(table->table_info, region_id, handle_id, context);

    std::stringstream ss;
    ss << "delete one row in " << database_name << "." + table_name << ", region #" << region_id;
    output(ss.str());
}

void dbgInsertRows(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    const Int64 concurrent_num = safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    const Int64 flush_num = safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[3]).value);
    const Int64 batch_num = safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[4]).value);
    const UInt64 min_strlen = safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[5]).value);
    const UInt64 max_strlen = safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[6]).value);

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
    RegionBench::concurrentBatchInsert(table->table_info, concurrent_num, flush_num,
        batch_num, min_strlen, max_strlen, context);

    output("wrote " + std::to_string(concurrent_num * flush_num * batch_num) + " row to " +
        database_name + "." + table_name + (" with raft commands"));
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
    Int64 tol = RegionBench::concurrentRangeOperate(table->table_info, start_handle, end_handle, context, magic_num, false);

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

}
