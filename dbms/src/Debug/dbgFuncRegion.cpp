#include <Common/typeid_cast.h>
#include <Debug/MockTiDB.h>
#include <Debug/MockTiKV.h>
#include <Debug/dbgFuncRegion.h>
#include <Debug/dbgTools.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/ProxyFFI.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiKVRange.h>

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
    if (args.size() < 5 || args.size() > 6)
    {
        throw Exception("Args not matched, should be: region-id, start-key, end-key, database-name, table-name[, partition-name]",
            ErrorCodes::BAD_ARGUMENTS);
    }

    RegionID region_id = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    HandleID start = (HandleID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[1]).value);
    HandleID end = (HandleID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[3]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[4]).name;
    const String & partition_id = args.size() == 6 ? std::to_string(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[5]).value)) : "";

    TableID table_id = RegionBench::getTableID(context, database_name, table_name, partition_id);

    TMTContext & tmt = context.getTMTContext();
    RegionPtr region = RegionBench::createRegion(table_id, region_id, start, end);
    tmt.getKVStore()->onSnapshot(region, nullptr, 0, tmt);

    std::stringstream ss;
    ss << "put region #" << region_id << ", range[" << start << ", " << end << ")"
       << " to table #" << table_id << " with kvstore.onSnapshot";
    output(ss.str());
}

void dbgFuncTryFlush(Context & context, const ASTs &, DBGInvoker::Printer output)
{
    TMTContext & tmt = context.getTMTContext();
    tmt.getRegionTable().tryFlushRegions();

    std::stringstream ss;
    ss << "region_table try flush regions";
    output(ss.str());
}

void dbgFuncTryFlushRegion(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 1)
    {
        throw Exception("Args not matched, should be: region-id", ErrorCodes::BAD_ARGUMENTS);
    }

    RegionID region_id = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value);

    TMTContext & tmt = context.getTMTContext();
    tmt.getRegionTable().tryFlushRegion(region_id);

    std::stringstream ss;
    ss << "region_table try flush region " << region_id;
    output(ss.str());
}

void dbgFuncDumpAllRegion(Context & context, TableID table_id, bool ignore_none, bool dump_status, DBGInvoker::Printer & output)
{
    size_t size = 0;
    context.getTMTContext().getKVStore()->traverseRegions([&](const RegionID region_id, const RegionPtr & region) {
        std::ignore = region_id;
        auto range = region->getHandleRangeByTable(table_id);
        size += 1;
        std::stringstream ss;

        if (range.first >= range.second && ignore_none)
            return;

        ss << region->toString(dump_status);
        ss << " ranges: " << DB::TiKVKeyRangeToDebugString(range) << ", ";
        ss << "state: " << raft_serverpb::PeerState_Name(region->peerState());
        if (auto s = region->dataInfo(); s.size() > 2)
            ss << ", " << s;
        output(ss.str());
    });
    output("total size: " + toString(size));
}

void dbgFuncDumpAllRegion(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() < 1)
        throw Exception("Args not matched, should be: table_id", ErrorCodes::BAD_ARGUMENTS);

    TableID table_id = (TableID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value);

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
    if (args.size() < 1)
        throw Exception("Args not matched, should be: region_id", ErrorCodes::BAD_ARGUMENTS);

    RegionID region_id = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value);

    TMTContext & tmt = context.getTMTContext();
    KVStorePtr & kvstore = tmt.getKVStore();
    RegionTable & region_table = tmt.getRegionTable();
    kvstore->mockRemoveRegion(region_id, region_table);

    std::stringstream ss;
    ss << "remove region #" << region_id;
    output(ss.str());
}

} // namespace DB
