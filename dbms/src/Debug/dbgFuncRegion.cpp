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
    RegionID region_id = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value);
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
        for (size_t i = 0; i < handle_column_size; i++)
        {
            auto & column_info = table_info.columns[table_info.getPrimaryIndexInfo().idx_cols[i].offset];
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

        std::stringstream ss;
        ss << "put region #" << region_id << ", range" << RecordKVFormat::DecodedTiKVKeyRangeToDebugString(region->getRange()->rawKeys())
           << " to table #" << table_id << " with kvstore.onSnapshot";
        output(ss.str());
    }
    else
    {
        HandleID start = (HandleID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[1]).value);
        HandleID end = (HandleID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value);

        TMTContext & tmt = context.getTMTContext();
        RegionPtr region = RegionBench::createRegion(table_id, region_id, start, end);
        tmt.getKVStore()->onSnapshot<RegionPtrWithBlock>(region, nullptr, 0, tmt);

        std::stringstream ss;
        ss << "put region #" << region_id << ", range[" << start << ", " << end << ")"
           << " to table #" << table_id << " with kvstore.onSnapshot";
        output(ss.str());
    }
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
        std::stringstream ss;
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

            ss << region->toString(dump_status);
            if (range.first >= range.second)
                ss << " ranges: [none], ";
            else
                ss << " ranges: [" << range.first.toString() << ", " << range.second.toString() << "), ";
        }
        else
        {
            if (*rawkeys.first >= *rawkeys.second && ignore_none)
                return;

            ss << region->toString(dump_status);
            ss << " ranges: " << RecordKVFormat::DecodedTiKVKeyRangeToDebugString(rawkeys) << ", ";
        }
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
