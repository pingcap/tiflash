#include <Common/typeid_cast.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>

#include <Storages/MutableSupport.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/applySnapshot.h>

#include <Storages/Transaction/tests/region_helper.h>

#include <Debug/MockTiDB.h>
#include <Debug/dbgFuncRegion.h>
#include <Debug/dbgTools.h>

#include <Interpreters/executeQuery.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int UNKNOWN_TABLE;
} // namespace ErrorCodes

TableID getTableID(Context & context, const std::string & database_name, const std::string & table_name)
{
    try
    {
        using TablePtr = MockTiDB::TablePtr;
        TablePtr table = MockTiDB::instance().getTableByName(database_name, table_name);
        return table->id();
    }
    catch (Exception & e)
    {
        if (e.code() != ErrorCodes::UNKNOWN_TABLE)
            throw;
    }

    auto storage = context.getTable(database_name, table_name);
    auto * merge_tree = dynamic_cast<StorageMergeTree *>(storage.get());
    auto table_info = merge_tree->getTableInfo();
    return table_info.id;
}

void dbgFuncPutRegion(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 5)
    {
        throw Exception("Args not matched, should be: region-id, start-key, end-key, database-name, table-name", ErrorCodes::BAD_ARGUMENTS);
    }

    RegionID region_id = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    RegionKey start = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[1]).value);
    RegionKey end = (RegionKey)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[3]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[4]).name;

    TableID table_id = getTableID(context, database_name, table_name);

    TMTContext & tmt = context.getTMTContext();
    RegionPtr region = RegionBench::createRegion(table_id, region_id, start, end);
    tmt.kvstore->onSnapshot(region, &context);

    std::stringstream ss;
    ss << "put region #" << region_id << ", range[" << start << ", " << end << ")"
       << " to table #" << table_id << " with kvstore.onSnapshot";
    output(ss.str());
}

void dbgFuncRegionSnapshot(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() < 5)
    {
        throw Exception("Args not matched, should be: region-id, start-key, end-key, database-name, table-name", ErrorCodes::BAD_ARGUMENTS);
    }

    RegionID region_id = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    RegionKey start = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[1]).value);
    RegionKey end = (RegionKey)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[3]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[4]).name;

    TableID table_id = getTableID(context, database_name, table_name);

    TMTContext & tmt = context.getTMTContext();

    enginepb::SnapshotRequest req;
    bool is_readed = false;

    *(req.mutable_state()->mutable_peer()) = createPeer(1, true);

    metapb::Region region_info;
    region_info.set_id(region_id);
    region_info.set_start_key(RecordKVFormat::genKey(table_id, start).getStr());
    region_info.set_end_key(RecordKVFormat::genKey(table_id, end).getStr());
    *(region_info.mutable_peers()->Add()) = createPeer(1, true);
    *(region_info.mutable_peers()->Add()) = createPeer(2, false);
    *(req.mutable_state()->mutable_region()) = region_info;

    *(req.mutable_state()->mutable_apply_state()) = initialApplyState();

    // TODO: Put data into snapshot cmd

    auto reader = [&](enginepb::SnapshotRequest * out) {
        if (is_readed)
            return false;
        *out = req;
        is_readed = true;
        return true;
    };
    applySnapshot(tmt.kvstore, reader, &context);

    std::stringstream ss;
    ss << "put region #" << region_id << ", range[" << start << ", " << end << ")"
       << " to table #" << table_id << " with raft commands";
    output(ss.str());
}

std::string getRegionKeyString(const TiKVRange::Handle s, const TiKVKey & k)
{
    try
    {
        if (s.type != TiKVHandle::HandleIDType::NORMAL)
        {
            String raw_key = k.empty() ? "" : RecordKVFormat::decodeTiKVKey(k);
            bool is_record = RecordKVFormat::isRecord(raw_key);
            std::stringstream ss;
            if (is_record)
            {
                ss << "h:" << RecordKVFormat::getHandle(raw_key);
                ss << "#" << RecordKVFormat::getTableId(raw_key);
            }
            else
            {
                ss << "v:[" << raw_key << "]";
            }
            return ss.str();
        }
        return toString(s.handle_id);
    }
    catch (...)
    {
        return "e:" + k.toHex();
    }
}

std::string getStartKeyString(TableID table_id, const TiKVKey & start_key)
{
    try
    {
        auto start_handle = TiKVRange::getRangeHandle<true>(start_key, table_id);
        return getRegionKeyString(start_handle, start_key);
    }
    catch (...)
    {
        return "e: " + start_key.toHex();
    }
}

std::string getEndKeyString(TableID table_id, const TiKVKey & end_key)
{
    try
    {
        auto end_handle = TiKVRange::getRangeHandle<false>(end_key, table_id);
        return getRegionKeyString(end_handle, end_key);
    }
    catch (...)
    {
        return "e: " + end_key.toHex();
    }
}

void dbgFuncDumpAllRegion(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    auto & tmt = context.getTMTContext();
    TableID table_id = (TableID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    size_t size = 0;
    tmt.kvstore->traverseRegions([&](const RegionID region_id, const RegionPtr & region) {
        std::ignore = region_id;
        auto range = region->getHandleRangeByTable(table_id);
        size += 1;
        std::stringstream ss;
        ss << "table #" << table_id << " " << region->toString() << " ranges: " << range.first.toString() << ", "
           << range.second.toString();
        output(ss.str());
    });
    output("total size: " + toString(size));
}

void dbgFuncDumpRegion(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() > 1)
    {
        throw Exception("Args not matched, should be: [show-region-range=false]", ErrorCodes::BAD_ARGUMENTS);
    }

    bool show_region = false;
    if (args.size() > 0)
        show_region = (std::string(typeid_cast<const ASTIdentifier &>(*args[0]).name) == "true");

    auto & tmt = context.getTMTContext();

    RegionTable::RegionMap regions;
    tmt.region_table.dumpRegionMap(regions);

    for (const auto & it : regions)
    {
        auto region_id = it.first;
        const auto & table_ids = it.second.tables;
        for (const auto table_id : table_ids)
        {
            std::stringstream region_range_info;
            if (show_region)
            {
                region_range_info << " (";
                RegionPtr region = tmt.kvstore->getRegion(region_id);
                if (!region)
                {
                    region_range_info << "not in kvstore";
                }
                else
                {
                    auto [start_key, end_key] = region->getRange();
                    region_range_info << getStartKeyString(table_id, start_key);
                    region_range_info << ", ";
                    region_range_info << getEndKeyString(table_id, end_key);
                    region_range_info << ")";
                }
            }

            std::stringstream ss;
            ss << "table #" << table_id << " region #" << region_id << region_range_info.str();
            output(ss.str());
        }
    }
}

void dbgFuncRegionRmData(Context & /*context*/, const ASTs & /*args*/, DBGInvoker::Printer /*output*/)
{
    // TODO: port from RegionPartitionMgr to RegionTable
    /*
    if (args.size() != 1)
        throw Exception("Args not matched, should be: region-id", ErrorCodes::BAD_ARGUMENTS);

    TMTContext & tmt = context.getTMTContext();

    RegionID region_id = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    RegionPtr region = tmt.kvstore.get(region_id);

    TiKVKey start_key(region->getMeta().region.start_key());
    TiKVKey end_key(region->getMeta().region.end_key());
    RegionKey start = RecordKVFormat::getHandle(start_key);
    RegionKey end = RecordKVFormat::getHandle(end_key);

    TableID table_id = RecordKVFormat::getTableId(start_key);

    // TODO: lock structure?
    RegionPartitionMgrPtr partitions = tmt.partition_mgrs.tryGet(table_id);
    partitions->removeRegion(region_id);

    std::stringstream ss;
    ss << "delete data of region #" << region_id << ", range[" << start << ", " << end << ")" << " in table #" << table_id;
    output(ss.str());
    */
}

size_t executeQueryAndCountRows(Context & context, const std::string & query)
{
    size_t count = 0;
    Context query_context = context;
    query_context.setSessionContext(query_context);
    BlockInputStreamPtr input = executeQuery(query, query_context, true, QueryProcessingStage::Complete).in;
    input->readPrefix();
    while (true)
    {
        Block block = input->read();
        if (!block)
            break;
        count += block.rows();
    }
    input->readSuffix();
    return count;
}

} // namespace DB
