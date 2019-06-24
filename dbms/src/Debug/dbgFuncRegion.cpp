#include <Common/typeid_cast.h>
#include <Debug/MockTiDB.h>
#include <Debug/MockTiKV.h>
#include <Debug/dbgFuncRegion.h>
#include <Debug/dbgTools.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/MutableSupport.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiKVRange.h>
#include <Storages/Transaction/applySnapshot.h>
#include <Storages/Transaction/tests/region_helper.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int UNKNOWN_TABLE;
} // namespace ErrorCodes

TableID getTableID(Context & context, const std::string & database_name, const std::string & table_name, const std::string & partition_name)
{
    try
    {
        using TablePtr = MockTiDB::TablePtr;
        TablePtr table = MockTiDB::instance().getTableByName(database_name, table_name);

        if (table->isPartitionTable())
            return table->getPartitionIDByName(partition_name);

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
    const String & partition_name = args.size() == 6 ? typeid_cast<const ASTIdentifier &>(*args[5]).name : "";

    TableID table_id = getTableID(context, database_name, table_name, partition_name);

    TMTContext & tmt = context.getTMTContext();
    RegionPtr region = RegionBench::createRegion(table_id, region_id, start, end);
    tmt.getKVStore()->onSnapshot(region, &tmt.getRegionTable());

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

void dbgFuncRegionSnapshotWithData(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    RegionID region_id = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    HandleID start = (HandleID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[3]).value);
    HandleID end = (HandleID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[4]).value);

    TableID table_id = getTableID(context, database_name, table_name, "");
    RegionPtr region = RegionBench::createRegion(table_id, region_id, start, end);

    auto args_begin = args.begin() + 5;
    auto args_end = args.end();

    MockTiDB::TablePtr table = MockTiDB::instance().getTableByName(database_name, table_name);
    const size_t len = table->table_info.columns.size() + 3;

    if ((args_end - args_begin) % len)
        throw Exception("Number of insert values and columns do not match.", ErrorCodes::LOGICAL_ERROR);

    TMTContext & tmt = context.getTMTContext();
    size_t cnt = 0;

    for (auto it = args_begin; it != args_end; it += len)
    {
        HandleID handle_id = (HandleID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*it[0]).value);
        Timestamp tso = (UInt8)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*it[1]).value);
        UInt8 del = (UInt8)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*it[2]).value);
        {
            std::vector<Field> fields;

            for (auto p = it + 3; p != it + len; ++p)
            {
                auto field = typeid_cast<const ASTLiteral *>((*p).get())->value;
                fields.emplace_back(field);
            }

            TiKVKey key = RecordKVFormat::genKey(table_id, handle_id);
            TiKVValue value = RecordKVFormat::EncodeRow(table->table_info, fields);
            UInt64 commit_ts = tso;
            UInt64 prewrite_ts = tso;
            TiKVValue commit_value;

            if (del)
                commit_value = RecordKVFormat::encodeWriteCfValue(Region::DelFlag, prewrite_ts);
            else
                commit_value = RecordKVFormat::encodeWriteCfValue(Region::PutFlag, prewrite_ts, value);

            TiKVKey commit_key = RecordKVFormat::appendTs(key, commit_ts);

            region->insert(Region::write_cf_name, commit_key, commit_value);
        }
        ++cnt;
        MockTiKV::instance().getRaftIndex(region_id);
    }

    applySnapshot(tmt.getKVStore(), region, &context);

    std::stringstream ss;
    ss << "put region #" << region_id << ", range[" << start << ", " << end << ")"
       << " to table #" << table_id << " with " << cnt << " records";
    output(ss.str());
}

void dbgFuncRegionSnapshot(Context & context, const ASTs & args, DBGInvoker::Printer output)
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
    const String & partition_name = args.size() == 6 ? typeid_cast<const ASTIdentifier &>(*args[5]).name : "";

    TableID table_id = getTableID(context, database_name, table_name, partition_name);

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

    (req.mutable_state()->mutable_apply_state())->set_applied_index(MockTiKV::instance().getRaftIndex(region_id));

    // TODO: Put data into snapshot cmd

    auto reader = [&](enginepb::SnapshotRequest * out) {
        if (is_readed)
            return false;
        *out = req;
        is_readed = true;
        return true;
    };
    applySnapshot(tmt.getKVStore(), reader, &context);

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
    if (args.size() < 1)
        throw Exception("Args not matched, should be: table_id", ErrorCodes::BAD_ARGUMENTS);

    auto & tmt = context.getTMTContext();
    TableID table_id = (TableID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value);

    bool ignore_none = false;
    if (args.size() > 1)
        ignore_none = (std::string(typeid_cast<const ASTIdentifier &>(*args[1]).name) == "true");

    size_t size = 0;
    tmt.getKVStore()->traverseRegions([&](const RegionID region_id, const RegionPtr & region) {
        std::ignore = region_id;
        auto range = region->getHandleRangeByTable(table_id);
        size += 1;
        std::stringstream ss;

        if (range.first >= range.second && ignore_none)
            return;

        ss << "table #" << table_id << " " << region->toString();
        if (range.first >= range.second)
            ss << " [none], ";
        else
            ss << " ranges: [" << range.first.toString() << ", " << range.second.toString() << "), ";
        ss << region->dataInfo();
        output(ss.str());
    });
    output("total size: " + toString(size));
}

} // namespace DB
