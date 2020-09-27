#include <Common/typeid_cast.h>
#include <Debug/MockTiDB.h>
#include <Debug/MockTiKV.h>
#include <Debug/dbgFuncRegion.h>
#include <Debug/dbgTools.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/ProxyFFIType.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiKVRange.h>
#include <Storages/Transaction/tests/region_helper.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int UNKNOWN_TABLE;
} // namespace ErrorCodes

TableID getTableID(Context & context, const std::string & database_name, const std::string & table_name, const std::string & partition_id)
{
    try
    {
        using TablePtr = MockTiDB::TablePtr;
        TablePtr table = MockTiDB::instance().getTableByName(database_name, table_name);

        if (table->isPartitionTable())
            return std::atoi(partition_id.c_str());

        return table->id();
    }
    catch (Exception & e)
    {
        if (e.code() != ErrorCodes::UNKNOWN_TABLE)
            throw;
    }

    auto storage = context.getTable(database_name, table_name);
    auto managed_storage = std::static_pointer_cast<IManageableStorage>(storage);
    auto table_info = managed_storage->getTableInfo();
    return table_info.id;
}

const TiDB::TableInfo getTableInfo(Context & context, const String & database_name, const String table_name)
{
    try
    {
        using TablePtr = MockTiDB::TablePtr;
        TablePtr table = MockTiDB::instance().getTableByName(database_name, table_name);

        return table->table_info;
    }
    catch (Exception & e)
    {
        if (e.code() != ErrorCodes::UNKNOWN_TABLE)
            throw;
    }

    auto storage = context.getTable(database_name, table_name);
    auto managed_storage = std::static_pointer_cast<IManageableStorage>(storage);
    return managed_storage->getTableInfo();
}

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
    TableID table_id = getTableID(context, database_name, table_name, partition_id);
    const auto & table_info = getTableInfo(context, database_name, table_name);
    size_t handle_column_size = table_info.is_common_handle ? table_info.getPrimaryIndexInfo().idx_cols.size() : 1;
    if (args_size < 3 + 2 * handle_column_size || args_size > 3 + 2 * handle_column_size + 1)
        throw Exception("Args not matched, should be: region-id, start-key, end-key, database-name, table-name[, partition-name]",
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
        tmt.getKVStore()->onSnapshot(region, nullptr, 0, tmt);

        std::stringstream ss;
        ss << "put region #" << region_id << ", range["
           << RecordKVFormat::DecodedTiKVKeyToReadableHandleString<true>(*region->getRange()->rawKeys().first) << ", "
           << RecordKVFormat::DecodedTiKVKeyToReadableHandleString<false>(*region->getRange()->rawKeys().second) << ")"
           << " to table #" << table_id << " with kvstore.onSnapshot";
        output(ss.str());
    }
    else
    {
        HandleID start = (HandleID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[1]).value);
        HandleID end = (HandleID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value);


        TMTContext & tmt = context.getTMTContext();
        RegionPtr region = RegionBench::createRegion(table_id, region_id, start, end);
        tmt.getKVStore()->onSnapshot(region, nullptr, 0, tmt);

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

void dbgFuncRegionSnapshotWithData(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    RegionID region_id = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    TableID table_id = getTableID(context, database_name, table_name, "");
    MockTiDB::TablePtr table = MockTiDB::instance().getTableByName(database_name, table_name);
    auto & table_info = table->table_info;
    bool is_common_handle = table_info.is_common_handle;
    size_t handle_column_size = is_common_handle ? table_info.getPrimaryIndexInfo().idx_cols.size() : 1;
    RegionPtr region;

    if (!is_common_handle)
    {
        HandleID start = (HandleID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[3]).value);
        HandleID end = (HandleID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[4]).value);
        region = RegionBench::createRegion(table_id, region_id, start, end);
    }
    else
    {
        std::vector<Field> start_keys;
        std::vector<Field> end_keys;
        for (size_t i = 0; i < handle_column_size; i++)
        {
            auto & column_info = table_info.columns[table_info.getPrimaryIndexInfo().idx_cols[i].offset];
            auto start_field = RegionBench::convertField(column_info, typeid_cast<const ASTLiteral &>(*args[3 + i]).value);
            TiDB::DatumBumpy start_datum = TiDB::DatumBumpy(start_field, column_info.tp);
            start_keys.emplace_back(start_datum.field());
            auto end_field
                = RegionBench::convertField(column_info, typeid_cast<const ASTLiteral &>(*args[3 + handle_column_size + i]).value);
            TiDB::DatumBumpy end_datum = TiDB::DatumBumpy(end_field, column_info.tp);
            end_keys.emplace_back(end_datum.field());
        }
        region = RegionBench::createRegion(table_info, region_id, start_keys, end_keys);
    }
    auto & rawkeys = region->getRange()->rawKeys();
    auto start_string = RecordKVFormat::DecodedTiKVKeyToReadableHandleString<true>(*rawkeys.first);
    auto end_string = RecordKVFormat::DecodedTiKVKeyToReadableHandleString<false>(*rawkeys.second);

    auto args_begin = args.begin() + 3 + handle_column_size * 2;
    auto args_end = args.end();

    const size_t len = table->table_info.columns.size() + 3;

    if ((args_end - args_begin) % len)
        throw Exception("Number of insert values and columns do not match.", ErrorCodes::LOGICAL_ERROR);

    TMTContext & tmt = context.getTMTContext();
    size_t cnt = 0;

    for (auto it = args_begin; it != args_end; it += len)
    {
        HandleID handle_id = is_common_handle ? 0 : (HandleID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*it[0]).value);
        Timestamp tso = (Timestamp)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*it[1]).value);
        UInt8 del = (UInt8)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*it[2]).value);
        {
            std::vector<Field> fields;

            for (auto p = it + 3; p != it + len; ++p)
            {
                auto field = typeid_cast<const ASTLiteral *>((*p).get())->value;
                fields.emplace_back(field);
            }

            TiKVKey key;
            if (is_common_handle)
            {
                std::vector<Field> keys;
                for (size_t i = 0; i < table_info.getPrimaryIndexInfo().idx_cols.size(); i++)
                {
                    auto & idx_col = table_info.getPrimaryIndexInfo().idx_cols[i];
                    auto & column_info = table_info.columns[idx_col.offset];
                    auto start_field = RegionBench::convertField(column_info, fields[idx_col.offset]);
                    TiDB::DatumBumpy start_datum = TiDB::DatumBumpy(start_field, column_info.tp);
                    keys.emplace_back(start_datum.field());
                }
                key = RecordKVFormat::genKey(table_info, keys);
            }
            else
                key = RecordKVFormat::genKey(table_id, handle_id);
            std::stringstream ss;
            RegionBench::encodeRow(table->table_info, fields, ss);
            TiKVValue value(ss.str());
            UInt64 commit_ts = tso;
            UInt64 prewrite_ts = tso;
            TiKVValue commit_value = del ? RecordKVFormat::encodeWriteCfValue(Region::DelFlag, prewrite_ts)
                                         : RecordKVFormat::encodeWriteCfValue(Region::PutFlag, prewrite_ts, value);
            TiKVKey commit_key = RecordKVFormat::appendTs(key, commit_ts);

            region->insert(ColumnFamilyType::Write, std::move(commit_key), std::move(commit_value));
        }
        ++cnt;
        MockTiKV::instance().getRaftIndex(region_id);
    }

    tmt.getKVStore()->tryApplySnapshot(region, context);

    std::stringstream ss;
    ss << "put region #" << region_id << ", range[" << start_string << ", " << end_string << ")"
       << " to table #" << table_id << " with " << cnt << " records";
    output(ss.str());
}

void dbgFuncRegionSnapshot(Context & context, const ASTs & args, DBGInvoker::Printer output)
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
    TableID table_id = getTableID(context, database_name, table_name, partition_id);
    const auto & table_info = getTableInfo(context, database_name, table_name);

    size_t handle_column_size = table_info.is_common_handle ? table_info.getPrimaryIndexInfo().idx_cols.size() : 1;
    if (args_size < 3 + 2 * handle_column_size || args_size > 3 + 2 * handle_column_size + 1)
        throw Exception("Args not matched, should be: region-id, start-key, end-key, database-name, table-name[, partition-name]",
            ErrorCodes::BAD_ARGUMENTS);


    TMTContext & tmt = context.getTMTContext();

    metapb::Region region_info;

    TiKVKey start_key;
    TiKVKey end_key;
    region_info.set_id(region_id);
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
        start_key = RecordKVFormat::genKey(table_info, start_keys);
        end_key = RecordKVFormat::genKey(table_info, end_keys);
    }
    else
    {
        HandleID start = (HandleID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[1]).value);
        HandleID end = (HandleID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value);
        start_key = RecordKVFormat::genKey(table_id, start);
        end_key = RecordKVFormat::genKey(table_id, end);
    }
    region_info.set_start_key(start_key.toString());
    region_info.set_end_key(end_key.toString());
    *region_info.add_peers() = createPeer(1, true);
    *region_info.add_peers() = createPeer(2, true);
    auto peer_id = 1;
    auto start_decoded_key = RecordKVFormat::decodeTiKVKey(start_key);
    auto end_decoded_key = RecordKVFormat::decodeTiKVKey(end_key);

    tmt.getKVStore()->handleApplySnapshot(
        std::move(region_info), peer_id, SnapshotViewArray(), MockTiKV::instance().getRaftIndex(region_id), RAFT_INIT_LOG_TERM, tmt);

    std::stringstream ss;
    ss << "put region #" << region_id << ", range[" << RecordKVFormat::DecodedTiKVKeyToReadableHandleString<true>(start_decoded_key) << ", "
       << RecordKVFormat::DecodedTiKVKeyToReadableHandleString<false>(end_decoded_key) << ")"
       << " to table #" << table_id << " with raft commands";
    output(ss.str());
}

std::string getRegionKeyString(const TiKVRange::Handle s, const TiKVKey & k)
{
    try
    {
        if (s.type != TiKVHandle::HandleIDType::NORMAL)
        {
            auto raw_key = k.empty() ? DecodedTiKVKey() : RecordKVFormat::decodeTiKVKey(k);
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
                ss << " [none], ";
            else
                ss << " ranges: [" << range.first.toString() << ", " << range.second.toString() << "), ";
        }
        else
        {
            if (*rawkeys.first >= *rawkeys.second && ignore_none)
                return;

            ss << region->toString(dump_status);
            if (*rawkeys.first >= *rawkeys.second)
                ss << " [none], ";
            else
                ss << " ranges: [" << RecordKVFormat::DecodedTiKVKeyToReadableHandleString<true>(*rawkeys.first) << ", "
                   << RecordKVFormat::DecodedTiKVKeyToReadableHandleString<false>(*rawkeys.second) << "), ";
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

void dbgFuncIngestSST(Context & context, const ASTs & args, DBGInvoker::Printer)
{
    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    RegionID region_id = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    RegionID start_handle = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[3]).value);
    RegionID end_handle = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[4]).value);
    MockTiDB::TablePtr table = MockTiDB::instance().getTableByName(database_name, table_name);

    std::vector<std::pair<TiKVKey, TiKVValue>> write_kv_list, default_kv_list;

    for (auto handle_id = start_handle; handle_id < end_handle; ++handle_id)
    {
        // make it have only one column Int64 just for test
        std::vector<Field> fields;
        fields.emplace_back(-handle_id);
        {
            TiKVKey key = RecordKVFormat::genKey(table->id(), handle_id);
            std::stringstream ss;
            RegionBench::encodeRow(table->table_info, fields, ss);
            TiKVValue prewrite_value(ss.str());
            UInt64 commit_ts = handle_id;
            UInt64 prewrite_ts = commit_ts;
            TiKVValue commit_value = RecordKVFormat::encodeWriteCfValue(Region::PutFlag, prewrite_ts);
            TiKVKey commit_key = RecordKVFormat::appendTs(key, commit_ts);
            TiKVKey prewrite_key = RecordKVFormat::appendTs(key, prewrite_ts);

            write_kv_list.emplace_back(std::make_pair(std::move(commit_key), std::move(commit_value)));
            default_kv_list.emplace_back(std::make_pair(std::move(prewrite_key), std::move(prewrite_value)));
        }
    }

    {
        std::vector<BaseBuffView> keys;
        std::vector<BaseBuffView> vals;
        for (const auto & kv : write_kv_list)
        {
            keys.push_back({kv.first.data(), kv.first.dataSize()});
            vals.push_back({kv.second.data(), kv.second.dataSize()});
        }
        std::vector<SnapshotView> snaps;
        snaps.push_back(SnapshotView{keys.data(), vals.data(), ColumnFamilyType::Write, keys.size()});

        auto & tmt = context.getTMTContext();
        tmt.getKVStore()->handleIngestSST(region_id, SnapshotViewArray{snaps.data(), snaps.size()},
            MockTiKV::instance().getRaftIndex(region_id), MockTiKV::instance().getRaftTerm(region_id), tmt);
    }

    {
        std::vector<BaseBuffView> keys;
        std::vector<BaseBuffView> vals;
        for (const auto & kv : default_kv_list)
        {
            keys.push_back({kv.first.data(), kv.first.dataSize()});
            vals.push_back({kv.second.data(), kv.second.dataSize()});
        }
        std::vector<SnapshotView> snaps;
        snaps.push_back(SnapshotView{keys.data(), vals.data(), ColumnFamilyType::Default, keys.size()});
        auto & tmt = context.getTMTContext();
        tmt.getKVStore()->handleIngestSST(region_id, SnapshotViewArray{snaps.data(), snaps.size()},
            MockTiKV::instance().getRaftIndex(region_id), MockTiKV::instance().getRaftTerm(region_id), tmt);
    }
}

} // namespace DB
