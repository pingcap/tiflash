#include <Common/FailPoint.h>
#include <Common/typeid_cast.h>
#include <Debug/MockTiDB.h>
#include <Debug/MockTiKV.h>
#include <Debug/dbgFuncMockRaftCommand.h>
#include <Debug/dbgFuncRegion.h>
#include <Debug/dbgTools.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateQuery.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <RaftStoreProxyFFI/ColumnFamily.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/ProxyFFI.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiKVRange.h>
#include <Storages/Transaction/tests/region_helper.h>

namespace DB
{

namespace FailPoints
{
extern const char force_set_sst_to_dtfile_block_size[];
extern const char force_set_sst_decode_rand[];
extern const char force_set_safepoint_when_decode_block[];
} // namespace FailPoints

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int UNKNOWN_TABLE;
} // namespace ErrorCodes

// DBGInvoke region_snapshot_data(database_name, table_name, region_id, start, end, handle_id1, tso1, del1, r1_c1, r1_c2, ..., handle_id2, tso2, del2, r2_c1, r2_c2, ... )
RegionPtr GenDbgRegionSnapshotWithData(Context & context, const ASTs & args)
{
    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    RegionID region_id = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    TableID table_id = RegionBench::getTableID(context, database_name, table_name, "");
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
        // Get start key and end key form multiple column if it is clustered_index.
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

    auto args_begin = args.begin() + 3 + handle_column_size * 2;
    auto args_end = args.end();

    const size_t len = table->table_info.columns.size() + 3;

    if ((args_end - args_begin) % len)
        throw Exception("Number of insert values and columns do not match.", ErrorCodes::LOGICAL_ERROR);

    // Parse row values
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

            TiKVKey key; // handle key
            if (is_common_handle)
            {
                std::vector<Field> keys; // handle key
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
        MockTiKV::instance().getRaftIndex(region_id);
    }
    return region;
}

// Mock to apply snapshot for region with some rows
// DBGInvoke region_snapshot_data(database_name, table_name, region_id, start, end, handle_id1, tso1, del1, r1_c1, r1_c2, ..., handle_id2, tso2, del2, r2_c1, r2_c2, ... )
void MockRaftCommand::dbgFuncRegionSnapshotWithData(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    auto region = GenDbgRegionSnapshotWithData(context, args);
    auto range_string = RecordKVFormat::DecodedTiKVKeyRangeToDebugString(region->getRange()->rawKeys());
    auto region_id = region->id();
    auto table_id = region->getMappedTableID();
    auto cnt = region->writeCFCount();

    // Mock to apply a snapshot with data in `region`
    auto & tmt = context.getTMTContext();
    context.getTMTContext().getKVStore()->checkAndApplySnapshot<RegionPtrWithBlock>(region, tmt);
    std::stringstream ss;
    ss << "put region #" << region_id << ", range" << range_string << " to table #" << table_id << " with " << cnt << " records";
    output(ss.str());
}

// Mock to apply an empty snapshot for region
// DBGInvoke region_snapshot(region-id, start-key, end-key, database-name, table-name[, partition-id])
void MockRaftCommand::dbgFuncRegionSnapshot(Context & context, const ASTs & args, DBGInvoker::Printer output)
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
        throw Exception("Args not matched, should be: region-id, start-key, end-key, database-name, table-name[, partition-name]",
            ErrorCodes::BAD_ARGUMENTS);

    TMTContext & tmt = context.getTMTContext();

    metapb::Region region_info;

    TiKVKey start_key;
    TiKVKey end_key;
    region_info.set_id(region_id);
    if (table_info.is_common_handle)
    {
        // Get start key and end key form multiple column if it is clustered_index.
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

    // Mock to apply an empty snapshot for region[region-id]
    tmt.getKVStore()->handleApplySnapshot(
        std::move(region_info), peer_id, SSTViewVec{nullptr, 0}, MockTiKV::instance().getRaftIndex(region_id), RAFT_INIT_LOG_TERM, tmt);

    std::stringstream ss;
    ss << "put region #" << region_id << ", range[" << RecordKVFormat::DecodedTiKVKeyToDebugString<true>(start_decoded_key) << ", "
       << RecordKVFormat::DecodedTiKVKeyToDebugString<false>(end_decoded_key) << ")"
       << " to table #" << table_id << " with raft commands";
    output(ss.str());
}

/// Some helper structure / functions for IngestSST

struct MockSSTReader
{
    using Key = std::pair<std::string, ColumnFamilyType>;
    struct Data : std::vector<std::pair<std::string, std::string>>
    {
        Data(const Data &) = delete;
        Data() = default;
    };

    MockSSTReader(const Data & data_) : iter(data_.begin()), end(data_.end()), remained(iter != end) {}

    static SSTReaderPtr ffi_get_cf_file_reader(const Data & data_) { return SSTReaderPtr{new MockSSTReader(data_)}; }

    bool ffi_remained() const { return iter != end; }

    BaseBuffView ffi_key() const { return {iter->first.data(), iter->first.length()}; }

    BaseBuffView ffi_val() const { return {iter->second.data(), iter->second.length()}; }

    void ffi_next() { ++iter; }

    static std::map<Key, MockSSTReader::Data> & getMockSSTData() { return MockSSTData; }

private:
    Data::const_iterator iter;
    Data::const_iterator end;
    bool remained;

    static std::map<Key, MockSSTReader::Data> MockSSTData;
};

std::map<MockSSTReader::Key, MockSSTReader::Data> MockSSTReader::MockSSTData;

SSTReaderPtr fn_get_sst_reader(SSTView v, RaftStoreProxyPtr)
{
    std::string s(v.path.data, v.path.len);
    auto iter = MockSSTReader::getMockSSTData().find({s, v.type});
    if (iter == MockSSTReader::getMockSSTData().end())
        throw Exception("Can not find data in MockSSTData, [key=" + s + "] [type=" + CFToName(v.type) + "]");
    auto & d = iter->second;
    return MockSSTReader::ffi_get_cf_file_reader(d);
}
uint8_t fn_remained(SSTReaderPtr ptr, ColumnFamilyType)
{
    auto reader = reinterpret_cast<MockSSTReader *>(ptr.inner);
    return reader->ffi_remained();
}
BaseBuffView fn_key(SSTReaderPtr ptr, ColumnFamilyType)
{
    auto reader = reinterpret_cast<MockSSTReader *>(ptr.inner);
    return reader->ffi_key();
}
BaseBuffView fn_value(SSTReaderPtr ptr, ColumnFamilyType)
{
    auto reader = reinterpret_cast<MockSSTReader *>(ptr.inner);
    return reader->ffi_val();
}
void fn_next(SSTReaderPtr ptr, ColumnFamilyType)
{
    auto reader = reinterpret_cast<MockSSTReader *>(ptr.inner);
    reader->ffi_next();
}
void fn_gc(SSTReaderPtr ptr, ColumnFamilyType)
{
    auto reader = reinterpret_cast<MockSSTReader *>(ptr.inner);
    delete reader;
}

class RegionMockTest
{
public:
    RegionMockTest(KVStorePtr kvstore_, RegionPtr region_) : kvstore(kvstore_), region(region_)
    {
        std::memset(&mock_proxy_helper, 0, sizeof(mock_proxy_helper));
        mock_proxy_helper.sst_reader_interfaces = SSTReaderInterfaces{
            .fn_get_sst_reader = fn_get_sst_reader,
            .fn_remained = fn_remained,
            .fn_key = fn_key,
            .fn_value = fn_value,
            .fn_next = fn_next,
            .fn_gc = fn_gc,
        };
        kvstore->proxy_helper = &mock_proxy_helper;
        region->proxy_helper = &mock_proxy_helper;
    }
    ~RegionMockTest()
    {
        kvstore->proxy_helper = nullptr;
        region->proxy_helper = nullptr;
    }

private:
    TiFlashRaftProxyHelper mock_proxy_helper;
    KVStorePtr kvstore;
    RegionPtr region;
};

void GenMockSSTData(const TiDB::TableInfo & table_info,
    TableID table_id,
    const String & store_key,
    UInt64 start_handle,
    UInt64 end_handle,
    UInt64 num_fields = 1,
    const std::unordered_set<ColumnFamilyType> & cfs = {ColumnFamilyType::Write, ColumnFamilyType::Default})
{
    MockSSTReader::Data write_kv_list, default_kv_list;
    size_t num_rows = end_handle - start_handle;

    UInt64 index = 0;
    for (auto handle_id = start_handle; handle_id < end_handle; ++handle_id, ++index)
    {
        std::vector<Field> fields;
        if (num_fields > 0)
        {
            // make it have one column Int64 just for test
            fields.emplace_back(-handle_id);
        }
        if (num_fields > 1 && index >= num_rows / 3)
        {
            // column String for test
            std::string s = "_" + DB::toString(handle_id);
            Field f(s.data(), s.size());
            fields.emplace_back(std::move(f));
        }
        if (num_fields > 2 && index >= 2 * num_rows / 3)
        {
            // column UInt64 for test
            fields.emplace_back(handle_id / 2);
        }

        // Check the MVCC (key-format and transaction model) for details
        // https://en.pingcap.com/blog/2016-11-17-mvcc-in-tikv#mvcc
        {
            TiKVKey key = RecordKVFormat::genKey(table_id, handle_id);
            std::stringstream ss;
            RegionBench::encodeRow(table_info, fields, ss);
            TiKVValue prewrite_value(ss.str());

            UInt64 prewrite_ts = handle_id;
            UInt64 commit_ts = prewrite_ts + 100; // Assume that commit_ts is larger that prewrite_ts

            TiKVKey prewrite_key = RecordKVFormat::appendTs(key, prewrite_ts);
            default_kv_list.emplace_back(std::make_pair(std::move(prewrite_key), std::move(prewrite_value)));

            TiKVKey commit_key = RecordKVFormat::appendTs(key, commit_ts);
            TiKVValue commit_value = RecordKVFormat::encodeWriteCfValue(Region::PutFlag, prewrite_ts);
            write_kv_list.emplace_back(std::make_pair(std::move(commit_key), std::move(commit_value)));
        }
    }

    MockSSTReader::getMockSSTData().clear();

    if (cfs.count(ColumnFamilyType::Write) > 0)
        MockSSTReader::getMockSSTData()[MockSSTReader::Key{store_key, ColumnFamilyType::Write}] = std::move(write_kv_list);
    if (cfs.count(ColumnFamilyType::Default) > 0)
        MockSSTReader::getMockSSTData()[MockSSTReader::Key{store_key, ColumnFamilyType::Default}] = std::move(default_kv_list);
}

// TODO: make it a more generic testing function
void GenMockSSTDataByHandles(const TiDB::TableInfo & table_info,
    TableID table_id,
    const String & store_key,
    const std::vector<UInt64> & handles,
    UInt64 num_fields = 1,
    const std::unordered_set<ColumnFamilyType> & cfs = {ColumnFamilyType::Write, ColumnFamilyType::Default})
{
    MockSSTReader::Data write_kv_list, default_kv_list;
    size_t num_rows = handles.size();

    for (size_t index = 0; index < handles.size(); ++index)
    {
        const auto handle_id = handles[index];
        std::vector<Field> fields;
        if (num_fields > 0)
        {
            // make it have one column Int64 just for test
            fields.emplace_back(-handle_id);
        }
        if (num_fields > 1 && index >= num_rows / 3)
        {
            // column String for test
            std::string s = "_" + DB::toString(handle_id);
            Field f(s.data(), s.size());
            fields.emplace_back(std::move(f));
        }
        if (num_fields > 2 && index >= 2 * num_rows / 3)
        {
            // column UInt64 for test
            fields.emplace_back(handle_id / 2);
        }

        // Check the MVCC (key-format and transaction model) for details
        // https://en.pingcap.com/blog/2016-11-17-mvcc-in-tikv#mvcc
        // The rows (primary key, timestamp) are sorted by primary key asc, timestamp desc in SSTFiles
        // https://github.com/pingcap/tics/issues/1864
        {
            TiKVKey key = RecordKVFormat::genKey(table_id, handle_id);
            std::stringstream ss;
            RegionBench::encodeRow(table_info, fields, ss);
            TiKVValue prewrite_value(ss.str());

            UInt64 prewrite_ts = 100000 + num_rows - index; // make it to be timestamp desc in SSTFiles
            UInt64 commit_ts = prewrite_ts + 100;           // Assume that commit_ts is larger that prewrite_ts

            TiKVKey prewrite_key = RecordKVFormat::appendTs(key, prewrite_ts);
            default_kv_list.emplace_back(std::make_pair(std::move(prewrite_key), std::move(prewrite_value)));

            TiKVKey commit_key = RecordKVFormat::appendTs(key, commit_ts);
            TiKVValue commit_value = RecordKVFormat::encodeWriteCfValue(Region::PutFlag, prewrite_ts);
            write_kv_list.emplace_back(std::make_pair(std::move(commit_key), std::move(commit_value)));
        }
    }

    MockSSTReader::getMockSSTData().clear();

    if (cfs.count(ColumnFamilyType::Write) > 0)
        MockSSTReader::getMockSSTData()[MockSSTReader::Key{store_key, ColumnFamilyType::Write}] = std::move(write_kv_list);
    if (cfs.count(ColumnFamilyType::Default) > 0)
        MockSSTReader::getMockSSTData()[MockSSTReader::Key{store_key, ColumnFamilyType::Default}] = std::move(default_kv_list);
}

// Simulate a region IngestSST raft command
// DBGInvoke region_ingest_sst(database_name, table_name, region_id, start, end)
void MockRaftCommand::dbgFuncIngestSST(Context & context, const ASTs & args, DBGInvoker::Printer)
{
    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    RegionID region_id = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    RegionID start_handle = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[3]).value);
    RegionID end_handle = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[4]).value);
    MockTiDB::TablePtr table = MockTiDB::instance().getTableByName(database_name, table_name);

    const auto & table_info = RegionBench::getTableInfo(context, database_name, table_name);
    if (table_info.is_common_handle)
        throw Exception("Mocking ingestSST to a common handle table is not supported", ErrorCodes::LOGICAL_ERROR);

    // Mock SST data for handle [star, end)
    auto region_id_str = std::to_string(region_id);
    GenMockSSTData(table->table_info, table->id(), region_id_str, start_handle, end_handle);

    auto & tmt = context.getTMTContext();
    auto & kvstore = tmt.getKVStore();
    auto region = kvstore->getRegion(region_id);

    FailPointHelper::enableFailPoint(FailPoints::force_set_sst_decode_rand);
    // Register some mock SST reading methods so that we can decode data in `MockSSTReader::MockSSTData`
    RegionMockTest mock_test(kvstore, region);

    {
        // Mocking ingest a SST for column family "Write"
        std::vector<SSTView> sst_views;
        sst_views.push_back(SSTView{
            ColumnFamilyType::Write,
            BaseBuffView{region_id_str.data(), region_id_str.length()},
        });
        kvstore->handleIngestSST(region_id,
            SSTViewVec{sst_views.data(), sst_views.size()},
            MockTiKV::instance().getRaftIndex(region_id),
            MockTiKV::instance().getRaftTerm(region_id),
            tmt);
    }

    {
        // Mocking ingest a SST for column family "Default"
        std::vector<SSTView> sst_views;
        sst_views.push_back(SSTView{
            ColumnFamilyType::Default,
            BaseBuffView{region_id_str.data(), region_id_str.length()},
        });
        kvstore->handleIngestSST(region_id,
            SSTViewVec{sst_views.data(), sst_views.size()},
            MockTiKV::instance().getRaftIndex(region_id),
            MockTiKV::instance().getRaftTerm(region_id),
            tmt);
    }
}

/// A structure to store data between snapshot pre-apply and apply-predecoded
struct GlobalRegionMap
{
    using Key = std::string;
    using BlockVal = std::pair<RegionPtr, RegionPtrWithBlock::CachePtr>;
    std::unordered_map<Key, BlockVal> regions_block;
    using SnapPath = std::pair<RegionPtr, std::vector<UInt64>>;
    std::unordered_map<Key, SnapPath> regions_snap_files;
    std::mutex mutex;

    void insertRegionCache(const Key & name, BlockVal && val)
    {
        auto _ = std::lock_guard(mutex);
        regions_block[name] = std::move(val);
    }
    BlockVal popRegionCache(const Key & name)
    {
        auto _ = std::lock_guard(mutex);
        if (auto it = regions_block.find(name); it == regions_block.end())
            throw Exception(std::string(__PRETTY_FUNCTION__) + " ... " + name);
        else
        {
            auto ret = std::move(it->second);
            regions_block.erase(it);
            return ret;
        }
    }

    void insertRegionSnap(const Key & name, SnapPath && val)
    {
        auto _ = std::lock_guard(mutex);
        regions_snap_files[name] = std::move(val);
    }
    SnapPath popRegionSnap(const Key & name)
    {
        auto _ = std::lock_guard(mutex);
        if (auto it = regions_snap_files.find(name); it == regions_snap_files.end())
            throw Exception(std::string(__PRETTY_FUNCTION__) + " ... " + name);
        else
        {
            auto ret = std::move(it->second);
            regions_snap_files.erase(it);
            return ret;
        }
    }
};

static GlobalRegionMap GLOBAL_REGION_MAP;

/// Mock to pre-decode snapshot to block then apply

extern RegionPtrWithBlock::CachePtr GenRegionPreDecodeBlockData(const RegionPtr &, Context &);
void MockRaftCommand::dbgFuncRegionSnapshotPreHandleBlock(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    std::stringstream ss;
    auto region = GenDbgRegionSnapshotWithData(context, args);
    const auto region_name = "__snap_" + std::to_string(region->id());
    ss << "pre-handle " << region->toString(false) << " snapshot with data " << region->dataInfo();
    auto & tmt = context.getTMTContext();
    auto block_cache = GenRegionPreDecodeBlockData(region, tmt.getContext());
    ss << ", pre-decode block cache";
    {
        ss << " {";
        ss << " schema_version: ?";
        ss << ", data_list size: " << block_cache->data_list_read.size();
        ss << ", block row: " << block_cache->block.rows() << " col: " << block_cache->block.columns()
           << " bytes: " << block_cache->block.bytes();
        ss << " }";
    }
    GLOBAL_REGION_MAP.insertRegionCache(region_name, {region, std::move(block_cache)});
    output(ss.str());
}

void MockRaftCommand::dbgFuncRegionSnapshotApplyBlock(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 1)
    {
        throw Exception("Args not matched, should be: region-id", ErrorCodes::BAD_ARGUMENTS);
    }

    RegionID region_id = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args.front()).value);
    auto [region, block_cache] = GLOBAL_REGION_MAP.popRegionCache("__snap_" + std::to_string(region_id));
    auto & tmt = context.getTMTContext();
    context.getTMTContext().getKVStore()->checkAndApplySnapshot<RegionPtrWithBlock>({region, std::move(block_cache)}, tmt);

    std::stringstream ss;
    ss << "success apply " << region->id() << " with block cache";
    output(ss.str());
}


/// Mock to pre-decode snapshot to DTFile(s) then apply

// Simulate a region pre-handle snapshot data to DTFiles
//    ./storage-client.sh "DBGInvoke region_snapshot_pre_handle_file(database_name, table_name, region_id, start, end, schema_string, pk_name[, test-fields=1, cfs="write,default"])"
void MockRaftCommand::dbgFuncRegionSnapshotPreHandleDTFiles(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() < 7 || args.size() > 9)
        throw Exception("Args not matched, should be: database_name, table_name, region_id, start, end, schema_string, pk_name"
                        " [, test-fields, cfs=\"write,default\"]",
            ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    RegionID region_id = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    RegionID start_handle = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[3]).value);
    RegionID end_handle = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[4]).value);

    const String schema_str = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[5]).value);
    String handle_pk_name = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[6]).value);

    UInt64 test_fields = 1;
    if (args.size() > 7)
        test_fields = (UInt64)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[7]).value);
    std::unordered_set<ColumnFamilyType> cfs;
    {
        String cfs_str = "write,default";
        if (args.size() > 8)
            cfs_str = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[8]).value);
        if (cfs_str.find("write") != std::string::npos)
            cfs.insert(ColumnFamilyType::Write);
        if (cfs_str.find("default") != std::string::npos)
            cfs.insert(ColumnFamilyType::Default);
    }

    // Parse a TableInfo from `schema_str` to generate data with this schema
    TiDB::TableInfoPtr mocked_table_info;
    {
        ASTPtr columns_ast;
        ParserColumnDeclarationList schema_parser;
        Tokens tokens(schema_str.data(), schema_str.data() + schema_str.length());
        TokenIterator pos(tokens);
        Expected expected;
        if (!schema_parser.parse(pos, columns_ast, expected))
            throw Exception("Invalid TiDB table schema", ErrorCodes::LOGICAL_ERROR);
        ColumnsDescription columns
            = InterpreterCreateQuery::getColumnsDescription(typeid_cast<const ASTExpressionList &>(*columns_ast), context);
        mocked_table_info = MockTiDB::parseColumns(table_name, columns, handle_pk_name, "dt");
    }

    MockTiDB::TablePtr table = MockTiDB::instance().getTableByName(database_name, table_name);
    const auto & table_info = RegionBench::getTableInfo(context, database_name, table_name);
    if (table_info.is_common_handle)
        throw Exception("Mocking pre handle SST files to DTFiles to a common handle table is not supported", ErrorCodes::LOGICAL_ERROR);

    // Mock SST data for handle [start, end)
    const auto region_name = "__snap_snap_" + std::to_string(region_id);
    GenMockSSTData(*mocked_table_info, table->id(), region_name, start_handle, end_handle, test_fields, cfs);

    auto & tmt = context.getTMTContext();
    auto & kvstore = tmt.getKVStore();
    auto old_region = kvstore->getRegion(region_id);

    // We may call this function mutiple time to mock some situation, try to reuse the region in `GLOBAL_REGION_MAP`
    // so that we can collect uncommitted data.
    UInt64 index = MockTiKV::instance().getRaftIndex(region_id) + 1;
    RegionPtr new_region = RegionBench::createRegion(table->id(), region_id, start_handle, end_handle + 10000, index);

    // Register some mock SST reading methods so that we can decode data in `MockSSTReader::MockSSTData`
    RegionMockTest mock_test(kvstore, new_region);

    std::vector<SSTView> sst_views;
    {
        if (cfs.count(ColumnFamilyType::Write) > 0)
            sst_views.push_back(SSTView{
                ColumnFamilyType::Write,
                BaseBuffView{region_name.data(), region_name.length()},
            });
        if (cfs.count(ColumnFamilyType::Default) > 0)
            sst_views.push_back(SSTView{
                ColumnFamilyType::Default,
                BaseBuffView{region_name.data(), region_name.length()},
            });
    }

    // set block size so that we can test for schema-sync while decoding dt files
    FailPointHelper::enableFailPoint(FailPoints::force_set_sst_to_dtfile_block_size);
    FailPointHelper::enableFailPoint(FailPoints::force_set_safepoint_when_decode_block);

    auto ingest_ids = kvstore->preHandleSnapshotToFiles(
        new_region, SSTViewVec{sst_views.data(), sst_views.size()}, index, MockTiKV::instance().getRaftTerm(region_id), tmt);
    GLOBAL_REGION_MAP.insertRegionSnap(region_name, {new_region, ingest_ids});

    FailPointHelper::disableFailPoint(FailPoints::force_set_safepoint_when_decode_block);
    {
        std::stringstream ss;
        ss << "Generate " << ingest_ids.size() << " files for [region_id=" << region_id << "]";
        output(ss.str());
    }
}

// Simulate a region pre-handle snapshot data to DTFiles
//    ./storage-client.sh "DBGInvoke region_snapshot_pre_handle_file_with_handles(database_name, table_name, region_id, schema_string, pk_name, handle0, handle1, ..., handlek)"
void MockRaftCommand::dbgFuncRegionSnapshotPreHandleDTFilesWithHandles(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() < 6)
        throw Exception(
            "Args not matched, should be: database_name, table_name, region_id, schema_string, pk_name, handle0, handle1, ..., handlek",
            ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    RegionID region_id = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value);

    const String schema_str = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[3]).value);
    String handle_pk_name = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[4]).value);

    std::vector<UInt64> handles;
    for (size_t i = 5; i < args.size(); ++i)
    {
        handles.push_back(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[i]).value));
    }

    UInt64 test_fields = 1;
    std::unordered_set<ColumnFamilyType> cfs;
    cfs.insert(ColumnFamilyType::Write);
    cfs.insert(ColumnFamilyType::Default);

    // Parse a TableInfo from `schema_str` to generate data with this schema
    TiDB::TableInfoPtr mocked_table_info;
    {
        ASTPtr columns_ast;
        ParserColumnDeclarationList schema_parser;
        Tokens tokens(schema_str.data(), schema_str.data() + schema_str.length());
        TokenIterator pos(tokens);
        Expected expected;
        if (!schema_parser.parse(pos, columns_ast, expected))
            throw Exception("Invalid TiDB table schema", ErrorCodes::LOGICAL_ERROR);
        ColumnsDescription columns
            = InterpreterCreateQuery::getColumnsDescription(typeid_cast<const ASTExpressionList &>(*columns_ast), context);
        mocked_table_info = MockTiDB::parseColumns(table_name, columns, handle_pk_name, "dt");
    }

    MockTiDB::TablePtr table = MockTiDB::instance().getTableByName(database_name, table_name);
    const auto & table_info = RegionBench::getTableInfo(context, database_name, table_name);
    if (table_info.is_common_handle)
        throw Exception("Mocking pre handle SST files to DTFiles to a common handle table is not supported", ErrorCodes::LOGICAL_ERROR);

    // Mock SST data for handle [start, end)
    const auto region_name = "__snap_snap_" + std::to_string(region_id);
    GenMockSSTDataByHandles(*mocked_table_info, table->id(), region_name, handles, test_fields, cfs);

    auto & tmt = context.getTMTContext();
    auto & kvstore = tmt.getKVStore();
    auto old_region = kvstore->getRegion(region_id);

    // We may call this function mutiple time to mock some situation, try to reuse the region in `GLOBAL_REGION_MAP`
    // so that we can collect uncommitted data.
    UInt64 index = MockTiKV::instance().getRaftIndex(region_id) + 1;
    UInt64 region_start_handle = handles[0];
    UInt64 region_end_handle = handles.back() + 10000;
    RegionPtr new_region = RegionBench::createRegion(table->id(), region_id, region_start_handle, region_end_handle, index);

    // Register some mock SST reading methods so that we can decode data in `MockSSTReader::MockSSTData`
    RegionMockTest mock_test(kvstore, new_region);

    std::vector<SSTView> sst_views;
    {
        if (cfs.count(ColumnFamilyType::Write) > 0)
            sst_views.push_back(SSTView{
                ColumnFamilyType::Write,
                BaseBuffView{region_name.data(), region_name.length()},
            });
        if (cfs.count(ColumnFamilyType::Default) > 0)
            sst_views.push_back(SSTView{
                ColumnFamilyType::Default,
                BaseBuffView{region_name.data(), region_name.length()},
            });
    }

    // set block size so that we can test for schema-sync while decoding dt files
    FailPointHelper::enableFailPoint(FailPoints::force_set_sst_to_dtfile_block_size);
    FailPointHelper::enableFailPoint(FailPoints::force_set_safepoint_when_decode_block);

    auto ingest_ids = kvstore->preHandleSnapshotToFiles(
        new_region, SSTViewVec{sst_views.data(), sst_views.size()}, index, MockTiKV::instance().getRaftTerm(region_id), tmt);
    GLOBAL_REGION_MAP.insertRegionSnap(region_name, {new_region, ingest_ids});

    FailPointHelper::disableFailPoint(FailPoints::force_set_safepoint_when_decode_block);
    {
        std::stringstream ss;
        ss << "Generate " << ingest_ids.size() << " files for [region_id=" << region_id << "]";
        output(ss.str());
    }
}

// Apply snapshot for a region. (apply a pre-handle snapshot)
//   ./storages-client.sh "DBGInvoke region_snapshot_apply_file(region_id)"
void MockRaftCommand::dbgFuncRegionSnapshotApplyDTFiles(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 1)
        throw Exception("Args not matched, should be: region-id", ErrorCodes::BAD_ARGUMENTS);

    RegionID region_id = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args.front()).value);
    const auto region_name = "__snap_snap_" + std::to_string(region_id);
    auto [new_region, ingest_ids] = GLOBAL_REGION_MAP.popRegionSnap(region_name);
    auto & tmt = context.getTMTContext();
    context.getTMTContext().getKVStore()->checkAndApplySnapshot<RegionPtrWithSnapshotFiles>(
        RegionPtrWithSnapshotFiles{new_region, std::move(ingest_ids)}, tmt);

    std::stringstream ss;
    ss << "success apply region " << new_region->id() << " with dt files";
    output(ss.str());
}

} // namespace DB
