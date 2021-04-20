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
extern const char force_set_prehandle_dtfile_block_size[];
}

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
    HandleID start = (HandleID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[3]).value);
    HandleID end = (HandleID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[4]).value);

    TableID table_id = RegionBench::getTableID(context, database_name, table_name, "");
    RegionPtr region = RegionBench::createRegion(table_id, region_id, start, end);

    auto args_begin = args.begin() + 5;
    auto args_end = args.end();

    MockTiDB::TablePtr table = MockTiDB::instance().getTableByName(database_name, table_name);
    const size_t len = table->table_info.columns.size() + 3;

    if ((args_end - args_begin) % len)
        throw Exception("Number of insert values and columns do not match.", ErrorCodes::LOGICAL_ERROR);

    // Parse row values
    for (auto it = args_begin; it != args_end; it += len)
    {
        HandleID handle_id = (HandleID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*it[0]).value);
        Timestamp tso = (Timestamp)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*it[1]).value);
        UInt8 del = (UInt8)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*it[2]).value);
        {
            std::vector<Field> fields;

            for (auto p = it + 3; p != it + len; ++p)
            {
                auto field = typeid_cast<const ASTLiteral *>((*p).get())->value;
                fields.emplace_back(field);
            }

            TiKVKey key = RecordKVFormat::genKey(table_id, handle_id);
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
    auto & rawkeys = region->getRange()->rawKeys();
    auto region_id = region->id();
    auto table_id = region->getMappedTableID();
    auto start = TiKVRange::getRangeHandle<true>(rawkeys.first, table_id).handle_id;
    auto end = TiKVRange::getRangeHandle<false>(rawkeys.second, table_id).handle_id;
    auto cnt = region->writeCFCount();

    // Mock to apply a snapshot with data in `region`
    auto & tmt = context.getTMTContext();
    context.getTMTContext().getKVStore()->checkAndApplySnapshot(region, tmt);
    std::stringstream ss;
    ss << "put region #" << region_id << ", range[" << start << ", " << end << ")"
       << " to table #" << table_id << " with " << cnt << " records";
    output(ss.str());
}

// Mock to apply an empty snapshot for region
// DBGInvoke region_snapshot(region-id, start-key, end-key, database-name, table-name[, partition-id])
void MockRaftCommand::dbgFuncRegionSnapshot(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() < 5 || args.size() > 6)
    {
        throw Exception("Args not matched, should be: region-id, start-key, end-key, database-name, table-name[, partition-id]",
            ErrorCodes::BAD_ARGUMENTS);
    }

    RegionID region_id = (RegionID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    HandleID start = (HandleID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[1]).value);
    HandleID end = (HandleID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[3]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[4]).name;
    const String & partition_id = args.size() == 6 ? typeid_cast<const ASTIdentifier &>(*args[5]).name : "";

    TableID table_id = RegionBench::getTableID(context, database_name, table_name, partition_id);

    TMTContext & tmt = context.getTMTContext();

    metapb::Region region_info;

    region_info.set_id(region_id);
    region_info.set_start_key(RecordKVFormat::genKey(table_id, start).getStr());
    region_info.set_end_key(RecordKVFormat::genKey(table_id, end).getStr());

    *region_info.add_peers() = createPeer(1, true);
    *region_info.add_peers() = createPeer(2, true);
    auto peer_id = 1;

    // Mock to apply an empty snapshot for region[region-id]
    tmt.getKVStore()->handleApplySnapshot(
        std::move(region_info), peer_id, SSTViewVec{nullptr, 0}, MockTiKV::instance().getRaftIndex(region_id), RAFT_INIT_LOG_TERM, tmt);

    std::stringstream ss;
    ss << "put region #" << region_id << ", range[" << start << ", " << end << ")"
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

        {
            TiKVKey key = RecordKVFormat::genKey(table_id, handle_id);
            std::stringstream ss;
            RegionBench::encodeRow(table_info, fields, ss);
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

    // Mock SST data for handle [star, end)
    auto region_id_str = std::to_string(region_id);
    GenMockSSTData(table->table_info, table->id(), region_id_str, start_handle, end_handle);

    auto & tmt = context.getTMTContext();
    auto & kvstore = tmt.getKVStore();
    auto region = kvstore->getRegion(region_id);

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
    context.getTMTContext().getKVStore()->checkAndApplySnapshot({region, std::move(block_cache)}, tmt);

    std::stringstream ss;
    ss << "success apply " << region->id() << " with block cache";
    output(ss.str());
}


} // namespace DB
