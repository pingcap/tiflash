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

#include <Common/FailPoint.h>
#include <Common/FmtUtils.h>
#include <Common/setThreadName.h>
#include <Common/typeid_cast.h>
#include <Debug/MockKVStore/MockSSTReader.h>
#include <Debug/MockKVStore/MockTiKV.h>
#include <Debug/MockTiDB.h>
#include <Debug/dbgKVStore/dbgFuncMockRaftCommand.h>
#include <Debug/dbgKVStore/dbgFuncRegion.h>
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
#include <Storages/DeltaMerge/ExternalDTFileInfo.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/IManageableStorage.h>
#include <Storages/KVStore/Decode/PartitionStreams.h>
#include <Storages/KVStore/Decode/RegionBlockReader.h>
#include <Storages/KVStore/Decode/TiKVRange.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/tests/region_helper.h>
#include <TiDB/Schema/TiDBSchemaManager.h>
#include <fmt/core.h>

#include "Common/Exception.h"

namespace DB
{
namespace FailPoints
{
extern const char force_set_sst_to_dtfile_block_size[];
extern const char force_set_safepoint_when_decode_block[];
extern const char pause_before_apply_raft_snapshot[];
} // namespace FailPoints

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int UNKNOWN_TABLE;
extern const int ILLFORMAT_RAFT_ROW;
extern const int TABLE_IS_DROPPED;
} // namespace ErrorCodes

// DBGInvoke region_snapshot_data(database_name, table_name, region_id, start, end, handle_id1, tso1, del1, r1_c1, r1_c2, ..., handle_id2, tso2, del2, r2_c1, r2_c2, ... )
RegionPtr GenDbgRegionSnapshotWithData(Context & context, const ASTs & args)
{
    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    auto region_id = static_cast<RegionID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value));
    TableID table_id = RegionBench::getTableID(context, database_name, table_name, "");
    MockTiDB::TablePtr table = MockTiDB::instance().getTableByName(database_name, table_name);
    auto & table_info = table->table_info;
    bool is_common_handle = table_info.is_common_handle;
    size_t handle_column_size = is_common_handle ? table_info.getPrimaryIndexInfo().idx_cols.size() : 1;
    RegionPtr region;

    if (!is_common_handle)
    {
        auto start = static_cast<HandleID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[3]).value));
        auto end = static_cast<HandleID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[4]).value));
        region = RegionBench::createRegion(table_id, region_id, start, end);
    }
    else
    {
        // Get start key and end key form multiple column if it is clustered_index.
        std::vector<Field> start_keys;
        std::vector<Field> end_keys;
        const auto & pk_idx_cols = table_info.getPrimaryIndexInfo().idx_cols;
        for (size_t i = 0; i < handle_column_size; i++)
        {
            auto & column_info = table_info.columns[pk_idx_cols[i].offset];
            auto start_field
                = RegionBench::convertField(column_info, typeid_cast<const ASTLiteral &>(*args[3 + i]).value);
            TiDB::DatumBumpy start_datum = TiDB::DatumBumpy(start_field, column_info.tp);
            start_keys.emplace_back(start_datum.field());
            auto end_field = RegionBench::convertField(
                column_info,
                typeid_cast<const ASTLiteral &>(*args[3 + handle_column_size + i]).value);
            TiDB::DatumBumpy end_datum = TiDB::DatumBumpy(end_field, column_info.tp);
            end_keys.emplace_back(end_datum.field());
        }
        region = RegionBench::createRegion(table_info, region_id, start_keys, end_keys);
    }

    auto args_begin = args.begin() + 3 + handle_column_size * 2;
    auto args_end = args.end();

    const size_t len = table->table_info.columns.size() + 3;

    RUNTIME_CHECK_MSG((((args_end - args_begin) % len) == 0), "Number of insert values and columns do not match.");

    // Parse row values
    for (auto it = args_begin; it != args_end; it += len)
    {
        HandleID handle_id = is_common_handle
            ? 0
            : static_cast<HandleID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*it[0]).value));
        auto tso = static_cast<Timestamp>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*it[1]).value));
        auto del = static_cast<UInt8>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*it[2]).value));
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
                const auto & pk_index = table_info.getPrimaryIndexInfo();
                for (const auto & idx_col : pk_index.idx_cols)
                {
                    auto & column_info = table_info.columns[idx_col.offset];
                    auto start_field = RegionBench::convertField(column_info, fields[idx_col.offset]);
                    TiDB::DatumBumpy start_datum = TiDB::DatumBumpy(start_field, column_info.tp);
                    keys.emplace_back(start_datum.field());
                }
                key = RecordKVFormat::genKey(table_info, keys);
            }
            else
                key = RecordKVFormat::genKey(table_id, handle_id);
            WriteBufferFromOwnString ss;
            RegionBench::encodeRow(table->table_info, fields, ss);
            TiKVValue value(ss.releaseStr());
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
    tmt.getKVStore()->checkAndApplyPreHandledSnapshot<RegionPtrWithSnapshotFiles>(region, tmt);
    // Decode the committed rows into Block and flush to the IStorage layer
    if (auto region_applied = tmt.getKVStore()->getRegion(region_id); region_applied)
    {
        tmt.getRegionTable().tryWriteBlockByRegion(region_applied);
    }
    output(fmt::format("put region #{}, range{} to table #{} with {} records", region_id, range_string, table_id, cnt));
}

// Mock to apply an empty snapshot for region
// DBGInvoke region_snapshot(region-id, start-key, end-key, database-name, table-name[, partition-id])
void MockRaftCommand::dbgFuncRegionSnapshot(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    auto region_id = static_cast<RegionID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value));
    bool has_partition_id = false;
    size_t args_size = args.size();
    if (dynamic_cast<ASTLiteral *>(args[args_size - 1].get()) != nullptr)
        has_partition_id = true;
    const String & partition_id = has_partition_id
        ? std::to_string(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[args_size - 1]).value))
        : "";
    size_t offset = has_partition_id ? 1 : 0;
    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[args_size - 2 - offset]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[args_size - 1 - offset]).name;
    TableID table_id = RegionBench::getTableID(context, database_name, table_name, partition_id);
    const auto & table_info = RegionBench::getTableInfo(context, database_name, table_name);

    size_t handle_column_size = table_info.is_common_handle ? table_info.getPrimaryIndexInfo().idx_cols.size() : 1;
    if (args_size < 3 + 2 * handle_column_size || args_size > 3 + 2 * handle_column_size + 1)
        throw Exception(
            "Args not matched, should be: region-id, start-key, end-key, database-name, table-name[, partition-name]",
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
            const auto & column_info = table_info.columns[table_info.getPrimaryIndexInfo().idx_cols[i].offset];
            auto start_field
                = RegionBench::convertField(column_info, typeid_cast<const ASTLiteral &>(*args[1 + i]).value);
            TiDB::DatumBumpy start_datum = TiDB::DatumBumpy(start_field, column_info.tp);
            start_keys.emplace_back(start_datum.field());
            auto end_field = RegionBench::convertField(
                column_info,
                typeid_cast<const ASTLiteral &>(*args[1 + handle_column_size + i]).value);
            TiDB::DatumBumpy end_datum = TiDB::DatumBumpy(end_field, column_info.tp);
            end_keys.emplace_back(end_datum.field());
        }
        start_key = RecordKVFormat::genKey(table_info, start_keys);
        end_key = RecordKVFormat::genKey(table_info, end_keys);
    }
    else
    {
        auto start = static_cast<HandleID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[1]).value));
        auto end = static_cast<HandleID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value));
        start_key = RecordKVFormat::genKey(table_id, start);
        end_key = RecordKVFormat::genKey(table_id, end);
    }
    region_info.set_start_key(start_key.toString());
    region_info.set_end_key(end_key.toString());
    *region_info.add_peers() = tests::createPeer(1, true);
    *region_info.add_peers() = tests::createPeer(2, true);
    auto peer_id = 1;
    auto start_decoded_key = RecordKVFormat::decodeTiKVKey(start_key);
    auto end_decoded_key = RecordKVFormat::decodeTiKVKey(end_key);

    // Mock to apply an empty snapshot for region[region-id]
    RegionBench::handleApplySnapshot(
        *tmt.getKVStore(),
        std::move(region_info),
        peer_id,
        SSTViewVec{nullptr, 0},
        MockTiKV::instance().getRaftIndex(region_id),
        RAFT_INIT_LOG_TERM,
        std::nullopt,
        tmt);

    output(fmt::format(
        "put region #{}, range[{}, {}) to table #{} with raft commands",
        region_id,
        RecordKVFormat::DecodedTiKVKeyToDebugString<true>(start_decoded_key),
        RecordKVFormat::DecodedTiKVKeyToDebugString<false>(end_decoded_key),
        table_id));
}

class RegionMockTest final
{
public:
    RegionMockTest(KVStore * kvstore_, RegionPtr region_);
    ~RegionMockTest();

    DISALLOW_COPY_AND_MOVE(RegionMockTest);

private:
    TiFlashRaftProxyHelper mock_proxy_helper{};
    const TiFlashRaftProxyHelper * ori_proxy_helper{};
    KVStore * kvstore;
    RegionPtr region;
};

RegionMockTest::RegionMockTest(KVStore * kvstore_, RegionPtr region_)
    : kvstore(kvstore_)
    , region(region_)
{
    if (kvstore->getProxyHelper())
    {
        ori_proxy_helper = kvstore->getProxyHelper();
        std::memcpy(&mock_proxy_helper, ori_proxy_helper, sizeof(mock_proxy_helper));
    }
    mock_proxy_helper.sst_reader_interfaces = make_mock_sst_reader_interface();
    kvstore->proxy_helper = &mock_proxy_helper;
    region->proxy_helper = &mock_proxy_helper;
}
RegionMockTest::~RegionMockTest()
{
    kvstore->proxy_helper = ori_proxy_helper;
    region->proxy_helper = ori_proxy_helper;
}

void GenMockSSTData(
    const TiDB::TableInfo & table_info,
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
            WriteBufferFromOwnString ss;
            RegionBench::encodeRow(table_info, fields, ss);
            TiKVValue prewrite_value(ss.releaseStr());

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
        MockSSTReader::getMockSSTData()[MockSSTReader::Key{store_key, ColumnFamilyType::Write}]
            = std::move(write_kv_list);
    if (cfs.count(ColumnFamilyType::Default) > 0)
        MockSSTReader::getMockSSTData()[MockSSTReader::Key{store_key, ColumnFamilyType::Default}]
            = std::move(default_kv_list);
}

// TODO: make it a more generic testing function
void GenMockSSTDataByHandles(
    const TiDB::TableInfo & table_info,
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
            WriteBufferFromOwnString ss;
            RegionBench::encodeRow(table_info, fields, ss);
            TiKVValue prewrite_value(ss.releaseStr());

            UInt64 prewrite_ts = 100000 + num_rows - index; // make it to be timestamp desc in SSTFiles
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
        MockSSTReader::getMockSSTData()[MockSSTReader::Key{store_key, ColumnFamilyType::Write}]
            = std::move(write_kv_list);
    if (cfs.count(ColumnFamilyType::Default) > 0)
        MockSSTReader::getMockSSTData()[MockSSTReader::Key{store_key, ColumnFamilyType::Default}]
            = std::move(default_kv_list);
}

// Simulate a region IngestSST raft command
// DBGInvoke region_ingest_sst(database_name, table_name, region_id, start, end)
void MockRaftCommand::dbgFuncIngestSST(Context & context, const ASTs & args, DBGInvoker::Printer)
{
    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    auto region_id = static_cast<RegionID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value));
    auto start_handle = static_cast<RegionID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[3]).value));
    auto end_handle = static_cast<RegionID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[4]).value));
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

    // Register some mock SST reading methods so that we can decode data in `MockSSTReader::MockSSTData`
    RegionMockTest mock_test(kvstore.get(), region);

    {
        // Mocking ingest a SST for column family "Write"
        std::vector<SSTView> sst_views;
        sst_views.push_back(SSTView{
            ColumnFamilyType::Write,
            BaseBuffView{region_id_str.data(), region_id_str.length()},
        });
        kvstore->handleIngestSST(
            region_id,
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
        kvstore->handleIngestSST(
            region_id,
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
    // using BlockVal = std::pair<RegionPtr, RegionPtrWithBlock::CachePtr>;
    // std::unordered_map<Key, BlockVal> regions_block;
    using SnapPath = std::pair<RegionPtr, std::vector<DM::ExternalDTFileInfo>>;
    std::unordered_map<Key, SnapPath> regions_snap_files;
    std::mutex mutex;

#if 0
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
#endif

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

#if 0
/// Mock to pre-decode snapshot to block then apply

/// Pre-decode region data into block cache and remove committed data from `region`
RegionPtrWithBlock::CachePtr GenRegionPreDecodeBlockData(const RegionPtr & region, Context & context)
{
    auto keyspace_id = region->getKeyspaceID();
    const auto & tmt = context.getTMTContext();
    {
        Timestamp gc_safe_point = 0;
        if (auto pd_client = tmt.getPDClient(); !pd_client->isMock())
        {
            gc_safe_point = PDClientHelper::getGCSafePointWithRetry(
                pd_client,
                keyspace_id,
                false,
                context.getSettingsRef().safe_point_update_interval_seconds);
        }
        /**
         * In 5.0.1, feature `compaction filter` is enabled by default. Under such feature tikv will do gc in write & default cf individually.
         * If some rows were updated and add tiflash replica, tiflash store may receive region snapshot with unmatched data in write & default cf sst files.
         */
        region->tryCompactionFilter(gc_safe_point);
    }
    std::optional<RegionDataReadInfoList> data_list_read = std::nullopt;
    try
    {
        data_list_read = ReadRegionCommitCache(region, true);
        if (!data_list_read)
            return nullptr;
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::ILLFORMAT_RAFT_ROW)
        {
            // br or lighting may write illegal data into tikv, skip pre-decode and ingest sst later.
            LOG_WARNING(
                Logger::get(__PRETTY_FUNCTION__),
                "Got error while reading region committed cache: {}. Skip pre-decode and keep original cache.",
                e.displayText());
            // set data_list_read and let apply snapshot process use empty block
            data_list_read = RegionDataReadInfoList();
        }
        else
            throw;
    }

    TableID table_id = region->getMappedTableID();
    Int64 schema_version = DEFAULT_UNSPECIFIED_SCHEMA_VERSION;
    Block res_block;

    const auto atomic_decode = [&](bool force_decode) -> bool {
        Stopwatch watch;
        auto storage = tmt.getStorages().get(keyspace_id, table_id);
        if (storage == nullptr || storage->isTombstone())
        {
            if (!force_decode) // Need to update.
                return false;
            if (storage == nullptr) // Table must have just been GC-ed.
                return true;
        }

        /// Get a structure read lock throughout decode, during which schema must not change.
        TableStructureLockHolder lock;
        try
        {
            lock = storage->lockStructureForShare(getThreadNameAndID());
        }
        catch (DB::Exception & e)
        {
            // If the storage is physical dropped (but not removed from `ManagedStorages`) when we want to decode snapshot, consider the decode done.
            if (e.code() == ErrorCodes::TABLE_IS_DROPPED)
                return true;
            else
                throw;
        }

        DecodingStorageSchemaSnapshotConstPtr decoding_schema_snapshot
            = storage->getSchemaSnapshotAndBlockForDecoding(lock, false, true).first;
        res_block = createBlockSortByColumnID(decoding_schema_snapshot);
        auto reader = RegionBlockReader(decoding_schema_snapshot);
        return reader.read(res_block, *data_list_read, force_decode);
    };

    /// In TiFlash, the actions between applying raft log and schema changes are not strictly synchronized.
    /// There could be a chance that some raft logs come after a table gets tombstoned. Take care of it when
    /// decoding data. Check the test case for more details.
    FAIL_POINT_PAUSE(FailPoints::pause_before_apply_raft_snapshot);

    if (!atomic_decode(false))
    {
        tmt.getSchemaSyncerManager()->syncSchemas(context, keyspace_id);

        if (!atomic_decode(true))
            throw Exception(
                "Pre-decode " + region->toString() + " cache to table " + std::to_string(table_id) + " block failed",
                ErrorCodes::LOGICAL_ERROR);
    }

    RemoveRegionCommitCache(region, *data_list_read);

    return std::make_unique<RegionPreDecodeBlockData>(std::move(res_block), schema_version, std::move(*data_list_read));
}
#endif

void MockRaftCommand::dbgFuncRegionSnapshotPreHandleBlock(
    Context & context,
    const ASTs & args,
    DBGInvoker::Printer output)
{
    FmtBuffer fmt_buf;
    auto region = GenDbgRegionSnapshotWithData(context, args);
    const auto region_name = "__snap_" + std::to_string(region->id());
    fmt_buf.fmtAppend("pre-handle {} snapshot with data {}", region->toString(false), region->dataInfo());
#if 0
    auto & tmt = context.getTMTContext();
    auto block_cache = GenRegionPreDecodeBlockData(region, tmt.getContext());
    fmt_buf.append(", pre-decode block cache");
    fmt_buf.fmtAppend(
        " {{ schema_version: ?, data_list size: {}, block row: {} col: {} bytes: {} }}",
        block_cache->data_list_read.size(),
        block_cache->block.rows(),
        block_cache->block.columns(),
        block_cache->block.bytes());
    GLOBAL_REGION_MAP.insertRegionCache(region_name, {region, std::move(block_cache)});
#endif
    output(fmt_buf.toString());
}

void MockRaftCommand::dbgFuncRegionSnapshotApplyBlock(
    Context & /*context*/,
    const ASTs & args,
    DBGInvoker::Printer output)
{
    if (args.size() != 1)
    {
        throw Exception("Args not matched, should be: region-id", ErrorCodes::BAD_ARGUMENTS);
    }

    auto region_id = static_cast<RegionID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args.front()).value));
#if 0
    auto [region, block_cache] = GLOBAL_REGION_MAP.popRegionCache("__snap_" + std::to_string(region_id));
    auto & tmt = context.getTMTContext();
    context.getTMTContext().getKVStore()->checkAndApplyPreHandledSnapshot<RegionPtrWithBlock>(
        {region, std::move(block_cache)},
        tmt);
#endif

    output(fmt::format("success apply {} with block cache", region_id));
}


/// Mock to pre-decode snapshot to DTFile(s) then apply

// Simulate a region pre-handle snapshot data to DTFiles
//    ./storage-client.sh "DBGInvoke region_snapshot_pre_handle_file(database_name, table_name, region_id, start, end, schema_string, pk_name[, test-fields=1, cfs="write,default"])"
void MockRaftCommand::dbgFuncRegionSnapshotPreHandleDTFiles(
    Context & context,
    const ASTs & args,
    DBGInvoker::Printer output)
{
    if (args.size() < 7 || args.size() > 9)
        throw Exception(
            "Args not matched, should be: database_name, table_name, region_id, start, end, schema_string, pk_name"
            " [, test-fields, cfs=\"write,default\"]",
            ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    auto region_id = static_cast<RegionID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value));
    auto start_handle = static_cast<RegionID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[3]).value));
    auto end_handle = static_cast<RegionID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[4]).value));

    const auto schema_str = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[5]).value);
    auto handle_pk_name = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[6]).value);

    UInt64 test_fields = 1;
    if (args.size() > 7)
        test_fields = static_cast<UInt64>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[7]).value));
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
        ColumnsDescription columns = InterpreterCreateQuery::getColumnsDescription(
            typeid_cast<const ASTExpressionList &>(*columns_ast),
            context);
        mocked_table_info = MockTiDB::parseColumns(table_name, columns, handle_pk_name);
    }

    MockTiDB::TablePtr table = MockTiDB::instance().getTableByName(database_name, table_name);
    const auto & table_info = RegionBench::getTableInfo(context, database_name, table_name);
    if (table_info.is_common_handle)
        throw Exception(
            "Mocking pre handle SST files to DTFiles to a common handle table is not supported",
            ErrorCodes::LOGICAL_ERROR);

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
    RegionMockTest mock_test(kvstore.get(), new_region);

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
    FailPointHelper::enableFailPoint(FailPoints::force_set_sst_to_dtfile_block_size, static_cast<size_t>(3));
    FailPointHelper::enableFailPoint(FailPoints::force_set_safepoint_when_decode_block);

    auto prehandle_result = kvstore->preHandleSnapshotToFiles(
        new_region,
        SSTViewVec{sst_views.data(), sst_views.size()},
        index,
        MockTiKV::instance().getRaftTerm(region_id),
        std::nullopt,
        tmt);
    GLOBAL_REGION_MAP.insertRegionSnap(region_name, {new_region, prehandle_result.ingest_ids});

    FailPointHelper::disableFailPoint(FailPoints::force_set_safepoint_when_decode_block);
    {
        output(fmt::format("Generate {} files for [region_id={}]", prehandle_result.ingest_ids.size(), region_id));
    }
}

// Simulate a region pre-handle snapshot data to DTFiles
//    ./storage-client.sh "DBGInvoke region_snapshot_pre_handle_file_with_handles(database_name, table_name, region_id, schema_string, pk_name, handle0, handle1, ..., handlek)"
void MockRaftCommand::dbgFuncRegionSnapshotPreHandleDTFilesWithHandles(
    Context & context,
    const ASTs & args,
    DBGInvoker::Printer output)
{
    if (args.size() < 6)
        throw Exception(
            "Args not matched, should be: database_name, table_name, region_id, schema_string, pk_name, handle0, "
            "handle1, ..., handlek",
            ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
    auto region_id = static_cast<RegionID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[2]).value));

    const auto schema_str = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[3]).value);
    auto handle_pk_name = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[4]).value);

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
        ColumnsDescription columns = InterpreterCreateQuery::getColumnsDescription(
            typeid_cast<const ASTExpressionList &>(*columns_ast),
            context);
        mocked_table_info = MockTiDB::parseColumns(table_name, columns, handle_pk_name);
    }

    MockTiDB::TablePtr table = MockTiDB::instance().getTableByName(database_name, table_name);
    const auto & table_info = RegionBench::getTableInfo(context, database_name, table_name);
    if (table_info.is_common_handle)
        throw Exception(
            "Mocking pre handle SST files to DTFiles to a common handle table is not supported",
            ErrorCodes::LOGICAL_ERROR);

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
    RegionPtr new_region
        = RegionBench::createRegion(table->id(), region_id, region_start_handle, region_end_handle, index);

    // Register some mock SST reading methods so that we can decode data in `MockSSTReader::MockSSTData`
    RegionMockTest mock_test(kvstore.get(), new_region);

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
    FailPointHelper::enableFailPoint(FailPoints::force_set_sst_to_dtfile_block_size, static_cast<size_t>(3));
    FailPointHelper::enableFailPoint(FailPoints::force_set_safepoint_when_decode_block);

    auto prehandle_result = kvstore->preHandleSnapshotToFiles(
        new_region,
        SSTViewVec{sst_views.data(), sst_views.size()},
        index,
        MockTiKV::instance().getRaftTerm(region_id),
        std::nullopt,
        tmt);
    GLOBAL_REGION_MAP.insertRegionSnap(region_name, {new_region, prehandle_result.ingest_ids});

    FailPointHelper::disableFailPoint(FailPoints::force_set_safepoint_when_decode_block);
    {
        output(fmt::format("Generate {} files for [region_id={}]", prehandle_result.ingest_ids.size(), region_id));
    }
}

// Apply snapshot for a region. (apply a pre-handle snapshot)
//   ./storages-client.sh "DBGInvoke region_snapshot_apply_file(region_id)"
void MockRaftCommand::dbgFuncRegionSnapshotApplyDTFiles(
    Context & context,
    const ASTs & args,
    DBGInvoker::Printer output)
{
    if (args.size() != 1)
        throw Exception("Args not matched, should be: region-id", ErrorCodes::BAD_ARGUMENTS);

    auto region_id = static_cast<RegionID>(safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args.front()).value));
    const auto region_name = "__snap_snap_" + std::to_string(region_id);
    auto [new_region, external_files] = GLOBAL_REGION_MAP.popRegionSnap(region_name);
    auto & tmt = context.getTMTContext();
    context.getTMTContext().getKVStore()->checkAndApplyPreHandledSnapshot<RegionPtrWithSnapshotFiles>(
        RegionPtrWithSnapshotFiles{new_region, std::move(external_files)},
        tmt);

    output(fmt::format("success apply region {} with dt files", new_region->id()));
}

} // namespace DB
