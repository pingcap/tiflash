// Copyright 2025 PingCAP, Inc.
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

#include <Common/Logger.h>
#include <Common/MemoryTracker.h>
#include <Common/PODArray.h>
#include <DataTypes/DataTypeString.h>
#include <Debug/MockKVStore/MockSSTGenerator.h>
#include <Debug/MockKVStore/MockUtils.h>
#include <Debug/MockTiDB.h>
#include <Debug/dbgTools.h>
#include <Poco/TemporaryFile.h>
#include <RaftStoreProxyFFI/ColumnFamily.h>
#include <Storages/KVStore/Decode/RegionDataRead.h>
#include <Storages/KVStore/MultiRaft/ApplySnapshot.h>
#include <Storages/KVStore/Read/LearnerRead.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/tests/region_kvstore_test.h>
#include <Storages/RegionQueryInfo.h>
#include <TiDB/Decode/RowCodec.h>
#include <TiDB/Schema/TiDBSchemaManager.h>
#include <common/config_common.h> // Included for `USE_JEMALLOC`

extern std::shared_ptr<MemoryTracker> root_of_kvstore_mem_trackers;
extern std::atomic<Int64> real_rss;


namespace DB::tests
{
using namespace DB::RecordKVFormat;

class RaftApplyMemoryDeathTest : public RegionKVStoreTest
{
protected:
    void SetUp() override
    {
        ::testing::FLAGS_gtest_death_test_style = "threadsafe";
        if (const char * previous = std::getenv("TIFLASH_TEMP_DIR"))
            previous_dir = previous;
        ASSERT_EQ(setenv("TIFLASH_TEMP_DIR", child_dir.path().c_str(), 1), 0);
    }

    void TearDown() override
    {
        if (previous_dir)
            setenv("TIFLASH_TEMP_DIR", previous_dir->c_str(), 1);
        else
            unsetenv("TIFLASH_TEMP_DIR");
    }

    void checkApply();

private:
    Poco::TemporaryFile child_dir{TiFlashTestEnv::getTemporaryPath()};
    std::optional<String> previous_dir;
};

TEST_F(RaftApplyMemoryDeathTest, FatalMessage)
{
    ASSERT_EXIT(
        {
            Poco::Logger::get("HandleWriteRaftCmd").setLevel(0);
            EngineStoreServerWrap server{};
            HandleWriteRaftCmd(&server, {}, {});
        },
        ::testing::ExitedWithCode(255),
        "Fatal Raft apply error in HandleWriteRaftCmd: .*server->tmt != nullptr");
}

TEST_F(RaftApplyMemoryDeathTest, SharedColumnRssLimit)
{
    ASSERT_EXIT(
        {
            RegionKVStoreTest::SetUp();
            checkApply();
            const auto * result = ::testing::UnitTest::GetInstance()->current_test_info()->result();
            for (int part = 0; part < result->total_part_count(); ++part)
                if (const auto & check = result->GetTestPartResult(part); check.failed())
                    std::cerr << check.file_name() << ':' << check.line_number() << " " << check.summary() << '\n';
            std::_Exit(HasFailure() ? 1 : 0);
        },
        ::testing::ExitedWithCode(0),
        "");
}

void RaftApplyMemoryDeathTest::checkApply()
{
    constexpr Int64 quota = 128 * 1024 * 1024;
    constexpr Int64 payload_size = 2 * 1024 * 1024;
    ASSERT_EQ(current_memory_tracker, nullptr);
    initStorageMemoryTracker(quota, 0);
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    auto & tmt = ctx.getTMTContext();
    initStorages();
    tmt.debugSetKVStore(kvstore);
    ctx.getSettingsRef().dt_segment_delta_cache_limit_size = quota;
    auto & tidb = MockTiDB::instance();
    const String database = "raft_apply_memory";
    tidb.newDataBase(database);
    ColumnsDescription columns;
    columns.ordinary.emplace_back("payload", std::make_shared<DataTypeString>());
    const auto table_id = tidb.newTable(database, "t", columns, tmt.getPDClient()->getTS(), "");
    tmt.getSchemaSyncerManager()->syncSchemas(ctx, NullspaceID);
    tmt.getSchemaSyncerManager()->syncTableSchema(ctx, NullspaceID, table_id);

    constexpr RegionID region_id = 1;
    proxy_instance->debugAddRegions(
        *kvstore,
        tmt,
        {region_id},
        {{genTableRecordStartKey(table_id), genTableRecordEndKey(table_id)}});
    auto region = kvstore->getRegion(region_id);
    auto key = genKey(table_id, 1, 111);
    const String payload(payload_size, 'a');
    WriteBufferFromOwnString buffer;
    encodeRowV1(*tidb.getTableInfoByID(table_id), {Field(payload)}, buffer);
    const auto encoded = buffer.releaseStr();
    const auto commit = encodeWriteCfValue(CFModifyFlag::PutFlag, 111).toString();
    auto [view, holder] = MockRaftStoreProxy::createWriteCmdsView(
        {key, key},
        {encoded, commit},
        {WriteCmdType::Put, WriteCmdType::Put},
        {ColumnFamilyType::Default, ColumnFamilyType::Write});
    kvstore->debugGetConfigMut().debugSetCompactLogConfig(0, 0, 0, 0);
    EngineStoreServerWrap server{.tmt = &tmt};
    auto index = region->appliedIndex();
    baseline_of_query_mem_tracker = 0;
    real_rss_file = 0;
    real_rss = quota + 1;
    auto check_limit = [&] {
        ASSERT_EQ(current_memory_tracker, nullptr);
        ASSERT_LT(sub_root_of_query_storage_task_mem_trackers->get() + payload_size, quota);
        EXPECT_THROW((PaddedPODArray<char>(payload_size)), TiFlashException);
    };
    auto write = [&] {
        ASSERT_EQ(HandleWriteRaftCmd(&server, view, {region_id, ++index, 6}), EngineStoreApplyRes::None);
        ASSERT_EQ(kvstore->getRegion(region_id)->appliedIndex(), index);
        EXPECT_GE(shared_column_data_mem_tracker->get(), payload_size);
        check_limit();
    };
    auto read_rows = [&](size_t expected) {
        real_rss = 0;
        SCOPE_EXIT({ real_rss = quota + 1; });
        auto storage = std::dynamic_pointer_cast<StorageDeltaMerge>(tmt.getStorages().get(NullspaceID, table_id));
        auto store = storage->getStore();
        auto streams = store->read(
            ctx,
            ctx.getSettingsRef(),
            store->getTableColumns(),
            {DM::RowKeyRange::newAll(false, 1)},
            1,
            std::numeric_limits<UInt64>::max(),
            nullptr,
            {},
            0,
            "raft_apply_memory");
        size_t rows = 0;
        for (auto & stream : streams)
        {
            stream->readPrefix();
            while (auto block = stream->read())
                for (size_t row = 0; row < block.rows(); ++row)
                {
                    EXPECT_TRUE((*block.getByName("payload").column)[row].safeGet<String>() == payload);
                    ++rows;
                }
            stream->readSuffix();
        }
        EXPECT_EQ(rows, expected);
    };

    write();
    const auto peak = shared_column_data_mem_tracker->getPeak();
    ASSERT_TRUE(TryFlushData(&server, region_id, 1, ++index, 6, 0, 0));
    EXPECT_EQ(region->appliedIndex(), index);
    EXPECT_GT(shared_column_data_mem_tracker->getPeak(), peak);
    check_limit();
    read_rows(1);

    write();
    raft_cmdpb::AdminRequest request;
    request.set_cmd_type(raft_cmdpb::PrepareFlashback);
    raft_cmdpb::AdminResponse response;
    response.set_cmd_type(request.cmd_type());
    const auto req = request.SerializeAsString();
    const auto resp = response.SerializeAsString();
    ASSERT_EQ(
        HandleAdminRaftCmd(&server, strIntoView(&req), strIntoView(&resp), {region_id, ++index, 6}),
        EngineStoreApplyRes::Persist);
    EXPECT_EQ(region->appliedIndex(), index);
    check_limit();

    MockSSTGenerator defaults(region_id, table_id, ColumnFamilyType::Default);
    MockSSTGenerator writes(region_id, table_id, ColumnFamilyType::Write);
    const auto ingest_key = genKey(table_id, 2, 111).toString();
    defaults.insert_raw(ingest_key, encoded);
    writes.insert_raw(ingest_key, commit);
    defaults.finish_file();
    writes.finish_file();
    defaults.freeze();
    writes.freeze();
    const std::array ssts{defaults.ssts().front(), writes.ssts().front()};
    const SSTViewVec snaps{ssts.data(), ssts.size()};
    write();
    ASSERT_EQ(HandleIngestSST(&server, snaps, {region_id, ++index, 6}), EngineStoreApplyRes::Persist);
    EXPECT_EQ(region->appliedIndex(), index);
    check_limit();
    read_rows(2);

    write();
    const auto meta = region->cloneMetaRegion();
    const auto peer = region->dumpRegionMetaSnapshot().peer.id();
    const auto snapshot_region
        = kvstore->genRegionPtr(region->cloneMetaRegion(), peer, ++index, 6, tmt.getRegionTable());
    try
    {
        kvstore->preHandleSnapshotToFiles(snapshot_region, snaps, index, 6, std::nullopt, tmt);
        ADD_FAILURE() << "Snapshot prehandle accepted an over-quota allocation";
    }
    catch (const Exception & error)
    {
        EXPECT_NE(error.message().find("RSS(Resident Set Size) much larger than limit"), String::npos)
            << error.message();
    }
    real_rss = 0;
    auto proxy_region = proxy_instance->getRegion(region_id);
    auto state = proxy_region->getState();
    state.set_state(raft_serverpb::PeerState::Applying);
    proxy_region->setState(std::move(state));
    const auto region_data = meta.SerializeAsString();
    const auto snapshot = PreHandleSnapshot(&server, strIntoView(&region_data), peer, snaps, index, 6);
    real_rss = quota + 1;
    ApplyPreHandledSnapshot(&server, snapshot.ptr, snapshot.type);
    GcRawCppPtr(snapshot.ptr, snapshot.type);
    EXPECT_EQ(kvstore->getRegion(region_id)->appliedIndex(), index);
    check_limit();
    read_rows(1);
    write();
    HandleDestroy(&server, region_id);
    EXPECT_EQ(kvstore->getRegion(region_id), nullptr);
    check_limit();
}

TEST_F(RegionKVStoreTest, MemoryTracker1)
try
{
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    auto & tmt = ctx.getTMTContext();
    initStorages();
    KVStore & kvs = getKVS();
    ctx.getTMTContext().debugSetKVStore(kvstore);
    auto table_id = proxy_instance->bootstrapTable(ctx, kvs, ctx.getTMTContext());
    MockRaftStoreProxy::FailCond cond;
    const auto decoded_lock_size = sizeof(DecodedLockCFValue) + sizeof(DecodedLockCFValue::Inner);

    auto & region_table = ctx.getTMTContext().getRegionTable();
    auto getStartEnd = [&](RegionID region_id) {
        return std::make_pair(
            RecordKVFormat::genKey(table_id, region_id),
            RecordKVFormat::genKey(table_id, region_id + 99));
    };
    auto pickKey = [&](RegionID region_id, UInt64 number) {
        return RecordKVFormat::genKey(table_id, region_id + number, 111);
    };
    auto pickWriteDefault = [&](RegionID, UInt64) {
        return proxy_instance->generateTiKVKeyValue(111, 999);
    };
    auto pickLock = [&](RegionID region_id, UInt64 number) {
        return RecordKVFormat::encodeLockCfValue(RecordKVFormat::CFModifyFlag::PutFlag, "PK", region_id + number, 999)
            .toString();
    };
    real_rss.store(1);
    kvs.setKVStoreMemoryLimit(1);
    {
        // default
        RegionID region_id = 4100;
        auto [start, end] = getStartEnd(region_id);
        auto str_key = pickKey(region_id, 1);
        auto [str_val_write, str_val_default] = pickWriteDefault(region_id, 1);
        auto str_lock_value = pickLock(region_id, 1);
        proxy_instance->debugAddRegions(kvs, ctx.getTMTContext(), {region_id}, {{start, end}});
        auto kvr1 = kvs.getRegion(region_id);
        auto [index, term]
            = proxy_instance
                  ->rawWrite(region_id, {str_key}, {str_val_default}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
        UNUSED(term);
        proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), str_key.dataSize() + str_val_default.size());
        ASSERT_EQ(kvr1->dataSize(), root_of_kvstore_mem_trackers->get());
        ASSERT_EQ(kvr1->dataSize(), kvr1->getData().totalSize());
        ASSERT_EQ(kvr1->getData().totalSize(), region_table.getTableRegionSize(NullspaceID, table_id));
        ASSERT_EQ(kvs.debug_memory_limit_warning_count, 1);
    }
    {
        // lock
        root_of_kvstore_mem_trackers->reset();
        RegionID region_id = 4200;
        auto [start, end] = getStartEnd(region_id);
        auto str_key = pickKey(region_id, 1);
        auto [str_val_write, str_val_default] = pickWriteDefault(region_id, 1);
        auto str_lock_value = pickLock(region_id, 1);
        proxy_instance->debugAddRegions(kvs, ctx.getTMTContext(), {region_id}, {{start, end}});
        auto kvr1 = kvs.getRegion(4100);
        auto kvr2 = kvs.getRegion(region_id);
        auto [index, term]
            = proxy_instance
                  ->rawWrite(region_id, {str_key}, {str_lock_value}, {WriteCmdType::Put}, {ColumnFamilyType::Lock});
        UNUSED(term);
        proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), str_key.dataSize() + str_lock_value.size());
        ASSERT_EQ(kvr2->dataSize(), str_key.dataSize() + str_lock_value.size());
        ASSERT_EQ(kvr2->dataSize() + decoded_lock_size, kvr2->getData().totalSize());
        ASSERT_EQ(
            kvr2->getData().totalSize() + kvr1->getData().totalSize(),
            region_table.getTableRegionSize(NullspaceID, table_id));
        ASSERT_EQ(kvs.debug_memory_limit_warning_count, 1);
    }
    {
        // lock with large txn
        root_of_kvstore_mem_trackers->reset();
        RegionID region_id = 4300;
        auto [start, end] = getStartEnd(region_id);
        auto str_key = pickKey(region_id, 1);
        auto [str_val_write, str_val_default] = pickWriteDefault(region_id, 1);
        auto str_lock_value = pickLock(region_id, 1);
        proxy_instance->debugAddRegions(kvs, ctx.getTMTContext(), {region_id}, {{start, end}});
        auto kvr1 = kvs.getRegion(4100);
        auto kvr2 = kvs.getRegion(4200);
        auto kvr3 = kvs.getRegion(region_id);
        ASSERT_NE(kvr3, nullptr);
        std::string short_value = "value";
        auto lock_for_update_ts = 7777, txn_size = 1;
        const std::vector<std::string> & async_commit = {"s1", "s2"};
        const std::vector<uint64_t> & rollback = {3, 4};
        auto lock_value2 = DB::RegionBench::encodeFullLockCfValue(
            Region::DelFlag,
            "primary key",
            421321,
            std::numeric_limits<UInt64>::max(),
            &short_value,
            66666,
            lock_for_update_ts,
            txn_size,
            async_commit,
            rollback,
            1111);
        auto [index, term]
            = proxy_instance
                  ->rawWrite(region_id, {str_key}, {str_lock_value}, {WriteCmdType::Put}, {ColumnFamilyType::Lock});
        UNUSED(term);
        proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), str_key.dataSize() + str_lock_value.size());
        ASSERT_EQ(kvr3->dataSize(), root_of_kvstore_mem_trackers->get());
        ASSERT_EQ(kvr3->dataSize() + decoded_lock_size, kvr3->getData().totalSize());
        ASSERT_EQ(
            kvr3->getData().totalSize() + kvr2->getData().totalSize() + kvr1->getData().totalSize(),
            region_table.getTableRegionSize(NullspaceID, table_id));
        ASSERT_EQ(kvs.debug_memory_limit_warning_count, 1);
    }
    {
        // insert & remove
        root_of_kvstore_mem_trackers->reset();
        RegionID region_id = 5000;
        auto origin_table_size = region_table.getTableRegionSize(NullspaceID, table_id);
        auto [start, end] = getStartEnd(region_id);
        auto str_key = pickKey(region_id, 1);
        auto [str_val_write, str_val_default] = pickWriteDefault(region_id, 1);
        auto str_lock_value = pickLock(region_id, 1);
        proxy_instance->debugAddRegions(kvs, ctx.getTMTContext(), {region_id}, {{start, end}});
        RegionPtr region = kvs.getRegion(region_id);
        region->insertFromSnap(tmt, "default", TiKVKey::copyFrom(str_key), TiKVValue::copyFrom(str_val_default));
        auto delta = str_key.dataSize() + str_val_default.size();
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), delta);
        ASSERT_EQ(region_table.getTableRegionSize(NullspaceID, table_id), origin_table_size + delta);
        region->removeDebug("default", TiKVKey::copyFrom(str_key));
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), 0);
        ASSERT_EQ(region->dataSize(), root_of_kvstore_mem_trackers->get());
        ASSERT_EQ(region->dataSize(), region->getData().totalSize());
        ASSERT_EQ(region_table.getTableRegionSize(NullspaceID, table_id), origin_table_size);
        ASSERT_EQ(kvs.debug_memory_limit_warning_count, 1);
    }
    ASSERT_EQ(root_of_kvstore_mem_trackers->get(), 0);
    {
        // insert
        RegionID region_id = 6000;
        root_of_kvstore_mem_trackers->reset();
        region_table.debugClearTableRegionSize(NullspaceID, table_id);
        auto [start, end] = getStartEnd(region_id);
        auto str_key = pickKey(region_id, 1);
        auto [str_val_write, str_val_default] = pickWriteDefault(region_id, 1);
        auto str_lock_value = pickLock(region_id, 1);
        proxy_instance->debugAddRegions(kvs, ctx.getTMTContext(), {region_id}, {{start, end}});
        RegionPtr region = kvs.getRegion(region_id);
        region->insertFromSnap(tmt, "default", TiKVKey::copyFrom(str_key), TiKVValue::copyFrom(str_val_default));
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), str_key.dataSize() + str_val_default.size());
        ASSERT_EQ(region->dataSize(), root_of_kvstore_mem_trackers->get());
        ASSERT_EQ(region->dataSize(), region->getData().totalSize());
        ASSERT_EQ(kvs.debug_memory_limit_warning_count, 1);
    }
    {
        // commit
        root_of_kvstore_mem_trackers->reset();
        region_table.debugClearTableRegionSize(NullspaceID, table_id);

        RegionID region_id = 8000;
        auto [start, end] = getStartEnd(region_id);
        auto str_key = pickKey(region_id, 1);
        auto [str_val_write, str_val_default] = pickWriteDefault(region_id, 1);
        auto str_lock_value = pickLock(region_id, 1);
        proxy_instance->debugAddRegions(kvs, ctx.getTMTContext(), {region_id}, {{start, end}});
        RegionPtr region = kvs.getRegion(region_id);
        region->insertFromSnap(tmt, "default", TiKVKey::copyFrom(str_key), TiKVValue::copyFrom(str_val_default));
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), str_key.dataSize() + str_val_default.size());
        region->insertFromSnap(tmt, "write", TiKVKey::copyFrom(str_key), TiKVValue::copyFrom(str_val_write));
        ASSERT_EQ(
            str_key.dataSize() * 2 + str_val_default.size() + str_val_write.size(),
            region_table.getTableRegionSize(NullspaceID, table_id));
        std::optional<RegionDataReadInfoList> data_list_read = ReadRegionCommitCache(region, true);
        ASSERT_TRUE(data_list_read);
        ASSERT_EQ(1, data_list_read->size());
        RemoveRegionCommitCache(region, *data_list_read);
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), 0);
        ASSERT_EQ(region->dataSize(), root_of_kvstore_mem_trackers->get());
        ASSERT_EQ(region->dataSize(), region->getData().totalSize());
        ASSERT_EQ(0, region_table.getTableRegionSize(NullspaceID, table_id));
        ASSERT_EQ(kvs.debug_memory_limit_warning_count, 1);
    }
    {
        // commit by handleWriteRaftCmd
        root_of_kvstore_mem_trackers->reset();
        region_table.debugClearTableRegionSize(NullspaceID, table_id);

        RegionID region_id = 8100;
        auto [start, end] = getStartEnd(region_id);
        auto str_key = pickKey(region_id, 1);
        auto [str_val_write, str_val_default] = pickWriteDefault(region_id, 1);
        auto str_lock_value = pickLock(region_id, 1);
        proxy_instance->debugAddRegions(kvs, ctx.getTMTContext(), {region_id}, {{start, end}});
        RegionPtr region = kvs.getRegion(region_id);
        {
            auto [view, holder] = MockRaftStoreProxy::createWriteCmdsView(
                {str_key, str_key},
                {str_val_default, str_val_write},
                {WriteCmdType::Put, WriteCmdType::Put},
                {ColumnFamilyType::Default, ColumnFamilyType::Write});
            region->handleWriteRaftCmd(view, 66, 6, ctx.getTMTContext());
            ASSERT_EQ(root_of_kvstore_mem_trackers->get(), 0);
            ASSERT_EQ(region->dataSize(), root_of_kvstore_mem_trackers->get());
            ASSERT_EQ(region->dataSize(), region->getData().totalSize());
            ASSERT_EQ(0, region_table.getTableRegionSize(NullspaceID, table_id));
            ASSERT_EQ(kvs.debug_memory_limit_warning_count, 1);
        }
        {
            auto [view, holder] = MockRaftStoreProxy::createWriteCmdsView(
                {str_key},
                {str_lock_value},
                {WriteCmdType::Put},
                {ColumnFamilyType::Lock});
            region->handleWriteRaftCmd(view, 67, 6, ctx.getTMTContext());
            ASSERT_EQ(kvs.debug_memory_limit_warning_count, 2);
        }
    }
    {
        // split & merge
        root_of_kvstore_mem_trackers->reset();
        RegionID region_id = 12000;
        RegionID region_id2 = 12002;
        auto [start, end] = getStartEnd(region_id);
        auto str_key = pickKey(region_id, 22);
        auto [str_val_write, str_val_default] = pickWriteDefault(region_id, 22);
        auto str_lock_value = pickLock(region_id, 22);
        region_table.debugClearTableRegionSize(NullspaceID, table_id);
        proxy_instance->debugAddRegions(kvs, ctx.getTMTContext(), {region_id}, {{start, end}});
        RegionPtr region = kvs.getRegion(region_id);
        region->insertFromSnap(tmt, "default", TiKVKey::copyFrom(str_key), TiKVValue::copyFrom(str_val_default));
        auto str_key2 = pickKey(region_id, 80);
        auto [str_val_write2, str_val_default2] = pickWriteDefault(region_id, 80);
        auto str_lock_value2 = pickLock(region_id, 80);
        region->insertFromSnap(tmt, "default", TiKVKey::copyFrom(str_key2), TiKVValue::copyFrom(str_val_default2));
        auto original_size = region_table.getTableRegionSize(NullspaceID, table_id);
        auto expected = str_key.dataSize() + str_val_default.size() + str_key2.dataSize() + str_val_default2.size();
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), expected);
        ASSERT_EQ(region->dataSize(), expected);
        auto new_region = splitRegion(
            region,
            RegionMeta(
                RegionBench::createPeer(region_id + 1, true),
                RegionBench::createMetaRegion(region_id2, table_id, 12050, 12099),
                initialApplyState()));
        ASSERT_EQ(original_size, region_table.getTableRegionSize(NullspaceID, table_id));
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), expected);
        ASSERT_EQ(region->dataSize(), str_key.dataSize() + str_val_default.size());
        ASSERT_EQ(new_region->dataSize(), str_key2.dataSize() + str_val_default2.size());
        ASSERT_EQ(region->dataSize(), region->getData().totalSize());
        ASSERT_EQ(new_region->dataSize(), new_region->getData().totalSize());
        region->mergeDataFrom(*new_region);
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), expected);
        ASSERT_EQ(region->dataSize(), expected);
        ASSERT_EQ(region->dataSize(), region->getData().totalSize());
        ASSERT_EQ(new_region->dataSize(), new_region->getData().totalSize());
        ASSERT_EQ(original_size, region_table.getTableRegionSize(NullspaceID, table_id));
        ASSERT_EQ(kvs.debug_memory_limit_warning_count, 2);
    }
    {
        // split & merge with lock
        root_of_kvstore_mem_trackers->reset();
        region_table.debugClearTableRegionSize(NullspaceID, table_id);
        RegionID region_id = 13100;
        RegionID region_id2 = 13102;
        auto [start, end] = getStartEnd(region_id);
        auto str_key = pickKey(region_id, 22);
        auto [str_val_write, str_val_default] = pickWriteDefault(region_id, 22);
        auto str_lock_value = pickLock(region_id, 22);
        proxy_instance->debugAddRegions(kvs, ctx.getTMTContext(), {region_id}, {{start, end}});
        RegionPtr region = kvs.getRegion(region_id);
        region->insertFromSnap(tmt, "lock", TiKVKey::copyFrom(str_key), TiKVValue::copyFrom(str_lock_value));
        auto expected = str_key.dataSize() + str_lock_value.size();
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), expected);
        auto str_key2 = pickKey(region_id, 80);
        std::string short_value(97, 'a');
        auto str_lock_value2
            = RecordKVFormat::encodeLockCfValue(RecordKVFormat::CFModifyFlag::PutFlag, "PK", 13180, 111, &short_value)
                  .toString();
        region->insertFromSnap(tmt, "lock", TiKVKey::copyFrom(str_key2), TiKVValue::copyFrom(str_lock_value2));
        auto original_size = region_table.getTableRegionSize(NullspaceID, table_id);
        expected += str_key2.dataSize() + str_lock_value2.size();
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), expected);
        auto new_region = splitRegion(
            region,
            RegionMeta(
                RegionBench::createPeer(region_id + 1, true),
                RegionBench::createMetaRegion(region_id2, table_id, 13150, 13199),
                initialApplyState()));
        ASSERT_EQ(original_size, region_table.getTableRegionSize(NullspaceID, table_id));
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), expected);
        ASSERT_EQ(region->dataSize(), str_key.dataSize() + str_lock_value.size());
        ASSERT_EQ(region->getData().totalSize(), region->dataSize() + decoded_lock_size);
        ASSERT_EQ(new_region->dataSize(), str_key2.dataSize() + str_lock_value2.size());
        ASSERT_EQ(new_region->getData().totalSize(), new_region->dataSize() + decoded_lock_size);
        region->mergeDataFrom(*new_region);
        ASSERT_EQ(original_size, region_table.getTableRegionSize(NullspaceID, table_id));
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), expected);
        ASSERT_EQ(region->dataSize(), expected);
        ASSERT_EQ(region->getData().totalSize(), region->dataSize() + 2 * decoded_lock_size);

        // replace a lock
        region->insertFromSnap(tmt, "lock", TiKVKey::copyFrom(str_key2), TiKVValue::copyFrom(str_lock_value2));
        auto str_lock_value2_2
            = RecordKVFormat::encodeLockCfValue(RecordKVFormat::CFModifyFlag::PutFlag, "PK", 13022, 111).toString();
        region->insertFromSnap(tmt, "lock", TiKVKey::copyFrom(str_key2), TiKVValue::copyFrom(str_lock_value2_2));
        expected -= short_value.size();
        expected -= 2; // Short value prefix and length
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), expected);
        ASSERT_EQ(region->dataSize(), expected);
        ASSERT_EQ(region->getData().totalSize(), region->dataSize() + 2 * decoded_lock_size);
        ASSERT_EQ(kvs.debug_memory_limit_warning_count, 2);
    }
    {
        // insert & snapshot
        UInt64 region_id = 14100;
        region_table.debugClearTableRegionSize(NullspaceID, table_id);
        root_of_kvstore_mem_trackers->reset();
        auto [start, end] = getStartEnd(region_id);
        proxy_instance->debugAddRegions(kvs, ctx.getTMTContext(), {region_id}, {{start, end}});
        RegionPtr region = kvs.getRegion(region_id);
        ASSERT_NE(region, nullptr);

        auto str_key2 = pickKey(region_id, 20);
        auto [str_val_write2, str_val_default2] = pickWriteDefault(region_id, 20);
        auto str_lock_value2 = pickLock(region_id, 20);

        auto str_key3 = pickKey(region_id, 80);
        auto [str_val_write3, str_val_default3] = pickWriteDefault(region_id, 80);
        auto str_lock_value3 = pickLock(region_id, 80);

        region->insertFromSnap(tmt, "default", TiKVKey::copyFrom(str_key3), TiKVValue::copyFrom(str_val_default3));
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), str_key3.dataSize() + str_val_default3.size());
        ASSERT_EQ(region->dataSize(), str_key3.dataSize() + str_val_default3.size());
        ASSERT_EQ(region->getData().totalSize(), region->dataSize());

        MockSSTReader::getMockSSTData().clear();
        MockSSTGenerator default_cf{region_id, table_id, ColumnFamilyType::Default};
        default_cf.insert(14180, str_val_default2);
        default_cf.finish_file();
        default_cf.freeze();
        kvs.mutProxyHelperUnsafe()->sst_reader_interfaces = make_mock_sst_reader_interface();
        proxy_instance->snapshot(kvs, ctx.getTMTContext(), region_id, {default_cf}, 0, 0, std::nullopt);
        ASSERT_EQ(region->dataSize(), str_key2.dataSize() + str_val_default2.size());
        ASSERT_EQ(region->getData().totalSize(), region->dataSize());
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), str_key2.dataSize() + str_val_default2.size());
        ASSERT_EQ(region->dataSize(), region_table.getTableRegionSize(NullspaceID, table_id));
        ASSERT_EQ(kvs.debug_memory_limit_warning_count, 3);
    }
    {
        // prehandle snapshot and drop
        UInt64 region_id = 14200;
        region_table.debugClearTableRegionSize(NullspaceID, table_id);
        root_of_kvstore_mem_trackers->reset();
        auto [start, end] = getStartEnd(region_id);
        proxy_instance->debugAddRegions(kvs, ctx.getTMTContext(), {region_id}, {{start, end}});
        RegionPtr region = kvs.getRegion(region_id);
        ASSERT_NE(region, nullptr);

        auto str_key2 = pickKey(region_id, 20);
        auto [str_val_write2, str_val_default2] = pickWriteDefault(region_id, 20);
        auto str_lock_value2 = pickLock(region_id, 20);

        MockSSTReader::getMockSSTData().clear();
        MockSSTGenerator default_cf{region_id, table_id, ColumnFamilyType::Default};
        default_cf.insert(14280, str_val_default2);
        default_cf.finish_file();
        default_cf.freeze();
        kvs.mutProxyHelperUnsafe()->sst_reader_interfaces = make_mock_sst_reader_interface();
        proxy_instance->snapshot(kvs, ctx.getTMTContext(), region_id, {default_cf}, 0, 0, std::nullopt, [&]() {
            ASSERT_EQ(
                region_table.getTableRegionSize(NullspaceID, table_id),
                str_key2.dataSize() + str_val_default2.size());
        });
        ASSERT_EQ(region->dataSize(), 0);
        ASSERT_EQ(region->getData().totalSize(), 0);
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), 0);
        ASSERT_EQ(region_table.getTableRegionSize(NullspaceID, table_id), 0);
        ASSERT_EQ(kvs.debug_memory_limit_warning_count, 4);
    }
    {
        // assign
        root_of_kvstore_mem_trackers->reset();
        region_table.debugClearTableRegionSize(NullspaceID, table_id);
        RegionID region_id = 15100;
        RegionID region_id2 = 15200;
        auto [start1, end1] = getStartEnd(1100);
        auto [start2, end2] = getStartEnd(1200);
        proxy_instance
            ->debugAddRegions(kvs, ctx.getTMTContext(), {region_id, region_id2}, {{start1, end1}, {start2, end2}});
        RegionPtr region = kvs.getRegion(region_id);
        RegionPtr region2 = kvs.getRegion(region_id2);

        auto str_key = pickKey(region_id, 70);
        auto [str_val_write, str_val_default] = pickWriteDefault(region_id, 70);
        auto str_lock_value = pickLock(region_id, 70);

        region->insertFromSnap(tmt, "default", TiKVKey::copyFrom(str_key), TiKVValue::copyFrom(str_val_default));
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), str_key.dataSize() + str_val_default.size());

        auto str_key2 = pickKey(region_id, 80);
        auto [str_val_write2, str_val_default2] = pickWriteDefault(region_id, 80);
        auto str_lock_value2 = pickLock(region_id, 80);
        region2->insertFromSnap(tmt, "default", TiKVKey::copyFrom(str_key2), TiKVValue::copyFrom(str_val_default2));
        region2->insertFromSnap(tmt, "default", TiKVKey::copyFrom(str_key), TiKVValue::copyFrom(str_val_default));

        region->assignRegion(std::move(*region2));
        ASSERT_EQ(
            root_of_kvstore_mem_trackers->get(),
            str_key.dataSize() + str_val_default.size() + str_key2.dataSize() + str_val_default2.size());
        ASSERT_EQ(region->dataSize(), root_of_kvstore_mem_trackers->get());
        ASSERT_EQ(region->dataSize(), region_table.getTableRegionSize(NullspaceID, table_id));
        ASSERT_EQ(
            region->dataSize(),
            str_key.dataSize() + str_val_default.size() + str_key2.dataSize() + str_val_default2.size());
        ASSERT_EQ(region->getData().totalSize(), region->dataSize());
        // `region2` is not allowed to access after move, however, we assert here in order to make sure the logic.
        ASSERT_EQ(region2->dataSize(), 0);
        ASSERT_EQ(region2->getData().totalSize(), region2->dataSize());
    }
    {
        // remove region
        RegionID region_id = 16000;
        root_of_kvstore_mem_trackers->reset();
        region_table.debugClearTableRegionSize(NullspaceID, table_id);
        auto [start, end] = getStartEnd(region_id);
        auto str_key = pickKey(region_id, 1);
        auto [str_val_write, str_val_default] = pickWriteDefault(region_id, 1);
        auto str_lock_value = pickLock(region_id, 1);
        proxy_instance->debugAddRegions(kvs, ctx.getTMTContext(), {region_id}, {{start, end}});
        auto kvr1 = kvs.getRegion(region_id);
        auto [index, term]
            = proxy_instance
                  ->rawWrite(region_id, {str_key}, {str_val_default}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
        UNUSED(term);
        proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
        kvs.handleDestroy(region_id, ctx.getTMTContext());
        ASSERT_EQ(0, region_table.getTableRegionSize(NullspaceID, table_id));
    }
}
CATCH

TEST_F(RegionKVStoreTest, MemoryTracker2)
try
{
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    auto & tmt = ctx.getTMTContext();
    initStorages();
    KVStore & kvs = getKVS();
    ctx.getTMTContext().debugSetKVStore(kvstore);
    auto table_id = proxy_instance->bootstrapTable(ctx, kvs, ctx.getTMTContext());

    auto & region_table = ctx.getTMTContext().getRegionTable();
    auto getStartEnd = [&](RegionID region_id) {
        return std::make_pair(
            RecordKVFormat::genKey(table_id, region_id),
            RecordKVFormat::genKey(table_id, region_id + 99));
    };
    auto pickKey = [&](RegionID region_id, UInt64 number) {
        return RecordKVFormat::genKey(table_id, region_id + number, 111);
    };
    auto pickWriteDefault = [&](RegionID, UInt64) {
        return proxy_instance->generateTiKVKeyValue(111, 999);
    };
    auto pickLock = [&](RegionID region_id, UInt64 number) {
        return RecordKVFormat::encodeLockCfValue(RecordKVFormat::CFModifyFlag::PutFlag, "PK", region_id + number, 999)
            .toString();
    };
    {
        // reload
        RegionID region_id = 7000;
        root_of_kvstore_mem_trackers->reset();
        region_table.debugClearTableRegionSize(NullspaceID, table_id);
        auto [start, end] = getStartEnd(region_id);
        auto str_key = pickKey(region_id, 1);
        auto [str_val_write, str_val_default] = pickWriteDefault(region_id, 1);
        auto str_lock_value = pickLock(region_id, 1);
        proxy_instance->debugAddRegions(kvs, ctx.getTMTContext(), {region_id}, {{start, end}});
        RegionPtr region = kvs.getRegion(region_id);
        root_of_kvstore_mem_trackers->reset();
        region->insertFromSnap(tmt, "default", TiKVKey::copyFrom(str_key), TiKVValue::copyFrom(str_val_default));
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), str_key.dataSize() + str_val_default.size());
        tryPersistRegion(kvs, region_id);
        root_of_kvstore_mem_trackers->reset();
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), 0);
        reloadKVSFromDisk(false);
        ctx.getTMTContext().debugSetKVStore(kvstore);
        region_table.restore();
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), str_key.dataSize() + str_val_default.size());
        ASSERT_EQ(region->dataSize(), root_of_kvstore_mem_trackers->get());
        ASSERT_EQ(region->dataSize(), region->getData().totalSize());
        // Only this region is persisted.
        ASSERT_EQ(region->dataSize(), region_table.getTableRegionSize(NullspaceID, table_id));
    }
}
CATCH


std::tuple<RegionPtr, PrehandleResult> genPreHandlingRegion(
    KVStore & kvs,
    RegionID region_id,
    TableID table_id,
    HandleID start,
    HandleID end,
    RegionTable & region_table,
    TMTContext & tmt)
{
    UInt64 peer_id = 100000 + region_id; // gen a fake peer_id
    auto meta = RegionBench::createMetaRegion(
        region_id,
        table_id,
        start,
        end,
        /*maybe_epoch=*/std::nullopt,
        /*maybe_peers=*/std::vector<metapb::Peer>{RegionBench::createPeer(peer_id, true)});
    auto new_region = kvs.genRegionPtr(
        std::move(meta),
        peer_id,
        /*index*/ RAFT_INIT_LOG_INDEX,
        /*term*/ RAFT_INIT_LOG_TERM,
        region_table);
    // empty snapshot
    SSTViewVec snaps{.views = nullptr, .len = 0};
    auto prehandle_result = kvs.preHandleSnapshotToFiles(new_region, snaps, 20, RAFT_INIT_LOG_TERM, std::nullopt, tmt);
    return {new_region, prehandle_result};
}

// A unit test that Region being removed/created concurrently
TEST_F(RegionKVStoreTest, RegionTableBeingRecreated)
try
{
    LoggerPtr log = Logger::get();
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    auto & tmt = ctx.getTMTContext();
    initStorages();
    KVStore & kvs = getKVS();
    ctx.getTMTContext().debugSetKVStore(kvstore);
    const auto table_id = proxy_instance->bootstrapTable(ctx, kvs, ctx.getTMTContext());

    auto & region_table = ctx.getTMTContext().getRegionTable();
    auto get_start_end = [&](Int64 range_beg, Int64 range_end) {
        return std::make_pair(RecordKVFormat::genKey(table_id, range_beg), RecordKVFormat::genKey(table_id, range_end));
    };

    const RegionID region_id_0 = 7000;
    {
        // Step 1: Generate a Region with region_id = 7000, range[100, 200)
        LOG_INFO(log, "Step 1");
        root_of_kvstore_mem_trackers->reset();
        region_table.debugClearTableRegionSize(NullspaceID, table_id);
        auto [start, end] = get_start_end(100, 200);
        proxy_instance->debugAddRegions(kvs, tmt, {region_id_0}, {{start, end}});

        root_of_kvstore_mem_trackers->reset();

        RegionPtr region = kvs.getRegion(region_id_0);
        auto str_key = RecordKVFormat::genKey(table_id, 105, 111);
        auto [str_val_write, str_val_default] = proxy_instance->generateTiKVKeyValue(111, 999);
        auto str_lock_value
            = RecordKVFormat::encodeLockCfValue(RecordKVFormat::CFModifyFlag::PutFlag, "PK", region_id_0, 999)
                  .toString();
        region->insertDebug("default", TiKVKey::copyFrom(str_key), TiKVValue::copyFrom(str_val_default));
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), str_key.dataSize() + str_val_default.size());
        tryPersistRegion(kvs, region_id_0);
        LOG_INFO(log, "Step 1: table_size: {}", region->getRegionTableSize());
    }

    RegionID pre_handle_region_id_1 = 7001;
    RegionID pre_handle_region_id_2 = 7002;
    {
        // Step 2: Mock that Region with region_id = 7001, range[200, 300) being pre-handle
        LOG_INFO(log, "Step 2");
        auto && [new_region_1, pre_handle_res_1]
            = genPreHandlingRegion(kvs, pre_handle_region_id_1, table_id, 200, 300, region_table, tmt);
        ASSERT_NE(new_region_1, nullptr);

        {
            // check that Region 7000 and pre-handle Region 7001 should share the same table_ctx
            auto region_0 = kvs.getRegion(region_id_0);
            ASSERT_NE(region_0, nullptr);
            ASSERT_EQ(region_0->getRegionTableSize(), new_region_1->getRegionTableSize());
            ASSERT_EQ(region_0->getRegionTableCtx().get(), new_region_1->getRegionTableCtx().get());
            LOG_INFO(log, "Step 2: table_size: {}", region_0->getRegionTableSize());
        }

        {
            // Step 3: Mock that Region with region_id = 7000 is removed
            // (This could lead to the RegionTable::Table instance being removed
            // because no known Region belong to the table_id).
            LOG_INFO(log, "Step 3");
            kvs.removeRegion(
                region_id_0,
                /*remove_data*/ true,
                region_table,
                kvs.genTaskLock(),
                kvs.region_manager.genRegionTaskLock(region_id_0));
        }

        // Step 4: Mock that Region with region_id = 7002, range[300, 400) being pre-handle
        // after region 7000 is removed.
        LOG_INFO(log, "Step 4");
        auto && [new_region_2, pre_handle_res_2]
            = genPreHandlingRegion(kvs, pre_handle_region_id_2, table_id, 300, 400, region_table, tmt);
        ASSERT_NE(new_region_2, nullptr);
        // Step 5: apply the pre-handle-region-1 and pre-handle-region-2
        kvs.applyPreHandledSnapshot(
            RegionPtrWithSnapshotFiles{new_region_2, std::move(pre_handle_res_2.ingest_ids)},
            tmt);
        kvs.applyPreHandledSnapshot(
            RegionPtrWithSnapshotFiles{new_region_1, std::move(pre_handle_res_1.ingest_ids)},
            tmt);
        {
            // check that Region 7001 and Region 7002 should share the same table_ctx
            ASSERT_EQ(new_region_2->getRegionTableSize(), new_region_1->getRegionTableSize());
            ASSERT_EQ(new_region_2->getRegionTableCtx().get(), new_region_1->getRegionTableCtx().get());
            LOG_INFO(log, "Step 5: table_size: {}", new_region_2->getRegionTableSize());
        }
    }
    RegionID region_id_3 = 7003;
    {
        // Step 6: Mock that new Region is added after all
        LOG_INFO(log, "Step 5");
        auto [start, end] = get_start_end(300, 400);
        proxy_instance->debugAddRegions(kvs, tmt, {region_id_3}, {{start, end}});

        {
            // check that Region 7001, 7002, 7003 should share the same table_ctx
            auto pre_handle_region_1 = kvs.getRegion(pre_handle_region_id_1);
            auto pre_handle_region_2 = kvs.getRegion(pre_handle_region_id_2);
            ASSERT_EQ(pre_handle_region_1->getRegionTableSize(), pre_handle_region_2->getRegionTableSize());
            ASSERT_EQ(pre_handle_region_1->getRegionTableCtx().get(), pre_handle_region_2->getRegionTableCtx().get());
            auto region_3 = kvs.getRegion(region_id_3);
            ASSERT_EQ(pre_handle_region_1->getRegionTableSize(), region_3->getRegionTableSize());
            ASSERT_EQ(pre_handle_region_1->getRegionTableCtx().get(), region_3->getRegionTableCtx().get());
            LOG_INFO(log, "Step 6: table_size: {}", region_3->getRegionTableSize());
        }
    }
    {
        // Step 7: If we insert some data into Region 7003, the size of table_ctx of all Region should be updated
        RegionPtr region = kvs.getRegion(region_id_3);
        auto str_key = RecordKVFormat::genKey(table_id, 105, 120);
        auto [str_val_write, str_val_default] = proxy_instance->generateTiKVKeyValue(120, 999);
        auto str_lock_value
            = RecordKVFormat::encodeLockCfValue(RecordKVFormat::CFModifyFlag::PutFlag, "PK", region_id_0, 999)
                  .toString();
        region->insertDebug("default", TiKVKey::copyFrom(str_key), TiKVValue::copyFrom(str_val_default));
        LOG_INFO(log, "Step 7: table_size: {}", region->getRegionTableSize());
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), str_key.dataSize() + str_val_default.size());
        {
            // check that Region 7001, 7002, 7003 should share the same table_ctx
            auto region_1 = kvs.getRegion(pre_handle_region_id_1);
            auto region_2 = kvs.getRegion(pre_handle_region_id_2);
            auto region_3 = kvs.getRegion(region_id_3);
            auto region_tbl_size = region_1->getRegionTableSize();
            ASSERT_EQ(region_tbl_size, region_2->getRegionTableSize());
            ASSERT_EQ(region_tbl_size, region_3->getRegionTableSize());
        }
    }
}
CATCH

#if USE_JEMALLOC // following tests depends on jemalloc
TEST(FFIJemallocTest, JemallocThread)
try
{
    std::thread t2([&]() {
        char * a = new char[888888];
        std::thread t1([&]() {
            auto [allocated, deallocated] = JointThreadInfoJeallocMap::getPtrs();
            ASSERT_TRUE(allocated != nullptr);
            ASSERT_EQ(*allocated, 0);
            ASSERT_TRUE(deallocated != nullptr);
            ASSERT_EQ(*deallocated, 0);
        });
        t1.join();
        auto [allocated, deallocated] = JointThreadInfoJeallocMap::getPtrs();
        ASSERT_TRUE(allocated != nullptr);
        ASSERT_GE(*allocated, 888888);
        ASSERT_TRUE(deallocated != nullptr);
        delete[] a;
    });
    t2.join();

    std::thread t3([&]() {
        // Will not cover mmap memory.
        auto [allocated, deallocated] = JointThreadInfoJeallocMap::getPtrs();
        char * a = new char[120];
        void * buf = mmap(nullptr, 6000, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        ASSERT_LT(*allocated, 6000);
        munmap(buf, 0);
        delete[] a;
    });
    t3.join();
}
CATCH

TEST_F(RegionKVStoreTest, StorageBgPool)
try
{
    using namespace std::chrono_literals;
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    auto & pool = ctx.getBackgroundPool();
    const auto size = TiFlashTestEnv::DEFAULT_BG_POOL_SIZE;
    std::atomic_bool b = false;

    JointThreadInfoJeallocMap & jm = *ctx.getJointThreadInfoJeallocMap();

    size_t original_size
        = TiFlashMetrics::instance().getStorageThreadMemory(TiFlashMetrics::MemoryAllocType::Alloc, "bg");

    auto t = pool.addTask(
        [&]() {
            auto * x = new int[1000];
            LOG_INFO(Logger::get(), "allocated");
            while (!b.load())
            {
                std::this_thread::sleep_for(1500ms);
            }
            delete[] x;
            LOG_INFO(Logger::get(), "released");
            return false;
        },
        false,
        5 * 60 * 1000);
    std::this_thread::sleep_for(500ms);

    jm.recordThreadAllocInfo();

    LOG_INFO(DB::Logger::get(), "bg pool size={}", size);
    UInt64 r = TiFlashMetrics::instance().getStorageThreadMemory(TiFlashMetrics::MemoryAllocType::Alloc, "bg");
    ASSERT_GE(r, original_size + sizeof(int) * 1000);
    jm.accessStorageMap([size](const JointThreadInfoJeallocMap::AllocMap & m) {
        // There are some other bg thread pools
        ASSERT_GE(m.size(), size) << m.size();
    });
    jm.accessProxyMap([](const JointThreadInfoJeallocMap::AllocMap & m) { ASSERT_EQ(m.size(), 0); });

    b.store(true);

    ctx.getBackgroundPool().removeTask(t);
}
CATCH
#endif
} // namespace DB::tests
