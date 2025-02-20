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

#include <Common/MemoryTracker.h>
#include <Debug/MockKVStore/MockSSTGenerator.h>
#include <Debug/MockTiDB.h>
#include <RaftStoreProxyFFI/ColumnFamily.h>
#include <Storages/KVStore/Read/LearnerRead.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/tests/region_kvstore_test.h>
#include <Storages/RegionQueryInfo.h>
#include <common/config_common.h> // Included for `USE_JEMALLOC`

extern std::shared_ptr<MemoryTracker> root_of_kvstore_mem_trackers;
extern std::atomic<Int64> real_rss;


namespace DB::tests
{
using namespace DB::RecordKVFormat;


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
        // lock with largetxn
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
        std::string shor_value = "value";
        auto lock_for_update_ts = 7777, txn_size = 1;
        const std::vector<std::string> & async_commit = {"s1", "s2"};
        const std::vector<uint64_t> & rollback = {3, 4};
        auto lock_value2 = DB::RegionBench::encodeFullLockCfValue(
            Region::DelFlag,
            "primary key",
            421321,
            std::numeric_limits<UInt64>::max(),
            &shor_value,
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
        auto originTableSize = region_table.getTableRegionSize(NullspaceID, table_id);
        auto [start, end] = getStartEnd(region_id);
        auto str_key = pickKey(region_id, 1);
        auto [str_val_write, str_val_default] = pickWriteDefault(region_id, 1);
        auto str_lock_value = pickLock(region_id, 1);
        proxy_instance->debugAddRegions(kvs, ctx.getTMTContext(), {region_id}, {{start, end}});
        RegionPtr region = kvs.getRegion(region_id);
        region->insertFromSnap(tmt, "default", TiKVKey::copyFrom(str_key), TiKVValue::copyFrom(str_val_default));
        auto delta = str_key.dataSize() + str_val_default.size();
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), delta);
        ASSERT_EQ(region_table.getTableRegionSize(NullspaceID, table_id), originTableSize + delta);
        region->remove("default", TiKVKey::copyFrom(str_key));
        ASSERT_EQ(root_of_kvstore_mem_trackers->get(), 0);
        ASSERT_EQ(region->dataSize(), root_of_kvstore_mem_trackers->get());
        ASSERT_EQ(region->dataSize(), region->getData().totalSize());
        ASSERT_EQ(region_table.getTableRegionSize(NullspaceID, table_id), originTableSize);
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
                createPeer(region_id + 1, true),
                createRegionInfo(
                    region_id2,
                    RecordKVFormat::genKey(table_id, 12050),
                    RecordKVFormat::genKey(table_id, 12099)),
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
                createPeer(region_id + 1, true),
                createRegionInfo(
                    region_id2,
                    RecordKVFormat::genKey(table_id, 13150),
                    RecordKVFormat::genKey(table_id, 13199)),
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
