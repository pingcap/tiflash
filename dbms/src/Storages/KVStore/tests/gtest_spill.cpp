// Copyright 2024 PingCAP, Inc.
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

#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/KVStore/Decode/RegionBlockReader.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/MultiRaft/Spill/RegionUncommittedDataList.h>
#include <Storages/KVStore/tests/region_kvstore_test.h>
#include <Storages/RegionQueryInfo.h>
#include <TiDB/Schema/SchemaSyncService.h>
#include <TiDB/Schema/TiDBSchemaManager.h>

namespace DB::FailPoints
{
extern const char force_write_to_large_txn_default[];
} // namespace DB::FailPoints

extern std::shared_ptr<MemoryTracker> root_of_kvstore_mem_trackers;

namespace DB::tests
{
class KVStoreSpillTest : public KVStoreTestBase
{
public:
    void SetUp() override
    {
        log = DB::Logger::get("KVStoreSpillTest");
        initStorages();
        KVStoreTestBase::SetUp();
        setupStorage();
    }

    void TearDown() override
    {
        storage->drop();
        KVStoreTestBase::TearDown();
    }

    void setupStorage()
    {
        auto & ctx = TiFlashTestEnv::getGlobalContext();
        KVStore & kvs = getKVS();
        DebugKVStore debug_kvs(kvs);
        ctx.getTMTContext().debugSetKVStore(kvstore);
        debug_kvs.mutRegionSerdeOpts().large_txn_enabled = true;
        table_id = proxy_instance->bootstrapTable(ctx, kvs, ctx.getTMTContext());
        auto maybe_storage = ctx.getTMTContext().getStorages().get(NullspaceID, table_id);
        RUNTIME_CHECK(maybe_storage);
        storage = std::dynamic_pointer_cast<StorageDeltaMerge>(maybe_storage);
    }

protected:
    StorageDeltaMergePtr storage;
    TableID table_id;
};

TEST_F(KVStoreSpillTest, CreateBlock)
try
{
    auto table_lock = storage->lockStructureForShare("foo_query_id");
    {
        auto [schema_snapshot, block] = storage->getSchemaSnapshotAndBlockForDecoding(table_lock, true, true);
        UNUSED(schema_snapshot);
        ASSERT_EQ(block->columns(), 4);
    }
    {
        auto [schema_snapshot, block] = storage->getSchemaSnapshotAndBlockForDecoding(table_lock, true, false);
        UNUSED(schema_snapshot);
        EXPECT_NO_THROW(block->getPositionByName(MutableSupport::delmark_column_name));
        EXPECT_THROW(block->getPositionByName(MutableSupport::version_column_name), Exception);
        ASSERT_EQ(block->columns(), 3);
    }
    {
        // getColId2BlockPosMap should be a sub-sequence of getColId2DefPosMap.
        auto [schema_snapshot, block] = storage->getSchemaSnapshotAndBlockForDecoding(table_lock, true, false);
        UNUSED(block);
        auto it2 = schema_snapshot->getColId2DefPosMap().begin();
        auto it2_end = schema_snapshot->getColId2DefPosMap().end();
        auto it = schema_snapshot->getColId2BlockPosMap().begin();
        auto it_end = schema_snapshot->getColId2BlockPosMap().end();
        for (; it != it_end && it2 != it2_end; it++)
        {
            if (it2->first == it->first)
                it2++;
        }
        ASSERT_TRUE(it == it_end && it2 == it2_end);
    }
}
CATCH

static DB::RegionUncommittedDataList buildUncommitReadList(TableID table_id, MockRaftStoreProxy * proxy_instance)
{
    DB::RegionUncommittedDataList data_list_read;

    auto str_key = RecordKVFormat::genKey(table_id, 1, 111);
    auto [str_val_write, str_val_default] = proxy_instance->generateTiKVKeyValue(111, 999);
    auto str_lock_value
        = RecordKVFormat::encodeLockCfValue(RecordKVFormat::CFModifyFlag::PutFlag, "PK", 111, 999).toString();
    auto pk = RecordKVFormat::getRawTiDBPK(RecordKVFormat::decodeTiKVKey(str_key));
    auto value = std::make_shared<TiKVValue>(TiKVValue::copyFrom(str_val_default));
    data_list_read.getInner().push_back(
        RegionUncommittedData(std::move(pk), RecordKVFormat::CFModifyFlag::PutFlag, value));

    return data_list_read;
}

TEST_F(KVStoreSpillTest, BlockReader)
try
{
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    {
        // Test if a block with correct cols can be constructed.
        auto table_lock = storage->lockStructureForShare("foo_query_id");
        auto [decoding_schema_snapshot, block_ptr]
            = storage->getSchemaSnapshotAndBlockForDecoding(table_lock, true, false);

        DB::RegionUncommittedDataList data_list_read = buildUncommitReadList(table_id, proxy_instance.get());

        auto reader = RegionBlockReader(decoding_schema_snapshot);
        ASSERT_TRUE(reader.read(*block_ptr, data_list_read, true));
        auto block_pos = decoding_schema_snapshot->getColId2BlockPosMap().find(1)->second;
        const auto & col_data = block_ptr->safeGetByPosition(block_pos);
        ASSERT_EQ(col_data.name, "a");
        ASSERT_EQ(col_data.column->getName(), "Int64");
    }
    {
        auto table_lock = storage->lockStructureForShare("foo_query_id");
        auto [decoding_schema_snapshot, block_ptr]
            = storage->getSchemaSnapshotAndBlockForDecoding(table_lock, true, false);

        DB::RegionUncommittedDataList data_list_read = buildUncommitReadList(table_id, proxy_instance.get());

        KVStore & kvs = getKVS();
        proxy_instance->bootstrapWithRegion(kvs, ctx.getTMTContext(), 1, std::nullopt);
        auto region = kvs.getRegion(1);
        RegionPtrWithBlock region_with_cache = RegionPtrWithBlock(region);
        // TODO(Spill) spill logic
        EXPECT_THROW(writeRegionDataToStorage(ctx, region_with_cache, data_list_read, log), Exception);
    }
}
CATCH

TEST_F(KVStoreSpillTest, LargeTxnDefaultCf)
try
{
    LargeTxnDefaultCf cf;
    auto str_key = RecordKVFormat::genKey(table_id, 1, 111);
    auto [str_val_write, str_val_default] = proxy_instance->generateTiKVKeyValue(111, 999);
    cf.insert(TiKVKey::copyFrom(str_key), TiKVValue::copyFrom(str_val_default));
    ASSERT_EQ(cf.getSize(), 1);
    ASSERT_EQ(cf.getTxnCount(), 1);
    auto str_key2 = RecordKVFormat::genKey(table_id, 2, 111);
    auto [str_val_write2, str_val_default2] = proxy_instance->generateTiKVKeyValue(111, 999);
    cf.insert(TiKVKey::copyFrom(str_key2), TiKVValue::copyFrom(str_val_default2));
    ASSERT_EQ(cf.getSize(), 2);
    ASSERT_EQ(cf.getTxnCount(), 1);

    LargeTxnDefaultCf cf2;
    auto str_key3 = RecordKVFormat::genKey(table_id, 3, 111);
    auto [str_val_write3, str_val_default3] = proxy_instance->generateTiKVKeyValue(111, 999);
    cf2.insert(TiKVKey::copyFrom(str_key3), TiKVValue::copyFrom(str_val_default3));
    auto str_key4 = RecordKVFormat::genKey(table_id, 4, 112);
    auto [str_val_write4, str_val_default4] = proxy_instance->generateTiKVKeyValue(112, 999);
    cf2.insert(TiKVKey::copyFrom(str_key4), TiKVValue::copyFrom(str_val_default4));
    cf.mergeFrom(cf2);
    ASSERT_EQ(cf.getSize(), 4);
    ASSERT_EQ(cf.getTxnCount(), 2);

    LargeTxnDefaultCf cf3;
    RegionRangeKeys new_range{TiKVKey::copyFrom(str_key), TiKVKey::copyFrom(str_key2)};
    cf.splitInto(new_range.comparableKeys(), cf3);
    ASSERT_EQ(cf.getSize(), 3);
    ASSERT_EQ(cf.getTxnCount(), 2);
    ASSERT_EQ(cf3.getSize(), 1);
    // Empty for start_ts=112
    ASSERT_EQ(cf3.getTxnCount(), 2);

    MemoryWriteBuffer wb_meta;
    cf.serializeMeta(wb_meta);
    MemoryWriteBuffer wb;
    cf.serialize(wb);
    LargeTxnDefaultCf cf_recover;
    auto txn_count_recover = LargeTxnDefaultCf::deserializeMeta(*wb_meta.tryGetReadBuffer());
    ASSERT_EQ(2, txn_count_recover);
    LargeTxnDefaultCf::deserialize(*wb.tryGetReadBuffer(), txn_count_recover, cf_recover);
    ASSERT_EQ(cf_recover.getSize(), 3);
    ASSERT_EQ(cf_recover.getTxnCount(), 2);
    ASSERT_TRUE(cf_recover.hasTxn(111));
    ASSERT_TRUE(cf_recover.hasTxn(112));

    {
        auto decoded = RecordKVFormat::genRawKey(table_id, 4);
        auto pk = RecordKVFormat::getRawTiDBPK(decoded);
        auto it = cf.find(pk, 112);
        ASSERT_TRUE(it.has_value());
        cf.erase(it);
        ASSERT_EQ(cf.getTxnCount(), 1);
    }
}
CATCH

TEST_F(KVStoreSpillTest, RegionOperations)
try
{
    RegionSerdeOpts opts;
    opts.large_txn_enabled = true;
    FailPointHelper::enableFailPoint(FailPoints::force_write_to_large_txn_default);
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    KVStore & kvs = getKVS();
    proxy_instance->bootstrapWithRegion(kvs, ctx.getTMTContext(), 1, std::nullopt);
    auto orig_region = kvs.getRegion(1);
    auto region = DebugRegion(orig_region);
    auto str_key = RecordKVFormat::genKey(table_id, 1, 111);
    auto [str_val_write, str_val_default] = proxy_instance->generateTiKVKeyValue(111, 999);
    auto str_key2 = RecordKVFormat::genKey(table_id, 2, 111);
    auto [str_val_write2, str_val_default2] = proxy_instance->generateTiKVKeyValue(111, 999);
    auto str_key3 = RecordKVFormat::genKey(table_id, 3, 112);
    auto [str_val_write3, str_val_default3] = proxy_instance->generateTiKVKeyValue(112, 999);
    auto str_key4 = RecordKVFormat::genKey(table_id, 4, 113);
    auto [str_val_write4, str_val_default4] = proxy_instance->generateTiKVKeyValue(113, 999);
    region->insert(ColumnFamilyType::Default, TiKVKey::copyFrom(str_key), TiKVValue::copyFrom(str_val_default));
    region->insert(ColumnFamilyType::Default, TiKVKey::copyFrom(str_key2), TiKVValue::copyFrom(str_val_default2));
    region->insert(ColumnFamilyType::Default, TiKVKey::copyFrom(str_key3), TiKVValue::copyFrom(str_val_default3));
    region->insert(ColumnFamilyType::Default, TiKVKey::copyFrom(str_key4), TiKVValue::copyFrom(str_val_default4));
    size_t s1 = str_key.dataSize() + str_val_default.size();
    size_t s2 = str_key2.dataSize() + str_val_default2.size();
    size_t s3 = str_key3.dataSize() + str_val_default3.size();
    size_t s4 = str_key4.dataSize() + str_val_default4.size();
    ASSERT_EQ(root_of_kvstore_mem_trackers->get(), s1 + s2 + s3 + s4);
    MemoryWriteBuffer wb;
    region->serialize(wb, opts);
    auto orig_region2 = Region::deserialize(*wb.tryGetReadBuffer());
    ASSERT_EQ(root_of_kvstore_mem_trackers->get(), (s1 + s2 + s3 + s4) * 2);
    auto region2 = DebugRegion(orig_region2);
    ASSERT_EQ(region2.debugData().largeDefautCf().getSize(), 4);
    ASSERT_EQ(region2.debugData().largeDefautCf().getTxnCount(), 3);
    ASSERT_TRUE(region2.debugData().largeDefautCf().hasTxn(111));
    ASSERT_TRUE(region2.debugData().largeDefautCf().hasTxn(112));
    ASSERT_TRUE(region2.debugData().largeDefautCf().hasTxn(113));
    ASSERT_EQ(region2.debugData().largeDefautCf().getTxnKeyCount(111), 2);
    ASSERT_EQ(region2.debugData().largeDefautCf().getTxnKeyCount(112), 1);
    ASSERT_EQ(region2.debugData().largeDefautCf().getTxnKeyCount(113), 1);
    auto orig_splitted = region.debugSplitInto(tests::createRegionMeta(2, table_id, 3, 5));
    ASSERT_EQ(root_of_kvstore_mem_trackers->get(), (s1 + s2 + s3 + s4) * 2);
    auto splitted = DebugRegion(orig_splitted);
    ASSERT_EQ(region.debugData().largeDefautCf().getSize(), 2);
    ASSERT_EQ(splitted.debugData().largeDefautCf().getSize(), 2);
    // Have an empty txn 111.
    ASSERT_EQ(splitted.debugData().largeDefautCf().getTxnCount(), 3);
    ASSERT_TRUE(splitted.debugData().largeDefautCf().hasTxn(112));
    ASSERT_TRUE(splitted.debugData().largeDefautCf().hasTxn(113));
    ASSERT_EQ(splitted.debugData().largeDefautCf().getTxnKeyCount(112), 1);
    ASSERT_EQ(splitted.debugData().largeDefautCf().getTxnKeyCount(113), 1);
    ASSERT_EQ(splitted.debugData().largeDefautCf().getTxnKeyCount(111), 0);

    orig_region2->assignRegion(std::move(*orig_splitted));
    // region: s1 + s2, region2: s3 + s4.
    ASSERT_EQ(root_of_kvstore_mem_trackers->get(), s1 + s2 + s3 + s4);
    ASSERT_EQ(region2.debugData().largeDefautCf().getTxnCount(), 3);
    ASSERT_TRUE(region2.debugData().largeDefautCf().hasTxn(112));
    ASSERT_TRUE(region2.debugData().largeDefautCf().hasTxn(113));
    ASSERT_EQ(region2.debugData().largeDefautCf().getTxnKeyCount(112), 1);
    ASSERT_EQ(region2.debugData().largeDefautCf().getTxnKeyCount(113), 1);
    ASSERT_EQ(region2.debugData().largeDefautCf().getTxnKeyCount(111), 0);

    region->mergeDataFrom(*orig_region2);
    ASSERT_EQ(region.debugData().largeDefautCf().getSize(), 4);
    auto [index, term]
        = proxy_instance->rawWrite(1, {str_key}, {str_val_write}, {WriteCmdType::Put}, {ColumnFamilyType::Write});
    MockRaftStoreProxy::FailCond cond;
    proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, 1, index, std::nullopt);
    ASSERT_EQ(region.debugData().largeDefautCf().getTxnKeyCount(111), 1);
    ASSERT_EQ(region.debugData().largeDefautCf().getSize(), 3);

    auto [index2, term2]
        = proxy_instance->rawWrite(1, {str_key3}, {str_val_write3}, {WriteCmdType::Del}, {ColumnFamilyType::Default});
    proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, 1, index2, std::nullopt);
    ASSERT_EQ(region.debugData().largeDefautCf().getSize(), 2);

    {
        // Ensures normal codes not affected.
        // TODO(Spill) Remove this test when GA.
        DebugKVStore debug_kvs(kvs);
        debug_kvs.mutRegionSerdeOpts().large_txn_enabled = false;
        auto lock = kvs.region_manager.genRegionTaskLock(1);
        kvs.persistRegion(*orig_region, lock, PersistRegionReason::Flush, "test");
        reloadKVSFromDisk();
        auto & kvs2 = getKVS();
        auto orig_region3 = kvs2.getRegion(1);
        auto region3 = DebugRegion(orig_region3);
        ASSERT_EQ(region3.debugData().largeDefautCf().getSize(), 0);
        ASSERT_EQ(region3.debugData().largeDefautCf().getTxnCount(), 0);
    }
    FailPointHelper::disableFailPoint(FailPoints::force_write_to_large_txn_default);
}
CATCH

} // namespace DB::tests