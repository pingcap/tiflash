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

namespace DB::tests
{
class KVStoreSpillTest : public KVStoreTestBase
{
public:
    void SetUp() override
    {
        log = DB::Logger::get("KVStoreSpillTest");
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
        initStorages();
        KVStore & kvs = getKVS();
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
        EXPECT_NO_THROW(block->getPositionByName(MutSup::delmark_column_name));
        EXPECT_THROW(block->getPositionByName(MutSup::version_column_name), Exception);
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
        RegionPtrWithSnapshotFiles region_with_cache = RegionPtrWithSnapshotFiles(region);
        // TODO(Spill) spill logic
        EXPECT_THROW(writeRegionDataToStorage(ctx, region_with_cache, data_list_read, log), Exception);
    }
}
CATCH

} // namespace DB::tests
