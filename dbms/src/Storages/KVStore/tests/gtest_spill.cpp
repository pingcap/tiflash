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
        auto columns = DM::tests::DMTestEnv::getDefaultTableColumns(pk_type);
        auto table_info = DM::tests::DMTestEnv::getMinimalTableInfo(/* table id */ 100, pk_type);
        auto astptr = DM::tests::DMTestEnv::getPrimaryKeyExpr("test_table", pk_type);

        storage = StorageDeltaMerge::create(
            "TiFlash",
            "default" /* db_name */,
            "test_table" /* table_name */,
            table_info,
            ColumnsDescription{columns},
            astptr,
            0,
            ctx);
        storage->startup();
    }

protected:
    StorageDeltaMergePtr storage;
    DM::tests::DMTestEnv::PkType pk_type = DM::tests::DMTestEnv::PkType::HiddenTiDBRowID;
};

TEST_F(KVStoreSpillTest, KVStoreSpill)
try
{
    auto table_lock = storage->lockStructureForShare("foo_query_id");
    {
        auto [schema_snapshot, block] = storage->getSchemaSnapshotAndBlockForDecoding(table_lock, true, true);
        UNUSED(schema_snapshot);
        ASSERT_EQ(block->columns(), 3);
    }
    {
        auto [schema_snapshot, block] = storage->getSchemaSnapshotAndBlockForDecoding(table_lock, true, false);
        UNUSED(schema_snapshot);
        EXPECT_NO_THROW(block->getPositionByName(MutableSupport::delmark_column_name));
        EXPECT_THROW(block->getPositionByName(MutableSupport::version_column_name), Exception);
        ASSERT_EQ(block->columns(), 2);
    }
}
CATCH

} // namespace DB::tests