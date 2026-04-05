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

#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/StoragePool/GlobalPageIdAllocator.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/DeltaMerge/WriteBatchesImpl.h>
#include <Storages/PathPool.h>
#include <TestUtils/TiFlashStorageTestBasic.h>

namespace DB::DM::tests
{
class WriteBatchesTest : public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        storage_path_pool
            = std::make_shared<StoragePathPool>(db_context->getPathPool().withTable("test", "t_100", false));
        page_id_allocator = std::make_shared<GlobalPageIdAllocator>();
        storage_pool = std::make_shared<StoragePool>(
            *db_context,
            NullspaceID,
            /*ns_id*/ 100,
            *storage_path_pool,
            page_id_allocator,
            "test.t_100");
    }

protected:
    GlobalPageIdAllocatorPtr page_id_allocator;
    std::shared_ptr<StoragePathPool> storage_path_pool;
    std::shared_ptr<StoragePool> storage_pool;
};

TEST_F(WriteBatchesTest, Commit)
{
    WriteBatches write_batches(*storage_pool);
    ASSERT_TRUE(write_batches.checkEmpty());

    // Add some writes
    std::string_view page1_data = "data1";
    std::string_view page2_data = "data2";
    write_batches.log.putPage(1, 0, page1_data);
    write_batches.data.putPage(2, 0, page2_data);

    // prepare the changes on the "meta"
    std::string_view page3_data = "data3";
    write_batches.meta.putPage(3, 0, page3_data);

    // Commit the writes
    write_batches.writeLogAndData();
    write_batches.writeMeta();
    ASSERT_TRUE(write_batches.checkEmpty());
}

TEST_F(WriteBatchesTest, CommitWithRemove)
{
    WriteBatches write_batches(*storage_pool);
    ASSERT_TRUE(write_batches.checkEmpty());

    // Add some writes
    std::string_view page1_data = "data1";
    std::string_view page2_data = "data2";
    write_batches.log.putPage(1, 0, page1_data);
    write_batches.data.putPage(2, 0, page2_data);
    write_batches.removed_log.delPage(4);
    write_batches.removed_data.delPage(5);

    // prepare the changes on the "meta"
    std::string_view page3_data = "data3";
    write_batches.meta.putPage(3, 0, page3_data);

    // Commit the writes
    write_batches.writeLogAndData();
    write_batches.writeMeta();
    // Commit the removes
    write_batches.writeRemoves();
    ASSERT_TRUE(write_batches.checkEmpty());
}

TEST_F(WriteBatchesTest, Rollback)
{
    WriteBatches write_batches(*storage_pool);
    ASSERT_TRUE(write_batches.checkEmpty());

    // Add some writes
    std::string_view page1_data = "data1";
    std::string_view page2_data = "data2";
    write_batches.log.putPage(1, 0, page1_data);
    write_batches.data.putPage(2, 0, page2_data);
    write_batches.removed_log.delPage(4);
    write_batches.removed_data.delPage(5);

    write_batches.writeLogAndData();

    // Detect conflict before writing "meta", rollback the writes
    write_batches.rollbackWrittenLogAndData();
    ASSERT_TRUE(write_batches.checkEmpty());
}
} // namespace DB::DM::tests
