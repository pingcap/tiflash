// Copyright 2022 PingCAP, Ltd.
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

#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/PathCapacityMetrics.h>
#include <Storages/PathPool.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/MockDiskDelegator.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

namespace DB
{
using namespace DM;
using namespace tests;
namespace PS::V3::tests
{
class PageStorageMixedTest : public DB::base::TiFlashStorageTestBasic
{
public:
    static void SetUpTestCase()
    {
        auto path = TiFlashTestEnv::getTemporaryPath("PageStorageMixedTestV3Path");
        dropDataOnDisk(path);
        createIfNotExist(path);

        std::vector<size_t> caps = {};
        Strings paths = {path};

        auto & global_context = TiFlashTestEnv::getGlobalContext();

        storage_path_pool_v3 = std::make_unique<PathPool>(Strings{path}, Strings{path}, Strings{}, std::make_shared<PathCapacityMetrics>(0, paths, caps, Strings{}, caps), global_context.getFileProvider(), true);

        global_context.setPageStorageRunMode(PageStorageRunMode::MIX_MODE);
        if (!global_context.getGlobalStoragePool())
            global_context.initializeGlobalStoragePoolIfNeed(*storage_path_pool_v3);
    }

    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        const auto & path = getTemporaryPath();
        createIfNotExist(path);

        auto & global_context = DB::tests::TiFlashTestEnv::getGlobalContext();

        std::vector<size_t> caps = {};
        Strings paths = {path};

        PathCapacityMetricsPtr cap_metrics = std::make_shared<PathCapacityMetrics>(0, paths, caps, Strings{}, caps);
        storage_path_pool_v2 = std::make_unique<StoragePathPool>(Strings{path}, Strings{path}, "test", "t1", true, cap_metrics, global_context.getFileProvider());

        global_context.setPageStorageRunMode(PageStorageRunMode::ONLY_V2);
        storage_pool_v2 = std::make_unique<StoragePool>(global_context, TEST_NAMESPACE_ID, *storage_path_pool_v2, "test.t1");

        global_context.setPageStorageRunMode(PageStorageRunMode::MIX_MODE);
        storage_pool_mix = std::make_unique<StoragePool>(global_context,
                                                         TEST_NAMESPACE_ID,
                                                         *storage_path_pool_v2,
                                                         "test.t1");

        reloadV2StoragePool();
    }

    PageStorageRunMode reloadMixedStoragePool()
    {
        DB::tests::TiFlashTestEnv::getContext().setPageStorageRunMode(PageStorageRunMode::MIX_MODE);
        PageStorageRunMode run_mode = storage_pool_mix->restore();
        page_writer_mix = storage_pool_mix->logWriter();
        page_reader_mix = storage_pool_mix->logReader();
        return run_mode;
    }

    void reloadV2StoragePool()
    {
        DB::tests::TiFlashTestEnv::getContext().setPageStorageRunMode(PageStorageRunMode::ONLY_V2);
        storage_pool_v2->restore();
        page_writer_v2 = storage_pool_v2->logWriter();
        page_reader_v2 = storage_pool_v2->logReader();
    }

protected:
    std::unique_ptr<StoragePathPool> storage_path_pool_v2;
    static std::unique_ptr<PathPool> storage_path_pool_v3;
    std::unique_ptr<StoragePool> storage_pool_v2;
    std::unique_ptr<StoragePool> storage_pool_mix;

    PageWriterPtr page_writer_v2;
    PageWriterPtr page_writer_mix;

    PageReaderPtr page_reader_v2;
    PageReaderPtr page_reader_mix;
};

std::unique_ptr<PathPool> PageStorageMixedTest::storage_path_pool_v3 = nullptr;

inline ::testing::AssertionResult getPageCompare(
    const char * /*buff_cmp_expr*/,
    const char * buf_size_expr,
    const char * /*page_cmp_expr*/,
    const char * page_id_expr,
    char * buff_cmp,
    const size_t buf_size,
    const Page & page_cmp,
    const PageId & page_id)
{
    if (page_cmp.data.size() != buf_size)
    {
        return testing::internal::EqFailure(
            DB::toString(buf_size).c_str(),
            DB::toString(page_cmp.data.size()).c_str(),
            buf_size_expr,
            "page.data.size()",
            false);
    }

    if (page_cmp.page_id != page_id)
    {
        return testing::internal::EqFailure(
            DB::toString(page_id).c_str(),
            DB::toString(page_cmp.page_id).c_str(),
            page_id_expr,
            "page.page_id",
            false);
    }

    if (strncmp(page_cmp.data.begin(), buff_cmp, buf_size) != 0)
    {
        return ::testing::AssertionFailure( //
            ::testing::Message(
                "Page data not match the buffer"));
    }

    return ::testing::AssertionSuccess();
}

#define ASSERT_PAGE_EQ(buff_cmp, buf_size, page_cmp, page_id) \
    ASSERT_PRED_FORMAT4(getPageCompare, buff_cmp, buf_size, page_cmp, page_id)
#define EXPECT_PAGE_EQ(buff_cmp, buf_size, page_cmp, page_id) \
    EXPECT_PRED_FORMAT4(getPageCompare, buff_cmp, buf_size, page_cmp, page_id)

TEST_F(PageStorageMixedTest, WriteRead)
try
{
    UInt64 tag = 0;
    const size_t buf_sz = 1024;
    char c_buff[buf_sz];
    for (size_t i = 0; i < buf_sz; ++i)
    {
        c_buff[i] = i % 0xff;
    }

    {
        WriteBatch batch;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(1, tag, buff, buf_sz);
        buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(2, tag, buff, buf_sz);
        page_writer_v2->write(std::move(batch), nullptr);
    }

    ASSERT_EQ(reloadMixedStoragePool(), PageStorageRunMode::MIX_MODE);
    {
        const auto & page1 = page_reader_mix->read(1);
        const auto & page2 = page_reader_mix->read(2);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page1, 1);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page2, 2);
    }

    {
        WriteBatch batch;
        const size_t buf_sz2 = 2048;
        char c_buff2[buf_sz2] = {0};

        ReadBufferPtr buff2 = std::make_shared<ReadBufferFromMemory>(c_buff2, sizeof(c_buff2));
        batch.putPage(3, tag, buff2, buf_sz2);
        page_writer_mix->write(std::move(batch), nullptr);

        const auto & page3 = page_reader_mix->read(3);
        ASSERT_PAGE_EQ(c_buff2, buf_sz2, page3, 3);
        reloadV2StoragePool();
        ASSERT_THROW(page_reader_v2->read(3), DB::Exception);
    }

    {
        // Revert v3
        WriteBatch batch;
        batch.delPage(3);
        page_writer_mix->write(std::move(batch), nullptr);
    }
}
CATCH

TEST_F(PageStorageMixedTest, Read)
try
{
    UInt64 tag = 0;
    const size_t buf_sz = 1024;
    char c_buff[buf_sz];
    for (size_t i = 0; i < buf_sz; ++i)
    {
        c_buff[i] = i % 0xff;
    }

    {
        WriteBatch batch;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(1, tag, buff, buf_sz);
        buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(2, tag, buff, buf_sz, {20, 120, 400, 200, 15, 75, 170, 24});
        buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(7, tag, buff, buf_sz, {500, 500, 24});
        page_writer_v2->write(std::move(batch), nullptr);
    }

    ASSERT_EQ(reloadMixedStoragePool(), PageStorageRunMode::MIX_MODE);
    {
        const auto & page1 = page_reader_mix->read(1);
        const auto & page2 = page_reader_mix->read(2);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page1, 1);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page2, 2);
    }

    const size_t buf_sz2 = 2048;
    char c_buff2[buf_sz2] = {0};
    {
        WriteBatch batch;
        ReadBufferPtr buff2 = std::make_shared<ReadBufferFromMemory>(c_buff2, sizeof(c_buff2));
        batch.putPage(3, tag, buff2, buf_sz2);
        buff2 = std::make_shared<ReadBufferFromMemory>(c_buff2, sizeof(c_buff2));
        batch.putPage(4, tag, buff2, buf_sz2, {20, 120, 400, 200, 15, 75, 170, 24, 500, 500, 24});
        page_writer_mix->write(std::move(batch), nullptr);
    }

    {
        PageIds page_ids = {1, 2, 3, 4};
        auto page_maps = page_reader_mix->read(page_ids);
        ASSERT_EQ(page_maps.size(), 4);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page_maps[1], 1);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page_maps[2], 2);
        ASSERT_PAGE_EQ(c_buff2, buf_sz2, page_maps[3], 3);
        ASSERT_PAGE_EQ(c_buff2, buf_sz2, page_maps[4], 4);

        // Read page ids which only exited in V2
        page_ids = {1, 2, 7};
        page_maps = page_reader_mix->read(page_ids);
        ASSERT_EQ(page_maps.size(), 3);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page_maps[1], 1);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page_maps[2], 2);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page_maps[7], 7);
    }

    {
        PageIds page_ids = {1, 2, 3, 4};
        PageHandler hander = [](DB::PageId /*page_id*/, const Page & /*page*/) {
        };
        ASSERT_NO_THROW(page_reader_mix->read(page_ids, hander));

        // Read page ids which only exited in V2
        page_ids = {1, 2, 7};
        ASSERT_NO_THROW(page_reader_mix->read(page_ids, hander));
    }

    {
        std::vector<PageStorage::PageReadFields> read_fields;
        read_fields.emplace_back(std::make_pair<PageId, PageStorage::FieldIndices>(2, {1, 3, 6}));
        read_fields.emplace_back(std::make_pair<PageId, PageStorage::FieldIndices>(4, {1, 3, 4, 8, 10}));
        read_fields.emplace_back(std::make_pair<PageId, PageStorage::FieldIndices>(7, {0, 1, 2}));
        PageMap page_maps = page_reader_mix->read(read_fields);
        ASSERT_EQ(page_maps.size(), 3);
        ASSERT_EQ(page_maps[2].page_id, 2);
        ASSERT_EQ(page_maps[2].field_offsets.size(), 3);
        ASSERT_EQ(page_maps[4].page_id, 4);
        ASSERT_EQ(page_maps[4].field_offsets.size(), 5);
        ASSERT_EQ(page_maps[7].page_id, 7);
        ASSERT_EQ(page_maps[7].field_offsets.size(), 3);
    }

    {
        // Read page ids which only exited in V2
        std::vector<PageStorage::PageReadFields> read_fields;
        read_fields.emplace_back(std::make_pair<PageId, PageStorage::FieldIndices>(2, {1, 3, 6}));
        ASSERT_NO_THROW(page_reader_mix->read(read_fields));
    }

    {
        // Revert v3
        WriteBatch batch;
        batch.delPage(3);
        batch.delPage(4);
        page_writer_mix->write(std::move(batch), nullptr);
    }
}
CATCH

TEST_F(PageStorageMixedTest, ReadWithSnapshot)
try
{
    UInt64 tag = 0;
    const size_t buf_sz = 1024;
    char c_buff[buf_sz];
    for (size_t i = 0; i < buf_sz; ++i)
    {
        c_buff[i] = i % 0xff;
    }

    {
        WriteBatch batch;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(1, tag, buff, buf_sz);
        buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(2, tag, buff, buf_sz, {20, 120, 400, 200, 15, 75, 170, 24});
        page_writer_v2->write(std::move(batch), nullptr);
    }

    ASSERT_EQ(reloadMixedStoragePool(), PageStorageRunMode::MIX_MODE);
    const size_t buf_sz2 = 2048;
    char c_buff2[buf_sz2] = {0};
    {
        WriteBatch batch;
        ReadBufferPtr buff2 = std::make_shared<ReadBufferFromMemory>(c_buff2, sizeof(c_buff2));
        batch.putPage(3, tag, buff2, buf_sz2);
        page_writer_mix->write(std::move(batch), nullptr);
    }

    auto snapshot_mix = page_reader_mix->getSnapshot("ReadWithSnapshotTest");

    {
        auto page_reader_mix_with_snap = storage_pool_mix->newLogReader(nullptr, snapshot_mix);

        const auto & page1 = page_reader_mix_with_snap.read(1);
        const auto & page2 = page_reader_mix_with_snap.read(2);
        const auto & page3 = page_reader_mix_with_snap.read(3);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page1, 1);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page2, 2);
        ASSERT_PAGE_EQ(c_buff2, buf_sz2, page3, 3);
    }

    {
        auto page_reader_mix_with_snap = storage_pool_mix->newLogReader(nullptr, true, "ReadWithSnapshotTest");
        const auto & page1 = page_reader_mix_with_snap.read(1);
        const auto & page2 = page_reader_mix_with_snap.read(2);
        const auto & page3 = page_reader_mix_with_snap.read(3);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page1, 1);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page2, 2);
        ASSERT_PAGE_EQ(c_buff2, buf_sz2, page3, 3);
    }

    {
        WriteBatch batch;
        ReadBufferPtr buff2 = std::make_shared<ReadBufferFromMemory>(c_buff2, sizeof(c_buff2));
        batch.putPage(4, tag, buff2, buf_sz2);
        page_writer_mix->write(std::move(batch), nullptr);
    }
    {
        auto page_reader_mix_with_snap = storage_pool_mix->newLogReader(nullptr, snapshot_mix);
        ASSERT_THROW(page_reader_mix_with_snap.read(4), DB::Exception);
    }

    {
        // Revert v3
        WriteBatch batch;
        batch.delPage(3);
        batch.delPage(4);
        page_writer_mix->write(std::move(batch), nullptr);
    }
}
CATCH


TEST_F(PageStorageMixedTest, PutExt)
try
{
    {
        WriteBatch batch;
        batch.putExternal(1, 0);
        batch.putExternal(2, 0);
        batch.putExternal(3, 0);
        page_writer_v2->write(std::move(batch), nullptr);
    }

    ASSERT_EQ(reloadMixedStoragePool(), PageStorageRunMode::MIX_MODE);
}
CATCH


TEST_F(PageStorageMixedTest, Del)
try
{
    const size_t buf_sz = 1024;
    char c_buff[buf_sz] = {0};

    {
        WriteBatch batch;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(1, 0, buff, buf_sz);
        page_writer_v2->write(std::move(batch), nullptr);
    }

    ASSERT_EQ(reloadMixedStoragePool(), PageStorageRunMode::MIX_MODE);

    {
        WriteBatch batch;
        batch.delPage(1);
        ASSERT_NO_THROW(page_writer_mix->write(std::move(batch), nullptr));
    }

    reloadV2StoragePool();
    ASSERT_THROW(page_reader_v2->read(1), DB::Exception);
}
CATCH

TEST_F(PageStorageMixedTest, Ref)
try
{
    const size_t buf_sz = 1024;
    char c_buff[buf_sz] = {0};

    {
        WriteBatch batch;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(7, 0, buff, buf_sz);
        buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(8, 0, buff, buf_sz, {20, 120, 400, 200, 15, 75, 170, 24});
        page_writer_v2->write(std::move(batch), nullptr);
    }

    {
        ASSERT_EQ(reloadMixedStoragePool(), PageStorageRunMode::MIX_MODE);
        const auto & entry = page_reader_mix->getPageEntry(8);
        ASSERT_EQ(entry.field_offsets.size(), 8);
    }

    {
        WriteBatch batch;
        batch.putRefPage(9, 7);
        // ASSERT_NO_THROW(page_writer_mix->write(std::move(batch), nullptr));
        page_writer_mix->write(std::move(batch), nullptr);
        ASSERT_EQ(page_reader_mix->getNormalPageId(9), 7);
    }

    reloadV2StoragePool();
    ASSERT_THROW(page_reader_v2->read(9), DB::Exception);

    {
        WriteBatch batch;
        batch.putRefPage(10, 8);

        ASSERT_NO_THROW(page_writer_mix->write(std::move(batch), nullptr));
        ASSERT_EQ(page_reader_mix->getNormalPageId(10), 8);

        std::vector<PageStorage::PageReadFields> read_fields;
        read_fields.emplace_back(std::pair<PageId, PageStorage::FieldIndices>(10, {0, 1, 2, 6}));

        PageMap page_maps = page_reader_mix->read(read_fields);
        ASSERT_EQ(page_maps.size(), 1);
        ASSERT_EQ(page_maps[10].page_id, 10);
        ASSERT_EQ(page_maps[10].field_offsets.size(), 4);
        ASSERT_EQ(page_maps[10].data.size(), 710);

        auto field_offset = page_maps[10].field_offsets;
        auto it = field_offset.begin();
        ASSERT_EQ(it->offset, 0);
        ++it;
        ASSERT_EQ(it->offset, 20);
        ++it;
        ASSERT_EQ(it->offset, 140);
        ++it;
        ASSERT_EQ(it->offset, 540);
    }

    {
        // Revert v3
        WriteBatch batch;
        batch.delPage(9);
        batch.delPage(10);
        page_writer_mix->write(std::move(batch), nullptr);
    }
}
CATCH


TEST_F(PageStorageMixedTest, MockDTIngest)
try
{
    {
        WriteBatch batch;
        batch.putExternal(100, 0);
        page_writer_v2->write(std::move(batch), nullptr);
    }

    ASSERT_EQ(reloadMixedStoragePool(), PageStorageRunMode::MIX_MODE);
    {
        // create dmf_1999
        // ingest to segment, create ref 2001 -> 1999
        // after ingest done, del 1999
        WriteBatch batch;
        batch.putExternal(1999, 0);
        batch.putRefPage(2001, 1999);
        batch.delPage(1999);
        ASSERT_NO_THROW(page_writer_mix->write(std::move(batch), nullptr));
    }

    {
        // mock that create ref by dtfile id, should fail
        WriteBatch batch;
        batch.putRefPage(2012, 1999);
        ASSERT_ANY_THROW(page_writer_mix->write(std::move(batch), nullptr));
    }

    {
        // mock that create ref by page id of dtfile, should be ok
        WriteBatch batch;
        batch.putRefPage(2012, 2001);
        ASSERT_NO_THROW(page_writer_mix->write(std::move(batch), nullptr));
    }

    // check 2012 -> 2001 => 2021 -> 1999
    ASSERT_EQ(page_reader_mix->getNormalPageId(2012), 1999);

    {
        // Revert v3
        WriteBatch batch;
        batch.delPage(2012);
        batch.delPage(2001);
        page_writer_mix->write(std::move(batch), nullptr);
    }
}
CATCH


TEST_F(PageStorageMixedTest, RefV2External)
try
{
    {
        WriteBatch batch;
        batch.putExternal(100, 0);
        batch.putRefPage(101, 100);
        batch.delPage(100);
        page_writer_v2->write(std::move(batch), nullptr);
    }

    ASSERT_EQ(reloadMixedStoragePool(), PageStorageRunMode::MIX_MODE);
    {
        WriteBatch batch;
        batch.putRefPage(102, 101);
        // Should not run into this case after we introduce `StoragePool::forceTransformDataV2toV3`
        ASSERT_ANY_THROW(page_writer_mix->write(std::move(batch), nullptr););
    }
    {
        // Revert v3
        WriteBatch batch;
        batch.delPage(102);
        page_writer_mix->write(std::move(batch), nullptr);
    }
}
CATCH


} // namespace PS::V3::tests
} // namespace DB
