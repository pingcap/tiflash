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

#include <Interpreters/Context.h>
#include <Poco/Logger.h>
#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/WriteBatchImpl.h>
#include <Storages/Page/WriteBatchWrapperImpl.h>
#include <Storages/PathCapacityMetrics.h>
#include <Storages/PathPool.h>
#include <TestUtils/MockDiskDelegator.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/logger_useful.h>
#include <fmt/ranges.h>
#include <gtest/gtest.h>

namespace DB::FailPoints
{
extern const char force_set_dtfile_exist_when_acquire_id[];
} // namespace DB::FailPoints

namespace DB
{
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

        storage_path_pool_v3 = std::make_unique<PathPool>(Strings{path}, Strings{path}, Strings{}, std::make_shared<PathCapacityMetrics>(0, paths, caps, Strings{}, caps), global_context.getFileProvider());

        global_context.setPageStorageRunMode(PageStorageRunMode::MIX_MODE);
    }

    void SetUp() override
    {
        auto & global_context = DB::tests::TiFlashTestEnv::getGlobalContext();
        global_context.setPageStorageRunMode(PageStorageRunMode::MIX_MODE);
        TiFlashStorageTestBasic::SetUp();
        const auto & path = getTemporaryPath();
        createIfNotExist(path);


        std::vector<size_t> caps = {};
        Strings paths = {path};

        PathCapacityMetricsPtr cap_metrics = std::make_shared<PathCapacityMetrics>(0, paths, caps, Strings{}, caps);
        storage_path_pool_v2 = std::make_unique<StoragePathPool>(Strings{path}, Strings{path}, "test", "t1", true, cap_metrics, global_context.getFileProvider());

        global_context.setPageStorageRunMode(PageStorageRunMode::ONLY_V2);
        storage_pool_v2 = std::make_unique<DM::StoragePool>(global_context, NullspaceID, TEST_NAMESPACE_ID, *storage_path_pool_v2, "test.t1");

        global_context.setPageStorageRunMode(PageStorageRunMode::MIX_MODE);
        storage_pool_mix = std::make_unique<DM::StoragePool>(*db_context,
                                                             NullspaceID,
                                                             TEST_NAMESPACE_ID,
                                                             *storage_path_pool_v2,
                                                             "test.t1");

        reloadV2StoragePool();
    }

    PageStorageRunMode reloadMixedStoragePool()
    {
        db_context->setPageStorageRunMode(PageStorageRunMode::MIX_MODE);
        PageStorageRunMode run_mode = storage_pool_mix->restore();
        page_writer_mix = storage_pool_mix->logWriter();
        page_reader_mix = storage_pool_mix->logReader();
        return run_mode;
    }

    PageReaderPtr newMixedPageReader(PageStorage::SnapshotPtr & snapshot)
    {
        return storage_pool_mix->newLogReader(nullptr, snapshot);
    }

    PageReaderPtr newMixedPageReader()
    {
        return storage_pool_mix->newLogReader(nullptr, true, "PageStorageMixedTest");
    }

    void reloadV2StoragePool()
    {
        db_context->setPageStorageRunMode(PageStorageRunMode::ONLY_V2);
        storage_pool_v2->restore();
        page_writer_v2 = storage_pool_v2->logWriter();
        page_reader_v2 = storage_pool_v2->logReader();
    }

protected:
    std::unique_ptr<StoragePathPool> storage_path_pool_v2;
    static std::unique_ptr<PathPool> storage_path_pool_v3;
    std::unique_ptr<DM::StoragePool> storage_pool_v2;
    std::unique_ptr<DM::StoragePool> storage_pool_mix;

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
    const PageIdU64 & page_id)
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
        std::vector<PageIdU64> page_ids = {1, 2, 3, 4};
        auto page_maps = page_reader_mix->read(page_ids);
        ASSERT_EQ(page_maps.size(), 4);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page_maps.at(1), 1);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page_maps.at(2), 2);
        ASSERT_PAGE_EQ(c_buff2, buf_sz2, page_maps.at(3), 3);
        ASSERT_PAGE_EQ(c_buff2, buf_sz2, page_maps.at(4), 4);

        // Read page ids which only exited in V2
        page_ids = {1, 2, 7};
        page_maps = page_reader_mix->read(page_ids);
        ASSERT_EQ(page_maps.size(), 3);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page_maps.at(1), 1);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page_maps.at(2), 2);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page_maps.at(7), 7);
    }

    {
        std::vector<PageStorage::PageReadFields> read_fields;
        read_fields.emplace_back(std::pair<PageIdU64, PageStorage::FieldIndices>(2, {1, 3, 6}));
        read_fields.emplace_back(std::pair<PageIdU64, PageStorage::FieldIndices>(4, {1, 3, 4, 8, 10}));
        read_fields.emplace_back(std::pair<PageIdU64, PageStorage::FieldIndices>(7, {0, 1, 2}));
        PageMapU64 page_maps = page_reader_mix->read(read_fields);
        ASSERT_EQ(page_maps.size(), 3);
        ASSERT_EQ(page_maps.at(2).page_id, 2);
        ASSERT_EQ(page_maps.at(2).field_offsets.size(), 3);
        ASSERT_EQ(page_maps.at(4).page_id, 4);
        ASSERT_EQ(page_maps.at(4).field_offsets.size(), 5);
        ASSERT_EQ(page_maps.at(7).page_id, 7);
        ASSERT_EQ(page_maps.at(7).field_offsets.size(), 3);
    }

    {
        // Read page ids which only exited in V2
        std::vector<PageStorage::PageReadFields> read_fields;
        read_fields.emplace_back(std::pair<PageIdU64, PageStorage::FieldIndices>(2, {1, 3, 6}));
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

        const auto & page1 = page_reader_mix_with_snap->read(1);
        const auto & page2 = page_reader_mix_with_snap->read(2);
        const auto & page3 = page_reader_mix_with_snap->read(3);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page1, 1);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page2, 2);
        ASSERT_PAGE_EQ(c_buff2, buf_sz2, page3, 3);
    }

    {
        auto page_reader_mix_with_snap = storage_pool_mix->newLogReader(nullptr, true, "ReadWithSnapshotTest");
        const auto & page1 = page_reader_mix_with_snap->read(1);
        const auto & page2 = page_reader_mix_with_snap->read(2);
        const auto & page3 = page_reader_mix_with_snap->read(3);
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
        ASSERT_THROW(page_reader_mix_with_snap->read(4), DB::Exception);
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
        read_fields.emplace_back(std::pair<PageIdU64, PageStorage::FieldIndices>(10, {0, 1, 2, 6}));

        PageMapU64 page_maps = page_reader_mix->read(read_fields);
        ASSERT_EQ(page_maps.size(), 1);
        ASSERT_EQ(page_maps.at(10).page_id, 10);
        ASSERT_EQ(page_maps.at(10).field_offsets.size(), 4);
        ASSERT_EQ(page_maps.at(10).data.size(), 710);

        auto field_offset = page_maps.at(10).field_offsets;
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

// v2 put 1, v2 ref 2->1, get snapshot s1, v3 del 1, read s1
TEST_F(PageStorageMixedTest, RefWithSnapshot)
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

    {
        WriteBatch batch;
        batch.putRefPage(2, 1);
        page_writer_v2->write(std::move(batch), nullptr);
    }

    {
        ASSERT_EQ(reloadMixedStoragePool(), PageStorageRunMode::MIX_MODE);
        ASSERT_EQ(page_reader_mix->getNormalPageId(2), 1);
    }

    auto snapshot_mix_mode = page_reader_mix->getSnapshot("ReadWithSnapshotAfterDelOrigin");
    {
        auto page_reader_mix_with_snap = storage_pool_mix->newLogReader(nullptr, snapshot_mix_mode);
        const auto & page1 = page_reader_mix_with_snap->read(1);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page1, 1);
    }

    {
        WriteBatch batch;
        batch.delPage(1);
        page_writer_mix->write(std::move(batch), nullptr);
    }

    {
        auto page_reader_mix_with_snap = storage_pool_mix->newLogReader(nullptr, snapshot_mix_mode);
        const auto & page1 = page_reader_mix_with_snap->read(1);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page1, 1);
    }
}
CATCH

// v2 put 1, v2 ref 2->1, get snapshot s1, v3 del 1, v3 del 2, read s1
TEST_F(PageStorageMixedTest, RefWithDelSnapshot)
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

    {
        WriteBatch batch;
        batch.putRefPage(2, 1);
        page_writer_v2->write(std::move(batch), nullptr);
    }

    {
        ASSERT_EQ(reloadMixedStoragePool(), PageStorageRunMode::MIX_MODE);
        ASSERT_EQ(page_reader_mix->getNormalPageId(2), 1);
    }

    auto snapshot_mix_mode = page_reader_mix->getSnapshot("ReadWithSnapshotAfterDelOrigin");
    {
        auto page_reader_mix_with_snap = storage_pool_mix->newLogReader(nullptr, snapshot_mix_mode);
        const auto & page1 = page_reader_mix_with_snap->read(1);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page1, 1);
    }

    {
        WriteBatch batch;
        batch.delPage(1);
        batch.delPage(2);
        page_writer_mix->write(std::move(batch), nullptr);
    }

    {
        auto page_reader_mix_with_snap = storage_pool_mix->newLogReader(nullptr, snapshot_mix_mode);
        const auto & page1 = page_reader_mix_with_snap->read(1);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page1, 1);
    }
}
CATCH

// v2 put 1, v2 ref 2->1, v3 del 1, get snapshot s1, v3 del 2, use s1 read 2
TEST_F(PageStorageMixedTest, RefWithDelSnapshot2)
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

    {
        WriteBatch batch;
        batch.putRefPage(2, 1);
        page_writer_v2->write(std::move(batch), nullptr);
    }

    {
        ASSERT_EQ(reloadMixedStoragePool(), PageStorageRunMode::MIX_MODE);
        ASSERT_EQ(page_reader_mix->getNormalPageId(2), 1);
    }

    {
        WriteBatch batch;
        batch.delPage(1);
        page_writer_mix->write(std::move(batch), nullptr);
    }

    auto snapshot_mix_mode = page_reader_mix->getSnapshot("ReadWithSnapshotAfterDelOrigin");
    {
        auto page_reader_mix_with_snap = storage_pool_mix->newLogReader(nullptr, snapshot_mix_mode);
        const auto & page1 = page_reader_mix_with_snap->read(2);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page1, 2);
    }

    {
        WriteBatch batch;
        batch.delPage(2);
        page_writer_mix->write(std::move(batch), nullptr);
    }

    {
        auto page_reader_mix_with_snap = storage_pool_mix->newLogReader(nullptr, snapshot_mix_mode);
        const auto & page1 = page_reader_mix_with_snap->read(2);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page1, 2);
    }
}
CATCH

// v2 put 1, v2 del 1, v3 put 2, v3 del 2
TEST_F(PageStorageMixedTest, GetMaxId)
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

    {
        WriteBatch batch;
        batch.delPage(1);
        page_writer_v2->write(std::move(batch), nullptr);
    }

    {
        WriteBatch batch;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(2, 0, buff, buf_sz);
        page_writer_v2->write(std::move(batch), nullptr);
    }

    {
        ASSERT_EQ(reloadMixedStoragePool(), PageStorageRunMode::MIX_MODE);
        ASSERT_EQ(storage_pool_mix->newLogPageId(), 3);
    }

    {
        WriteBatch batch;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(3, 0, buff, buf_sz);
        page_writer_mix->write(std::move(batch), nullptr);
    }

    {
        WriteBatch batch;
        batch.delPage(3);
        page_writer_mix->write(std::move(batch), nullptr);
    }

    {
        ASSERT_EQ(reloadMixedStoragePool(), PageStorageRunMode::MIX_MODE);
        //        ASSERT_EQ(storage_pool_mix->newLogPageId(), 4); // max id for v3 will not be updated, ignore this check
    }
}
CATCH

TEST_F(PageStorageMixedTest, GetMaxIdAfterUpgraded)
try
{
    const size_t buf_sz = 1024;
    char c_buff[buf_sz] = {0};

    const PageIdU64 max_data_page_id_allocated = (1 << 30) + 1;
    const PageIdU64 max_meta_page_id_allocated = (1 << 28) + 1;
    {
        // Prepare a StoragePool with
        // - 0 pages in "log" (must be 0)
        // - some pages in "data" with `max_data_page_id_allocated`
        // - some pages in "meta" with `max_meta_page_id_allocated`
        {
            WriteBatch batch;
            ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
            batch.putPage(1, 0, buff, buf_sz);
            batch.putRefPage(2, 1);
            ReadBufferPtr buff2 = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
            batch.putPage(1 << 30, 0, buff2, buf_sz);
            batch.putRefPage(max_data_page_id_allocated, 1 << 30);
            storage_pool_v2->dataWriter()->write(std::move(batch), nullptr);
        }
        {
            WriteBatch batch;
            ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
            batch.putPage(max_meta_page_id_allocated, 0, buff, buf_sz);
            storage_pool_v2->metaWriter()->write(std::move(batch), nullptr);
        }
    }

    // Mock that after upgraded, the run_mode is transformed to `ONLY_V3`
    ASSERT_EQ(reloadMixedStoragePool(), PageStorageRunMode::ONLY_V3);

    // Disable some failpoint to avoid it affect the allocated id
    DB::FailPointHelper::disableFailPoint(DB::FailPoints::force_set_dtfile_exist_when_acquire_id);
    SCOPE_EXIT({ DB::FailPointHelper::enableFailPoint(DB::FailPoints::force_set_dtfile_exist_when_acquire_id); });

    // Allocate new id for "data" should be larger than `max_data_page_id_allocated`
    auto d = storage_path_pool_v2->getStableDiskDelegator();
    EXPECT_GT(storage_pool_mix->newDataPageIdForDTFile(d, "GetMaxIdAfterUpgraded"), max_data_page_id_allocated);

    // Allocate new id for "meta" should be larger than `max_meta_page_id_allocated`
    EXPECT_GT(storage_pool_mix->newMetaPageId(), max_meta_page_id_allocated);
}
CATCH

TEST_F(PageStorageMixedTest, ReuseV2ID)
try
{
    const size_t buf_sz = 1024;
    char c_buff[buf_sz] = {0};
    {
        {
            WriteBatch batch;
            ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
            batch.putPage(1, 0, buff, buf_sz);
            page_writer_v2->write(std::move(batch), nullptr);
        }
        {
            WriteBatch batch;
            batch.delPage(1);
            page_writer_v2->write(std::move(batch), nullptr);
        }
    }

    ASSERT_EQ(reloadMixedStoragePool(), PageStorageRunMode::ONLY_V3);
    ASSERT_EQ(storage_pool_mix->newLogPageId(), 2);

    {
        WriteBatch batch;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(1, 0, buff, buf_sz);
        page_writer_mix->write(std::move(batch), nullptr);
    }

    {
        WriteBatch batch;
        batch.delPage(1);
        page_writer_mix->write(std::move(batch), nullptr);
    }

    ASSERT_EQ(reloadMixedStoragePool(), PageStorageRunMode::ONLY_V3);
    //        ASSERT_EQ(storage_pool_mix->newLogPageId(), 2); // max id for v3 will not be updated, ignore this check
}
CATCH

// v2 put 1, v3 ref 2->1, reload, check max id, get snapshot s1, v3 del 1, get snapshot s2, v3 del 2, get snapshot s3, check snapshots
TEST_F(PageStorageMixedTest, V3RefV2WithSnapshot)
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
        batch.putRefPage(2, 1);
        page_writer_mix->write(std::move(batch), nullptr);
    }

    {
        ASSERT_EQ(reloadMixedStoragePool(), PageStorageRunMode::MIX_MODE);
        ASSERT_EQ(page_reader_mix->getNormalPageId(2), 1);
        //        ASSERT_EQ(storage_pool_mix->newLogPageId(), 3); // max id for v3 will not be updated, ignore this check
    }

    auto snapshot_before_del = page_reader_mix->getSnapshot("ReadWithSnapshotBeforeDelOrigin");

    {
        WriteBatch batch;
        batch.delPage(1);
        page_writer_mix->write(std::move(batch), nullptr);
    }

    auto snapshot_after_del_origin = page_reader_mix->getSnapshot("ReadWithSnapshotAfterDelOrigin");

    {
        auto page_reader_mix_with_snap = storage_pool_mix->newLogReader(nullptr, snapshot_before_del);
        const auto & page1 = page_reader_mix_with_snap->read(1);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page1, 1);
    }

    {
        WriteBatch batch;
        batch.delPage(2);
        page_writer_mix->write(std::move(batch), nullptr);
    }

    auto snapshot_after_del_all = page_reader_mix->getSnapshot("ReadWithSnapshotAfterDelAll");

    {
        auto page_reader_mix_with_snap = storage_pool_mix->newLogReader(nullptr, snapshot_after_del_origin);
        const auto & page1 = page_reader_mix_with_snap->read(2);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page1, 2);
    }

    {
        auto page_reader_mix_with_snap = storage_pool_mix->newLogReader(nullptr, snapshot_after_del_origin);
        const auto & page1 = page_reader_mix_with_snap->read(2);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page1, 2);
    }

    {
        auto page_reader_mix_with_snap = storage_pool_mix->newLogReader(nullptr, snapshot_after_del_all);
        ASSERT_ANY_THROW(page_reader_mix_with_snap->read(2));
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


TEST_F(PageStorageMixedTest, RefV2External2)
try
{
    auto logger = DB::Logger::get("PageStorageMixedTest");
    {
        WriteBatch batch;
        batch.putExternal(100, 0);
        batch.putRefPage(101, 100);
        batch.delPage(100);
        batch.putExternal(102, 0);
        page_writer_v2->write(std::move(batch), nullptr);
    }

    ASSERT_EQ(reloadMixedStoragePool(), PageStorageRunMode::MIX_MODE);
    {
        WriteBatch batch;
        batch.putExternal(100, 0);
        batch.putRefPage(101, 100);
        batch.delPage(100);
        batch.putExternal(102, 0);
        page_writer_mix->writeIntoV3(std::move(batch), nullptr);
    }
    {
        auto snap = storage_pool_mix->log_storage_v2->getSnapshot("zzz"); // must hold
        // after transform to v3, delete these from v2
        WriteBatch batch;
        batch.delPage(100);
        batch.delPage(101);
        batch.delPage(102);
        page_writer_mix->writeIntoV2(std::move(batch), nullptr);
    }

    {
        LOG_INFO(logger, "first check alive id in v2");
        auto alive_dt_ids_in_v2 = storage_pool_mix->log_storage_v2->getAliveExternalPageIds(TEST_NAMESPACE_ID);
        EXPECT_EQ(alive_dt_ids_in_v2.size(), 0);

        storage_pool_mix->log_storage_v3->gc(false, nullptr, nullptr);
        auto alive_dt_ids_in_v3 = storage_pool_mix->log_storage_v3->getAliveExternalPageIds(TEST_NAMESPACE_ID);
        ASSERT_EQ(alive_dt_ids_in_v3.size(), 2);
        auto iter = alive_dt_ids_in_v3.begin();
        EXPECT_EQ(*iter, 100);
        iter++;
        EXPECT_EQ(*iter, 102);
    }

    {
        LOG_INFO(logger, "remove 100, create 105");
        DM::StorageSnapshot snap(*storage_pool_mix, nullptr, "xxx", true); // must hold and write
        // write delete again
        WriteBatch batch;
        batch.delPage(100);
        batch.putExternal(105, 0);
        page_writer_mix->write(std::move(batch), nullptr);
        LOG_INFO(logger, "done");
    }
    {
        LOG_INFO(logger, "remove 101, create 106");
        DM::StorageSnapshot snap(*storage_pool_mix, nullptr, "xxx", true); // must hold and write
        // write delete again
        WriteBatch batch;
        batch.delPage(101);
        batch.putExternal(106, 0);
        page_writer_mix->write(std::move(batch), nullptr);
        LOG_INFO(logger, "done");
    }
    {
        LOG_INFO(logger, "remove 102, create 107");
        DM::StorageSnapshot snap(*storage_pool_mix, nullptr, "xxx", true); // must hold and write
        // write delete again
        WriteBatch batch;
        batch.delPage(102);
        batch.putExternal(107, 0);
        page_writer_mix->write(std::move(batch), nullptr);
        LOG_INFO(logger, "done");
    }

    {
        LOG_INFO(logger, "second check alive id in v2");
        auto alive_dt_ids_in_v2 = storage_pool_mix->log_storage_v2->getAliveExternalPageIds(TEST_NAMESPACE_ID);
        EXPECT_EQ(alive_dt_ids_in_v2.size(), 0) << fmt::format("{}", alive_dt_ids_in_v2);

        storage_pool_mix->log_storage_v3->gc(false, nullptr, nullptr);
        auto alive_dt_ids_in_v3 = storage_pool_mix->log_storage_v3->getAliveExternalPageIds(TEST_NAMESPACE_ID);
        ASSERT_EQ(alive_dt_ids_in_v3.size(), 3) << fmt::format("{}", alive_dt_ids_in_v3);
        auto iter = alive_dt_ids_in_v3.begin();
        EXPECT_EQ(*iter, 105);
        iter++;
        EXPECT_EQ(*iter, 106);
        iter++;
        EXPECT_EQ(*iter, 107);
    }
    {
        LOG_INFO(logger, "third check alive id in v2");
        auto alive_dt_ids_in_v2 = storage_pool_mix->log_storage_v2->getAliveExternalPageIds(TEST_NAMESPACE_ID);
        EXPECT_EQ(alive_dt_ids_in_v2.size(), 0) << fmt::format("{}", alive_dt_ids_in_v2);

        storage_pool_mix->log_storage_v3->gc(false, nullptr, nullptr);
        auto alive_dt_ids_in_v3 = storage_pool_mix->log_storage_v3->getAliveExternalPageIds(TEST_NAMESPACE_ID);
        ASSERT_EQ(alive_dt_ids_in_v3.size(), 3) << fmt::format("{}", alive_dt_ids_in_v3);
        auto iter = alive_dt_ids_in_v3.begin();
        EXPECT_EQ(*iter, 105);
        iter++;
        EXPECT_EQ(*iter, 106);
        iter++;
        EXPECT_EQ(*iter, 107);
    }

    {
        // cleanup v3
        WriteBatch batch;
        batch.delPage(100);
        batch.delPage(101);
        batch.delPage(102);
        batch.delPage(105);
        batch.delPage(106);
        batch.delPage(107);
        page_writer_mix->write(std::move(batch), nullptr);
    }
}
CATCH

TEST_F(PageStorageMixedTest, ReadWithSnapshotAfterMergeDelta)
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
    // Thread A create snapshot for read
    auto snapshot_mix_before_merge_delta = page_reader_mix->getSnapshot("ReadWithSnapshotAfterMergeDelta");
    {
        auto page_reader_mix_with_snap = newMixedPageReader(snapshot_mix_before_merge_delta);
        const auto & page1 = page_reader_mix_with_snap->read(1);
        const auto & page2 = page_reader_mix_with_snap->read(2);
        const auto & page3 = page_reader_mix_with_snap->read(3);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page1, 1);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page2, 2);
        ASSERT_PAGE_EQ(c_buff2, buf_sz2, page3, 3);
    }
    {
        auto page_reader_mix_with_snap = newMixedPageReader();
        const auto & page1 = page_reader_mix_with_snap->read(1);
        const auto & page2 = page_reader_mix_with_snap->read(2);
        const auto & page3 = page_reader_mix_with_snap->read(3);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page1, 1);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page2, 2);
        ASSERT_PAGE_EQ(c_buff2, buf_sz2, page3, 3);
    }
    // Thread B apply merge delta, create page 4, and delete the origin page 1, 3
    {
        WriteBatch batch;
        ReadBufferPtr buff2 = std::make_shared<ReadBufferFromMemory>(c_buff2, sizeof(c_buff2));
        batch.putPage(4, tag, buff2, buf_sz2);
        batch.delPage(1);
        batch.delPage(3);
        page_writer_mix->write(std::move(batch), nullptr);
    }
    // Thread A continue to read 1, 3
    {
        auto page_reader_mix_with_snap = newMixedPageReader(snapshot_mix_before_merge_delta);
        // read 1, 3 with snapshot, should be success
        const auto & page1 = page_reader_mix_with_snap->read(1);
        const auto & page3 = page_reader_mix_with_snap->read(3);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page1, 1);
        ASSERT_PAGE_EQ(c_buff2, buf_sz2, page3, 3);
        ASSERT_THROW(page_reader_mix_with_snap->read(4), DB::Exception);
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

TEST_F(PageStorageMixedTest, refWithSnapshot2)
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
        page_writer_v2->write(std::move(batch), nullptr);
    }

    {
        WriteBatch batch;
        batch.putRefPage(2, 1);
        page_writer_v2->write(std::move(batch), nullptr);
    }

    // Change to mix mode here
    ASSERT_EQ(reloadMixedStoragePool(), PageStorageRunMode::MIX_MODE);

    auto snapshot_mix = page_reader_mix->getSnapshot("");
    {
        WriteBatch batch;
        batch.delPage(1);
        batch.delPage(2);
        page_writer_mix->write(std::move(batch), nullptr);
    }

    {
        auto page_maps = newMixedPageReader(snapshot_mix)->read({1, 2});
        ASSERT_EQ(page_maps.size(), 2);

        ASSERT_PAGE_EQ(c_buff, buf_sz, page_maps.at(1), 1);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page_maps.at(2), 2);
    }
}
CATCH

TEST_F(PageStorageMixedTest, refWithSnapshot3)
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
        // to keep mix mode
        batch.putExternal(10, 1);
        page_writer_v2->write(std::move(batch), nullptr);
    }

    {
        WriteBatch batch;
        batch.putRefPage(2, 1);
        page_writer_v2->write(std::move(batch), nullptr);
    }

    {
        WriteBatch batch;
        batch.delPage(1);
        batch.delPage(2);
        page_writer_v2->write(std::move(batch), nullptr);
    }

    // Change to mix mode here
    ASSERT_EQ(reloadMixedStoragePool(), PageStorageRunMode::MIX_MODE);

    {
        WriteBatch batch;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(1, tag, buff, buf_sz);
        page_writer_mix->write(std::move(batch), nullptr);
    }

    {
        WriteBatch batch;
        batch.putRefPage(2, 1);
        page_writer_mix->write(std::move(batch), nullptr);
    }

    auto snapshot_mix = page_reader_mix->getSnapshot("");
    {
        WriteBatch batch;
        batch.delPage(1);
        batch.delPage(2);
        page_writer_mix->write(std::move(batch), nullptr);
    }

    {
        auto page_maps = newMixedPageReader(snapshot_mix)->read({1, 2});
        ASSERT_EQ(page_maps.size(), 2);

        ASSERT_PAGE_EQ(c_buff, buf_sz, page_maps.at(1), 1);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page_maps.at(2), 2);
    }
}
CATCH

TEST_F(PageStorageMixedTest, refWithSnapshot4)
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
        page_writer_v2->write(std::move(batch), nullptr);
    }

    {
        WriteBatch batch;
        batch.putRefPage(2, 1);
        page_writer_v2->write(std::move(batch), nullptr);
    }

    // Change to mix mode here
    ASSERT_EQ(reloadMixedStoragePool(), PageStorageRunMode::MIX_MODE);

    {
        WriteBatch batch;
        batch.delPage(2);
        page_writer_mix->write(std::move(batch), nullptr);
    }

    {
        auto page1 = page_reader_mix->read(1);

        ASSERT_PAGE_EQ(c_buff, buf_sz, page1, 1);
    }
}
CATCH

TEST_F(PageStorageMixedTest, refWithSnapshot5)
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
        page_writer_v2->write(std::move(batch), nullptr);
    }

    {
        WriteBatch batch;
        batch.putRefPage(2, 1);
        page_writer_v2->write(std::move(batch), nullptr);
    }

    {
        WriteBatch batch;
        batch.delPage(1);
        page_writer_v2->write(std::move(batch), nullptr);
    }

    // Change to mix mode here
    ASSERT_EQ(reloadMixedStoragePool(), PageStorageRunMode::MIX_MODE);

    {
        auto page1 = page_reader_mix->read(2);

        ASSERT_PAGE_EQ(c_buff, buf_sz, page1, 2);
    }
}
CATCH

TEST_F(PageStorageMixedTest, refWithSnapshot6)
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
        page_writer_v2->write(std::move(batch), nullptr);
    }

    {
        WriteBatch batch;
        batch.putRefPage(2, 1);
        page_writer_v2->write(std::move(batch), nullptr);
    }

    // Change to mix mode here
    ASSERT_EQ(reloadMixedStoragePool(), PageStorageRunMode::MIX_MODE);

    {
        WriteBatch batch;
        batch.delPage(1);
        page_writer_mix->write(std::move(batch), nullptr);
    }

    {
        auto page1 = page_reader_mix->read(2);

        ASSERT_PAGE_EQ(c_buff, buf_sz, page1, 2);
    }
}
CATCH

TEST_F(PageStorageMixedTest, ReadWithSnapshot2)
try
{
    UInt64 tag = 0;
    const size_t buf_sz = 1;
    char c_buff1[buf_sz];
    c_buff1[0] = 1;

    char c_buff2[buf_sz];
    c_buff2[0] = 2;

    {
        WriteBatch batch;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff1, buf_sz);
        batch.putPage(1, tag, buff, buf_sz);
        page_writer_v2->write(std::move(batch), nullptr);
    }

    // Change to mix mode here
    ASSERT_EQ(reloadMixedStoragePool(), PageStorageRunMode::MIX_MODE);

    auto snapshot_mix = page_reader_mix->getSnapshot("");
    {
        WriteBatch batch;
        batch.delPage(1);
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff2, buf_sz);
        batch.putPage(1, tag, buff, buf_sz);
        page_writer_mix->write(std::move(batch), nullptr);
    }

    {
        auto page1 = newMixedPageReader(snapshot_mix)->read(1);
        ASSERT_PAGE_EQ(c_buff1, buf_sz, page1, 1);
    }

    {
        auto page1 = page_reader_mix->read(1);
        ASSERT_PAGE_EQ(c_buff2, buf_sz, page1, 1);
    }

    {
        // Revert v3
        WriteBatch batch;
        batch.delPage(1);
        page_writer_mix->write(std::move(batch), nullptr);
    }
}
CATCH


} // namespace PS::V3::tests
} // namespace DB
