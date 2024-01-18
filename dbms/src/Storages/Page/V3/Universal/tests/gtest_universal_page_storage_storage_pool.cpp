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
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormatImpl.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/Page/WriteBatchWrapperImpl.h>
#include <Storages/PathCapacityMetrics.h>
#include <Storages/PathPool.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <fmt/ranges.h>
#include <gtest/gtest.h>

namespace DB
{
using namespace tests;
namespace PS::V3::tests
{
class UniPageStorageStoragePoolTest : public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override
    {
        auto & global_context = DB::tests::TiFlashTestEnv::getGlobalContext();
        old_run_mode = global_context.getPageStorageRunMode();
        global_context.setPageStorageRunMode(PageStorageRunMode::UNI_PS);
        TiFlashStorageTestBasic::SetUp();
        auto path = TiFlashTestEnv::getTemporaryPath("UniPageStorageStoragePoolTest");
        std::vector<size_t> caps = {};
        Strings paths = {path};
        PathCapacityMetricsPtr cap_metrics = std::make_shared<PathCapacityMetrics>(0, paths, caps, Strings{}, caps);
        storage_path_pool_v2 = std::make_unique<StoragePathPool>(
            Strings{path},
            Strings{path},
            "test",
            "t1",
            true,
            cap_metrics,
            global_context.getFileProvider());
        path_pool = std::make_unique<PathPool>(
            Strings{path},
            Strings{path},
            Strings{},
            cap_metrics,
            global_context.getFileProvider());
        storage_pool = std::make_unique<DM::StoragePool>(
            global_context,
            NullspaceID,
            TEST_NAMESPACE_ID,
            *storage_path_pool_v2,
            "test.t1");
    }

    void TearDown() override
    {
        auto & global_context = DB::tests::TiFlashTestEnv::getGlobalContext();
        global_context.setPageStorageRunMode(old_run_mode);
    }

private:
    PageStorageRunMode old_run_mode;
    std::unique_ptr<StoragePathPool> storage_path_pool_v2;
    std::unique_ptr<PathPool> path_pool;

protected:
    std::unique_ptr<DM::StoragePool> storage_pool;
};


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
            ::testing::Message("Page data not match the buffer"));
    }

    return ::testing::AssertionSuccess();
}

#define ASSERT_PAGE_EQ(buff_cmp, buf_size, page_cmp, page_id) \
    ASSERT_PRED_FORMAT4(getPageCompare, buff_cmp, buf_size, page_cmp, page_id)
#define EXPECT_PAGE_EQ(buff_cmp, buf_size, page_cmp, page_id) \
    EXPECT_PRED_FORMAT4(getPageCompare, buff_cmp, buf_size, page_cmp, page_id)

TEST_F(UniPageStorageStoragePoolTest, WriteRead)
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
        WriteBatchWrapper batch{
            PageStorageRunMode::UNI_PS,
            UniversalPageIdFormat::toFullPrefix(NullspaceID, StorageType::Log, TEST_NAMESPACE_ID)};
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(1, tag, buff, buf_sz);
        buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(2, tag, buff, buf_sz);
        storage_pool->logWriter()->write(std::move(batch), nullptr);
    }

    {
        const auto & page1 = storage_pool->logReader()->read(1);
        const auto & page2 = storage_pool->logReader()->read(2);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page1, 1);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page2, 2);
    }

    {
        WriteBatchWrapper batch{
            PageStorageRunMode::UNI_PS,
            UniversalPageIdFormat::toFullPrefix(NullspaceID, StorageType::Log, TEST_NAMESPACE_ID)};
        const size_t buf_sz2 = 2048;
        char c_buff2[buf_sz2] = {0};

        ReadBufferPtr buff2 = std::make_shared<ReadBufferFromMemory>(c_buff2, sizeof(c_buff2));
        batch.putPage(3, tag, buff2, buf_sz2);
        storage_pool->logWriter()->write(std::move(batch), nullptr);

        const auto & page3 = storage_pool->logReader()->read(3);
        ASSERT_PAGE_EQ(c_buff2, buf_sz2, page3, 3);
    }

    {
        WriteBatchWrapper batch{
            PageStorageRunMode::UNI_PS,
            UniversalPageIdFormat::toFullPrefix(NullspaceID, StorageType::Log, TEST_NAMESPACE_ID)};
        batch.delPage(3);
        storage_pool->logWriter()->write(std::move(batch), nullptr);
        ASSERT_ANY_THROW(storage_pool->logReader()->read(3));
    }
}
CATCH

TEST_F(UniPageStorageStoragePoolTest, ReadWithSnapshot)
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
        WriteBatchWrapper batch{
            PageStorageRunMode::UNI_PS,
            UniversalPageIdFormat::toFullPrefix(NullspaceID, StorageType::Log, TEST_NAMESPACE_ID)};
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(1, tag, buff, buf_sz);
        buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(2, tag, buff, buf_sz, {20, 120, 400, 200, 15, 75, 170, 24});
        storage_pool->logWriter()->write(std::move(batch), nullptr);
    }
    const size_t buf_sz2 = 2048;
    char c_buff2[buf_sz2] = {0};
    {
        WriteBatchWrapper batch{
            PageStorageRunMode::UNI_PS,
            UniversalPageIdFormat::toFullPrefix(NullspaceID, StorageType::Log, TEST_NAMESPACE_ID)};
        ReadBufferPtr buff2 = std::make_shared<ReadBufferFromMemory>(c_buff2, sizeof(c_buff2));
        batch.putPage(3, tag, buff2, buf_sz2);
        storage_pool->logWriter()->write(std::move(batch), nullptr);
    }

    auto snapshot = storage_pool->logReader()->getSnapshot("ReadWithSnapshotTest");
    {
        auto page_reader_with_snap = storage_pool->newLogReader(nullptr, snapshot);

        const auto & page1 = page_reader_with_snap->read(1);
        const auto & page2 = page_reader_with_snap->read(2);
        const auto & page3 = page_reader_with_snap->read(3);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page1, 1);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page2, 2);
        ASSERT_PAGE_EQ(c_buff2, buf_sz2, page3, 3);
    }

    {
        WriteBatchWrapper batch{
            PageStorageRunMode::UNI_PS,
            UniversalPageIdFormat::toFullPrefix(NullspaceID, StorageType::Log, TEST_NAMESPACE_ID)};
        batch.delPage(3);
        ReadBufferPtr buff2 = std::make_shared<ReadBufferFromMemory>(c_buff2, sizeof(c_buff2));
        batch.putPage(4, tag, buff2, buf_sz2);
        storage_pool->logWriter()->write(std::move(batch), nullptr);
    }
    {
        auto page_reader_with_snap = storage_pool->newLogReader(nullptr, snapshot);
        const auto & page3 = page_reader_with_snap->read(3);
        ASSERT_PAGE_EQ(c_buff2, buf_sz2, page3, 3);
        ASSERT_THROW(page_reader_with_snap->read(4), DB::Exception);
    }
}
CATCH

TEST_F(UniPageStorageStoragePoolTest, PutExt)
try
{
    {
        WriteBatchWrapper batch{
            PageStorageRunMode::UNI_PS,
            UniversalPageIdFormat::toFullPrefix(NullspaceID, StorageType::Log, TEST_NAMESPACE_ID)};
        batch.putExternal(1, 0);
        batch.putExternal(2, 0);
        batch.putExternal(3, 0);
        storage_pool->logWriter()->write(std::move(batch), nullptr);
    }

    auto uni_ps = storage_pool->global_context.getWriteNodePageStorage();
    auto external_ids = uni_ps->page_directory->getAliveExternalIds(
        UniversalPageIdFormat::toFullPrefix(NullspaceID, StorageType::Log, TEST_NAMESPACE_ID));
    ASSERT_EQ((*external_ids).size(), 3);
    ASSERT_TRUE((*external_ids).find(1) != (*external_ids).end());
    ASSERT_TRUE((*external_ids).find(2) != (*external_ids).end());
    ASSERT_TRUE((*external_ids).find(3) != (*external_ids).end());
}
CATCH

TEST_F(UniPageStorageStoragePoolTest, Ref)
try
{
    const size_t buf_sz = 1024;
    char c_buff[buf_sz] = {0};

    {
        WriteBatchWrapper batch{
            PageStorageRunMode::UNI_PS,
            UniversalPageIdFormat::toFullPrefix(NullspaceID, StorageType::Log, TEST_NAMESPACE_ID)};
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(7, 0, buff, buf_sz);
        buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(8, 0, buff, buf_sz, {20, 120, 400, 200, 15, 75, 170, 24});
        storage_pool->logWriter()->write(std::move(batch), nullptr);
    }

    {
        const auto & entry = storage_pool->logReader()->getPageEntry(8);
        ASSERT_EQ(entry.field_offsets.size(), 8);
    }

    {
        WriteBatchWrapper batch{
            PageStorageRunMode::UNI_PS,
            UniversalPageIdFormat::toFullPrefix(NullspaceID, StorageType::Log, TEST_NAMESPACE_ID)};
        batch.putRefPage(9, 7);
        storage_pool->logWriter()->write(std::move(batch), nullptr);
        ASSERT_EQ(storage_pool->logReader()->getNormalPageId(9), 7);
        const auto & page = storage_pool->logReader()->read(9);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page, 9);
    }

    {
        WriteBatchWrapper batch{
            PageStorageRunMode::UNI_PS,
            UniversalPageIdFormat::toFullPrefix(NullspaceID, StorageType::Log, TEST_NAMESPACE_ID)};
        batch.delPage(7);
        storage_pool->logWriter()->write(std::move(batch), nullptr);
        ASSERT_EQ(storage_pool->logReader()->getNormalPageId(9), 7);
        const auto & page = storage_pool->logReader()->read(9);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page, 9);
    }

    {
        WriteBatchWrapper batch{
            PageStorageRunMode::UNI_PS,
            UniversalPageIdFormat::toFullPrefix(NullspaceID, StorageType::Log, TEST_NAMESPACE_ID)};
        batch.putRefPage(10, 8);

        ASSERT_NO_THROW(storage_pool->logWriter()->write(std::move(batch), nullptr));
        ASSERT_EQ(storage_pool->logReader()->getNormalPageId(10), 8);

        std::vector<PageStorage::PageReadFields> read_fields;
        read_fields.emplace_back(std::pair<PageIdU64, PageStorage::FieldIndices>(10, {0, 1, 2, 6}));

        PageMapU64 page_maps = storage_pool->logReader()->read(read_fields);
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
}
CATCH

TEST_F(UniPageStorageStoragePoolTest, RefWithSnapshot)
try
{
    const size_t buf_sz = 1024;
    char c_buff[buf_sz] = {0};

    {
        WriteBatchWrapper batch{
            PageStorageRunMode::UNI_PS,
            UniversalPageIdFormat::toFullPrefix(NullspaceID, StorageType::Log, TEST_NAMESPACE_ID)};
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(7, 0, buff, buf_sz);
        buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(8, 0, buff, buf_sz, {20, 120, 400, 200, 15, 75, 170, 24});
        storage_pool->logWriter()->write(std::move(batch), nullptr);
    }

    {
        const auto & entry = storage_pool->logReader()->getPageEntry(8);
        ASSERT_EQ(entry.field_offsets.size(), 8);
    }

    {
        WriteBatchWrapper batch{
            PageStorageRunMode::UNI_PS,
            UniversalPageIdFormat::toFullPrefix(NullspaceID, StorageType::Log, TEST_NAMESPACE_ID)};
        batch.putRefPage(9, 7);
        storage_pool->logWriter()->write(std::move(batch), nullptr);
        ASSERT_EQ(storage_pool->logReader()->getNormalPageId(9), 7);
        const auto & page = storage_pool->logReader()->read(9);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page, 9);
    }

    auto snapshot = storage_pool->logReader()->getSnapshot("ReadWithSnapshotTest");

    {
        WriteBatchWrapper batch{
            PageStorageRunMode::UNI_PS,
            UniversalPageIdFormat::toFullPrefix(NullspaceID, StorageType::Log, TEST_NAMESPACE_ID)};
        batch.delPage(7);
        batch.delPage(9);
        storage_pool->logWriter()->write(std::move(batch), nullptr);
    }

    {
        auto page_reader_with_snap = storage_pool->newLogReader(nullptr, snapshot);
        ASSERT_EQ(page_reader_with_snap->getNormalPageId(9), 7);
        const auto & page = page_reader_with_snap->read(9);
        ASSERT_PAGE_EQ(c_buff, buf_sz, page, 9);
    }
}
CATCH

} // namespace PS::V3::tests
} // namespace DB
