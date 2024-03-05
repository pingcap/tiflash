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

#include <Common/CurrentMetrics.h>
#include <Common/FailPoint.h>
#include <IO/Buffer/ReadBufferFromMemory.h>
#include <IO/FileProvider/FileProvider.h>
#include <Interpreters/Context.h>
#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/File.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/PatternFormatter.h>
#include <Storages/BackgroundProcessingPool.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/V2/PageDefines.h>
#include <Storages/Page/V2/PageFile.h>
#include <Storages/Page/V2/PageStorage.h>
#include <Storages/Page/WriteBatchImpl.h>
#include <Storages/PathPool.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/logger_useful.h>

namespace DB
{
namespace FailPoints
{
extern const char exception_before_page_file_write_sync[];
extern const char force_set_page_file_write_errno[];
} // namespace FailPoints

namespace ErrorCodes
{
extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
} // namespace ErrorCodes

namespace PS::V2::tests
{
class PageStorageV2Test : public DB::base::TiFlashStorageTestBasic
{
public:
    PageStorageV2Test()
        : file_provider{DB::tests::TiFlashTestEnv::getDefaultFileProvider()}
    {}

protected:
    static void SetUpTestCase() {}

    void SetUp() override
    {
        bkg_pool = std::make_shared<DB::BackgroundProcessingPool>(4, "bg-page-");
        TiFlashStorageTestBasic::SetUp();
        // drop dir if exists
        path_pool = std::make_unique<StoragePathPool>(db_context->getPathPool().withTable("test", "t1", false));
        // default test config
        config.file_roll_size = 512;
        config.gc_min_files = 1;

        storage = reopenWithConfig(config);
    }

    std::shared_ptr<PageStorage> reopenWithConfig(const PageStorageConfig & config_)
    {
        auto delegator = path_pool->getPSDiskDelegatorSingle("log");
        auto storage = std::make_shared<PageStorage>("test.t", delegator, config_, file_provider, *bkg_pool);
        storage->restore();
        return storage;
    }

protected:
    PageStorageConfig config;
    std::shared_ptr<BackgroundProcessingPool> bkg_pool;
    std::shared_ptr<PageStorage> storage;
    std::unique_ptr<StoragePathPool> path_pool;
    const FileProviderPtr file_provider;
};

TEST_F(PageStorageV2Test, WriteRead)
try
{
    const UInt64 tag = 0;
    const size_t buf_sz = 1024;
    char c_buff[buf_sz];
    for (size_t i = 0; i < buf_sz; ++i)
    {
        c_buff[i] = i % 0xff;
    }

    {
        WriteBatch batch;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(0, tag, buff, buf_sz);
        buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(1, tag, buff, buf_sz);
        storage->write(std::move(batch));
    }

    DB::Page page0 = storage->read(0);
    ASSERT_EQ(page0.data.size(), buf_sz);
    ASSERT_EQ(page0.page_id, 0UL);
    for (size_t i = 0; i < buf_sz; ++i)
    {
        EXPECT_EQ(*(page0.data.begin() + i), static_cast<char>(i % 0xff));
    }
    DB::Page page1 = storage->read(1);
    ASSERT_EQ(page1.data.size(), buf_sz);
    ASSERT_EQ(page1.page_id, 1UL);
    for (size_t i = 0; i < buf_sz; ++i)
    {
        EXPECT_EQ(*(page1.data.begin() + i), static_cast<char>(i % 0xff));
    }
}
CATCH

TEST_F(PageStorageV2Test, WriteMultipleBatchRead)
try
{
    const UInt64 tag = 0;
    const size_t buf_sz = 1024;
    char c_buff[buf_sz];
    for (size_t i = 0; i < buf_sz; ++i)
    {
        c_buff[i] = i % 0xff;
    }

    {
        WriteBatch batch;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(0, tag, buff, buf_sz);
        storage->write(std::move(batch));
    }
    {
        WriteBatch batch;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(1, tag, buff, buf_sz);
        storage->write(std::move(batch));
    }

    DB::Page page0 = storage->read(0);
    ASSERT_EQ(page0.data.size(), buf_sz);
    ASSERT_EQ(page0.page_id, 0UL);
    for (size_t i = 0; i < buf_sz; ++i)
    {
        EXPECT_EQ(*(page0.data.begin() + i), static_cast<char>(i % 0xff));
    }
    DB::Page page1 = storage->read(1);
    ASSERT_EQ(page1.data.size(), buf_sz);
    ASSERT_EQ(page1.page_id, 1UL);
    for (size_t i = 0; i < buf_sz; ++i)
    {
        EXPECT_EQ(*(page1.data.begin() + i), static_cast<char>(i % 0xff));
    }
}
CATCH

TEST_F(PageStorageV2Test, WriteReadAfterGc)
try
{
    const size_t buf_sz = 256;
    char c_buff[buf_sz];

    const size_t num_repeat = 10;
    PageId pid = 1;
    const char page0_byte = 0x3f;
    {
        // put page0
        WriteBatch batch;
        memset(c_buff, page0_byte, buf_sz);
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(0, 0, buff, buf_sz);
        storage->write(std::move(batch));
    }
    // repeated put page1
    for (size_t n = 1; n <= num_repeat; ++n)
    {
        WriteBatch batch;
        memset(c_buff, n, buf_sz);
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(pid, 0, buff, buf_sz);
        storage->write(std::move(batch));
    }

    {
        DB::Page page0 = storage->read(0);
        ASSERT_EQ(page0.data.size(), buf_sz);
        ASSERT_EQ(page0.page_id, 0UL);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page0.data.begin() + i), page0_byte);
        }

        DB::Page page1 = storage->read(pid);
        ASSERT_EQ(page1.data.size(), buf_sz);
        ASSERT_EQ(page1.page_id, pid);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page1.data.begin() + i), static_cast<char>(num_repeat % 0xff));
        }
    }

    storage->gc();

    {
        DB::Page page0 = storage->read(0);
        ASSERT_EQ(page0.data.size(), buf_sz);
        ASSERT_EQ(page0.page_id, 0UL);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page0.data.begin() + i), page0_byte);
        }

        DB::Page page1 = storage->read(pid);
        ASSERT_EQ(page1.data.size(), buf_sz);
        ASSERT_EQ(page1.page_id, pid);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page1.data.begin() + i), static_cast<char>(num_repeat % 0xff));
        }
    }
}
CATCH

TEST_F(PageStorageV2Test, WriteReadGcExternalPage)
try
{
    {
        WriteBatch batch;
        batch.putExternal(0, 0);
        batch.putRefPage(1, 0);
        batch.putExternal(1024, 0);
        storage->write(std::move(batch));
    }

    size_t times_remover_called = 0;

    ExternalPageCallbacks callbacks;
    callbacks.scanner = []() -> ExternalPageCallbacks::PathAndIdsVec {
        return {};
    };
    callbacks.remover = [&times_remover_called](
                            const ExternalPageCallbacks::PathAndIdsVec &,
                            const std::set<PageId> & normal_page_ids) -> void {
        times_remover_called += 1;
        ASSERT_EQ(normal_page_ids.size(), 2UL);
        EXPECT_GT(normal_page_ids.count(0), 0UL);
        EXPECT_GT(normal_page_ids.count(1024), 0UL);
    };
    storage->registerExternalPagesCallbacks(callbacks);
    {
        SCOPED_TRACE("fist gc");
        storage->gc();
        EXPECT_EQ(times_remover_called, 1UL);
    }

    auto snapshot = storage->getSnapshot();

    {
        WriteBatch batch;
        batch.putRefPage(2, 1); // ref 2 -> 1 -> 0
        batch.delPage(1); // free ref 1 -> 0
        batch.delPage(1024); // free normal page 1024
        storage->write(std::move(batch));
    }

    {
        SCOPED_TRACE("gc with snapshot");
        storage->gc();
        EXPECT_EQ(times_remover_called, 2UL);
    }

    {
        DB::Page page0 = storage->read(0);
        ASSERT_EQ(page0.data.size(), 0UL);
        ASSERT_EQ(page0.page_id, 0UL);

        DB::Page page2 = storage->read(2);
        ASSERT_EQ(page2.data.size(), 0UL);
        ASSERT_EQ(page2.page_id, 2UL);
    }

    snapshot.reset();
    callbacks.remover = [&times_remover_called](
                            const ExternalPageCallbacks::PathAndIdsVec &,
                            const std::set<PageId> & normal_page_ids) -> void {
        times_remover_called += 1;
        ASSERT_EQ(normal_page_ids.size(), 1UL);
        EXPECT_GT(normal_page_ids.count(0), 0UL);
    };
    storage->registerExternalPagesCallbacks(callbacks);
    {
        SCOPED_TRACE("gc with snapshot released");
        storage->gc();
        EXPECT_EQ(times_remover_called, 3UL);
    }
}
CATCH

TEST_F(PageStorageV2Test, IdempotentDelAndRef)
try
{
    const size_t buf_sz = 1024;
    char c_buff[buf_sz];

    {
        // Page1 should be written to PageFile{1, 0}
        WriteBatch batch;
        memset(c_buff, 0xf, buf_sz);
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(1, 0, buff, buf_sz);

        storage->write(std::move(batch));
    }

    {
        // RefPage 2 -> 1, Del Page 1 should be written to PageFile{2, 0}
        WriteBatch batch;
        batch.putRefPage(2, 1);
        batch.delPage(1);
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(1000, 0, buff, buf_sz);

        storage->write(std::move(batch));
    }

    {
        // Another RefPage 2 -> 1, Del Page 1 should be written to PageFile{3, 0}
        WriteBatch batch;
        batch.putRefPage(2, 1);
        batch.delPage(1);

        storage->write(std::move(batch));
    }

    {
        auto snap = storage->getConcreteSnapshot();
        auto ref_entry = snap->version()->find(1);
        ASSERT_FALSE(ref_entry);

        ref_entry = snap->version()->find(2);
        ASSERT_TRUE(ref_entry);
        ASSERT_EQ(ref_entry->file_id, 1UL);
        ASSERT_EQ(ref_entry->ref, 1UL);

        auto normal_entry = snap->version()->findNormalPageEntry(1);
        ASSERT_TRUE(normal_entry);
        ASSERT_EQ(normal_entry->file_id, 1UL);
        ASSERT_EQ(normal_entry->ref, 1UL);

        // Point to the same entry
        ASSERT_EQ(ref_entry->offset, normal_entry->offset);
    }

    storage = reopenWithConfig(config);

    {
        auto snap = storage->getConcreteSnapshot();
        auto ref_entry = snap->version()->find(1);
        ASSERT_FALSE(ref_entry);

        ref_entry = snap->version()->find(2);
        ASSERT_TRUE(ref_entry);
        ASSERT_EQ(ref_entry->file_id, 1UL);
        ASSERT_EQ(ref_entry->ref, 1UL);

        auto normal_entry = snap->version()->findNormalPageEntry(1);
        ASSERT_TRUE(normal_entry);
        ASSERT_EQ(normal_entry->file_id, 1UL);
        ASSERT_EQ(normal_entry->ref, 1UL);

        // Point to the same entry
        ASSERT_EQ(ref_entry->offset, normal_entry->offset);
    }
}
CATCH

TEST_F(PageStorageV2Test, ListPageFiles)
try
{
    constexpr size_t buf_sz = 128;
    char c_buff[buf_sz];

    {
        // Create a Legacy PageFile_1_0
        WriteBatch wb;
        memset(c_buff, 0xf, buf_sz);
        auto buf = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        wb.putPage(1, 0, buf, buf_sz);
        storage->write(std::move(wb));

        auto f = PageFile::openPageFileForRead(
            1,
            0,
            storage->delegator->defaultPath(),
            file_provider,
            PageFile::Type::Formal,
            storage->log);
        f.setLegacy();
    }

    {
        // Create a Checkpoint PageFile_2_0
        WriteBatch wb;
        memset(c_buff, 0xf, buf_sz);
        auto buf = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        wb.putPage(1, 0, buf, buf_sz);

        auto f = PageFile::newPageFile(
            2,
            0,
            storage->delegator->defaultPath(),
            file_provider,
            PageFile::Type::Temp,
            storage->log);
        {
            auto w = f.createWriter(false, true);

            PageEntriesEdit edit;
            (void)w->write(wb, edit);
        }
        f.setCheckpoint();
    }

    {
        PageStorage::ListPageFilesOption opt;
        opt.ignore_legacy = true;
        auto page_files = storage->listAllPageFiles(file_provider, storage->delegator, storage->log, opt);
        // Legacy should be ignored
        ASSERT_EQ(page_files.size(), 1UL);
        for (const auto & page_file : page_files)
        {
            EXPECT_TRUE(page_file.getType() != PageFile::Type::Legacy);
        }
    }

    {
        PageStorage::ListPageFilesOption opt;
        opt.ignore_checkpoint = true;
        auto page_files = storage->listAllPageFiles(file_provider, storage->delegator, storage->log, opt);
        // Snapshot should be ignored
        ASSERT_EQ(page_files.size(), 1UL);
        for (const auto & page_file : page_files)
        {
            EXPECT_TRUE(page_file.getType() != PageFile::Type::Checkpoint);
        }
    }
}
CATCH

TEST_F(PageStorageV2Test, RenewWriter)
try
{
    constexpr size_t buf_sz = 100;
    char c_buff[buf_sz];

    {
        WriteBatch wb;
        memset(c_buff, 0xf, buf_sz);
        auto buf = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        wb.putPage(1, 0, buf, buf_sz);
        storage->write(std::move(wb));
    }

    {
        PageStorage::ListPageFilesOption opt;
        auto page_files = storage->listAllPageFiles(file_provider, storage->delegator, storage->log, opt);
        ASSERT_EQ(page_files.size(), 1UL);
    }

    PageStorageConfig config;
    config.file_roll_size = 10; // make it easy to renew a new page file for write
    storage = reopenWithConfig(config);

    {
        PageStorage::ListPageFilesOption opt;
        auto page_files = storage->listAllPageFiles(file_provider, storage->delegator, storage->log, opt);
        ASSERT_EQ(page_files.size(), 2UL);
    }
}
CATCH

/// Check if we can correctly do read / write after restore from disk.
TEST_F(PageStorageV2Test, WriteReadRestore)
try
{
    const UInt64 tag = 0;
    const size_t buf_sz = 1024;
    char c_buff[buf_sz];
    for (size_t i = 0; i < buf_sz; ++i)
    {
        c_buff[i] = i % 0xff;
    }

    PageStorageConfig tmp_config = config;
    tmp_config.file_roll_size = 128 * MB;
    storage = reopenWithConfig(tmp_config);

    {
        WriteBatch batch;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(0, tag, buff, buf_sz);
        buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(1, tag, buff, buf_sz);
        storage->write(std::move(batch));
    }

    // Read
    {
        DB::Page page0 = storage->read(0);
        ASSERT_EQ(page0.data.size(), buf_sz);
        ASSERT_EQ(page0.page_id, 0UL);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page0.data.begin() + i), static_cast<char>(i % 0xff));
        }
        DB::Page page1 = storage->read(1);
        ASSERT_EQ(page1.data.size(), buf_sz);
        ASSERT_EQ(page1.page_id, 1UL);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page1.data.begin() + i), static_cast<char>(i % 0xff));
        }
    }

    // restore
    storage = reopenWithConfig(tmp_config);

    // Read again
    {
        DB::Page page0 = storage->read(0);
        ASSERT_EQ(page0.data.size(), buf_sz);
        ASSERT_EQ(page0.page_id, 0UL);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page0.data.begin() + i), static_cast<char>(i % 0xff));
        }
        DB::Page page1 = storage->read(1);
        ASSERT_EQ(page1.data.size(), buf_sz);
        ASSERT_EQ(page1.page_id, 1UL);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page1.data.begin() + i), static_cast<char>(i % 0xff));
        }
    }

    {
        // Check whether write is correctly.
        {
            WriteBatch batch;
            ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
            batch.putPage(2, tag, buff, buf_sz);
            storage->write(std::move(batch));
        }
        // Read to check
        {
            DB::Page page0 = storage->read(0);
            ASSERT_EQ(page0.data.size(), buf_sz);
            ASSERT_EQ(page0.page_id, 0UL);
            for (size_t i = 0; i < buf_sz; ++i)
            {
                EXPECT_EQ(*(page0.data.begin() + i), static_cast<char>(i % 0xff));
            }
            DB::Page page1 = storage->read(1);
            ASSERT_EQ(page1.data.size(), buf_sz);
            ASSERT_EQ(page1.page_id, 1UL);
            for (size_t i = 0; i < buf_sz; ++i)
            {
                EXPECT_EQ(*(page1.data.begin() + i), static_cast<char>(i % 0xff));
            }
            DB::Page page2 = storage->read(2);
            ASSERT_EQ(page2.data.size(), buf_sz);
            ASSERT_EQ(page2.page_id, 2UL);
            for (size_t i = 0; i < buf_sz; ++i)
            {
                EXPECT_EQ(*(page2.data.begin() + i), static_cast<char>(i % 0xff));
            }
        }
    }

    // Restore. This ensure last write is correct.
    storage = reopenWithConfig(tmp_config);

    // Read again to check all data.
    {
        DB::Page page0 = storage->read(0);
        ASSERT_EQ(page0.data.size(), buf_sz);
        ASSERT_EQ(page0.page_id, 0UL);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page0.data.begin() + i), static_cast<char>(i % 0xff));
        }
        DB::Page page1 = storage->read(1);
        ASSERT_EQ(page1.data.size(), buf_sz);
        ASSERT_EQ(page1.page_id, 1UL);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page1.data.begin() + i), static_cast<char>(i % 0xff));
        }
        DB::Page page2 = storage->read(2);
        ASSERT_EQ(page2.data.size(), buf_sz);
        ASSERT_EQ(page2.page_id, 2UL);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page2.data.begin() + i), static_cast<char>(i % 0xff));
        }
    }
}
CATCH

TEST_F(PageStorageV2Test, WriteReadWithSpecifyFields)
try
{
    const UInt64 tag = 0;
    const size_t buf_sz = 1024;
    char c_buff[buf_sz];
    for (size_t i = 0; i < buf_sz; ++i)
        c_buff[i] = i % 0xff;

    // field index, size
    std::map<size_t, size_t> page0_fields = {{0, 16}, {1, 48}, {2, 192}, {3, 256}, {4, 10}, {5, 502}};
    std::map<size_t, size_t> page1_fields = {{0, 20}, {1, 20}, {2, 59}, {3, 29}, {4, 896}};

    {
        WriteBatch batch;
        PageFieldSizes p0_sizes;
        for (auto [idx, sz] : page0_fields)
        {
            (void)idx;
            p0_sizes.emplace_back(sz);
        }
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(0, tag, buff, buf_sz, p0_sizes);

        PageFieldSizes p1_sizes;
        for (auto [idx, sz] : page1_fields)
        {
            (void)idx;
            p1_sizes.emplace_back(sz);
        }
        buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(1, tag, buff, buf_sz, p1_sizes);
        storage->write(std::move(batch));
    }

    size_t offset = 0;
    PageFieldOffsets page0_offsets;
    for (auto [idx, size] : page0_fields)
    {
        (void)idx;
        page0_offsets.emplace_back(offset);
        offset += size;
    }
    offset = 0;
    PageFieldOffsets page1_offsets;
    for (auto [idx, size] : page1_fields)
    {
        (void)idx;
        page1_offsets.emplace_back(offset);
        offset += size;
    }

    {
        // Read as their sequence
        std::vector<PageStorage::PageReadFields> read_fields;
        PageStorage::PageReadFields p0_fields{0, {0, 1, 3, 5}};
        read_fields.push_back(p0_fields);
        PageStorage::PageReadFields p1_fields{1, {0, 2, 4}};
        read_fields.push_back(p1_fields);

        auto pages = storage->read(read_fields);
        ASSERT_EQ(pages.size(), 2UL);

        {
            Page page0 = pages.at(0);
            ASSERT_EQ(page0.page_id, 0UL);
            for (auto index : p0_fields.second)
            {
                auto data = page0.getFieldData(index);
                ASSERT_EQ(data.size(), page0_fields.at(index));
                // check page data
                for (size_t i = 0; i < data.size(); ++i)
                {
                    EXPECT_EQ(*(data.begin() + i), static_cast<char>((page0_offsets[index] + i) % 0xff))
                        << "Page0, field index: " << index << ", offset: " << i //
                        << ", offset inside page: " << page0_offsets[index] + i;
                }
            }
        }
        {
            DB::Page page1 = pages.at(1);
            ASSERT_EQ(page1.page_id, 1UL);
            for (auto index : p1_fields.second)
            {
                auto data = page1.getFieldData(index);
                ASSERT_EQ(data.size(), page1_fields.at(index));
                // check page data
                for (size_t i = 0; i < data.size(); ++i)
                    EXPECT_EQ(*(data.begin() + i), static_cast<char>((page1_offsets[index] + i) % 0xff))
                        << "Page1, field index: " << index << ", offset: " << i //
                        << ", offset inside page: " << page1_offsets[index] + i;
            }
        }
    }

    {
        // Read in random sequence
        std::vector<PageStorage::PageReadFields> read_fields;
        PageStorage::PageReadFields p0_fields{0, {3, 0, 1, 5}};
        read_fields.push_back(p0_fields);
        PageStorage::PageReadFields p1_fields{1, {3, 4, 2}};
        read_fields.push_back(p1_fields);

        auto pages = storage->read(read_fields);
        ASSERT_EQ(pages.size(), 2UL);

        {
            DB::Page page0 = pages.at(0);
            ASSERT_EQ(page0.page_id, 0UL);
            ASSERT_EQ(page0.field_offsets.size(), p0_fields.second.size());
            for (auto index : p0_fields.second)
            {
                auto data = page0.getFieldData(index);
                ASSERT_EQ(data.size(), page0_fields.at(index));
                // check page data
                for (size_t i = 0; i < data.size(); ++i)
                {
                    EXPECT_EQ(*(data.begin() + i), static_cast<char>((page0_offsets[index] + i) % 0xff))
                        << "Page0, field index: " << index << ", offset: " << i //
                        << ", offset inside page: " << page0_offsets[index] + i;
                }
            }
        }
        {
            DB::Page page1 = pages.at(1);
            ASSERT_EQ(page1.page_id, 1UL);
            ASSERT_EQ(page1.field_offsets.size(), p1_fields.second.size());
            for (auto index : p1_fields.second)
            {
                auto data = page1.getFieldData(index);
                ASSERT_EQ(data.size(), page1_fields.at(index));
                // check page data
                for (size_t i = 0; i < data.size(); ++i)
                    EXPECT_EQ(*(data.begin() + i), static_cast<char>((page1_offsets[index] + i) % 0xff))
                        << "Page1, field index: " << index << ", offset: " << i //
                        << ", offset inside page: " << page1_offsets[index] + i;
            }
        }
    }

    {
        // Read as single page
        DB::Page page0 = storage->read(0);
        ASSERT_EQ(page0.data.size(), buf_sz);
        ASSERT_EQ(page0.page_id, 0UL);
        for (size_t i = 0; i < buf_sz; ++i)
            EXPECT_EQ(*(page0.data.begin() + i), static_cast<char>(i % 0xff));
        ASSERT_EQ(page0.fieldSize(), page0_fields.size());
        for (const auto & [idx, sz] : page0_fields)
        {
            ASSERT_EQ(page0.getFieldData(idx).size(), sz);
        }

        DB::Page page1 = storage->read(1);
        ASSERT_EQ(page1.data.size(), buf_sz);
        ASSERT_EQ(page1.page_id, 1UL);
        for (size_t i = 0; i < buf_sz; ++i)
            EXPECT_EQ(*(page1.data.begin() + i), static_cast<char>(i % 0xff));
        ASSERT_EQ(page1.fieldSize(), page1_fields.size());
        for (const auto & [idx, sz] : page1_fields)
        {
            ASSERT_EQ(page1.getFieldData(idx).size(), sz);
        }
    }
}
CATCH

TEST_F(PageStorageV2Test, IgnoreIncompleteWriteBatch1)
try
{
    // If there is any incomplete write batch, we should able to ignore those
    // broken write batches and continue to write more data.

    const size_t buf_sz = 1024;
    char buf[buf_sz];
    {
        WriteBatch batch;
        memset(buf, 0x01, buf_sz);
        batch.putPage(
            1,
            0,
            std::make_shared<ReadBufferFromMemory>(buf, buf_sz),
            buf_sz,
            PageFieldSizes{{32, 64, 79, 128, 196, 256, 269}});
        batch.putPage(
            2,
            0,
            std::make_shared<ReadBufferFromMemory>(buf, buf_sz),
            buf_sz,
            PageFieldSizes{{64, 79, 128, 196, 256, 301}});
        batch.putRefPage(3, 2);
        batch.putRefPage(4, 2);
        try
        {
            FailPointHelper::enableFailPoint(FailPoints::exception_before_page_file_write_sync);
            storage->write(std::move(batch));
        }
        catch (DB::Exception & e)
        {
            if (e.code() != ErrorCodes::FAIL_POINT_ERROR)
                throw;
        }
    }

    // Restore, the broken meta should be ignored
    storage = reopenWithConfig(PageStorageConfig{});

    {
        size_t num_pages = 0;
        storage->traverse([&num_pages](const DB::Page &) { num_pages += 1; });
        ASSERT_EQ(num_pages, 0);
    }

    // Continue to write some pages
    {
        WriteBatch batch;
        memset(buf, 0x02, buf_sz);
        batch.putPage(
            1,
            0,
            std::make_shared<ReadBufferFromMemory>(buf, buf_sz),
            buf_sz, //
            PageFieldSizes{{32, 128, 196, 256, 12, 99, 1, 300}});
        storage->write(std::move(batch));

        auto page1 = storage->read(1);
        ASSERT_EQ(page1.data.size(), buf_sz);
        for (size_t i = 0; i < page1.data.size(); ++i)
        {
            const auto * p = page1.data.begin();
            EXPECT_EQ(*p, 0x02);
        }
    }

    // Restore again, we should be able to read page 1
    storage = reopenWithConfig(PageStorageConfig{});

    {
        size_t num_pages = 0;
        storage->traverse([&num_pages](const Page &) { num_pages += 1; });
        ASSERT_EQ(num_pages, 1);

        auto page1 = storage->read(1);
        ASSERT_EQ(page1.data.size(), buf_sz);
        for (size_t i = 0; i < page1.data.size(); ++i)
        {
            const auto * p = page1.data.begin();
            EXPECT_EQ(*p, 0x02);
        }
    }
}
CATCH

TEST_F(PageStorageV2Test, IgnoreIncompleteWriteBatch2)
try
{
    // If there is any incomplete write batch, we should able to ignore those
    // broken write batches and continue to write more data.

    const size_t buf_sz = 1024;
    char buf[buf_sz];
    {
        WriteBatch batch;
        memset(buf, 0x01, buf_sz);
        batch.putPage(
            1,
            0,
            std::make_shared<ReadBufferFromMemory>(buf, buf_sz),
            buf_sz,
            PageFieldSizes{{32, 64, 79, 128, 196, 256, 269}});
        batch.putPage(
            2,
            0,
            std::make_shared<ReadBufferFromMemory>(buf, buf_sz),
            buf_sz,
            PageFieldSizes{{64, 79, 128, 196, 256, 301}});
        batch.putRefPage(3, 2);
        batch.putRefPage(4, 2);
        try
        {
            FailPointHelper::enableFailPoint(FailPoints::force_set_page_file_write_errno);
            SCOPE_EXIT({ FailPointHelper::disableFailPoint(FailPoints::force_set_page_file_write_errno); });
            storage->write(std::move(batch));
        }
        catch (DB::Exception & e)
        {
            // Mock to catch and ignore the exception in background thread
            if (e.code() != ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR)
                throw;
        }
    }

    {
        size_t num_pages = 0;
        storage->traverse([&num_pages](const Page &) { num_pages += 1; });
        ASSERT_EQ(num_pages, 0);
    }

    // Continue to write some pages
    {
        WriteBatch batch;
        memset(buf, 0x02, buf_sz);
        batch.putPage(
            1,
            0,
            std::make_shared<ReadBufferFromMemory>(buf, buf_sz),
            buf_sz, //
            PageFieldSizes{{32, 128, 196, 256, 12, 99, 1, 300}});
        storage->write(std::move(batch));

        auto page1 = storage->read(1);
        ASSERT_EQ(page1.data.size(), buf_sz);
        for (size_t i = 0; i < page1.data.size(); ++i)
        {
            const auto * p = page1.data.begin();
            EXPECT_EQ(*p, 0x02);
        }
    }

    // Restore again, we should be able to read page 1
    storage = reopenWithConfig(PageStorageConfig{});

    {
        size_t num_pages = 0;
        storage->traverse([&num_pages](const Page &) { num_pages += 1; });
        ASSERT_EQ(num_pages, 1);

        auto page1 = storage->read(1);
        ASSERT_EQ(page1.data.size(), buf_sz);
        for (size_t i = 0; i < page1.data.size(); ++i)
        {
            const auto * p = page1.data.begin();
            EXPECT_EQ(*p, 0x02);
        }
    }
}
CATCH

/**
 * PageStorage tests with predefine Page1 && Page2
 */
class PageStorageV2With2PagesTest : public PageStorageV2Test
{
public:
    PageStorageV2With2PagesTest() = default;

protected:
    void SetUp() override
    {
        PageStorageV2Test::SetUp();
        // put predefine Page1, Page2
        const size_t buf_sz = 1024;
        char buf[buf_sz];
        {
            WriteBatch wb;
            memset(buf, 0x01, buf_sz);
            wb.putPage(1, 0, std::make_shared<ReadBufferFromMemory>(buf, buf_sz), buf_sz);
            storage->write(std::move(wb));
        }
        {
            WriteBatch wb;
            memset(buf, 0x02, buf_sz);
            wb.putPage(2, 0, std::make_shared<ReadBufferFromMemory>(buf, buf_sz), buf_sz);
            storage->write(std::move(wb));
        }
    }
};

TEST_F(PageStorageV2With2PagesTest, UpdateRefPages)
{
    /// update on RefPage, all references get updated.
    const UInt64 tag = 0;
    // put ref page: RefPage3 -> Page2
    {
        WriteBatch batch;
        batch.putRefPage(3, 2);
        storage->write(std::move(batch));
    }
    const size_t buf_sz = 1024;
    char buf[buf_sz];
    // if update PageId == 3 or PageId == 2, both RefPage3 && Page2 get updated
    {
        // update RefPage3
        WriteBatch batch;
        char ch_to_update = 0x0f;
        memset(buf, ch_to_update, buf_sz);
        batch.putPage(3, tag, std::make_shared<ReadBufferFromMemory>(buf, buf_sz), buf_sz);
        storage->write(std::move(batch));

        // check RefPage3 and Page2 both get updated
        const Page page3 = storage->read(3);
        for (size_t i = 0; i < page3.data.size(); ++i)
        {
            EXPECT_EQ(*(page3.data.begin() + i), ch_to_update);
        }
        const Page page2 = storage->read(2);
        for (size_t i = 0; i < page2.data.size(); ++i)
        {
            EXPECT_EQ(*(page2.data.begin() + i), ch_to_update);
        }
    }
    {
        // update Page2
        WriteBatch batch;
        char ch_to_update = 0xef;
        memset(buf, ch_to_update, buf_sz);
        batch.putPage(2, tag, std::make_shared<ReadBufferFromMemory>(buf, buf_sz), buf_sz);
        storage->write(std::move(batch));

        // check RefPage3 and Page2 both get updated
        const Page page3 = storage->read(3);
        for (size_t i = 0; i < page3.data.size(); ++i)
        {
            EXPECT_EQ(*(page3.data.begin() + i), ch_to_update);
        }
        const Page page2 = storage->read(2);
        for (size_t i = 0; i < page2.data.size(); ++i)
        {
            EXPECT_EQ(*(page2.data.begin() + i), ch_to_update);
        }
    }
}

TEST_F(PageStorageV2With2PagesTest, DeleteRefPages)
{
    // put ref page: RefPage3 -> Page2, RefPage4 -> Page2
    {
        WriteBatch batch;
        batch.putRefPage(3, 2);
        batch.putRefPage(4, 2);
        storage->write(std::move(batch));
    }
    { // tests for delete Page
        // delete RefPage3, RefPage4 don't get deleted
        {
            WriteBatch batch;
            batch.delPage(3);
            storage->write(std::move(batch));
            EXPECT_FALSE(storage->getEntry(3).isValid());
            EXPECT_TRUE(storage->getEntry(4).isValid());
        }
        // delete RefPage4
        {
            WriteBatch batch;
            batch.delPage(4);
            storage->write(std::move(batch));
            EXPECT_FALSE(storage->getEntry(4).isValid());
        }
    }
}

TEST_F(PageStorageV2With2PagesTest, PutRefPagesOverRefPages)
{
    /// put ref page to ref page, ref path collapse to normal page
    {
        WriteBatch batch;
        // RefPage3 -> Page1
        batch.putRefPage(3, 1);
        // RefPage4 -> RefPage3 -> Page1
        batch.putRefPage(4, 3);
        storage->write(std::move(batch));
    }

    const auto p0entry = storage->getEntry(1);

    {
        // check that RefPage3 -> Page1
        auto entry = storage->getEntry(3);
        ASSERT_EQ(entry.fileIdLevel(), p0entry.fileIdLevel());
        ASSERT_EQ(entry.offset, p0entry.offset);
        ASSERT_EQ(entry.size, p0entry.size);
        const Page page3 = storage->read(3);
        for (size_t i = 0; i < page3.data.size(); ++i)
        {
            EXPECT_EQ(*(page3.data.begin() + i), 0x01);
        }
    }

    {
        // check that RefPage4 -> Page1
        auto entry = storage->getEntry(4);
        ASSERT_EQ(entry.fileIdLevel(), p0entry.fileIdLevel());
        ASSERT_EQ(entry.offset, p0entry.offset);
        ASSERT_EQ(entry.size, p0entry.size);
        const Page page4 = storage->read(4);
        for (size_t i = 0; i < page4.data.size(); ++i)
        {
            EXPECT_EQ(*(page4.data.begin() + i), 0x01);
        }
    }
}

TEST_F(PageStorageV2With2PagesTest, PutDuplicateRefPages)
{
    /// put duplicated RefPages in different WriteBatch
    {
        WriteBatch batch;
        batch.putRefPage(3, 1);
        storage->write(std::move(batch));

        WriteBatch batch2;
        batch2.putRefPage(3, 1);
        storage->write(std::move(batch));
        // now Page1's entry has ref count == 2 but not 3
    }
    PageEntry entry1 = storage->getEntry(1);
    ASSERT_TRUE(entry1.isValid());
    PageEntry entry3 = storage->getEntry(3);
    ASSERT_TRUE(entry3.isValid());

    EXPECT_EQ(entry1.fileIdLevel(), entry3.fileIdLevel());
    EXPECT_EQ(entry1.offset, entry3.offset);
    EXPECT_EQ(entry1.size, entry3.size);
    EXPECT_EQ(entry1.checksum, entry3.checksum);

    // check Page1's entry has ref count == 2 but not 1
    {
        WriteBatch batch;
        batch.delPage(1);
        storage->write(std::move(batch));
        PageEntry entry_after_del1 = storage->getEntry(3);
        ASSERT_TRUE(entry_after_del1.isValid());
        EXPECT_EQ(entry1.fileIdLevel(), entry_after_del1.fileIdLevel());
        EXPECT_EQ(entry1.offset, entry_after_del1.offset);
        EXPECT_EQ(entry1.size, entry_after_del1.size);
        EXPECT_EQ(entry1.checksum, entry_after_del1.checksum);

        WriteBatch batch2;
        batch2.delPage(3);
        storage->write(std::move(batch2));
        PageEntry entry_after_del2 = storage->getEntry(3);
        ASSERT_FALSE(entry_after_del2.isValid());
    }
}

TEST_F(PageStorageV2With2PagesTest, PutCollapseDuplicatedRefPages)
{
    /// put duplicated RefPages due to ref-path-collapse
    {
        WriteBatch batch;
        // RefPage3 -> Page1
        batch.putRefPage(3, 1);
        // RefPage4 -> RefPage3, collapse to RefPage4 -> Page1
        batch.putRefPage(4, 3);
        storage->write(std::move(batch));

        WriteBatch batch2;
        // RefPage4 -> Page1, duplicated due to ref-path-collapse
        batch2.putRefPage(4, 1);
        storage->write(std::move(batch));
        // now Page1's entry has ref count == 3 but not 2
    }

    PageEntry entry1 = storage->getEntry(1);
    ASSERT_TRUE(entry1.isValid());
    PageEntry entry3 = storage->getEntry(3);
    ASSERT_TRUE(entry3.isValid());
    PageEntry entry4 = storage->getEntry(4);
    ASSERT_TRUE(entry4.isValid());

    EXPECT_EQ(entry1.fileIdLevel(), entry4.fileIdLevel());
    EXPECT_EQ(entry1.offset, entry4.offset);
    EXPECT_EQ(entry1.size, entry4.size);
    EXPECT_EQ(entry1.checksum, entry4.checksum);

    // check Page1's entry has ref count == 3 but not 2
    {
        WriteBatch batch;
        batch.delPage(1);
        batch.delPage(4);
        storage->write(std::move(batch));
        PageEntry entry_after_del2 = storage->getEntry(3);
        ASSERT_TRUE(entry_after_del2.isValid());
        EXPECT_EQ(entry1.fileIdLevel(), entry_after_del2.fileIdLevel());
        EXPECT_EQ(entry1.offset, entry_after_del2.offset);
        EXPECT_EQ(entry1.size, entry_after_del2.size);
        EXPECT_EQ(entry1.checksum, entry_after_del2.checksum);

        WriteBatch batch2;
        batch2.delPage(3);
        storage->write(std::move(batch2));
        PageEntry entry_after_del3 = storage->getEntry(3);
        ASSERT_FALSE(entry_after_del3.isValid());
    }
}

TEST_F(PageStorageV2With2PagesTest, AddRefPageToNonExistPage)
try
{
    {
        WriteBatch batch;
        // RefPage3 -> non-exist Page999
        batch.putRefPage(3, 999);
        ASSERT_NO_THROW(storage->write(std::move(batch)));
    }

    ASSERT_FALSE(storage->getEntry(3).isValid());
    ASSERT_THROW(storage->read(3), DB::Exception);
    // storage->read(3);

    // Invalid Pages is filtered after reopen PageStorage
    ASSERT_NO_THROW(reopenWithConfig(config));
    ASSERT_FALSE(storage->getEntry(3).isValid());
    ASSERT_THROW(storage->read(3), DB::Exception);
    // storage->read(3);

    // Test Add RefPage to non exists page with snapshot acuqired.
    {
        auto snap = storage->getSnapshot();
        {
            WriteBatch batch;
            // RefPage3 -> non-exist Page999
            batch.putRefPage(8, 999);
            ASSERT_NO_THROW(storage->write(std::move(batch)));
        }

        ASSERT_FALSE(storage->getEntry(8).isValid());
        ASSERT_THROW(storage->read(8), DB::Exception);
        // storage->read(8);
    }
    // Invalid Pages is filtered after reopen PageStorage
    ASSERT_NO_THROW(reopenWithConfig(config));
    ASSERT_FALSE(storage->getEntry(8).isValid());
    ASSERT_THROW(storage->read(8), DB::Exception);
    // storage->read(8);
}
CATCH

namespace
{
CurrentMetrics::Value getPSMVCCNumSnapshots()
{
    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
    {
        if (i == CurrentMetrics::PSMVCCNumSnapshots)
        {
            return CurrentMetrics::values[i].load(std::memory_order_relaxed);
        }
    }
    throw Exception(std::string(CurrentMetrics::getDescription(CurrentMetrics::PSMVCCNumSnapshots)) + " not found.");
}

} // namespace


TEST_F(PageStorageV2With2PagesTest, SnapshotReadSnapshotVersion)
{
    char ch_before = 0x01;
    char ch_update = 0xFF;

    EXPECT_EQ(getPSMVCCNumSnapshots(), 0);
    auto snapshot = storage->getSnapshot();
    EXPECT_EQ(getPSMVCCNumSnapshots(), 1);
    PageEntry p1_snapshot_entry = storage->getEntry(1, snapshot);

    {
        // write new version of Page1
        const size_t buf_sz = 1024;
        char buf[buf_sz];
        {
            WriteBatch wb;
            memset(buf, ch_update, buf_sz);
            wb.putPage(1, 0, std::make_shared<ReadBufferFromMemory>(buf, buf_sz), buf_sz);
            wb.putPage(3, 0, std::make_shared<ReadBufferFromMemory>(buf, buf_sz), buf_sz);
            storage->write(std::move(wb));
        }
    }

    {
        /// read without snapshot
        PageEntry p1_entry = storage->getEntry(1);
        ASSERT_NE(p1_entry.checksum, p1_snapshot_entry.checksum);

        Page page1 = storage->read(1);
        ASSERT_EQ(*page1.data.begin(), ch_update);

        // Page3
        PageEntry p3_entry = storage->getEntry(3);
        ASSERT_TRUE(p3_entry.isValid());
        Page page3 = storage->read(3);
        ASSERT_EQ(*page3.data.begin(), ch_update);
    }

    {
        /// read with snapshot
        // getEntry with snapshot
        PageEntry p1_entry = storage->getEntry(1, snapshot);
        ASSERT_EQ(p1_entry.checksum, p1_snapshot_entry.checksum);

        // read(PageId) with snapshot
        Page page1 = storage->read(1, nullptr, snapshot);
        ASSERT_EQ(*page1.data.begin(), ch_before);

        // read(vec<PageId>) with snapshot
        PageIds ids{
            1,
        };
        auto pages = storage->read(ids, nullptr, snapshot);
        ASSERT_EQ(pages.count(1), 1UL);
        ASSERT_EQ(*pages.at(1).data.begin(), ch_before);
        // TODO read(vec<PageId>, callback) with snapshot

        // new page do appear while read with snapshot
        PageEntry p3_entry = storage->getEntry(3, snapshot);
        ASSERT_FALSE(p3_entry.isValid());
        ASSERT_THROW({ storage->read(3, nullptr, snapshot); }, DB::Exception);
    }
}

TEST_F(PageStorageV2With2PagesTest, GetIdenticalSnapshots)
{
    char ch_before = 0x01;
    char ch_update = 0xFF;
    PageEntry p1_snapshot_entry = storage->getEntry(1);
    EXPECT_EQ(getPSMVCCNumSnapshots(), 0);
    auto s1 = storage->getSnapshot();
    EXPECT_EQ(getPSMVCCNumSnapshots(), 1);
    auto s2 = storage->getSnapshot();
    EXPECT_EQ(getPSMVCCNumSnapshots(), 2);
    auto s3 = storage->getSnapshot();
    EXPECT_EQ(getPSMVCCNumSnapshots(), 3);

    {
        // write new version of Page1
        const size_t buf_sz = 1024;
        char buf[buf_sz];
        {
            WriteBatch wb;
            memset(buf, ch_update, buf_sz);
            wb.putPage(1, 0, std::make_shared<ReadBufferFromMemory>(buf, buf_sz), buf_sz);
            wb.putPage(3, 0, std::make_shared<ReadBufferFromMemory>(buf, buf_sz), buf_sz);
            storage->write(std::move(wb));
        }
    }

    /// read with snapshot
    const PageIds ids{
        1,
    };
    // getEntry with snapshot
    PageEntry p1_entry = storage->getEntry(1, s1);
    ASSERT_EQ(p1_entry.checksum, p1_snapshot_entry.checksum);
    p1_entry = storage->getEntry(1, s2);
    ASSERT_EQ(p1_entry.checksum, p1_snapshot_entry.checksum);
    p1_entry = storage->getEntry(1, s3);
    ASSERT_EQ(p1_entry.checksum, p1_snapshot_entry.checksum);
    // read(PageId) with snapshot
    Page page1 = storage->read(1, nullptr, s1);
    ASSERT_EQ(*page1.data.begin(), ch_before);
    page1 = storage->read(1, nullptr, s2);
    ASSERT_EQ(*page1.data.begin(), ch_before);
    page1 = storage->read(1, nullptr, s3);
    ASSERT_EQ(*page1.data.begin(), ch_before);
    // read(vec<PageId>) with snapshot
    auto pages = storage->read(ids, nullptr, s1);
    ASSERT_EQ(pages.count(1), 1UL);
    ASSERT_EQ(*pages.at(1).data.begin(), ch_before);
    pages = storage->read(ids, nullptr, s2);
    ASSERT_EQ(pages.count(1), 1UL);
    ASSERT_EQ(*pages.at(1).data.begin(), ch_before);
    pages = storage->read(ids, nullptr, s3);
    ASSERT_EQ(pages.count(1), 1UL);
    ASSERT_EQ(*pages.at(1).data.begin(), ch_before);
    // TODO read(vec<PageId>, callback) with snapshot
    // without snapshot
    p1_entry = storage->getEntry(1);
    ASSERT_NE(p1_entry.checksum, p1_snapshot_entry.checksum);

    s1.reset(); /// free snapshot 1
    EXPECT_EQ(getPSMVCCNumSnapshots(), 2);

    // getEntry with snapshot
    p1_entry = storage->getEntry(1, s2);
    ASSERT_EQ(p1_entry.checksum, p1_snapshot_entry.checksum);
    p1_entry = storage->getEntry(1, s3);
    ASSERT_EQ(p1_entry.checksum, p1_snapshot_entry.checksum);
    // read(PageId) with snapshot
    page1 = storage->read(1, nullptr, s2);
    ASSERT_EQ(*page1.data.begin(), ch_before);
    page1 = storage->read(1, nullptr, s3);
    ASSERT_EQ(*page1.data.begin(), ch_before);
    // read(vec<PageId>) with snapshot
    ASSERT_EQ(*pages.at(1).data.begin(), ch_before);
    pages = storage->read(ids, nullptr, s2);
    ASSERT_EQ(pages.count(1), 1UL);
    ASSERT_EQ(*pages.at(1).data.begin(), ch_before);
    pages = storage->read(ids, nullptr, s3);
    ASSERT_EQ(pages.count(1), 1UL);
    ASSERT_EQ(*pages.at(1).data.begin(), ch_before);
    // TODO read(vec<PageId>, callback) with snapshot
    // without snapshot
    p1_entry = storage->getEntry(1);
    ASSERT_NE(p1_entry.checksum, p1_snapshot_entry.checksum);

    s2.reset(); /// free snapshot 2
    EXPECT_EQ(getPSMVCCNumSnapshots(), 1);

    // getEntry with snapshot
    p1_entry = storage->getEntry(1, s3);
    ASSERT_EQ(p1_entry.checksum, p1_snapshot_entry.checksum);
    // read(PageId) with snapshot
    page1 = storage->read(1, nullptr, s3);
    ASSERT_EQ(*page1.data.begin(), ch_before);
    // read(vec<PageId>) with snapshot
    pages = storage->read(ids, nullptr, s3);
    ASSERT_EQ(pages.count(1), 1UL);
    ASSERT_EQ(*pages.at(1).data.begin(), ch_before);
    // TODO read(vec<PageId>, callback) with snapshot
    // without snapshot
    p1_entry = storage->getEntry(1);
    ASSERT_NE(p1_entry.checksum, p1_snapshot_entry.checksum);

    s3.reset(); /// free snapshot 3
    EXPECT_EQ(getPSMVCCNumSnapshots(), 0);

    // without snapshot
    p1_entry = storage->getEntry(1);
    ASSERT_NE(p1_entry.checksum, p1_snapshot_entry.checksum);
}

} // namespace PS::V2::tests
} // namespace DB
