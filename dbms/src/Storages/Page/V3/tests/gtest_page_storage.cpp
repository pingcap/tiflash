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

#include <Common/FailPoint.h>
#include <Common/SyncPoint/Ctl.h>
#include <IO/BaseFile/PosixRandomAccessFile.h>
#include <IO/BaseFile/RandomAccessFile.h>
#include <IO/BaseFile/RateLimiter.h>
#include <IO/Encryption/MockKeyManager.h>
#include <Interpreters/Context.h>
#include <Storages/Page/ConfigSettings.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/PageDefines.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/PageStorageImpl.h>
#include <Storages/Page/V3/WAL/WALReader.h>
#include <Storages/Page/V3/tests/entries_helper.h>
#include <Storages/Page/V3/tests/gtest_page_storage.h>
#include <Storages/Page/WriteBatchImpl.h>
#include <Storages/PathPool.h>
#include <TestUtils/MockDiskDelegator.h>
#include <TestUtils/MockReadLimiter.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/types.h>

#include <ext/scope_guard.h>
#include <future>
#include <random>

namespace DB
{
namespace FailPoints
{
extern const char exception_before_page_file_write_sync[];
extern const char force_set_page_file_write_errno[];
extern const char force_pick_all_blobs_to_full_gc[];
} // namespace FailPoints

namespace PS::V3::tests
{

TEST_F(PageStorageTest, WriteRead)
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
        page_storage->write(std::move(batch));
    }

    DB::Page page0 = page_storage->read(0);
    ASSERT_EQ(page0.data.size(), buf_sz);
    ASSERT_EQ(page0.page_id, 0UL);
    for (size_t i = 0; i < buf_sz; ++i)
    {
        EXPECT_EQ(*(page0.data.begin() + i), static_cast<char>(i % 0xff));
    }
    DB::Page page1 = page_storage->read(1);
    ASSERT_EQ(page1.data.size(), buf_sz);
    ASSERT_EQ(page1.page_id, 1UL);
    for (size_t i = 0; i < buf_sz; ++i)
    {
        EXPECT_EQ(*(page1.data.begin() + i), static_cast<char>(i % 0xff));
    }
}
CATCH

TEST_F(PageStorageTest, WriteReadWithIOLimiter)
try
{
    // In this case, WalStore throput is very low.
    // Because we only have 5 record to write.
    size_t wb_nums = 5;
    PageIdU64 page_id = 50;
    size_t buff_size = 100ul * 1024;
    const size_t rate_target = buff_size - 1;

    char c_buff[wb_nums * buff_size];

    WriteBatch wbs[wb_nums];
    u128::PageEntriesEdit edits[wb_nums];

    for (size_t i = 0; i < wb_nums; ++i)
    {
        for (size_t j = 0; j < buff_size; ++j)
        {
            c_buff[j + i * buff_size] = static_cast<char>((j & 0xff) + i);
        }

        ReadBufferPtr buff
            = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff + i * buff_size), buff_size);
        wbs[i].putPage(page_id + i, /* tag */ 0, buff, buff_size);
    }
    WriteLimiterPtr write_limiter = std::make_shared<WriteLimiter>(rate_target, LimiterType::UNKNOW, 20);

    AtomicStopwatch write_watch;
    for (size_t i = 0; i < wb_nums; ++i)
    {
        page_storage->write(std::move(wbs[i]), write_limiter);
    }
    auto write_elapsed = write_watch.elapsedSeconds();
    auto write_actual_rate = write_limiter->getTotalBytesThrough() / write_elapsed;

    // It must lower than 1.30
    // But we do have some disk rw, so don't set GE
    EXPECT_LE(write_actual_rate / rate_target, 1.30);

    Int64 consumed = 0;
    auto get_stat = [&consumed]() {
        return consumed;
    };

    {
        ReadLimiterPtr read_limiter = std::make_shared<MockReadLimiter>(get_stat, rate_target, LimiterType::UNKNOW);

        AtomicStopwatch read_watch;
        for (size_t i = 0; i < wb_nums; ++i)
        {
            page_storage->readImpl(TEST_NAMESPACE_ID, page_id + i, read_limiter, nullptr, true);
        }

        auto read_elapsed = read_watch.elapsedSeconds();
        auto read_actual_rate = read_limiter->getTotalBytesThrough() / read_elapsed;
        EXPECT_LE(read_actual_rate / rate_target, 1.30);
    }

    {
        ReadLimiterPtr read_limiter = std::make_shared<MockReadLimiter>(get_stat, rate_target, LimiterType::UNKNOW);

        std::vector<PageIdU64> page_ids;
        for (size_t i = 0; i < wb_nums; ++i)
        {
            page_ids.emplace_back(page_id + i);
        }

        AtomicStopwatch read_watch;
        page_storage->readImpl(TEST_NAMESPACE_ID, page_ids, read_limiter, nullptr, true);

        auto read_elapsed = read_watch.elapsedSeconds();
        auto read_actual_rate = read_limiter->getTotalBytesThrough() / read_elapsed;
        EXPECT_LE(read_actual_rate / rate_target, 1.30);
    }
}
CATCH

TEST_F(PageStorageTest, GCWithReadLimiter)
try
{
    // In this case, WALStore throput is very low.
    // Because we only have 10 record to write.
    const size_t buff_size = 10ul * 1024;
    char c_buff[buff_size];

    const size_t num_repeat = 5;

    // put page [1,num_repeat]
    for (size_t n = 1; n <= num_repeat; ++n)
    {
        WriteBatch batch;
        memset(c_buff, n, buff_size);
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(n, 0, buff, buff_size);
        page_storage->write(std::move(batch));
    }

    // put page [num_repeat + 1, num_repeat * 6]
    for (size_t n = num_repeat + 1; n <= num_repeat * 6; ++n)
    {
        WriteBatch batch;
        memset(c_buff, n, buff_size);
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(1, 0, buff, buff_size);
        page_storage->write(std::move(batch));
    }

    const size_t rate_target = buff_size - 1;

    Int64 consumed = 0;
    auto get_stat = [&consumed]() {
        return consumed;
    };
    ReadLimiterPtr read_limiter = std::make_shared<MockReadLimiter>(get_stat, rate_target, LimiterType::UNKNOW);

    AtomicStopwatch read_watch;
    page_storage->gc(/*not_skip*/ false, nullptr, read_limiter);

    auto elapsed = read_watch.elapsedSeconds();
    auto read_actual_rate = read_limiter->getTotalBytesThrough() / elapsed;
    EXPECT_LE(read_actual_rate / rate_target, 1.30);
}
CATCH

TEST_F(PageStorageTest, GCWithWriteLimiter)
try
{
    // In this case, BlobStore throput is very low.
    // Because we only need 1024* 150bytes to new blob.
    const size_t buff_size = 10;
    char c_buff[buff_size];

    const size_t num_repeat = 1024 * 300ul;

    for (size_t n = 1; n <= num_repeat; ++n)
    {
        WriteBatch batch;
        memset(c_buff, n, buff_size);
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(n <= num_repeat / 2 ? n : 1, 0, buff, buff_size);
        page_storage->write(std::move(batch));
    }

    const size_t rate_target = DB::PAGE_META_ROLL_SIZE - 1;

    WriteLimiterPtr write_limiter = std::make_shared<WriteLimiter>(rate_target, LimiterType::UNKNOW, 20);

    AtomicStopwatch write_watch;
    page_storage->gc(/*not_skip*/ false, write_limiter, nullptr);

    auto elapsed = write_watch.elapsedSeconds();
    auto read_actual_rate = write_limiter->getTotalBytesThrough() / elapsed;

    EXPECT_LE(read_actual_rate / rate_target, 1.30);
}
CATCH

TEST_F(PageStorageTest, GCWithWriteLimiter2)
try
{
    // In this case, BlobStore throput is very low.
    // Because we only need 1bytes * to new blob.
    const size_t buff_size = 1024 * 300ul;
    char c_buff[buff_size];

    const size_t num_repeat = 8;

    // put page [1,num_repeat]
    for (size_t n = 1; n <= num_repeat; ++n)
    {
        WriteBatch batch;
        memset(c_buff, n, buff_size);
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(n, 0, buff, buff_size);
        page_storage->write(std::move(batch));
    }

    // put page [num_repeat + 1, num_repeat * 6]
    for (size_t n = num_repeat + 1; n <= num_repeat * 6; ++n)
    {
        WriteBatch batch;
        memset(c_buff, n, buff_size);
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(1, 0, buff, buff_size);
        page_storage->write(std::move(batch));
    }

    // It is meanless, Because in GC, BlobStore will compact all data(<512M) in single IO
    // But we still can make sure through is corrent.
    const size_t rate_target = buff_size - 1;

    WriteLimiterPtr write_limiter = std::make_shared<WriteLimiter>(rate_target, LimiterType::UNKNOW, 20);

    AtomicStopwatch write_watch;
    page_storage->gc(/*not_skip*/ false, write_limiter, nullptr);

    auto elapsed = write_watch.elapsedSeconds();
    auto read_actual_rate = write_limiter->getTotalBytesThrough() / elapsed;
    EXPECT_LE(read_actual_rate / rate_target, 1.30);
}
CATCH

TEST_F(PageStorageTest, WriteReadWithEncryption)
try
{
    const UInt64 tag = 0;
    const size_t buf_sz = 1024;
    char c_buff[buf_sz];
    for (size_t i = 0; i < buf_sz; ++i)
    {
        c_buff[i] = i % 0xff;
    }

    KeyManagerPtr key_manager = std::make_shared<MockKeyManager>(true);
    const auto enc_file_provider = std::make_shared<FileProvider>(key_manager, true);
    auto delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(getTemporaryPath());
    auto page_storage_enc = std::make_shared<PageStorageImpl>("test.t", delegator, config, enc_file_provider);
    page_storage_enc->restore();
    {
        WriteBatch batch;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(1, tag, buff, buf_sz);
        buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(2, tag, buff, buf_sz);
        page_storage_enc->write(std::move(batch));
    }

    // Make sure that we can't restore from no-enc pagestore.
    // Because WALStore can't get any record from it.

    page_storage->restore();
    ASSERT_ANY_THROW(page_storage->read(1));

    page_storage_enc = std::make_shared<PageStorageImpl>("test.t", delegator, config, enc_file_provider);
    page_storage_enc->restore();

    DB::Page page1 = page_storage_enc->read(1);
    ASSERT_EQ(page1.data.size(), buf_sz);
    ASSERT_EQ(page1.page_id, 1UL);
    for (size_t i = 0; i < buf_sz; ++i)
    {
        EXPECT_EQ(*(page1.data.begin() + i), static_cast<char>(i % 0xff));
    }
    DB::Page page2 = page_storage_enc->read(2);
    ASSERT_EQ(page2.data.size(), buf_sz);
    ASSERT_EQ(page2.page_id, 2UL);
    for (size_t i = 0; i < buf_sz; ++i)
    {
        EXPECT_EQ(*(page2.data.begin() + i), static_cast<char>(i % 0xff));
    }

    char c_buff_read[buf_sz] = {0};

    // Make sure in-disk data is encrypted.

    RandomAccessFilePtr file_read = std::make_shared<PosixRandomAccessFile>(
        fmt::format(
            "{}/{}{}",
            getTemporaryPath(),
            BlobFile::BLOB_PREFIX_NAME,
            PageTypeUtils::nextFileID(PageType::Normal, 1)),
        -1,
        nullptr);
    file_read->pread(c_buff_read, buf_sz, 0);
    ASSERT_NE(c_buff_read, c_buff);
    file_read->pread(c_buff_read, buf_sz, buf_sz);
    ASSERT_NE(c_buff_read, c_buff);
}
CATCH


TEST_F(PageStorageTest, ReadNULL)
try
{
    {
        WriteBatch batch;
        batch.putExternal(0, 0);
        page_storage->write(std::move(batch));
    }
    const auto & page = page_storage->read(0);
    ASSERT_EQ(page.data.begin(), nullptr);
}
CATCH

TEST_F(PageStorageTest, readNotThrowOnNotFound)
try
{
    const size_t buf_sz = 100;
    char c_buff[buf_sz] = {0};

    {
        const auto & page = page_storage->readImpl(TEST_NAMESPACE_ID, 1, nullptr, nullptr, false);
        ASSERT_FALSE(page.isValid());
    }

    {
        WriteBatch batch;
        batch.putPage(1, 0, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz);
        batch.putPage(3, 0, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz);
        batch.putPage(4, 0, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz, {20, 20, 30, 30});
        page_storage->write(std::move(batch));
    }

    {
        std::vector<PageIdU64> page_ids = {1, 2, 5};
        // readImpl(TEST_NAMESPACE_ID, page_ids, nullptr, nullptr, true);
        auto page_maps = page_storage->readImpl(TEST_NAMESPACE_ID, page_ids, nullptr, nullptr, false);
        ASSERT_EQ(page_maps.at(1).page_id, 1);
        ASSERT_FALSE(page_maps.at(2).isValid());
        ASSERT_FALSE(page_maps.at(5).isValid());

        const auto & page1 = page_storage->readImpl(TEST_NAMESPACE_ID, 1, nullptr, nullptr, false);
        ASSERT_EQ(page1.page_id, 1);

        const auto & page2 = page_storage->readImpl(TEST_NAMESPACE_ID, 2, nullptr, nullptr, false);
        ASSERT_FALSE(page2.isValid());

        std::vector<PageStorage::PageReadFields> fields{
            {4, {0, 1, 2}},
            {6, {0, 1, 2}},
            {2, {0, 1, 2}},
            {5, {0, 1, 2}},
        };

        page_maps = page_storage->readImpl(TEST_NAMESPACE_ID, fields, nullptr, nullptr, false);
        ASSERT_EQ(page_maps.at(4).page_id, 4);
        ASSERT_EQ(page_maps.at(4).fieldSize(), 3);
        ASSERT_EQ(page_maps.at(4).data.size(), 20 + 20 + 30);
        // the invalid page ids in input param are returned with INVALID_ID
        ASSERT_GT(page_maps.count(6), 0);
        ASSERT_EQ(page_maps.at(6).isValid(), false);
        ASSERT_GT(page_maps.count(2), 0);
        ASSERT_EQ(page_maps.at(2).isValid(), false);
        ASSERT_GT(page_maps.count(5), 0);
        ASSERT_EQ(page_maps.at(5).isValid(), false);
    }
    {
        // Read with id can also fetch the fieldOffsets
        auto page_4 = page_storage->readImpl(TEST_NAMESPACE_ID, 4, nullptr, nullptr, false);
        ASSERT_EQ(page_4.fieldSize(), 4);
        ASSERT_EQ(page_4.getFieldData(0).size(), 20);
        ASSERT_EQ(page_4.getFieldData(1).size(), 20);
        ASSERT_EQ(page_4.getFieldData(2).size(), 30);
        ASSERT_EQ(page_4.getFieldData(3).size(), 30);

        auto page_field_sizes = PageUtil::getFieldSizes(page_4.field_offsets, page_4.data.size());
        ASSERT_EQ(page_field_sizes.size(), 4);
        ASSERT_EQ(page_field_sizes[0], 20);
        ASSERT_EQ(page_field_sizes[1], 20);
        ASSERT_EQ(page_field_sizes[2], 30);
        ASSERT_EQ(page_field_sizes[3], 30);
    }
    {
        // Read with ids can also fetch the fieldOffsets
        std::vector<PageIdU64> page_ids{4};
        auto pages = page_storage->readImpl(TEST_NAMESPACE_ID, page_ids, nullptr, nullptr, false);
        ASSERT_EQ(pages.size(), 1);
        ASSERT_GT(pages.count(4), 0);
        auto page_4 = pages.at(4);
        ASSERT_EQ(page_4.fieldSize(), 4);
        ASSERT_EQ(page_4.getFieldData(0).size(), 20);
        ASSERT_EQ(page_4.getFieldData(1).size(), 20);
        ASSERT_EQ(page_4.getFieldData(2).size(), 30);
        ASSERT_EQ(page_4.getFieldData(3).size(), 30);

        auto page_field_sizes = PageUtil::getFieldSizes(page_4.field_offsets, page_4.data.size());
        ASSERT_EQ(page_field_sizes.size(), 4);
        ASSERT_EQ(page_field_sizes[0], 20);
        ASSERT_EQ(page_field_sizes[1], 20);
        ASSERT_EQ(page_field_sizes[2], 30);
        ASSERT_EQ(page_field_sizes[3], 30);
    }
}
CATCH

TEST_F(PageStorageTest, WriteMultipleBatchRead1)
try
{
    {
        WriteBatch batch;
        batch.putPage(0, default_tag, getDefaultBuffer(), buf_sz);
        page_storage->write(std::move(batch));
    }
    {
        WriteBatch batch;
        batch.putPage(1, default_tag, getDefaultBuffer(), buf_sz);
        page_storage->write(std::move(batch));
    }

    DB::Page page0 = page_storage->read(0);
    ASSERT_EQ(page0.data.size(), buf_sz);
    ASSERT_EQ(page0.page_id, 0);
    for (size_t i = 0; i < buf_sz; ++i)
    {
        EXPECT_EQ(*(page0.data.begin() + i), static_cast<char>(i % 0xff));
    }
    DB::Page page1 = page_storage->read(1);
    ASSERT_EQ(page1.data.size(), buf_sz);
    ASSERT_EQ(page1.page_id, 1);
    for (size_t i = 0; i < buf_sz; ++i)
    {
        EXPECT_EQ(*(page1.data.begin() + i), static_cast<char>(i % 0xff));
    }
}
CATCH

TEST_F(PageStorageTest, WriteMultipleBatchRead2)
try
{
    const UInt64 tag = 0;
    const size_t buf_sz = 1024;
    char c_buff1[buf_sz], c_buff2[buf_sz];
    for (size_t i = 0; i < buf_sz; ++i)
    {
        c_buff1[i] = i % 0xff;
        c_buff2[i] = i % 0xff + 1;
    }

    {
        WriteBatch batch;
        ReadBufferPtr buff1 = std::make_shared<ReadBufferFromMemory>(c_buff1, buf_sz);
        ReadBufferPtr buff2 = std::make_shared<ReadBufferFromMemory>(c_buff2, buf_sz);
        batch.putPage(0, tag, buff1, buf_sz);
        batch.putPage(1, tag, buff2, buf_sz);
        page_storage->write(std::move(batch));
    }

    DB::Page page0 = page_storage->read(0);
    ASSERT_EQ(page0.data.size(), buf_sz);
    ASSERT_EQ(page0.page_id, 0UL);
    for (size_t i = 0; i < buf_sz; ++i)
    {
        EXPECT_EQ(*(page0.data.begin() + i), static_cast<char>(i % 0xff));
    }
    DB::Page page1 = page_storage->read(1);
    ASSERT_EQ(page1.data.size(), buf_sz);
    ASSERT_EQ(page1.page_id, 1UL);
    for (size_t i = 0; i < buf_sz; ++i)
    {
        EXPECT_EQ(*(page1.data.begin() + i), static_cast<char>(i % 0xff + 1));
    }
}
CATCH

TEST_F(PageStorageTest, MultipleWriteRead)
{
    size_t page_id_max = 100;
    for (DB::PageIdU64 page_id = 0; page_id <= page_id_max; ++page_id)
    {
        std::mt19937 size_gen;
        size_gen.seed(time(nullptr));
        std::uniform_int_distribution<> dist(0, 3000);

        const size_t buff_sz = 2 * DB::MB + dist(size_gen);
        char * buff = static_cast<char *>(malloc(buff_sz)); // NOLINT(cppcoreguidelines-no-malloc)
        if (buff == nullptr)
        {
            throw DB::Exception("Alloc fix memory failed.", DB::ErrorCodes::LOGICAL_ERROR);
        }

        const char buff_ch = page_id % 0xFF;
        memset(buff, buff_ch, buff_sz);
        DB::MemHolder holder
            = DB::createMemHolder(buff, [&](char * p) { free(p); }); // NOLINT(cppcoreguidelines-no-malloc)

        auto read_buff = std::make_shared<DB::ReadBufferFromMemory>(const_cast<char *>(buff), buff_sz);

        DB::WriteBatch wb;
        wb.putPage(page_id, 0, read_buff, read_buff->buffer().size());
        page_storage->write(std::move(wb));
    }

    for (DB::PageIdU64 page_id = 0; page_id <= page_id_max; ++page_id)
    {
        page_storage->read(page_id);
    }
}

TEST_F(PageStorageTest, WriteReadOnSamePageId)
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
        page_storage->write(std::move(batch));

        DB::Page page0 = page_storage->read(0);
        ASSERT_EQ(page0.data.size(), buf_sz);
        ASSERT_EQ(page0.page_id, 0UL);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page0.data.begin() + i), static_cast<char>(i % 0xff));
        }
    }

    for (char & i : c_buff)
    {
        i = 0x1;
    }

    {
        WriteBatch batch;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(0, tag, buff, buf_sz);
        page_storage->write(std::move(batch));

        DB::Page page0 = page_storage->read(0);
        ASSERT_EQ(page0.data.size(), buf_sz);
        ASSERT_EQ(page0.page_id, 0UL);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page0.data.begin() + i), static_cast<char>(0x01));
        }
    }
}

TEST_F(PageStorageTest, WriteReadAfterGc)
try
{
    const size_t buf_sz = 256;
    char c_buff[buf_sz];

    const size_t num_repeat = 10;
    PageIdU64 pid = 1;
    const char page0_byte = 0x3f;
    {
        // put page0
        WriteBatch batch;
        memset(c_buff, page0_byte, buf_sz);
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(0, 0, buff, buf_sz);
        page_storage->write(std::move(batch));
    }
    // repeated put page1
    for (size_t n = 1; n <= num_repeat; ++n)
    {
        WriteBatch batch;
        memset(c_buff, n, buf_sz);
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(pid, 0, buff, buf_sz);
        page_storage->write(std::move(batch));
    }

    {
        DB::Page page0 = page_storage->read(0);
        ASSERT_EQ(page0.data.size(), buf_sz);
        ASSERT_EQ(page0.page_id, 0UL);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page0.data.begin() + i), page0_byte);
        }

        DB::Page page1 = page_storage->read(pid);
        ASSERT_EQ(page1.data.size(), buf_sz);
        ASSERT_EQ(page1.page_id, pid);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page1.data.begin() + i), static_cast<char>(num_repeat % 0xff));
        }
    }

    page_storage->gc();

    {
        DB::Page page0 = page_storage->read(0);
        ASSERT_EQ(page0.data.size(), buf_sz);
        ASSERT_EQ(page0.page_id, 0UL);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page0.data.begin() + i), page0_byte);
        }

        DB::Page page1 = page_storage->read(pid);
        ASSERT_EQ(page1.data.size(), buf_sz);
        ASSERT_EQ(page1.page_id, pid);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page1.data.begin() + i), static_cast<char>(num_repeat % 0xff));
        }
    }
}
CATCH

TEST_F(PageStorageTest, DeadLockInMVCC)
try
{
    WriteBatch batch;
    {
        batch.putExternal(0, 0);
        batch.putRefPage(1, 0);
        batch.putExternal(1024, 0);
        page_storage->write(std::move(batch));
    }

    auto snapshot = page_storage->getSnapshot();

    {
        WriteBatch batch;
        batch.putRefPage(2, 1); // ref 2 -> 1 -> 0
        batch.delPage(1); // free ref 1 -> 0
        batch.delPage(1024); // free normal page 1024
        page_storage->write(std::move(batch));
    }
}
CATCH

TEST_F(PageStorageTest, IngestFile)
{
    WriteBatch wb;
    {
        wb.putExternal(100, 0);
        wb.putRefPage(101, 100);
        wb.putRefPage(102, 100);
        wb.delPage(100);
        page_storage->write(std::move(wb));
    }

    auto snapshot = page_storage->getSnapshot();

    EXPECT_ANY_THROW(page_storage->getNormalPageId(TEST_NAMESPACE_ID, 100, snapshot));
    EXPECT_EQ(100, page_storage->getNormalPageId(TEST_NAMESPACE_ID, 101, snapshot));
    EXPECT_EQ(100, page_storage->getNormalPageId(TEST_NAMESPACE_ID, 102, snapshot));

    size_t times_remover_called = 0;
    ExternalPageCallbacks callbacks;
    callbacks.scanner = []() -> ExternalPageCallbacks::PathAndIdsVec {
        return {};
    };
    callbacks.remover = [&times_remover_called](
                            const ExternalPageCallbacks::PathAndIdsVec &,
                            const std::set<PageIdU64> & living_page_ids) -> void {
        times_remover_called += 1;
        EXPECT_EQ(living_page_ids.size(), 1);
        EXPECT_GT(living_page_ids.count(100), 0);
    };
    callbacks.prefix = TEST_NAMESPACE_ID;
    page_storage->registerExternalPagesCallbacks(callbacks);
    page_storage->gc();
    ASSERT_EQ(times_remover_called, 1);
    page_storage->gc();
    ASSERT_EQ(times_remover_called, 2);
    page_storage->unregisterExternalPagesCallbacks(callbacks.prefix);
    page_storage->gc();
    ASSERT_EQ(times_remover_called, 2);
}

// TBD : enable after wal apply and restore
TEST_F(PageStorageTest, DISABLED_IgnoreIncompleteWriteBatch1)
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
            page_storage->write(std::move(batch));
        }
        catch (DB::Exception & e)
        {
            if (e.code() != ErrorCodes::FAIL_POINT_ERROR)
                throw;
        }
    }

    // Restore, the broken meta should be ignored
    page_storage = reopenWithConfig(PageStorageConfig{});

    {
        size_t num_pages = 0;
        page_storage->traverse([&num_pages](const DB::Page &) { num_pages += 1; });
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
        page_storage->write(std::move(batch));

        auto page1 = page_storage->read(1);
        ASSERT_EQ(page1.data.size(), buf_sz);
        for (size_t i = 0; i < page1.data.size(); ++i)
        {
            const auto * p = page1.data.begin();
            EXPECT_EQ(*p, 0x02);
        }
    }

    // Restore again, we should be able to read page 1
    page_storage = reopenWithConfig(PageStorageConfig{});

    {
        size_t num_pages = 0;
        page_storage->traverse([&num_pages](const Page &) { num_pages += 1; });
        ASSERT_EQ(num_pages, 1);

        auto page1 = page_storage->read(1);
        ASSERT_EQ(page1.data.size(), buf_sz);
        for (size_t i = 0; i < page1.data.size(); ++i)
        {
            const auto * p = page1.data.begin();
            EXPECT_EQ(*p, 0x02);
        }
    }
}
CATCH

// TBD : enable after wal apply and restore
TEST_F(PageStorageTest, DISABLED_IgnoreIncompleteWriteBatch2)
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
            page_storage->write(std::move(batch));
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
        page_storage->traverse([&num_pages](const Page &) { num_pages += 1; });
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
        page_storage->write(std::move(batch));

        auto page1 = page_storage->read(1);
        ASSERT_EQ(page1.data.size(), buf_sz);
        for (size_t i = 0; i < page1.data.size(); ++i)
        {
            const auto * p = page1.data.begin();
            EXPECT_EQ(*p, 0x02);
        }
    }

    // Restore again, we should be able to read page 1
    page_storage = reopenWithConfig(PageStorageConfig{});

    {
        size_t num_pages = 0;
        page_storage->traverse([&num_pages](const Page &) { num_pages += 1; });
        ASSERT_EQ(num_pages, 1);

        auto page1 = page_storage->read(1);
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
class PageStorageWith2PagesTest : public PageStorageTest
{
public:
    PageStorageWith2PagesTest() = default;

protected:
    void SetUp() override
    {
        PageStorageTest::SetUp();

        // put predefine Page1, Page2
        const size_t buf_sz = 1024;
        char buf1[buf_sz], buf2[buf_sz];
        {
            WriteBatch wb;
            memset(buf1, 0x01, buf_sz);
            memset(buf2, 0x02, buf_sz);

            wb.putPage(1, 0, std::make_shared<ReadBufferFromMemory>(buf1, buf_sz), buf_sz);
            wb.putPage(2, 0, std::make_shared<ReadBufferFromMemory>(buf2, buf_sz), buf_sz);

            page_storage->write(std::move(wb));
        }
    }
};


TEST_F(PageStorageWith2PagesTest, DeleteRefPages)
{
    // put ref page: RefPage3 -> Page2, RefPage4 -> Page2
    {
        WriteBatch batch;
        batch.putRefPage(3, 2);
        batch.putRefPage(4, 2);
        page_storage->write(std::move(batch));
    }
    { // tests for delete Page
        // delete RefPage3, RefPage4 don't get deleted
        {
            WriteBatch batch;
            batch.delPage(3);
            page_storage->write(std::move(batch));
            EXPECT_FALSE(page_storage->getEntry(3).isValid());
            EXPECT_TRUE(page_storage->getEntry(4).isValid());
        }
        // delete RefPage4
        {
            WriteBatch batch;
            batch.delPage(4);
            page_storage->write(std::move(batch));
            EXPECT_FALSE(page_storage->getEntry(4).isValid());
        }
    }
}

TEST_F(PageStorageWith2PagesTest, PutRefPagesOverRefPages)
{
    /// put ref page to ref page, ref path collapse to normal page
    {
        WriteBatch batch;
        // RefPage3 -> Page1
        batch.putRefPage(3, 1);
        // RefPage4 -> RefPage3 -> Page1
        batch.putRefPage(4, 2);
        page_storage->write(std::move(batch));
    }

    const auto p0entry = page_storage->getEntry(1);
    const auto p2entry = page_storage->getEntry(2);

    {
        // check that RefPage3 -> Page1
        auto entry = page_storage->getEntry(3);
        ASSERT_EQ(entry.fileIdLevel(), p0entry.fileIdLevel());
        ASSERT_EQ(entry.offset, p0entry.offset);
        ASSERT_EQ(entry.size, p0entry.size);
        const Page page3 = page_storage->read(3);
        for (size_t i = 0; i < page3.data.size(); ++i)
        {
            EXPECT_EQ(*(page3.data.begin() + i), 0x01);
        }
    }

    {
        // check that RefPage4 -> Page1
        auto entry = page_storage->getEntry(4);
        ASSERT_EQ(entry.fileIdLevel(), p2entry.fileIdLevel());
        ASSERT_EQ(entry.offset, p2entry.offset);
        ASSERT_EQ(entry.size, p2entry.size);
        const Page page4 = page_storage->read(4);
        for (size_t i = 0; i < page4.data.size(); ++i)
        {
            EXPECT_EQ(*(page4.data.begin() + i), 0x02);
        }
    }
}

TEST_F(PageStorageWith2PagesTest, PutDuplicateRefPages)
{
    /// put duplicated RefPages in different WriteBatch
    {
        WriteBatch batch;
        batch.putRefPage(3, 1);
        page_storage->write(std::move(batch));

        WriteBatch batch2;
        batch2.putRefPage(3, 1);
        page_storage->write(std::move(batch2));
        // now Page1's entry has ref count == 2 but not 3
    }
    PageEntry entry1 = page_storage->getEntry(1);
    ASSERT_TRUE(entry1.isValid());
    PageEntry entry3 = page_storage->getEntry(3);
    ASSERT_TRUE(entry3.isValid());

    EXPECT_EQ(entry1.fileIdLevel(), entry3.fileIdLevel());
    EXPECT_EQ(entry1.offset, entry3.offset);
    EXPECT_EQ(entry1.size, entry3.size);
    EXPECT_EQ(entry1.checksum, entry3.checksum);

    // check Page1's entry has ref count == 2 but not 1
    {
        WriteBatch batch;
        batch.delPage(1);
        page_storage->write(std::move(batch));
        PageEntry entry_after_del1 = page_storage->getEntry(3);
        ASSERT_TRUE(entry_after_del1.isValid());
        EXPECT_EQ(entry1.fileIdLevel(), entry_after_del1.fileIdLevel());
        EXPECT_EQ(entry1.offset, entry_after_del1.offset);
        EXPECT_EQ(entry1.size, entry_after_del1.size);
        EXPECT_EQ(entry1.checksum, entry_after_del1.checksum);

        WriteBatch batch2;
        batch2.delPage(3);
        page_storage->write(std::move(batch2));
        PageEntry entry_after_del2 = page_storage->getEntry(3);
        ASSERT_FALSE(entry_after_del2.isValid());
    }
}

TEST_F(PageStorageWith2PagesTest, PutCollapseDuplicatedRefPages)
{
    /// put duplicated RefPages due to ref-path-collapse
    {
        WriteBatch batch;
        // RefPage3 -> Page1
        batch.putRefPage(3, 1);
        // RefPage4 -> RefPage3, collapse to RefPage4 -> Page1
        batch.putRefPage(4, 3);
        page_storage->write(std::move(batch));

        WriteBatch batch2;
        // RefPage4 -> Page1, duplicated due to ref-path-collapse
        batch2.putRefPage(4, 1);
        page_storage->write(std::move(batch2));
        // now Page1's entry has ref count == 3 but not 2
    }

    PageEntry entry1 = page_storage->getEntry(1);
    ASSERT_TRUE(entry1.isValid());
    PageEntry entry3 = page_storage->getEntry(3);
    ASSERT_TRUE(entry3.isValid());
    PageEntry entry4 = page_storage->getEntry(4);
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
        page_storage->write(std::move(batch));
        PageEntry entry_after_del2 = page_storage->getEntry(3);
        ASSERT_TRUE(entry_after_del2.isValid());
        EXPECT_EQ(entry1.fileIdLevel(), entry_after_del2.fileIdLevel());
        EXPECT_EQ(entry1.offset, entry_after_del2.offset);
        EXPECT_EQ(entry1.size, entry_after_del2.size);
        EXPECT_EQ(entry1.checksum, entry_after_del2.checksum);

        WriteBatch batch2;
        batch2.delPage(3);
        page_storage->write(std::move(batch2));
        PageEntry entry_after_del3 = page_storage->getEntry(3);
        ASSERT_FALSE(entry_after_del3.isValid());
    }
}

TEST_F(PageStorageWith2PagesTest, RemoveReadOnlyFile)
{
    PageStorageConfig cfg;
    cfg.blob_heavy_gc_valid_rate = 1.0;
    page_storage = reopenWithConfig(cfg);

    auto blob_file1 = Poco::File(getTemporaryPath() + "/blobfile_10");
    auto blob_file2 = Poco::File(getTemporaryPath() + "/blobfile_20");
    ASSERT_EQ(blob_file1.exists(), true);
    ASSERT_EQ(blob_file2.exists(), false);

    // full gc happens, rewrite page from blobfile_1 to blobfile_2
    bool flag = page_storage->gcImpl(true, nullptr, nullptr);
    ASSERT_EQ(flag, true);
    ASSERT_EQ(blob_file1.exists(), true);
    ASSERT_EQ(blob_file2.exists(), true);

    // cleanup blobfile_1
    flag = page_storage->gcImpl(true, nullptr, nullptr);
    ASSERT_EQ(blob_file1.exists(), false);
    ASSERT_EQ(blob_file2.exists(), true);
    EXPECT_EQ(flag, true);
}

TEST_F(PageStorageWith2PagesTest, ReuseEmptyFileAfterRestart)
{
    {
        // delete the pages, the blobfile become "empty"
        WriteBatch wb;
        wb.delPage(1);
        wb.delPage(2);
        page_storage->write(std::move(wb));
    }

    PageStorageConfig cfg;
    cfg.blob_heavy_gc_valid_rate = 1.0;
    page_storage = reopenWithConfig(cfg);

    auto blob_file1 = Poco::File(getTemporaryPath() + "/blobfile_10");
    auto blob_file2 = Poco::File(getTemporaryPath() + "/blobfile_20");
    ASSERT_EQ(blob_file1.exists(), true);
    ASSERT_EQ(blob_file2.exists(), false);

    // the "empty" blobfile_1 will be reused for later writing,
    // no full gc happens.
    bool flag = page_storage->gcImpl(true, nullptr, nullptr);
    ASSERT_EQ(flag, false);
    ASSERT_EQ(blob_file1.exists(), true);
    ASSERT_EQ(blob_file2.exists(), false);
}

TEST_F(PageStorageWith2PagesTest, DISABLED_AddRefPageToNonExistPage)
try
{
    {
        WriteBatch batch;
        // RefPage3 -> non-exist Page999
        batch.putRefPage(3, 999);
        ASSERT_NO_THROW(page_storage->write(std::move(batch)));
    }

    ASSERT_FALSE(page_storage->getEntry(3).isValid());
    ASSERT_THROW(page_storage->read(3), DB::Exception);
    // page_storage->read(3);

    // Invalid Pages is filtered after reopen PageStorage
    ASSERT_NO_THROW(reopenWithConfig(config));
    ASSERT_FALSE(page_storage->getEntry(3).isValid());
    ASSERT_THROW(page_storage->read(3), DB::Exception);
    // page_storage->read(3);

    // Test Add RefPage to non exists page with snapshot acuqired.
    {
        auto snap = page_storage->getSnapshot();
        {
            WriteBatch batch;
            // RefPage3 -> non-exist Page999
            batch.putRefPage(8, 999);
            ASSERT_NO_THROW(page_storage->write(std::move(batch)));
        }

        ASSERT_FALSE(page_storage->getEntry(8).isValid());
        ASSERT_THROW(page_storage->read(8), DB::Exception);
        // page_storage->read(8);
    }
    // Invalid Pages is filtered after reopen PageStorage
    ASSERT_NO_THROW(reopenWithConfig(config));
    ASSERT_FALSE(page_storage->getEntry(8).isValid());
    ASSERT_THROW(page_storage->read(8), DB::Exception);
    // page_storage->read(8);
}
CATCH

TEST_F(PageStorageTest, WriteReadGcExternalPage)
try
{
    WriteBatch batch;
    {
        // External 0, 1024
        // Ref 1->0
        batch.putExternal(0, 0);
        batch.putRefPage(1, 0);
        batch.putExternal(1024, 0);
        page_storage->write(std::move(batch));
    }

    size_t times_remover_called = 0;

    enum
    {
        STAGE_SNAP_KEEP = 1,
        STAGE_SNAP_RELEASED = 2,
    } test_stage;
    test_stage = STAGE_SNAP_KEEP;

    ExternalPageCallbacks callbacks;
    callbacks.scanner = []() -> ExternalPageCallbacks::PathAndIdsVec {
        return {};
    };
    callbacks.remover = [&times_remover_called, &test_stage](
                            const ExternalPageCallbacks::PathAndIdsVec &,
                            const std::set<PageIdU64> & living_page_ids) -> void {
        times_remover_called += 1;
        switch (test_stage)
        {
        case STAGE_SNAP_KEEP:
        {
            // 0, 1024 are still alive
            EXPECT_EQ(living_page_ids.size(), 2);
            EXPECT_GT(living_page_ids.count(0), 0);
            EXPECT_GT(living_page_ids.count(1024), 0);
            break;
        }
        case STAGE_SNAP_RELEASED:
        {
            /// After `snapshot` released, 1024 should be removed from `living`
            EXPECT_EQ(living_page_ids.size(), 1);
            EXPECT_GT(living_page_ids.count(0), 0);
            break;
        }
        }
    };
    callbacks.prefix = TEST_NAMESPACE_ID;
    page_storage->registerExternalPagesCallbacks(callbacks);
    {
        SCOPED_TRACE("fist gc");
        page_storage->gc();
        EXPECT_EQ(times_remover_called, 1);
    }

    auto snapshot = page_storage->getSnapshot();

    {
        WriteBatch batch;
        batch.putRefPage(2, 1); // ref 2 -> 1 ==> 2 -> 0
        batch.delPage(1); // free ref 1 -> 0
        batch.delPage(1024); // free ext page 1024
        // External: 0, 1024(deleted)
        // Ref: 2->0, 1->0(deleted)
        page_storage->write(std::move(batch));
    }

    {
        // With `snapshot` is being held, nothing is need to be deleted
        SCOPED_TRACE("gc with snapshot");
        page_storage->gc();
        EXPECT_EQ(times_remover_called, 2);
    }

    {
        auto ori_id_0 = page_storage->getNormalPageId(TEST_NAMESPACE_ID, 0, nullptr);
        ASSERT_EQ(ori_id_0, 0);
        auto ori_id_2 = page_storage->getNormalPageId(TEST_NAMESPACE_ID, 2, nullptr);
        ASSERT_EQ(ori_id_2, 0);
        ASSERT_EQ(1024, page_storage->getNormalPageId(TEST_NAMESPACE_ID, 1024, snapshot));
        ASSERT_EQ(0, page_storage->getNormalPageId(TEST_NAMESPACE_ID, 1, snapshot));
        ASSERT_ANY_THROW(page_storage->getNormalPageId(TEST_NAMESPACE_ID, 1024, nullptr));
        ASSERT_ANY_THROW(page_storage->getNormalPageId(TEST_NAMESPACE_ID, 1, nullptr));
    }

    /// After `snapshot` released, 1024 should be removed from `living`
    snapshot.reset();
    test_stage = STAGE_SNAP_RELEASED;
    {
        SCOPED_TRACE("gc with snapshot released");
        page_storage->gc();
        EXPECT_EQ(times_remover_called, 3);
    }
}
CATCH

TEST_F(PageStorageTest, ConcurrencyAddExtCallbacks)
try
{
    NamespaceID ns_id1 = TEST_NAMESPACE_ID;
    NamespaceID ns_id2 = TEST_NAMESPACE_ID + 1;
    {
        WriteBatch wb(ns_id1);
        wb.putExternal(20, 0);
        page_storage->write(std::move(wb));
    }
    {
        WriteBatch wb(ns_id2);
        wb.putExternal(20, 0);
        page_storage->write(std::move(wb));
    }

    auto ptr = std::make_shared<Int32>(100); // mock the `StorageDeltaMerge`
    ExternalPageCallbacks callbacks;
    callbacks.prefix = ns_id1;
    callbacks.scanner = [ptr_weak_ref = std::weak_ptr<Int32>(ptr)]() -> ExternalPageCallbacks::PathAndIdsVec {
        auto ptr = ptr_weak_ref.lock();
        if (!ptr)
            return {};

        (*ptr) += 1; // mock access the storage inside callback
        return {};
    };
    callbacks.remover = [ptr_weak_ref = std::weak_ptr<Int32>(
                             ptr)](const ExternalPageCallbacks::PathAndIdsVec &, const std::set<PageIdU64> &) -> void {
        auto ptr = ptr_weak_ref.lock();
        if (!ptr)
            return;

        (*ptr) += 1; // mock access the storage inside callback
    };
    page_storage->registerExternalPagesCallbacks(callbacks);

    // Start a PageStorage gc and suspend it before clean external page
    auto sp_gc = SyncPointCtl::enableInScope("before_PageStorageImpl::cleanExternalPage_execute_callbacks");
    auto th_gc = std::async([&]() { page_storage->gcImpl(/*not_skip*/ true, nullptr, nullptr); });
    sp_gc.waitAndPause();

    // mock table created while gc is running
    {
        ExternalPageCallbacks new_callbacks;
        new_callbacks.prefix = ns_id2;
        new_callbacks.scanner = [ptr_weak_ref = std::weak_ptr<Int32>(ptr)]() -> ExternalPageCallbacks::PathAndIdsVec {
            auto ptr = ptr_weak_ref.lock();
            if (!ptr)
                return {};

            (*ptr) += 1; // mock access the storage inside callback
            return {};
        };
        new_callbacks.remover = [ptr_weak_ref = std::weak_ptr<Int32>(ptr)](
                                    const ExternalPageCallbacks::PathAndIdsVec &,
                                    const std::set<PageIdU64> &) -> void {
            auto ptr = ptr_weak_ref.lock();
            if (!ptr)
                return;

            (*ptr) += 1; // mock access the storage inside callback
        };
        page_storage->registerExternalPagesCallbacks(new_callbacks);
    }

    sp_gc.next(); // continue the gc
    th_gc.get();

    ASSERT_EQ(*ptr, 100 + 4);
}
CATCH

TEST_F(PageStorageTest, ConcurrencyRemoveExtCallbacks)
try
{
    auto ptr = std::make_shared<Int32>(100); // mock the `StorageDeltaMerge`
    ExternalPageCallbacks callbacks;
    callbacks.prefix = TEST_NAMESPACE_ID;
    callbacks.scanner = [ptr_weak_ref = std::weak_ptr<Int32>(ptr)]() -> ExternalPageCallbacks::PathAndIdsVec {
        auto ptr = ptr_weak_ref.lock();
        if (!ptr)
            return {};

        (*ptr) += 1; // mock access the storage inside callback
        return {};
    };
    callbacks.remover = [ptr_weak_ref = std::weak_ptr<Int32>(
                             ptr)](const ExternalPageCallbacks::PathAndIdsVec &, const std::set<PageIdU64> &) -> void {
        auto ptr = ptr_weak_ref.lock();
        if (!ptr)
            return;

        (*ptr) += 1; // mock access the storage inside callback
    };
    page_storage->registerExternalPagesCallbacks(callbacks);

    // Start a PageStorage gc and suspend it before clean external page
    auto sp_gc = SyncPointCtl::enableInScope("before_PageStorageImpl::cleanExternalPage_execute_callbacks");
    auto th_gc = std::async([&]() { page_storage->gcImpl(/*not_skip*/ true, nullptr, nullptr); });
    sp_gc.waitAndPause();

    // mock table dropped while gc is running
    page_storage->unregisterExternalPagesCallbacks(TEST_NAMESPACE_ID);
    ptr = nullptr;

    sp_gc.next(); // continue the gc
    th_gc.get();
}
CATCH

TEST_F(PageStorageTest, GcReuseSpaceThenRestore)
try
{
    DB::UInt64 tag = 0;
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
        page_storage->write(std::move(batch));
    }
    {
        WriteBatch batch;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(1, tag, buff, buf_sz);
        page_storage->write(std::move(batch));
    }

    {
        SCOPED_TRACE("fist gc");
        page_storage->gc();
    }

    {
        WriteBatch batch;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(1, tag, buff, buf_sz);
        page_storage->write(std::move(batch));
    }

    page_storage.reset();
    page_storage = reopenWithConfig(config);
}
CATCH


TEST_F(PageStorageTest, readRefAfterRestore)
try
{
    const size_t buf_sz = 1024;
    char c_buff[buf_sz];

    for (size_t i = 0; i < buf_sz; ++i)
    {
        c_buff[i] = i % 0xff;
    }

    {
        WriteBatch batch;
        batch.putPage(
            1,
            0,
            std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz),
            buf_sz,
            PageFieldSizes{{32, 64, 79, 128, 196, 256, 269}});
        batch.putRefPage(3, 1);
        batch.delPage(1);
        batch.putPage(4, 0, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz, {});
        page_storage->write(std::move(batch));
    }

    page_storage = reopenWithConfig(config);

    {
        WriteBatch batch;
        memset(c_buff, 0, buf_sz);
        batch.putPage(5, 0, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz, {});
        page_storage->write(std::move(batch));
    }

    std::vector<PageStorage::PageReadFields> fields;
    PageStorage::PageReadFields field;
    field.first = 3;
    field.second = {0, 1, 2, 3, 4, 5, 6};
    fields.emplace_back(field);

    ASSERT_NO_THROW(page_storage->read(fields));
}
CATCH


TEST_F(PageStorageTest, putExternalAfterRestore)
try
{
    {
        WriteBatch batch;
        batch.putExternal(1999, 0);
        page_storage->write(std::move(batch));
    }

    page_storage = reopenWithConfig(config);

    auto alive_ids = page_storage->getAliveExternalPageIds(TEST_NAMESPACE_ID);
    ASSERT_EQ(alive_ids.size(), 1);
    ASSERT_EQ(*alive_ids.begin(), 1999);

    {
        WriteBatch batch;
        batch.putExternal(1999, 0);
        page_storage->write(std::move(batch));
    }

    alive_ids = page_storage->getAliveExternalPageIds(TEST_NAMESPACE_ID);
    ASSERT_EQ(alive_ids.size(), 1);
    ASSERT_EQ(*alive_ids.begin(), 1999);
}
CATCH

TEST_F(PageStorageTest, GetMaxId)
try
{
    NamespaceID small = 20;
    NamespaceID medium = 50;
    NamespaceID large = 100;

    {
        WriteBatch batch{small};
        batch.putExternal(1, 0);
        batch.putExternal(1999, 0);
        batch.putExternal(2000, 0);
        page_storage->write(std::move(batch));
        // ASSERT_EQ(page_storage->getMaxId(), 2000); // max id will not be updated, ignore this check
    }

    {
        page_storage = reopenWithConfig(config);
        ASSERT_EQ(page_storage->getMaxId(), 2000);
    }

    {
        WriteBatch batch{medium};
        batch.putExternal(1, 0);
        batch.putExternal(100, 0);
        batch.putExternal(200, 0);
        page_storage->write(std::move(batch));
        ASSERT_EQ(page_storage->getMaxId(), 2000);
    }

    {
        page_storage = reopenWithConfig(config);
        ASSERT_EQ(page_storage->getMaxId(), 2000);
    }

    {
        WriteBatch batch{large};
        batch.putExternal(1, 0);
        batch.putExternal(20000, 0);
        batch.putExternal(20001, 0);
        page_storage->write(std::move(batch));
        // ASSERT_EQ(page_storage->getMaxId(), 20001); //  max id will not be updated, ignore this check
    }

    {
        page_storage = reopenWithConfig(config);
        ASSERT_EQ(page_storage->getMaxId(), 20001);
    }
}
CATCH

TEST_F(PageStorageTest, CleanAfterDecreaseRef)
try
{
    // Make it in log_1_0
    {
        WriteBatch batch;
        batch.putExternal(1, 0);
        page_storage->write(std::move(batch));
    }

    page_storage = reopenWithConfig(config);

    // Make it in log_2_0
    {
        WriteBatch batch;
        batch.putExternal(1, 0);
        batch.putRefPage(2, 1);
        batch.delPage(1);
        batch.delPage(2);
        page_storage->write(std::move(batch));
    }
    page_storage = reopenWithConfig(config);

    auto alive_ids = page_storage->getAliveExternalPageIds(TEST_NAMESPACE_ID);
    ASSERT_EQ(alive_ids.size(), 0);
}
CATCH

TEST_F(PageStorageTest, TruncateBlobFile)
try
{
    const size_t buf_sz = 1024;
    char c_buff[buf_sz];

    for (size_t i = 0; i < buf_sz; ++i)
    {
        c_buff[i] = i % 0xff;
    }

    {
        WriteBatch batch;
        batch.putPage(1, 0, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz, {});
        page_storage->write(std::move(batch));
    }

    auto blob_file = Poco::File(getTemporaryPath() + "/blobfile_10");

    page_storage = reopenWithConfig(config);
    EXPECT_GT(blob_file.getSize(), 0);

    {
        WriteBatch batch;
        batch.delPage(1);
        page_storage->write(std::move(batch));
    }
    page_storage = reopenWithConfig(config);
    page_storage->gc(/*not_skip*/ false, nullptr, nullptr);
    EXPECT_EQ(blob_file.getSize(), 0);
}
CATCH

TEST_F(PageStorageTest, EntryTagAfterFullGC)
try
{
    {
        PageStorageConfig config;
        config.blob_heavy_gc_valid_rate = 1.0; /// always run full gc
        page_storage = reopenWithConfig(config);
    }

    const size_t buf_sz = 1024;
    char c_buff[buf_sz];

    for (size_t i = 0; i < buf_sz; ++i)
    {
        c_buff[i] = i % 0xff;
    }

    PageIdU64 page_id = 120;
    UInt64 tag = 12345;
    {
        WriteBatch batch;
        batch.putPage(page_id, tag, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz, {});
        page_storage->write(std::move(batch));
    }

    {
        auto entry = page_storage->getEntry(page_id);
        ASSERT_EQ(entry.tag, tag);
        auto page = page_storage->read(page_id);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page.data.begin() + i), static_cast<char>(i % 0xff));
        }
    }

    auto done_full_gc = page_storage->gc();
    EXPECT_TRUE(done_full_gc);

    {
        auto entry = page_storage->getEntry(page_id);
        ASSERT_EQ(entry.tag, tag);
        auto page = page_storage->read(page_id);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page.data.begin() + i), static_cast<char>(i % 0xff));
        }
    }
}
CATCH

TEST_F(PageStorageTest, DumpPageStorageSnapshot)
try
{
    {
        PageStorageConfig config;
        config.blob_heavy_gc_valid_rate = 1.0; /// always run full gc
        config.wal_roll_size = 1 * 1024 * 1024; /// make the wal file more easy to roll
        config.wal_max_persisted_log_files = 10; /// avoid checkpoint when gc
        page_storage = reopenWithConfig(config);
    }

    const size_t buf_sz = 1024;
    char c_buff[buf_sz];

    for (size_t i = 0; i < buf_sz; ++i)
    {
        c_buff[i] = i % 0xff;
    }

    PageIdU64 page_id0 = 120;
    {
        WriteBatch batch;
        batch.putPage(page_id0, 0, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz, {});
        page_storage->write(std::move(batch));
    }

    // create a snapshot to avoid page0 being GC-ed
    auto snap = page_storage->getSnapshot();

    {
        WriteBatch batch;
        batch.delPage(page_id0);
        page_storage->write(std::move(batch));
    }

    // write until there are more than one wal file
    while (getLogFileNum() <= 1)
    {
        WriteBatch batch;
        PageIdU64 page_id1 = 130;
        batch.putPage(page_id1, 0, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz, {});
        page_storage->write(std::move(batch));
    }

    // read with latest snapshot, we can not get page0
    ASSERT_ANY_THROW(page_storage->read(page_id0));

    // after the page0 get deleted in previouse log file,
    // write an upsert entry into the current writing log file
    auto done_full_gc = page_storage->gc();
    EXPECT_TRUE(done_full_gc);

    auto done_snapshot = page_storage->page_directory->tryDumpSnapshot(nullptr, /* force */ true);
    ASSERT_TRUE(done_snapshot);

    {
        PageStorageConfig config;
        page_storage = reopenWithConfig(config);
    }

    // After restored from disk, we should not see page0 again
    // or it could be an entry pointing to a non-exist BlobFile
    ASSERT_ANY_THROW(page_storage->read(page_id0));
}
CATCH

TEST_F(PageStorageTest, DumpPageStorageSnapshotWithRefPage)
try
{
    {
        PageStorageConfig config;
        config.blob_heavy_gc_valid_rate = 1.0; /// always run full gc
        config.wal_roll_size = 1 * 1024 * 1024; /// make the wal file more easy to roll
        config.wal_max_persisted_log_files = 10; /// avoid checkpoint when gc
        page_storage = reopenWithConfig(config);
    }

    const size_t buf_sz = 1024;
    char c_buff[buf_sz];

    for (size_t i = 0; i < buf_sz; ++i)
    {
        c_buff[i] = i % 0xff;
    }

    PageIdU64 page_id0 = 120;
    {
        WriteBatch batch;
        batch.putPage(page_id0, 0, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz, {});
        page_storage->write(std::move(batch));
    }
    PageIdU64 page_id1 = 121;
    {
        WriteBatch batch;
        batch.putRefPage(page_id1, page_id0);
        page_storage->write(std::move(batch));
    }
    // create a snapshot to avoid gc
    auto snap = page_storage->getSnapshot();

    {
        WriteBatch batch;
        batch.delPage(page_id0);
        page_storage->write(std::move(batch));
    }
    {
        WriteBatch batch;
        batch.delPage(page_id1);
        page_storage->write(std::move(batch));
    }

    // write until there are more than one wal file
    while (getLogFileNum() <= 1)
    {
        WriteBatch batch;
        PageIdU64 page_id2 = 130;
        batch.putPage(page_id2, 0, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz, {});
        page_storage->write(std::move(batch));
    }
    ASSERT_ANY_THROW(page_storage->read(page_id0));

    // after the page0 get deleted in previouse log file,
    // write an upsert entry into the current writing log file
    auto done_full_gc = page_storage->gc();
    EXPECT_TRUE(done_full_gc);

    auto done_snapshot = page_storage->page_directory->tryDumpSnapshot(nullptr, /* force */ true);
    ASSERT_TRUE(done_snapshot);

    {
        PageStorageConfig config;
        page_storage = reopenWithConfig(config);
    }

    // After restored from disk, we should not see page0 again
    // or it could be an entry pointing to a non-exist BlobFile
    ASSERT_ANY_THROW(page_storage->read(page_id0));
}
CATCH

TEST_F(PageStorageTest, WriteEmptyPage)
try
{
    {
        PageStorageConfig config;
        config.blob_heavy_gc_valid_rate = 1.0; /// always run full gc
        page_storage = reopenWithConfig(config);
    }

    const size_t buf_sz = 1024;
    char c_buff[buf_sz];

    for (size_t i = 0; i < buf_sz; ++i)
    {
        c_buff[i] = i % 0xff;
    }

    UInt64 tag = 0;
    PageIdU64 page_id1 = 131;
    PageIdU64 page_id2 = 132;
    {
        WriteBatch batch;
        batch.putPage(page_id1, tag, "", {}); // empty page
        page_storage->write(std::move(batch));
    }
    {
        WriteBatch batch;
        batch.putPage(page_id2, tag, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz, {});
        page_storage->write(std::move(batch));
    }

    {
        auto page = page_storage->read(page_id1);
        ASSERT_EQ(page.page_id, page_id1);
        ASSERT_EQ(page.data.size(), 0);
    }
    {
        auto page = page_storage->read(page_id2);
        ASSERT_EQ(page.page_id, page_id2);
        ASSERT_EQ(page.data.size(), buf_sz);
    }

    // delete empty page
    {
        WriteBatch batch;
        batch.delPage(page_id1);
        page_storage->write(std::move(batch));
    }

    {
        auto page = page_storage->readImpl(TEST_NAMESPACE_ID, page_id1, nullptr, nullptr, /*throw_on_not_exist*/ false);
        ASSERT_FALSE(page.isValid());
    }
    {
        auto page = page_storage->read(page_id2);
        ASSERT_EQ(page.page_id, page_id2);
        ASSERT_EQ(page.data.size(), buf_sz);
    }

    {
        PageStorageConfig config;
        page_storage = reopenWithConfig(config);
    }
}
CATCH

TEST_F(PageStorageTest, RestoreWithEmptyPage)
try
{
    {
        PageStorageConfig config;
        config.blob_heavy_gc_valid_rate = 1.0; /// always run full gc
        page_storage = reopenWithConfig(config);
    }

    const size_t buf_sz = 1024;
    char c_buff[buf_sz];

    for (size_t i = 0; i < buf_sz; ++i)
    {
        c_buff[i] = i % 0xff;
    }

    UInt64 tag = 0;
    PageIdU64 page_id0 = 120;
    PageIdU64 page_id1 = 131;
    PageIdU64 page_id2 = 122;
    {
        WriteBatch batch;
        batch.putPage(page_id0, tag, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz, {});
        batch.putPage(page_id1, tag, "", {}); // empty page
        page_storage->write(std::move(batch));
    }
    page_storage->freezeDataFiles(); // new write will be written to new BlobFile
    {
        WriteBatch batch;
        batch.putPage(page_id2, tag, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz, {});
        page_storage->write(std::move(batch));
    }

    {
        auto page = page_storage->read(page_id0);
        ASSERT_EQ(page.page_id, page_id0);
        ASSERT_EQ(page.data.size(), buf_sz);
    }
    {
        auto page = page_storage->read(page_id1);
        ASSERT_EQ(page.page_id, page_id1);
        ASSERT_EQ(page.data.size(), 0);
    }
    {
        auto page = page_storage->read(page_id2);
        ASSERT_EQ(page.page_id, page_id2);
        ASSERT_EQ(page.data.size(), buf_sz);
    }

    FailPointHelper::enableFailPoint(FailPoints::force_pick_all_blobs_to_full_gc);
    auto done_full_gc = page_storage->gc();
    EXPECT_TRUE(done_full_gc);

    // When restoring from disk, we will first restore two non-empty page,
    // then restore the empty page. No exception should be thrown.
    {
        PageStorageConfig config;
        page_storage = reopenWithConfig(config);
    }
}
CATCH

TEST_F(PageStorageTest, ReloadConfig)
try
{
    auto & global_context = DB::tests::TiFlashTestEnv::getContext()->getGlobalContext();
    auto & settings = global_context.getSettingsRef();
    auto old_dt_page_gc_threshold = settings.dt_page_gc_threshold;

    settings.dt_page_gc_threshold = 0.6;
    page_storage->reloadSettings(getConfigFromSettings(settings));
    ASSERT_EQ(page_storage->blob_store.config.heavy_gc_valid_rate, 0.6);
    ASSERT_EQ(page_storage->blob_store.blob_stats.config.heavy_gc_valid_rate, 0.6);

    // change config twice make sure the test select a value different from default value
    settings.dt_page_gc_threshold = 0.8;
    page_storage->reloadSettings(getConfigFromSettings(settings));
    ASSERT_EQ(page_storage->blob_store.config.heavy_gc_valid_rate, 0.8);
    ASSERT_EQ(page_storage->blob_store.blob_stats.config.heavy_gc_valid_rate, 0.8);

    settings.dt_page_gc_threshold = old_dt_page_gc_threshold;
}
CATCH

} // namespace PS::V3::tests
} // namespace DB
