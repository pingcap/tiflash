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
#include <Common/Logger.h>
#include <IO/BaseFile/RateLimiter.h>
#include <IO/Buffer/ReadBufferFromMemory.h>
#include <Poco/Logger.h>
#include <Storages/Page/PageConstants.h>
#include <Storages/Page/PageDefinesBase.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/PageDefines.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/tests/entries_helper.h>
#include <Storages/Page/WriteBatchImpl.h>
#include <TestUtils/MockDiskDelegator.h>
#include <TestUtils/MockReadLimiter.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <ext/scope_guard.h>

namespace DB::FailPoints
{
extern const char exception_after_large_write_exceed[];
extern const char force_pick_all_blobs_to_full_gc[];
} // namespace DB::FailPoints

namespace DB::PS::V3::tests
{
using u128::PageEntriesEdit;
using BlobStore = BlobStore<u128::BlobStoreTrait>;
constexpr size_t path_num = 3;

class BlobStoreTest : public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override
    {
        auto path = getTemporaryPath();
        DB::tests::TiFlashTestEnv::tryRemovePath(path);
        createIfNotExist(path);
        Strings paths;
        for (size_t i = 0; i < path_num; i++)
        {
            paths.emplace_back(fmt::format("{}/{}", path, i));
        }
        delegator = std::make_shared<DB::tests::MockDiskDelegatorMulti>(paths);

        for (size_t i = 0; i < fixed_buffer_size; ++i)
        {
            fixed_buffer[i] = i % 0xff;
        }
    }

    static size_t getTotalStatsNum(const BlobStats::StatsMap & stats_map)
    {
        size_t total_stats_num = 0;
        for (const auto & iter : stats_map)
        {
            total_stats_num += iter.second.size();
        }
        return total_stats_num;
    }

protected:
    BlobConfig config;
    PageTypeAndConfig page_type_and_config{
        {PageType::Normal, PageTypeConfig{.heavy_gc_valid_rate = config.heavy_gc_valid_rate}},
        {PageType::RaftData, PageTypeConfig{.heavy_gc_valid_rate = config.heavy_gc_valid_rate_raft_data}},
    };

    PSDiskDelegatorPtr delegator;

    char fixed_buffer[1024]{};
    const size_t fixed_buffer_size = sizeof(fixed_buffer);
};

TEST_F(BlobStoreTest, Restore)
try
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getDefaultFileProvider();
    config.file_limit_size = 2560;
    auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config, page_type_and_config);

    BlobFileId file_id1 = 10;
    BlobFileId file_id2 = 12;

    const auto & paths = delegator->listPaths();
    for (const auto & path : paths)
    {
        createIfNotExist(path);
    }
    Poco::File(fmt::format("{}/{}{}", paths[rand() % path_num], BlobFile::BLOB_PREFIX_NAME, file_id1)).createFile();
    Poco::File(fmt::format("{}/{}{}", paths[rand() % path_num], BlobFile::BLOB_PREFIX_NAME, file_id2)).createFile();
    blob_store.registerPaths();

    {
        blob_store.blob_stats.restoreByEntry(PageEntryV3{
            .file_id = file_id1,
            .size = 128,
            .padded_size = 0,
            .tag = 0,
            .offset = 1024,
            .checksum = 0x4567,
        });
        blob_store.blob_stats.restoreByEntry(PageEntryV3{
            .file_id = file_id1,
            .size = 512,
            .padded_size = 0,
            .tag = 0,
            .offset = 2048,
            .checksum = 0x4567,
        });
        blob_store.blob_stats.restoreByEntry(PageEntryV3{
            .file_id = file_id2,
            .size = 512,
            .padded_size = 0,
            .tag = 0,
            .offset = 2048,
            .checksum = 0x4567,
        });
        blob_store.blob_stats.restore();
    }

    // check spacemap updated
    {
        for (const auto & [path, stats] : blob_store.blob_stats.getStats())
        {
            (void)path;
            for (const auto & stat : stats)
            {
                if (stat->id == file_id1)
                {
                    ASSERT_EQ(stat->sm_total_size, 2560);
                    ASSERT_EQ(stat->sm_valid_size, 640);
                    ASSERT_EQ(stat->sm_max_caps, 1024);
                }
                else if (stat->id == file_id2)
                {
                    ASSERT_EQ(stat->sm_total_size, 2560);
                    ASSERT_EQ(stat->sm_valid_size, 512);
                    ASSERT_EQ(stat->sm_max_caps, 2048);
                }
            }
        }
    }
}
CATCH


TEST_F(BlobStoreTest, RestoreWithInvalidBlob)
try
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getDefaultFileProvider();
    config.file_limit_size = 1024;

    // Generate blob [1,2,3]
    auto write_blob_datas = [](BlobStore & blob_store) {
        PageIdU64 page_id = 55;
        size_t buff_size = 1024;
        char c_buff[buff_size];
        memset(c_buff, 0x1, buff_size);

        // write blob 1
        {
            WriteBatch write_batch;
            write_batch.putPage(
                page_id,
                /* tag */ 0,
                std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff), buff_size),
                buff_size);
            blob_store.write(std::move(write_batch));
        }

        // write blob 2
        {
            WriteBatch write_batch;
            write_batch.putPage(
                page_id + 1,
                /* tag */ 0,
                std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff), buff_size),
                buff_size);
            blob_store.write(std::move(write_batch));
        }

        // write blob 3
        {
            WriteBatch write_batch;
            write_batch.putPage(
                page_id + 2,
                /* tag */ 0,
                std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff), buff_size),
                buff_size);
            blob_store.write(std::move(write_batch));
        }
    };

    auto check_in_disk_file = [](const Strings & paths, std::vector<BlobFileId> exited_blobs) -> bool {
        for (const auto blob_id : exited_blobs)
        {
            bool exists = false;
            for (const auto & path : paths)
            {
                Poco::File file(fmt::format("{}/{}{}", path, BlobFile::BLOB_PREFIX_NAME, blob_id));
                if (file.exists())
                {
                    exists = true;
                    break;
                }
            }
            if (!exists)
            {
                return false;
            }
        }
        return true;
    };

    auto restore_blobs = [](BlobStore & blob_store, std::vector<BlobFileId> blob_ids) {
        blob_store.registerPaths();
        for (const auto & id : blob_ids)
        {
            blob_store.blob_stats.restoreByEntry(PageEntryV3{
                .file_id = id,
                .size = 1024,
                .padded_size = 0,
                .tag = 0,
                .offset = 0,
                .checksum = 0x4567,
            });
        }
    };

    // Case 1, all of blob been restored
    {
        auto test_paths = delegator->listPaths();

        auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config, page_type_and_config);
        write_blob_datas(blob_store);

        ASSERT_TRUE(check_in_disk_file(test_paths, {10, 20, 30}));

        auto blob_store_check = BlobStore(getCurrentTestName(), file_provider, delegator, config, page_type_and_config);
        restore_blobs(blob_store_check, {10, 20, 30});

        blob_store_check.blob_stats.restore();

        ASSERT_TRUE(check_in_disk_file(test_paths, {10, 20, 30}));
        for (const auto & path : test_paths)
        {
            DB::tests::TiFlashTestEnv::tryRemovePath(path);
            createIfNotExist(path);
        }
    }

    // Case 2, only recover blob 1
    {
        auto test_paths = delegator->listPaths();
        auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config, page_type_and_config);
        write_blob_datas(blob_store);

        ASSERT_TRUE(check_in_disk_file(test_paths, {10, 20, 30}));

        auto blob_store_check = BlobStore(getCurrentTestName(), file_provider, delegator, config, page_type_and_config);
        restore_blobs(blob_store_check, {10});

        blob_store_check.blob_stats.restore();

        ASSERT_TRUE(check_in_disk_file(test_paths, {10}));
        for (const auto & path : test_paths)
        {
            DB::tests::TiFlashTestEnv::tryRemovePath(path);
            createIfNotExist(path);
        }
    }

    // Case 3, only recover blob 2
    {
        auto test_paths = delegator->listPaths();
        auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config, page_type_and_config);
        write_blob_datas(blob_store);

        ASSERT_TRUE(check_in_disk_file(test_paths, {10, 20, 30}));

        auto blob_store_check = BlobStore(getCurrentTestName(), file_provider, delegator, config, page_type_and_config);
        restore_blobs(blob_store_check, {20});

        blob_store_check.blob_stats.restore();

        ASSERT_TRUE(check_in_disk_file(test_paths, {20}));
        for (const auto & path : test_paths)
        {
            DB::tests::TiFlashTestEnv::tryRemovePath(path);
            createIfNotExist(path);
        }
    }

    // Case 4, only recover blob 3
    {
        auto test_paths = delegator->listPaths();
        auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config, page_type_and_config);
        write_blob_datas(blob_store);

        ASSERT_TRUE(check_in_disk_file(test_paths, {10, 20, 30}));

        auto blob_store_check = BlobStore(getCurrentTestName(), file_provider, delegator, config, page_type_and_config);
        restore_blobs(blob_store_check, {30});

        blob_store_check.blob_stats.restore();

        ASSERT_TRUE(check_in_disk_file(test_paths, {30}));
        for (const auto & path : test_paths)
        {
            DB::tests::TiFlashTestEnv::tryRemovePath(path);
            createIfNotExist(path);
        }
    }

    // Case 5, recover a not exist blob
    {
        auto test_paths = delegator->listPaths();
        auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config, page_type_and_config);
        write_blob_datas(blob_store);

        ASSERT_TRUE(check_in_disk_file(test_paths, {10, 20, 30}));

        auto blob_store_check = BlobStore(getCurrentTestName(), file_provider, delegator, config, page_type_and_config);
        ASSERT_THROW(restore_blobs(blob_store_check, {40}), DB::Exception);
    }
}
CATCH

TEST_F(BlobStoreTest, testWriteRead)
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getDefaultFileProvider();
    PageIdU64 page_id = 50;
    size_t buff_nums = 21;
    size_t buff_size = 123;

    auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config, page_type_and_config);
    char c_buff[buff_size * buff_nums];

    WriteBatch wb;

    for (size_t i = 0; i < buff_nums; ++i)
    {
        for (size_t j = 0; j < buff_size; ++j)
        {
            c_buff[j + i * buff_size] = static_cast<char>((j & 0xff) + i);
        }

        ReadBufferPtr buff
            = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff + i * buff_size), buff_size);
        wb.putPage(page_id++, /* tag */ 0, buff, buff_size);
    }

    ASSERT_EQ(wb.getTotalDataSize(), buff_nums * buff_size);
    PageEntriesEdit edit = blob_store.write(std::move(wb));
    ASSERT_EQ(edit.size(), buff_nums);

    char c_buff_read[buff_size * buff_nums];

    size_t index = 0;
    for (const auto & record : edit.getRecords())
    {
        ASSERT_EQ(record.type, EditRecordType::PUT);
        ASSERT_EQ(record.entry.offset, index * buff_size);
        ASSERT_EQ(record.entry.size, buff_size);
        ASSERT_EQ(record.entry.file_id, 10);

        // Read directly from the file
        blob_store.read(
            buildV3Id(TEST_NAMESPACE_ID, page_id),
            record.entry.file_id,
            record.entry.offset,
            c_buff_read + index * buff_size,
            record.entry.size,
            /* ReadLimiterPtr */ nullptr);

        ASSERT_EQ(strncmp(c_buff + index * buff_size, c_buff_read + index * buff_size, record.entry.size), 0);
        index++;
    }
    ASSERT_EQ(index, buff_nums);

    page_id = 50;
    PageIDAndEntriesV3 entries = {};

    for (const auto & record : edit.getRecords())
    {
        entries.emplace_back(std::make_pair(page_id++, record.entry));
    }

    // Test `PageMap` read
    page_id = 50;
    index = 0;
    auto page_map = blob_store.read(entries);
    for (auto & [id, page] : page_map)
    {
        ASSERT_EQ(id, page_id++);
        ASSERT_EQ(page.data.size(), buff_size);
        ASSERT_EQ(strncmp(c_buff + index * buff_size, page.data.begin(), page.data.size()), 0);
        index++;
    }
    ASSERT_EQ(index, buff_nums);

    // Test single `Page` read
    index = 0;
    for (auto & entry : entries)
    {
        auto page = blob_store.read(entry);
        ASSERT_EQ(page.data.size(), buff_size);
        ASSERT_EQ(strncmp(c_buff + index * buff_size, page.data.begin(), page.data.size()), 0);
        index++;
    }
    ASSERT_EQ(index, buff_nums);
}

TEST_F(BlobStoreTest, testWriteReadWithIOLimiter)
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getDefaultFileProvider();

    PageIdU64 page_id = 50;
    size_t wb_nums = 5;
    size_t buff_size = 10ul * 1024;
    const size_t rate_target = buff_size - 1;
    PageTypeAndConfig page_type_and_config{
        {PageType::Normal, PageTypeConfig{.heavy_gc_valid_rate = config.heavy_gc_valid_rate}},
        {PageType::RaftData, PageTypeConfig{.heavy_gc_valid_rate = config.heavy_gc_valid_rate_raft_data}},
    };
    auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config, page_type_and_config);
    char c_buff[wb_nums * buff_size];

    WriteBatch wbs[wb_nums];
    PageEntriesEdit edits[wb_nums];

    for (size_t i = 0; i < wb_nums; ++i)
    {
        for (size_t j = 0; j < buff_size; ++j)
        {
            c_buff[j + i * buff_size] = static_cast<char>((j & 0xff) + i);
        }

        ReadBufferPtr buff
            = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff + i * buff_size), buff_size);
        wbs[i].putPage(page_id++, /* tag */ 0, buff, buff_size);
    }

    WriteLimiterPtr write_limiter = std::make_shared<WriteLimiter>(rate_target, LimiterType::UNKNOW, 20);

    AtomicStopwatch write_watch;
    for (size_t i = 0; i < wb_nums; ++i)
    {
        edits[i] = blob_store.write(std::move(wbs[i]), PageType::Normal, write_limiter);
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

    char c_buff_read[wb_nums * buff_size];
    {
        ReadLimiterPtr read_limiter = std::make_shared<MockReadLimiter>(get_stat, rate_target, LimiterType::UNKNOW);

        AtomicStopwatch read_watch;
        for (size_t i = 0; i < wb_nums; ++i)
        {
            for (const auto & record : edits[i].getRecords())
            {
                blob_store.read(
                    buildV3Id(TEST_NAMESPACE_ID, page_id),
                    record.entry.file_id,
                    record.entry.offset,
                    c_buff_read + i * buff_size,
                    record.entry.size,
                    read_limiter);
            }
        }

        auto read_elapsed = read_watch.elapsedSeconds();
        auto read_actual_rate = read_limiter->getTotalBytesThrough() / read_elapsed;
        EXPECT_LE(read_actual_rate / rate_target, 1.30);
    }

    PageIDAndEntriesV3 entries = {};
    for (size_t i = 0; i < wb_nums; ++i)
    {
        for (const auto & record : edits[i].getRecords())
        {
            entries.emplace_back(std::make_pair(record.page_id, record.entry));
        }
    }

    {
        ReadLimiterPtr read_limiter = std::make_shared<MockReadLimiter>(get_stat, rate_target, LimiterType::UNKNOW);

        AtomicStopwatch read_watch;

        // Test `PageMap` read
        blob_store.read(entries, read_limiter);
        auto read_elapsed = read_watch.elapsedSeconds();
        auto read_actual_rate = read_limiter->getTotalBytesThrough() / read_elapsed;
        EXPECT_LE(read_actual_rate / rate_target, 1.30);
    }

    {
        ReadLimiterPtr read_limiter = std::make_shared<MockReadLimiter>(get_stat, rate_target, LimiterType::UNKNOW);

        AtomicStopwatch read_watch;

        // Test single `Page` read
        for (auto & entry : entries)
        {
            blob_store.read(entry, read_limiter);
        }
        auto read_elapsed = read_watch.elapsedSeconds();
        auto read_actual_rate = read_limiter->getTotalBytesThrough() / read_elapsed;
        EXPECT_LE(read_actual_rate / rate_target, 1.30);
    }
}
TEST_F(BlobStoreTest, testWriteReadWithFiled)
try
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getDefaultFileProvider();
    PageIdU64 page_id1 = 50;
    PageIdU64 page_id2 = 51;
    PageIdU64 page_id3 = 53;

    size_t buff_size = 120;
    WriteBatch wb;

    auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config, page_type_and_config);
    char c_buff[buff_size];

    for (size_t j = 0; j < buff_size; ++j)
    {
        c_buff[j] = static_cast<char>(j & 0xff);
    }

    ReadBufferPtr buff1 = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff), buff_size);
    ReadBufferPtr buff2 = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff), buff_size);
    ReadBufferPtr buff3 = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff), buff_size);
    wb.putPage(page_id1, /* tag */ 0, buff1, buff_size, {20, 40, 40, 20});
    wb.putPage(page_id2, /* tag */ 0, buff2, buff_size, {10, 50, 20, 20, 20});
    wb.putPage(page_id3, /* tag */ 0, buff3, buff_size, {10, 5, 20, 20, 15, 5, 15, 30});
    PageEntriesEdit edit = blob_store.write(std::move(wb));
    ASSERT_EQ(edit.size(), 3);

    BlobStore::FieldReadInfo read_info1(
        buildV3Id(TEST_NAMESPACE_ID, page_id1),
        edit.getRecords()[0].entry,
        {0, 1, 2, 3});
    BlobStore::FieldReadInfo read_info2(buildV3Id(TEST_NAMESPACE_ID, page_id2), edit.getRecords()[1].entry, {2, 4});
    BlobStore::FieldReadInfo read_info3(buildV3Id(TEST_NAMESPACE_ID, page_id3), edit.getRecords()[2].entry, {1, 3});

    BlobStore::FieldReadInfos read_infos = {read_info1, read_info2, read_info3};

    const auto & page_map = blob_store.read(read_infos, nullptr);
    ASSERT_EQ(page_map.size(), 3);

    for (const auto & [pageid, page] : page_map)
    {
        if (pageid == page_id1)
        {
            ASSERT_EQ(page.page_id, page_id1);
            ASSERT_EQ(page.data.size(), buff_size);
            ASSERT_EQ(strncmp(page.data.begin(), c_buff, buff_size), 0);
        }
        else if (pageid == page_id2)
        {
            ASSERT_EQ(page.page_id, page_id2);
            // the buffer size read is equal to the fields size we read
            // field {2, 4}
            ASSERT_EQ(page.data.size(), 40);
            ASSERT_EQ(strncmp(page.data.begin(), &c_buff[60], 20), 0);
            ASSERT_EQ(strncmp(&page.data.begin()[20], &c_buff[100], 20), 0);
        }
        else if (pageid == page_id3)
        {
            ASSERT_EQ(page.page_id, page_id3);
            // the buffer size read is equal to the fields size we read
            // field {1, 3}
            ASSERT_EQ(page.data.size(), 25);
            ASSERT_EQ(strncmp(page.data.begin(), &c_buff[10], 5), 0);
            ASSERT_EQ(strncmp(&page.data.begin()[5], &c_buff[35], 20), 0);
        }
    }
}
CATCH

TEST_F(BlobStoreTest, testFeildOffsetWriteRead)
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getDefaultFileProvider();
    PageIdU64 page_id = 50;
    size_t buff_size = 20;
    size_t buff_nums = 5;
    PageFieldSizes field_sizes = {1, 2, 3, 4, 5, 2, 1, 1, 1};

    std::vector<PageFieldOffset> offsets;
    PageFieldOffset off = 0;
    for (auto data_sz : field_sizes)
    {
        offsets.emplace_back(off);
        off += data_sz;
    }

    auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config, page_type_and_config);
    char c_buff[buff_size * buff_nums];

    WriteBatch wb;

    for (size_t i = 0; i < buff_nums; ++i)
    {
        for (size_t j = 0; j < buff_size; ++j)
        {
            c_buff[j + i * buff_size] = static_cast<char>((j & 0xff) + i);
        }

        ReadBufferPtr buff
            = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff + i * buff_size), buff_size);
        wb.putPage(page_id++, /* tag */ 0, buff, buff_size, field_sizes);
    }

    ASSERT_EQ(wb.getTotalDataSize(), buff_nums * buff_size);
    PageEntriesEdit edit = blob_store.write(std::move(wb));
    ASSERT_EQ(edit.size(), buff_nums);

    char c_buff_read[buff_size * buff_nums];

    size_t index = 0;
    for (const auto & record : edit.getRecords())
    {
        ASSERT_EQ(record.type, EditRecordType::PUT);
        ASSERT_EQ(record.entry.offset, index * buff_size);
        ASSERT_EQ(record.entry.size, buff_size);
        ASSERT_EQ(record.entry.file_id, 10);

        PageFieldSizes check_field_sizes;
        for (const auto & [field_offset, crc] : record.entry.field_offsets)
        {
            check_field_sizes.emplace_back(field_offset);
            ASSERT_TRUE(crc);
        }

        ASSERT_EQ(check_field_sizes, offsets);

        // Read
        blob_store.read(
            buildV3Id(TEST_NAMESPACE_ID, page_id),
            record.entry.file_id,
            record.entry.offset,
            c_buff_read + index * buff_size,
            record.entry.size,
            /* ReadLimiterPtr */ nullptr);

        ASSERT_EQ(strncmp(c_buff + index * buff_size, c_buff_read + index * buff_size, record.entry.size), 0);
        index++;
    }
    ASSERT_EQ(index, buff_nums);
}

TEST_F(BlobStoreTest, testWrite)
try
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getDefaultFileProvider();
    auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config, page_type_and_config);

    PageIdU64 page_id = 50;
    const size_t buff_size = 1024;
    {
        WriteBatch wb;
        char c_buff1[buff_size] = {0};
        char c_buff2[buff_size] = {0};

        for (size_t i = 0; i < buff_size; ++i)
        {
            c_buff1[i] = i & 0xff;
            c_buff2[i] = static_cast<char>((i & 0xff) + 1);
        }

        ReadBufferPtr buff1 = std::make_shared<ReadBufferFromMemory>(c_buff1, buff_size);
        ReadBufferPtr buff2 = std::make_shared<ReadBufferFromMemory>(c_buff2, buff_size);

        wb.putPage(page_id, /*tag*/ 0, buff1, buff_size);
        wb.putPage(page_id, /*tag*/ 0, buff2, buff_size);

        PageEntriesEdit edit = blob_store.write(std::move(wb));
        ASSERT_EQ(edit.size(), 2);

        auto records = edit.getRecords();
        auto record = records[0];

        ASSERT_EQ(record.type, EditRecordType::PUT);
        ASSERT_EQ(record.page_id.low, page_id);
        ASSERT_EQ(record.entry.offset, 0);
        ASSERT_EQ(record.entry.size, buff_size);
        ASSERT_EQ(record.entry.file_id, 10);

        record = records[1];
        ASSERT_EQ(record.type, EditRecordType::PUT);
        ASSERT_EQ(record.page_id.low, page_id);
        ASSERT_EQ(record.entry.offset, buff_size);
        ASSERT_EQ(record.entry.size, buff_size);
        ASSERT_EQ(record.entry.file_id, 10);
    }


    {
        WriteBatch wb;
        wb.putRefPage(page_id + 1, page_id);
        wb.delPage(page_id + 1);
        wb.delPage(page_id);

        PageEntriesEdit edit = blob_store.write(std::move(wb));
        ASSERT_EQ(edit.size(), 3);

        auto records = edit.getRecords();
        auto record = records[0];

        ASSERT_EQ(record.type, EditRecordType::REF);
        ASSERT_EQ(record.page_id.low, page_id + 1);
        ASSERT_EQ(record.ori_page_id.low, page_id);

        record = records[1];
        ASSERT_EQ(record.type, EditRecordType::DEL);
        ASSERT_EQ(record.page_id.low, page_id + 1);

        record = records[2];
        ASSERT_EQ(record.type, EditRecordType::DEL);
        ASSERT_EQ(record.page_id.low, page_id);
    }

    {
        WriteBatch wb;
        char c_buff[buff_size];

        for (size_t i = 0; i < buff_size; ++i)
        {
            c_buff[i] = i & 0xff;
        }

        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, buff_size);
        wb.putPage(page_id, /*tag*/ 0, buff, buff_size);
        wb.putRefPage(page_id + 1, page_id);
        wb.delPage(page_id);

        PageEntriesEdit edit = blob_store.write(std::move(wb));
        auto records = edit.getRecords();

        auto record = records[0];
        ASSERT_EQ(record.type, EditRecordType::PUT);
        ASSERT_EQ(record.page_id.low, page_id);
        ASSERT_EQ(record.entry.offset, buff_size * 2);
        ASSERT_EQ(record.entry.size, buff_size);
        ASSERT_EQ(record.entry.file_id, 10);

        record = records[1];
        ASSERT_EQ(record.type, EditRecordType::REF);
        ASSERT_EQ(record.page_id.low, page_id + 1);
        ASSERT_EQ(record.ori_page_id.low, page_id);

        record = records[2];
        ASSERT_EQ(record.type, EditRecordType::DEL);
        ASSERT_EQ(record.page_id.low, page_id);
    }
}
CATCH

// BlobStore allow (page size > blob_file_limit)
TEST_F(BlobStoreTest, DISABLED_testWriteOutOfLimitSize)
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getDefaultFileProvider();
    size_t buff_size = 100;

    {
        config.file_limit_size = buff_size - 1;
        auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config, page_type_and_config);

        WriteBatch wb;
        char c_buff[buff_size];
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff), buff_size);
        wb.putPage(50, /*tag*/ 0, buff, buff_size);

        bool catch_exception = false;
        try
        {
            blob_store.write(std::move(wb));
        }
        catch (DB::Exception & e)
        {
            catch_exception = true;
        }
        ASSERT_TRUE(catch_exception);
    }

    config.file_limit_size = buff_size;

    size_t buffer_sizes[] = {buff_size, buff_size - 1, buff_size / 2 + 1};
    for (const auto & buf_size : buffer_sizes)
    {
        auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config, page_type_and_config);

        char c_buff1[buf_size];
        char c_buff2[buf_size];

        ReadBufferPtr buff1 = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff1), buf_size);
        ReadBufferPtr buff2 = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff2), buf_size);
        {
            WriteBatch wb;

            wb.putPage(50, /*tag*/ 0, buff1, buf_size);

            auto edit = blob_store.write(std::move(wb));
            ASSERT_EQ(edit.size(), 1);

            auto records = edit.getRecords();
            auto record = records[0];
            ASSERT_EQ(record.type, EditRecordType::PUT);
            ASSERT_EQ(record.page_id.low, 50);
            ASSERT_EQ(record.entry.offset, 0);
            ASSERT_EQ(record.entry.size, buf_size);
            ASSERT_EQ(record.entry.file_id, 1);
        }

        {
            WriteBatch wb;
            wb.putPage(51, /*tag*/ 0, buff2, buf_size);
            auto edit = blob_store.write(std::move(wb));
            ASSERT_EQ(edit.size(), 1);

            auto records = edit.getRecords();
            auto record = records[0];
            ASSERT_EQ(record.type, EditRecordType::PUT);
            ASSERT_EQ(record.page_id.low, 51);
            ASSERT_EQ(record.entry.offset, 0);
            ASSERT_EQ(record.entry.size, buf_size);
            ASSERT_EQ(record.entry.file_id, 2);
        }
    }
}

TEST_F(BlobStoreTest, testBlobStoreGcStats)
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getDefaultFileProvider();
    size_t buff_size = 1024;
    size_t buff_nums = 10;
    PageIdU64 page_id = 50;
    auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config, page_type_and_config);
    std::list<size_t> remove_entries_idx1 = {1, 3, 4, 7, 9};
    std::list<size_t> remove_entries_idx2 = {6, 8};

    WriteBatch wb;
    char c_buff[buff_size * buff_nums];
    {
        for (size_t i = 0; i < buff_nums; ++i)
        {
            for (size_t j = 0; j < buff_size; ++j)
            {
                c_buff[j + i * buff_size] = static_cast<char>((j & 0xff) + i);
            }
            ReadBufferPtr buff
                = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff + i * buff_size), buff_size);
            wb.putPage(page_id, /* tag */ 0, buff, buff_size);
        }
    }

    auto edit = blob_store.write(std::move(wb));

    size_t idx = 0;
    PageEntriesV3 entries_del1, entries_del2, remain_entries;
    for (const auto & record : edit.getRecords())
    {
        bool deleted = false;
        for (size_t index : remove_entries_idx1)
        {
            if (idx == index)
            {
                entries_del1.emplace_back(record.entry);
                deleted = true;
                break;
            }
        }

        for (size_t index : remove_entries_idx2)
        {
            if (idx == index)
            {
                entries_del2.emplace_back(record.entry);
                deleted = true;
                break;
            }
        }
        if (!deleted)
        {
            remain_entries.emplace_back(record.entry);
        }

        idx++;
    }

    // After remove `entries_del1`.
    // Remain entries index [0, 2, 5, 6, 8]
    blob_store.remove(entries_del1);
    ASSERT_EQ(entries_del1.begin()->file_id, 10);

    auto stat = blob_store.blob_stats.blobIdToStat(10);

    ASSERT_EQ(stat->sm_valid_rate, 0.5);
    ASSERT_EQ(stat->sm_total_size, buff_size * buff_nums);
    ASSERT_EQ(stat->sm_valid_size, buff_size * 5);

    // After remove `entries_del2`.
    // Remain entries index [0, 2, 5].
    // But file size still is 10 * 1024
    blob_store.remove(entries_del2);

    ASSERT_EQ(stat->sm_valid_rate, 0.3);
    ASSERT_EQ(stat->sm_total_size, buff_size * buff_nums);
    ASSERT_EQ(stat->sm_valid_size, buff_size * 3);

    const auto & gc_stats = blob_store.getGCStats();
    ASSERT_TRUE(gc_stats.empty());

    ASSERT_EQ(stat->sm_valid_rate, 0.5);
    ASSERT_EQ(stat->sm_total_size, buff_size * 6);
    ASSERT_EQ(stat->sm_valid_size, buff_size * 3);

    // Check disk file have been truncate to right margin
    String path = blob_store.getBlobFile(10)->getPath();
    Poco::File blob_file_in_disk(path);
    ASSERT_EQ(blob_file_in_disk.getSize(), stat->sm_total_size);

    // Check whether the stat can be totally removed
    stat->changeToReadOnly();
    blob_store.remove(remain_entries);
    ASSERT_EQ(getTotalStatsNum(blob_store.blob_stats.getStats()), 0);
}

TEST_F(BlobStoreTest, testBlobStoreGcStats2)
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getDefaultFileProvider();
    size_t buff_size = 1024;
    size_t buff_nums = 10;
    PageIdU64 page_id = 50;
    auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config, page_type_and_config);
    std::list<size_t> remove_entries_idx = {0, 1, 2, 3, 4, 5, 6, 7};

    WriteBatch wb;
    char c_buff[buff_size * buff_nums];
    {
        for (size_t i = 0; i < buff_nums; ++i)
        {
            for (size_t j = 0; j < buff_size; ++j)
            {
                c_buff[j + i * buff_size] = static_cast<char>((j & 0xff) + i);
            }
            ReadBufferPtr buff
                = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff + i * buff_size), buff_size);
            wb.putPage(page_id, /* tag */ 0, buff, buff_size);
        }
    }

    auto edit = blob_store.write(std::move(wb));

    size_t idx = 0;
    PageEntriesV3 entries_del;
    for (const auto & record : edit.getRecords())
    {
        for (size_t index : remove_entries_idx)
        {
            if (idx == index)
            {
                entries_del.emplace_back(record.entry);
                break;
            }
        }

        idx++;
    }

    // After remove `entries_del`.
    // Remain entries index [8, 9].
    blob_store.remove(entries_del);

    auto stat = blob_store.blob_stats.blobIdToStat(10);

    const auto & gc_stats = blob_store.getGCStats();
    ASSERT_FALSE(gc_stats.empty());

    ASSERT_EQ(stat->sm_valid_rate, 0.2);
    ASSERT_EQ(stat->sm_total_size, buff_size * buff_nums);
    ASSERT_EQ(stat->sm_valid_size, buff_size * 2);

    // Then we must do heavy GC
    ASSERT_EQ(*(gc_stats.begin()->second.begin()), 10);
}

TEST_F(BlobStoreTest, testBlobStoreRaftDataGcStats)
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getDefaultFileProvider();
    size_t buff_size = 1024;
    size_t buff_nums = 10;
    PageIdU64 page_id = 50;
    PageTypeAndConfig page_type_and_config;
    page_type_and_config.emplace(PageType::Normal, PageTypeConfig{.heavy_gc_valid_rate = 0.5});
    page_type_and_config.emplace(PageType::RaftData, PageTypeConfig{.heavy_gc_valid_rate = 0.05});
    auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config, page_type_and_config);
    std::list<size_t> remove_entries_idx = {0, 1, 2, 3, 4, 5, 6, 7};

    WriteBatch wb;
    char c_buff[buff_size * buff_nums];
    {
        for (size_t i = 0; i < buff_nums; ++i)
        {
            for (size_t j = 0; j < buff_size; ++j)
            {
                c_buff[j + i * buff_size] = static_cast<char>((j & 0xff) + i);
            }
            ReadBufferPtr buff
                = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff + i * buff_size), buff_size);
            wb.putPage(page_id, /* tag */ 0, buff, buff_size);
        }
    }

    auto edit = blob_store.write(std::move(wb), PageType::RaftData, nullptr);

    size_t idx = 0;
    PageEntriesV3 entries_del;
    for (const auto & record : edit.getRecords())
    {
        for (size_t index : remove_entries_idx)
        {
            if (idx == index)
            {
                entries_del.emplace_back(record.entry);
                break;
            }
        }

        idx++;
    }

    // After remove `entries_del`.
    // Remain entries index [8, 9].
    blob_store.remove(entries_del);

    auto stat = blob_store.blob_stats.blobIdToStat(PageTypeUtils::nextFileID(PageType::RaftData, 1));
    ASSERT_NE(stat, nullptr);
    const auto & gc_stats = blob_store.getGCStats();
    // No full gc for raft data
    ASSERT_TRUE(gc_stats.empty());

    ASSERT_EQ(stat->sm_valid_rate, 0.2);
    ASSERT_EQ(stat->sm_total_size, buff_size * buff_nums);
    ASSERT_EQ(stat->sm_valid_size, buff_size * 2);
}


TEST_F(BlobStoreTest, GC)
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getDefaultFileProvider();
    PageIdU64 page_id = 50;
    size_t buff_nums = 21;
    size_t buff_size = 123;

    auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config, page_type_and_config);
    char c_buff[buff_size * buff_nums];

    WriteBatch wb;

    for (size_t i = 0; i < buff_nums; ++i)
    {
        for (size_t j = 0; j < buff_size; ++j)
        {
            c_buff[j + i * buff_size] = static_cast<char>((j & 0xff) + i);
        }

        ReadBufferPtr buff
            = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff + i * buff_size), buff_size);
        wb.putPage(page_id++, /* tag */ 0, buff, buff_size);
    }

    ASSERT_EQ(wb.getTotalDataSize(), buff_nums * buff_size);
    PageEntriesEdit edit = blob_store.write(std::move(wb));
    ASSERT_EQ(edit.size(), buff_nums);

    PageDirectory<u128::PageDirectoryTrait>::GcEntries versioned_pageid_entries;
    for (const auto & record : edit.getRecords())
    {
        versioned_pageid_entries.emplace_back(buildV3Id(TEST_NAMESPACE_ID, page_id), 1, record.entry);
    }
    PageDirectory<u128::PageDirectoryTrait>::GcEntriesMap gc_context;
    gc_context[10] = versioned_pageid_entries;

    // Before we do BlobStore we need change BlobFile0 to Read-Only
    auto stat = blob_store.blob_stats.blobIdToStat(10);
    stat->changeToReadOnly();

    using PageTypeAndGcInfo = typename u128::PageDirectoryType::PageTypeAndGcInfo;
    PageTypeAndGcInfo page_type_and_gc_info;
    page_type_and_gc_info.emplace_back(PageType::Normal, gc_context, static_cast<PageSize>(buff_size * buff_nums));
    const auto & gc_edit = blob_store.gc(page_type_and_gc_info);

    // Check copy_list which will apply for Mvcc
    ASSERT_EQ(gc_edit.size(), buff_nums);
    auto it = versioned_pageid_entries.begin();
    for (const auto & record : gc_edit.getRecords())
    {
        ASSERT_EQ(record.page_id.low, page_id);
        auto it_entry = std::get<2>(*it);
        ASSERT_EQ(record.entry.file_id, 20);
        ASSERT_EQ(record.entry.checksum, it_entry.checksum);
        ASSERT_EQ(record.entry.size, it_entry.size);
        it++;
    }

    // Check blobfile1
    Poco::File file1(blob_store.getBlobFile(10)->getPath());
    Poco::File file2(blob_store.getBlobFile(20)->getPath());
    ASSERT_TRUE(file1.exists());
    ASSERT_TRUE(file2.exists());
    ASSERT_EQ(file1.getSize(), file2.getSize());
    ASSERT_EQ(blob_store.blob_stats.blobIdToStat(20)->sm_total_size, file2.getSize());
}


TEST_F(BlobStoreTest, GCMigirateBigData)
try
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getDefaultFileProvider();
    PageIdU64 fixed_page_id = 50;
    PageIdU64 page_id = fixed_page_id;
    size_t buff_nums = 20;
    size_t buff_size = 20;

    BlobConfig config_with_small_file_limit_size;
    config_with_small_file_limit_size.file_limit_size = 100;
    auto blob_store = BlobStore(
        getCurrentTestName(),
        file_provider,
        delegator,
        config_with_small_file_limit_size,
        page_type_and_config);
    char c_buff[buff_size * buff_nums];


    PageDirectory<u128::PageDirectoryTrait>::GcEntriesMap gc_context;

    for (size_t i = 0; i < buff_nums; ++i)
    {
        for (size_t j = 0; j < buff_size; ++j)
        {
            c_buff[j + i * buff_size] = static_cast<char>((j & 0xff) + i);
        }

        ReadBufferPtr buff
            = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff + i * buff_size), buff_size);
        WriteBatch wb;
        wb.putPage(page_id, /* tag */ 0, buff, buff_size);
        PageEntriesEdit edit = blob_store.write(std::move(wb));

        const auto & records = edit.getRecords();
        ASSERT_EQ(records.size(), 1);
        if (gc_context.find(records[0].entry.file_id) == gc_context.end())
        {
            PageDirectory<u128::PageDirectoryTrait>::GcEntries versioned_pageid_entries;
            versioned_pageid_entries.emplace_back(page_id, 1, records[0].entry);
            gc_context[records[0].entry.file_id] = std::move(versioned_pageid_entries);
        }
        else
        {
            gc_context[records[0].entry.file_id].emplace_back(page_id, 1, records[0].entry);
        }

        page_id++;
    }

    using PageTypeAndGcInfo = typename u128::PageDirectoryType::PageTypeAndGcInfo;
    PageTypeAndGcInfo page_type_and_gc_info;
    page_type_and_gc_info.emplace_back(PageType::Normal, gc_context, static_cast<PageSize>(buff_size * buff_nums));
    const auto & edit = blob_store.gc(page_type_and_gc_info);
    ASSERT_EQ(edit.size(), buff_nums);
}
CATCH

TEST_F(BlobStoreTest, ReadByFieldReadInfos)
try
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getDefaultFileProvider();
    PageIdU64 fixed_page_id = 50;
    PageIdU64 page_id = fixed_page_id;
    size_t buff_nums = 20;
    size_t buff_size = 20;

    BlobConfig config_with_small_file_limit_size;
    config_with_small_file_limit_size.file_limit_size = 100;
    auto blob_store = BlobStore(
        getCurrentTestName(),
        file_provider,
        delegator,
        config_with_small_file_limit_size,
        page_type_and_config);
    char c_buff[buff_size * buff_nums];


    BlobStore::FieldReadInfos read_infos;
    for (size_t i = 0; i < buff_nums; ++i)
    {
        for (size_t j = 0; j < buff_size; ++j)
        {
            c_buff[j + i * buff_size] = static_cast<char>(buildV3Id(TEST_NAMESPACE_ID, page_id + j));
        }

        ReadBufferPtr buff
            = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff + i * buff_size), buff_size);
        PageFieldSizes field_sizes{1, 2, 4, 8, (buff_size - 1 - 2 - 4 - 8)};
        WriteBatch wb;
        wb.putPage(page_id, /* tag */ 0, buff, buff_size, field_sizes);
        PageEntriesEdit edit = blob_store.write(std::move(wb));

        const auto & records = edit.getRecords();
        ASSERT_EQ(records.size(), 1);
        read_infos.emplace_back(
            BlobStore::FieldReadInfo(buildV3Id(TEST_NAMESPACE_ID, page_id), records[0].entry, {0, 1, 2, 3, 4}));

        page_id++;
    }

    auto page_map = blob_store.read(read_infos);
    for (size_t i = 0; i < buff_nums; ++i)
    {
        PageIdU64 reading_id = fixed_page_id + i;
        Page page = page_map.at(reading_id);
        ASSERT_EQ(page.fieldSize(), 5);
    }
}
CATCH

TEST_F(BlobStoreTest, LargeWrite)
try
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getMockFileProvider();
    PageIdU64 fixed_page_id = 50;
    PageIdU64 page_id = fixed_page_id;

    BlobConfig test_config;
    test_config.file_limit_size = 4 * MB;
    auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, test_config, page_type_and_config);

    // PUT page_id 50 into blob 1 (normal write)
    {
        MemoryWriteBuffer buffer;
        size_t serialized_size = 200;
        buffer.write(fixed_buffer, serialized_size);

        WriteBatch wb;
        ReadBufferPtr read_buff = buffer.tryGetReadBuffer();
        wb.putPage(page_id, /* tag */ 0, read_buff, serialized_size);
        PageEntriesEdit edit = blob_store.write(std::move(wb));

        const auto & records = edit.getRecords();
        ASSERT_EQ(records.size(), 1);
        ASSERT_EQ(records[0].entry.file_id, 10);
        ASSERT_EQ(records[0].entry.offset, 0);
        ASSERT_EQ(records[0].entry.size, serialized_size);

        const auto & stat = blob_store.blob_stats.blobIdToStat(10);
        ASSERT_TRUE(stat->isNormal());
        ASSERT_EQ(stat->sm_max_caps, test_config.file_limit_size - serialized_size);
        ASSERT_DOUBLE_EQ(stat->sm_valid_rate, 1.0);
        ASSERT_EQ(stat->sm_valid_size, serialized_size);
        ASSERT_EQ(stat->sm_total_size, serialized_size);

        // Verify read
        Page page = blob_store.read(std::make_pair(buildV3Id(TEST_NAMESPACE_ID, page_id), records[0].entry), nullptr);
        ASSERT_TRUE(page.isValid());
        ASSERT_EQ(page.data.size(), serialized_size);

        page_id++;
    }

    // PUT page_id 51 into blob 2 (large write)
    {
        MemoryWriteBuffer buffer;
        size_t serialized_size = 0;
        while (buffer.count() < test_config.file_limit_size * 2)
        {
            buffer.write(fixed_buffer, fixed_buffer_size);
            serialized_size += fixed_buffer_size;
        }

        WriteBatch wb;
        ReadBufferPtr read_buff = buffer.tryGetReadBuffer();
        wb.putPage(page_id, /* tag */ 0, read_buff, serialized_size);
        PageEntriesEdit edit = blob_store.write(std::move(wb));

        const auto & records = edit.getRecords();
        ASSERT_EQ(records.size(), 1);
        ASSERT_EQ(records[0].entry.file_id, 20);
        ASSERT_EQ(records[0].entry.offset, 0);
        ASSERT_EQ(records[0].entry.size, serialized_size);

        // verify blobstat
        const auto & stat = blob_store.blob_stats.blobIdToStat(20);
        ASSERT_TRUE(stat->isReadOnly()); // large write, this stat is read only
        ASSERT_EQ(stat->sm_max_caps, 0);
        ASSERT_DOUBLE_EQ(stat->sm_valid_rate, 1.0);
        ASSERT_EQ(stat->sm_valid_size, serialized_size);
        ASSERT_EQ(stat->sm_total_size, serialized_size);

        // Verify read
        Page page = blob_store.read(std::make_pair(buildV3Id(TEST_NAMESPACE_ID, page_id), records[0].entry), nullptr);
        ASSERT_TRUE(page.isValid());
        ASSERT_EQ(page.data.size(), serialized_size);

        page_id++;
    }

    // PUT page_id 52 into blob 1 (normal write)
    {
        MemoryWriteBuffer buffer;
        size_t serialized_size = 100;
        buffer.write(fixed_buffer, serialized_size);

        WriteBatch wb;
        ReadBufferPtr read_buff = buffer.tryGetReadBuffer();
        wb.putPage(page_id, /* tag */ 0, read_buff, serialized_size);
        PageEntriesEdit edit = blob_store.write(std::move(wb));

        const auto & records = edit.getRecords();
        ASSERT_EQ(records.size(), 1);
        ASSERT_EQ(records[0].entry.file_id, 10);
        ASSERT_EQ(records[0].entry.offset, 200);
        ASSERT_EQ(records[0].entry.size, 100);

        const auto & stat = blob_store.blob_stats.blobIdToStat(10);
        ASSERT_TRUE(stat->isNormal());
        ASSERT_EQ(stat->sm_max_caps, test_config.file_limit_size - 200 - 100);
        ASSERT_DOUBLE_EQ(stat->sm_valid_rate, 1.0);
        ASSERT_EQ(stat->sm_valid_size, 300);
        ASSERT_EQ(stat->sm_total_size, 300);

        // Verify read
        Page page = blob_store.read(std::make_pair(buildV3Id(TEST_NAMESPACE_ID, page_id), records[0].entry), nullptr);
        ASSERT_TRUE(page.isValid());
        ASSERT_EQ(page.data.size(), serialized_size);

        page_id++;
    }

    // PUT page_id 53 into blob 3 range [0,300]
    {
        MemoryWriteBuffer buffer;
        size_t serialized_size = 0;
        while (buffer.count() < test_config.file_limit_size * 1.5)
        {
            buffer.write(fixed_buffer, fixed_buffer_size);
            serialized_size += fixed_buffer_size;
        }

        WriteBatch wb;
        ReadBufferPtr read_buff = buffer.tryGetReadBuffer();
        wb.putPage(page_id, /* tag */ 0, read_buff, serialized_size);
        PageEntriesEdit edit = blob_store.write(std::move(wb));

        const auto & records = edit.getRecords();
        ASSERT_EQ(records.size(), 1);
        ASSERT_EQ(records[0].entry.file_id, 30);
        ASSERT_EQ(records[0].entry.offset, 0);
        ASSERT_EQ(records[0].entry.size, serialized_size);

        const auto & stat = blob_store.blob_stats.blobIdToStat(30);
        ASSERT_TRUE(stat->isReadOnly()); // large write, this stat is read only
        ASSERT_EQ(stat->sm_max_caps, 0);
        ASSERT_DOUBLE_EQ(stat->sm_valid_rate, 1.0);
        ASSERT_EQ(stat->sm_valid_size, serialized_size);
        ASSERT_EQ(stat->sm_total_size, serialized_size);

        // Verify read
        Page page = blob_store.read(std::make_pair(buildV3Id(TEST_NAMESPACE_ID, page_id), records[0].entry), nullptr);
        ASSERT_TRUE(page.isValid());
        ASSERT_EQ(page.data.size(), serialized_size);

        page_id++;
    }

    // Large write compose by multiple puts
    {
        std::vector<double> test_scales = {1.5, 0.2, 0.4, 1.1};
        std::vector<size_t> actual_size;

        WriteBatch wb;
        for (const auto & scale : test_scales)
        {
            size_t serialized_size = 0;
            MemoryWriteBuffer buffer;
            while (buffer.count() < test_config.file_limit_size * scale)
            {
                buffer.write(fixed_buffer, fixed_buffer_size);
                serialized_size += fixed_buffer_size;
            }
            wb.putPage(page_id++, /* tag */ 0, buffer.tryGetReadBuffer(), serialized_size);
            actual_size.emplace_back(serialized_size);
        }
        ASSERT_EQ(test_scales.size(), actual_size.size());

        PageEntriesEdit edit = blob_store.write(std::move(wb));

        const auto & records = edit.getRecords();
        ASSERT_EQ(records.size(), 4);

        // PUT page_id 54 into blob 40 (large write)
        ASSERT_EQ(records[0].page_id.low, 54);
        ASSERT_EQ(records[0].entry.file_id, 40);
        ASSERT_EQ(records[0].entry.offset, 0);
        ASSERT_EQ(records[0].entry.size, actual_size[0]);

        // PUT page_id 55 into blob 10 or 30
        ASSERT_EQ(records[1].page_id.low, 55);
        ASSERT_TRUE(records[1].entry.file_id == 10 || records[1].entry.file_id == 30);
        ASSERT_EQ(records[1].entry.size, actual_size[1]);

        // PUT page_id 56 into blob 5
        ASSERT_EQ(records[2].page_id.low, 56);
        ASSERT_TRUE(records[2].entry.file_id == 10 || records[2].entry.file_id == 30);
        ASSERT_EQ(records[2].entry.size, actual_size[2]);

        // PUT page_id 57 into blob 6 (large write)
        ASSERT_EQ(records[3].page_id.low, 57);
        ASSERT_EQ(records[3].entry.file_id, 50);
        ASSERT_EQ(records[3].entry.offset, 0);
        ASSERT_EQ(records[3].entry.size, actual_size[3]);
    }
}
CATCH

TEST_F(BlobStoreTest, LargeWriteWithFields)
try
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getMockFileProvider();
    PageIdU64 fixed_page_id = 50;
    PageIdU64 page_id = fixed_page_id;

    BlobConfig test_config;
    test_config.file_limit_size = 4 * MB;
    auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, test_config, page_type_and_config);

    // PUT page_id 50 into blob 1 (large write)
    {
        MemoryWriteBuffer buffer;
        size_t serialized_size = 0;
        while (buffer.count() < test_config.file_limit_size * 2)
        {
            buffer.write(fixed_buffer, fixed_buffer_size);
            serialized_size += fixed_buffer_size;
        }

        WriteBatch wb;
        ReadBufferPtr read_buff = buffer.tryGetReadBuffer();
        PageFieldSizes field_sizes = [&]() {
            PageFieldSizes sizes{1035, 1 * MB + 24, 30, 50, 70, 67, 89, 97};
            auto sum_of_sizes = std::accumulate(field_sizes.begin(), field_sizes.end(), 0UL);
            field_sizes.emplace_back(serialized_size - sum_of_sizes);
            return sizes;
        }();
        wb.putPage(page_id, /* tag */ 0, read_buff, serialized_size, field_sizes);
        PageEntriesEdit edit = blob_store.write(std::move(wb));

        const auto & records = edit.getRecords();
        ASSERT_EQ(records.size(), 1);
        ASSERT_EQ(records[0].entry.file_id, 10);
        ASSERT_EQ(records[0].entry.offset, 0);
        ASSERT_EQ(records[0].entry.size, serialized_size);

        // verify blobstat
        const auto & stat = blob_store.blob_stats.blobIdToStat(10);
        ASSERT_TRUE(stat->isReadOnly()); // large write, this stat is read only
        ASSERT_EQ(stat->sm_max_caps, 0);
        ASSERT_DOUBLE_EQ(stat->sm_valid_rate, 1.0);
        ASSERT_EQ(stat->sm_valid_size, serialized_size);
        ASSERT_EQ(stat->sm_total_size, serialized_size);

        // Verify read
        {
            Page page
                = blob_store.read(std::make_pair(buildV3Id(TEST_NAMESPACE_ID, page_id), records[0].entry), nullptr);
            ASSERT_TRUE(page.isValid());
            ASSERT_EQ(page.data.size(), serialized_size);
        }

        // Verify read with fields
        {
            BlobStore::FieldReadInfos to_read{
                BlobStore::FieldReadInfo(
                    buildV3Id(TEST_NAMESPACE_ID, page_id),
                    records[0].entry,
                    {0, 1, 2, 3, 4, 5, 6, 7, 8}),
            };
            BlobStore::PageMap page_map = blob_store.read(to_read, nullptr);
            ASSERT_NE(page_map.find(page_id), page_map.end());
            ASSERT_EQ(page_map.at(page_id).fieldSize(), to_read[0].fields.size());
        }

        // Verify read with fields
        {
            BlobStore::FieldReadInfos to_read{
                BlobStore::FieldReadInfo(
                    buildV3Id(TEST_NAMESPACE_ID, page_id),
                    records[0].entry,
                    {0, 1, 2, 3, 4, 5, 6}),
            };
            BlobStore::PageMap page_map = blob_store.read(to_read, nullptr);
            ASSERT_NE(page_map.find(page_id), page_map.end());
            ASSERT_EQ(page_map.at(page_id).fieldSize(), to_read[0].fields.size());
        }

        // Verify read with fields
        {
            BlobStore::FieldReadInfos to_read{
                BlobStore::FieldReadInfo(buildV3Id(TEST_NAMESPACE_ID, page_id), records[0].entry, {1, 2, 3, 5, 6, 8}),
            };
            BlobStore::PageMap page_map = blob_store.read(to_read, nullptr);
            ASSERT_NE(page_map.find(page_id), page_map.end());
            ASSERT_EQ(page_map.at(page_id).fieldSize(), to_read[0].fields.size());
        }

        page_id++;
    }
}
CATCH


TEST_F(BlobStoreTest, LargeWriteWithFailed)
try
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getMockFileProvider();
    PageIdU64 fixed_page_id = 50;
    PageIdU64 page_id = fixed_page_id;

    BlobConfig test_config;
    test_config.file_limit_size = 4 * MB;
    auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, test_config, page_type_and_config);

    // PUT page_id 50 into blob 1 (normal write)
    {
        MemoryWriteBuffer buffer;
        size_t serialized_size = 200;
        buffer.write(fixed_buffer, serialized_size);

        WriteBatch wb;
        ReadBufferPtr read_buff = buffer.tryGetReadBuffer();
        wb.putPage(page_id, /* tag */ 0, read_buff, serialized_size);
        PageEntriesEdit edit = blob_store.write(std::move(wb));

        const auto & records = edit.getRecords();
        ASSERT_EQ(records.size(), 1);
        ASSERT_EQ(records[0].entry.file_id, 10);
        ASSERT_EQ(records[0].entry.offset, 0);
        ASSERT_EQ(records[0].entry.size, serialized_size);

        const auto & stat = blob_store.blob_stats.blobIdToStat(10);
        ASSERT_TRUE(stat->isNormal());
        ASSERT_EQ(stat->sm_max_caps, test_config.file_limit_size - serialized_size);
        ASSERT_DOUBLE_EQ(stat->sm_valid_rate, 1.0);
        ASSERT_EQ(stat->sm_valid_size, serialized_size);
        ASSERT_EQ(stat->sm_total_size, serialized_size);

        // Verify read
        Page page = blob_store.read(std::make_pair(buildV3Id(TEST_NAMESPACE_ID, page_id), records[0].entry), nullptr);
        ASSERT_TRUE(page.isValid());
        ASSERT_EQ(page.data.size(), serialized_size);

        page_id++;
    }

    // PUT page_id 51 into blob 2 (large write, fail happen)
    {
        MemoryWriteBuffer buffer;
        size_t serialized_size = 0;
        while (buffer.count() < test_config.file_limit_size * 2)
        {
            buffer.write(fixed_buffer, fixed_buffer_size);
            serialized_size += fixed_buffer_size;
        }

        const auto num_stats = blob_store.blob_stats.getStats().size();
        ASSERT_EQ(num_stats, 1);

        WriteBatch wb;
        ReadBufferPtr read_buff = buffer.tryGetReadBuffer();
        wb.putPage(page_id, /* tag */ 0, read_buff, serialized_size);

        // throw an exception after write exceed `test_config.file_limit_size`
        FailPointHelper::enableFailPoint(
            FailPoints::exception_after_large_write_exceed,
            static_cast<size_t>(test_config.file_limit_size));
        try
        {
            blob_store.write(std::move(wb));
        }
        catch (DB::Exception & e)
        {
            ASSERT_EQ(e.code(), ErrorCodes::FAIL_POINT_ERROR);
        }

        // no new-added stat
        ASSERT_EQ(blob_store.blob_stats.blobIdToStat(20, /*ignore_not_exist*/ true), nullptr);

        page_id++;
    }
}
CATCH

TEST_F(BlobStoreTest, LargeWriteRemove)
try
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getMockFileProvider();
    PageIdU64 fixed_page_id = 50;
    PageIdU64 page_id = fixed_page_id;

    BlobConfig config_with_small_file_limit_size;
    config_with_small_file_limit_size.file_limit_size = 400;
    auto blob_store = BlobStore(
        getCurrentTestName(),
        file_provider,
        delegator,
        config_with_small_file_limit_size,
        page_type_and_config);

    {
        const size_t size_500 = 500;
        char c_buff[size_500];

        WriteBatch wb;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff), size_500);
        wb.putPage(page_id, /* tag */ 0, buff, size_500);
        PageEntriesEdit edit = blob_store.write(std::move(wb));

        const auto & gc_info = blob_store.getGCStats();
        ASSERT_TRUE(gc_info.empty());

        // only one blob_file for the large write
        ASSERT_EQ(getTotalStatsNum(blob_store.blob_stats.getStats()), 1);
        const auto & records = edit.getRecords();
        ASSERT_EQ(records.size(), 1);
        blob_store.remove({records[0].entry});
        ASSERT_EQ(getTotalStatsNum(blob_store.blob_stats.getStats()), 0);
    }
}
CATCH

TEST_F(BlobStoreTest, ReadEmptyPageAfterAllValidEntriesRmoved)
try
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getMockFileProvider();

    BlobConfig config_with_small_file_limit_size;
    config_with_small_file_limit_size.file_limit_size = 400;
    auto blob_store = BlobStore(
        getCurrentTestName(),
        file_provider,
        delegator,
        config_with_small_file_limit_size,
        page_type_and_config);

    {
        const size_t size_500 = 500;
        char c_buff[size_500];

        WriteBatch wb;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, size_500);
        wb.putPage(50, /* tag */ 0, buff, size_500);
        wb.putPage(51, 0, std::make_shared<ReadBufferFromMemory>(c_buff, 0), 0);
        wb.putPage(52, 0, std::make_shared<ReadBufferFromMemory>(c_buff, 20), 20);
        wb.putPage(53, 0, std::make_shared<ReadBufferFromMemory>(c_buff, 0), 0);
        PageEntriesEdit edit = blob_store.write(std::move(wb));

        // make the blob_file to be READ_ONLY
        FailPointHelper::enableFailPoint(FailPoints::force_pick_all_blobs_to_full_gc);
        SCOPE_EXIT({ FailPointHelper::disableFailPoint(FailPoints::force_pick_all_blobs_to_full_gc); });
        const auto & gc_info = blob_store.getGCStats();
        ASSERT_FALSE(gc_info.empty());

        // one blob_file for the large write, another for the other pages
        ASSERT_EQ(getTotalStatsNum(blob_store.blob_stats.getStats()), 2);
        const auto & records = edit.getRecords();
        ASSERT_EQ(records.size(), 4);
        // remove the non-empty entries
        blob_store.removeEntries({records[0].entry, records[2].entry});

        /// try to read the empty pages after the blob_file get removed, no exception should happen
        // read-1-entry
        blob_store.read(PageIDAndEntryV3(51, records[1].entry), nullptr);
        blob_store.read(PageIDAndEntryV3(53, records[3].entry), nullptr);
        // read multiple entry
        PageIDAndEntriesV3 entries{PageIDAndEntryV3(51, records[1].entry), PageIDAndEntryV3(53, records[3].entry)};
        blob_store.read(entries, nullptr);

        ASSERT_EQ(getTotalStatsNum(blob_store.blob_stats.getStats()), 0);
    }
}
CATCH

TEST_F(BlobStoreTest, LargeWriteRegisterPath)
try
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getMockFileProvider();
    PageIdU64 fixed_page_id = 50;
    PageIdU64 page_id = fixed_page_id;

    BlobConfig config_with_small_file_limit_size;
    config_with_small_file_limit_size.file_limit_size = 400;

    PageEntryV3 entry_from_write;
    {
        auto blob_store = BlobStore(
            getCurrentTestName(),
            file_provider,
            delegator,
            config_with_small_file_limit_size,
            page_type_and_config);
        size_t size_500 = 500;
        char c_buff[size_500];

        WriteBatch wb;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff), size_500);
        wb.putPage(page_id, /* tag */ 0, buff, size_500);
        auto edit = blob_store.write(std::move(wb));
        const auto & records = edit.getRecords();
        ASSERT_EQ(records.size(), 1);
        entry_from_write = records[0].entry;
    }

    {
        auto blob_store = BlobStore(
            getCurrentTestName(),
            file_provider,
            delegator,
            config_with_small_file_limit_size,
            page_type_and_config);
        blob_store.registerPaths();

        blob_store.blob_stats.restoreByEntry(entry_from_write);
        blob_store.blob_stats.restore();
        const auto & stat = blob_store.blob_stats.blobIdToStat(10);
        ASSERT_EQ(stat->sm_max_caps, 0);
        ASSERT_DOUBLE_EQ(stat->sm_valid_rate, 1.0);
        ASSERT_EQ(stat->sm_valid_size, 500);
        ASSERT_EQ(stat->sm_total_size, 500);
    }
}
CATCH

TEST_F(BlobStoreTest, TestRestartWithSmallerFileLimitSize)
try
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getMockFileProvider();
    PageIdU64 page_id = 50;

    BlobConfig config_with_small_file_limit_size;
    config_with_small_file_limit_size.file_limit_size = 800;

    PageEntryV3 entry_from_write1;
    PageEntryV3 entry_from_write2;
    {
        auto blob_store = BlobStore(
            getCurrentTestName(),
            file_provider,
            delegator,
            config_with_small_file_limit_size,
            page_type_and_config);
        size_t size_500 = 500;
        size_t size_200 = 200;
        char c_buff1[size_500];
        char c_buff2[size_200];

        WriteBatch wb;
        ReadBufferPtr buff1 = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff1), size_500);
        ReadBufferPtr buff2 = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff2), size_200);
        wb.putPage(page_id, /* tag */ 0, buff1, size_500);
        wb.putPage(page_id + 1, /* tag */ 0, buff2, size_200);
        auto edit = blob_store.write(std::move(wb));
        const auto & records = edit.getRecords();
        ASSERT_EQ(records.size(), 2);
        entry_from_write1 = records[0].entry;
        entry_from_write2 = records[1].entry;
        ASSERT_EQ(entry_from_write1.size, 500);
        ASSERT_EQ(entry_from_write2.size, 200);

        ASSERT_TRUE(blob_store.blob_stats.blobIdToStat(10)->isNormal());
    }

    config_with_small_file_limit_size.file_limit_size = 400;
    {
        auto blob_store = BlobStore(
            getCurrentTestName(),
            file_provider,
            delegator,
            config_with_small_file_limit_size,
            page_type_and_config);
        blob_store.registerPaths();
        blob_store.blob_stats.restoreByEntry(entry_from_write1);
        blob_store.blob_stats.restoreByEntry(entry_from_write2);
        blob_store.blob_stats.restore();
        const auto & stat = blob_store.blob_stats.blobIdToStat(10);
        ASSERT_EQ(stat->sm_max_caps, 0);
        ASSERT_DOUBLE_EQ(stat->sm_valid_rate, 1.0);
        ASSERT_EQ(stat->sm_valid_size, 700);
        ASSERT_EQ(stat->sm_total_size, 700);
        ASSERT_TRUE(stat->isReadOnly());

        blob_store.remove({entry_from_write1});

        // new write will create new blob file
        size_t size_100 = 100;
        char c_buff[size_100];

        WriteBatch wb;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff), size_100);
        wb.putPage(page_id, /* tag */ 0, buff, size_100);
        auto edit = blob_store.write(std::move(wb));
        const auto & records = edit.getRecords();
        ASSERT_EQ(records.size(), 1);
        ASSERT_EQ(records[0].entry.file_id, 20);
        ASSERT_EQ(getTotalStatsNum(blob_store.blob_stats.getStats()), 2);

        // remove one shot blob file
        blob_store.remove({entry_from_write2});
        ASSERT_EQ(getTotalStatsNum(blob_store.blob_stats.getStats()), 1);
    }
}
CATCH

TEST_F(BlobStoreTest, LargeWriteGC)
try
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getMockFileProvider();
    PageIdU64 page_id1 = 50;
    PageIdU64 page_id2 = 51;
    PageIdU64 page_id3 = 52;

    BlobConfig config_with_small_file_limit_size;
    config_with_small_file_limit_size.file_limit_size = 800;

    PageEntryV3 entry_from_write1;
    PageEntryV3 entry_from_write2;
    PageEntryV3 entry_from_write3;
    auto blob_store = BlobStore(
        getCurrentTestName(),
        file_provider,
        delegator,
        config_with_small_file_limit_size,
        page_type_and_config);
    {
        size_t size_100 = 100;
        size_t size_500 = 500;
        size_t size_200 = 200;
        char c_buff1[size_100];
        char c_buff2[size_500];
        char c_buff3[size_200];

        WriteBatch wb;
        ReadBufferPtr buff1 = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff1), size_100);
        ReadBufferPtr buff2 = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff2), size_500);
        ReadBufferPtr buff3 = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff3), size_200);
        wb.putPage(page_id1, /* tag */ 0, buff1, size_100);
        wb.putPage(page_id2, /* tag */ 0, buff2, size_500);
        wb.putPage(page_id3, /* tag */ 0, buff3, size_200);
        auto edit = blob_store.write(std::move(wb));
        const auto & records = edit.getRecords();
        ASSERT_EQ(records.size(), 3);
        entry_from_write1 = records[0].entry;
        entry_from_write2 = records[1].entry;
        entry_from_write3 = records[2].entry;

        ASSERT_TRUE(blob_store.blob_stats.blobIdToStat(10)->isNormal());
    }

    config_with_small_file_limit_size.file_limit_size = 400;
    config_with_small_file_limit_size.heavy_gc_valid_rate = 0.99;
    {
        blob_store.reloadConfig(config_with_small_file_limit_size);

        Poco::File file1(blob_store.getBlobFile(10)->getPath());
        ASSERT_EQ(file1.getSize(), 800);
        ASSERT_TRUE(blob_store.blob_stats.blobIdToStat(10)->isNormal()); // BlobStat type doesn't change after reload
        blob_store.remove({entry_from_write3});
        auto blob_need_gc = blob_store.getGCStats();
        ASSERT_EQ(blob_need_gc.size(), 0);
        ASSERT_EQ(file1.getSize(), 600);

        blob_store.remove({entry_from_write1});

        const auto & blob_need_gc2 = blob_store.getGCStats();
        ASSERT_EQ(blob_need_gc2.size(), 1);
        PageDirectory<u128::PageDirectoryTrait>::GcEntriesMap gc_context;
        PageDirectory<u128::PageDirectoryTrait>::GcEntries versioned_pageid_entries;
        versioned_pageid_entries.emplace_back(page_id2, 1, entry_from_write2);
        gc_context[10] = versioned_pageid_entries;
        using PageTypeAndGcInfo = typename u128::PageDirectoryType::PageTypeAndGcInfo;
        PageTypeAndGcInfo page_type_and_gc_info;
        page_type_and_gc_info.emplace_back(PageType::Normal, gc_context, 500);
        PageEntriesEdit gc_edit = blob_store.gc(page_type_and_gc_info);
        const auto & records = gc_edit.getRecords();
        ASSERT_EQ(records.size(), 1);
        ASSERT_EQ(records[0].entry.file_id, 20);
        ASSERT_EQ(records[0].entry.size, 500);
    }
}
CATCH
} // namespace DB::PS::V3::tests
