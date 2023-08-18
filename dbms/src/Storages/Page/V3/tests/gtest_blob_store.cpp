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

#include <Common/Logger.h>
#include <Encryption/RateLimiter.h>
#include <IO/ReadBufferFromMemory.h>
#include <Poco/Logger.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/tests/entries_helper.h>
#include <Storages/Page/WriteBatch.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/MockDiskDelegator.h>
#include <TestUtils/MockReadLimiter.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB::PS::V3::tests
{
using BlobStat = BlobStore::BlobStats::BlobStat;
using BlobStats = BlobStore::BlobStats;

constexpr size_t path_num = 3;

class BlobStoreStatsTest : public DB::base::TiFlashStorageTestBasic
{
public:
    BlobStoreStatsTest()
        : logger(Logger::get("BlobStoreStatsTest"))
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
    }

protected:
    BlobStore::Config config;
    LoggerPtr logger;
    PSDiskDelegatorPtr delegator;
};

static size_t getTotalStatsNum(const BlobStore::BlobStats::StatsMap & stats_map)
{
    size_t total_stats_num = 0;
    for (auto iter = stats_map.begin(); iter != stats_map.end(); iter++)
    {
        total_stats_num += iter->second.size();
    }
    return total_stats_num;
}

TEST_F(BlobStoreStatsTest, RestoreEmpty)
{
    BlobStats stats(logger, delegator, config);

    stats.restore();

    auto stats_copy = stats.getStats();
    ASSERT_TRUE(stats_copy.empty());

    EXPECT_EQ(stats.roll_id, 1);
    EXPECT_NO_THROW(stats.createStat(stats.roll_id, stats.lock()));
}

TEST_F(BlobStoreStatsTest, Restore)
try
{
    BlobStats stats(logger, delegator, config);

    BlobFileId file_id1 = 10;
    BlobFileId file_id2 = 12;

    {
        const auto & lock = stats.lock();
        stats.createStatNotChecking(file_id1, lock);
        stats.createStatNotChecking(file_id2, lock);
    }

    {
        stats.restoreByEntry(PageEntryV3{
            .file_id = file_id1,
            .size = 128,
            .tag = 0,
            .offset = 1024,
            .checksum = 0x4567,
        });
        stats.restoreByEntry(PageEntryV3{
            .file_id = file_id1,
            .size = 512,
            .tag = 0,
            .offset = 2048,
            .checksum = 0x4567,
        });
        stats.restoreByEntry(PageEntryV3{
            .file_id = file_id2,
            .size = 512,
            .tag = 0,
            .offset = 2048,
            .checksum = 0x4567,
        });
        stats.restore();
    }

    auto stats_copy = stats.getStats();

    ASSERT_EQ(stats_copy.size(), std::min(getTotalStatsNum(stats_copy), path_num));
    ASSERT_EQ(getTotalStatsNum(stats_copy), 2);
    EXPECT_EQ(stats.roll_id, 13);

    auto stat1 = stats.blobIdToStat(file_id1);
    EXPECT_EQ(stat1->sm_total_size, 2048 + 512);
    EXPECT_EQ(stat1->sm_valid_size, 128 + 512);
    auto stat2 = stats.blobIdToStat(file_id2);
    EXPECT_EQ(stat2->sm_total_size, 2048 + 512);
    EXPECT_EQ(stat2->sm_valid_size, 512);

    // This will throw exception since we try to create
    // a new file bigger than restored `roll_id`
    EXPECT_ANY_THROW({ stats.createStat(14, stats.lock()); });

    EXPECT_ANY_THROW({ stats.createStat(file_id1, stats.lock()); });
    EXPECT_ANY_THROW({ stats.createStat(file_id2, stats.lock()); });
    EXPECT_ANY_THROW({ stats.createStat(stats.roll_id + 1, stats.lock()); });
}
CATCH

TEST_F(BlobStoreStatsTest, testStats)
{
    BlobStats stats(logger, delegator, config);

    auto stat = stats.createStat(0, stats.lock());

    ASSERT_TRUE(stat);
    ASSERT_TRUE(stat->smap);
    stats.createStat(1, stats.lock());
    stats.createStat(2, stats.lock());

    auto stats_copy = stats.getStats();

    ASSERT_EQ(stats_copy.size(), std::min(getTotalStatsNum(stats_copy), path_num));
    ASSERT_EQ(getTotalStatsNum(stats_copy), 3);
    ASSERT_EQ(stats.roll_id, 3);

    stats.eraseStat(0, stats.lock());
    stats.eraseStat(1, stats.lock());
    ASSERT_EQ(getTotalStatsNum(stats.getStats()), 1);
    ASSERT_EQ(stats.roll_id, 3);
}


TEST_F(BlobStoreStatsTest, testStat)
{
    BlobFileId blob_file_id = 0;
    BlobStore::BlobStats::BlobStatPtr stat;

    BlobStats stats(logger, delegator, config);

    std::tie(stat, blob_file_id) = stats.chooseStat(10, stats.lock());
    ASSERT_EQ(blob_file_id, 1);
    ASSERT_FALSE(stat);

    // still 0
    std::tie(stat, blob_file_id) = stats.chooseStat(10, stats.lock());
    ASSERT_EQ(blob_file_id, 1);
    ASSERT_FALSE(stat);

    stats.createStat(0, stats.lock());
    std::tie(stat, blob_file_id) = stats.chooseStat(10, stats.lock());
    ASSERT_EQ(blob_file_id, INVALID_BLOBFILE_ID);
    ASSERT_TRUE(stat);

    auto offset = stat->getPosFromStat(10, stats.lock());
    ASSERT_EQ(offset, 0);

    offset = stat->getPosFromStat(100, stats.lock());
    ASSERT_EQ(offset, 10);

    offset = stat->getPosFromStat(20, stats.lock());
    ASSERT_EQ(offset, 110);

    ASSERT_EQ(stat->sm_total_size, 10 + 100 + 20);
    ASSERT_EQ(stat->sm_valid_size, 10 + 100 + 20);
    ASSERT_EQ(stat->sm_valid_rate, 1);

    stat->removePosFromStat(10, 100, stats.lock());
    ASSERT_EQ(stat->sm_total_size, 10 + 100 + 20);
    ASSERT_EQ(stat->sm_valid_size, 10 + 20);
    ASSERT_LE(stat->sm_valid_rate, 1);

    offset = stat->getPosFromStat(110, stats.lock());
    ASSERT_EQ(offset, 130);
    ASSERT_EQ(stat->sm_total_size, 10 + 100 + 20 + 110);
    ASSERT_EQ(stat->sm_valid_size, 10 + 20 + 110);
    ASSERT_LE(stat->sm_valid_rate, 1);

    offset = stat->getPosFromStat(90, stats.lock());
    ASSERT_EQ(offset, 10);
    ASSERT_EQ(stat->sm_total_size, 10 + 100 + 20 + 110);
    ASSERT_EQ(stat->sm_valid_size, 10 + 20 + 110 + 90);
    ASSERT_LE(stat->sm_valid_rate, 1);

    // Unmark the last range
    stat->removePosFromStat(130, 110, stats.lock());
    ASSERT_EQ(stat->sm_total_size, 10 + 100 + 20 + 110);
    ASSERT_EQ(stat->sm_valid_size, 10 + 20 + 90);
    ASSERT_LE(stat->sm_valid_rate, 1);

    /**
     * now used space looks like:
     *  [0,10) [10,100) [110,130) 
     * And total size still is 10 + 100 + 20 + 110
     * Then after we add a range which size is 120
     * Total size should plus 10, rather than 120.
     * And the postion return should be last range freed.
     */
    offset = stat->getPosFromStat(120, stats.lock());
    ASSERT_EQ(offset, 130);
    ASSERT_EQ(stat->sm_total_size, 10 + 100 + 20 + 110 + 10);
    ASSERT_EQ(stat->sm_valid_size, 10 + 20 + 90 + 120);
    ASSERT_LE(stat->sm_valid_rate, 1);
}

TEST_F(BlobStoreStatsTest, testFullStats)
{
    BlobFileId blob_file_id = 0;
    BlobStore::BlobStats::BlobStatPtr stat;
    BlobFileOffset offset = 0;

    BlobStats stats(logger, delegator, config);

    stat = stats.createStat(1, stats.lock());
    offset = stat->getPosFromStat(BLOBFILE_LIMIT_SIZE - 1, stats.lock());
    ASSERT_EQ(offset, 0);

    // Can't get pos from a full stat
    offset = stat->getPosFromStat(100, stats.lock());
    ASSERT_EQ(offset, INVALID_BLOBFILE_OFFSET);

    // Stat internal property should not changed
    ASSERT_EQ(stat->sm_total_size, BLOBFILE_LIMIT_SIZE - 1);
    ASSERT_EQ(stat->sm_valid_size, BLOBFILE_LIMIT_SIZE - 1);
    ASSERT_LE(stat->sm_valid_rate, 1);

    // Won't choose full one
    std::tie(stat, blob_file_id) = stats.chooseStat(100, stats.lock());
    ASSERT_EQ(blob_file_id, 2);
    ASSERT_FALSE(stat);

    // A new stat can use
    stat = stats.createStat(blob_file_id, stats.lock());
    offset = stat->getPosFromStat(100, stats.lock());
    ASSERT_EQ(offset, 0);

    // Remove the stat which id is 0 , now remain the stat which id is 1
    stats.eraseStat(1, stats.lock());

    // Then full the stat which id 2
    offset = stat->getPosFromStat(BLOBFILE_LIMIT_SIZE - 100, stats.lock());
    ASSERT_EQ(offset, 100);

    // Then choose stat , it should return the stat id 3
    // Stat which id is 2 is full.
    std::tie(stat, blob_file_id) = stats.chooseStat(100, stats.lock());
    ASSERT_EQ(blob_file_id, 3);
    ASSERT_FALSE(stat);
}

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

        // Note although set config.cached_fd_size to 0, the cache fd size in blobstore still have capacity 1.
        // Decrease cache size to make problems more easily be exposed.
        config.cached_fd_size = 0;
    }

protected:
    BlobStore::Config config;
    PSDiskDelegatorPtr delegator;
};

TEST_F(BlobStoreTest, Restore)
try
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getContext().getFileProvider();
    config.file_limit_size = 2560;
    auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config);

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
            .tag = 0,
            .offset = 1024,
            .checksum = 0x4567,
        });
        blob_store.blob_stats.restoreByEntry(PageEntryV3{
            .file_id = file_id1,
            .size = 512,
            .tag = 0,
            .offset = 2048,
            .checksum = 0x4567,
        });
        blob_store.blob_stats.restoreByEntry(PageEntryV3{
            .file_id = file_id2,
            .size = 512,
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
    const auto file_provider = DB::tests::TiFlashTestEnv::getContext().getFileProvider();
    config.file_limit_size = 1024;

    // Generate blob [1,2,3]
    auto write_blob_datas = [](BlobStore & blob_store) {
        WriteBatch write_batch;
        PageId page_id = 55;
        size_t buff_size = 1024;
        char c_buff[buff_size];
        memset(c_buff, 0x1, buff_size);

        // write blob 1
        write_batch.putPage(page_id, /* tag */ 0, std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff), buff_size), buff_size);
        blob_store.write(write_batch, nullptr);
        write_batch.clear();

        // write blob 2
        write_batch.putPage(page_id + 1, /* tag */ 0, std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff), buff_size), buff_size);
        blob_store.write(write_batch, nullptr);
        write_batch.clear();

        // write blob 3
        write_batch.putPage(page_id + 2, /* tag */ 0, std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff), buff_size), buff_size);
        blob_store.write(write_batch, nullptr);
        write_batch.clear();
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
                .tag = 0,
                .offset = 0,
                .checksum = 0x4567,
            });
        }
    };

    // Case 1, all of blob been restored
    {
        auto test_paths = delegator->listPaths();
        auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config);
        write_blob_datas(blob_store);

        ASSERT_TRUE(check_in_disk_file(test_paths, {1, 2, 3}));

        auto blob_store_check = BlobStore(getCurrentTestName(), file_provider, delegator, config);
        restore_blobs(blob_store_check, {1, 2, 3});

        blob_store_check.blob_stats.restore();

        ASSERT_TRUE(check_in_disk_file(test_paths, {1, 2, 3}));
        for (const auto & path : test_paths)
        {
            DB::tests::TiFlashTestEnv::tryRemovePath(path);
            createIfNotExist(path);
        }
    }

    // Case 2, only recover blob 1
    {
        auto test_paths = delegator->listPaths();
        auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config);
        write_blob_datas(blob_store);

        ASSERT_TRUE(check_in_disk_file(test_paths, {1, 2, 3}));

        auto blob_store_check = BlobStore(getCurrentTestName(), file_provider, delegator, config);
        restore_blobs(blob_store_check, {1});

        blob_store_check.blob_stats.restore();

        ASSERT_TRUE(check_in_disk_file(test_paths, {1}));
        for (const auto & path : test_paths)
        {
            DB::tests::TiFlashTestEnv::tryRemovePath(path);
            createIfNotExist(path);
        }
    }

    // Case 3, only recover blob 2
    {
        auto test_paths = delegator->listPaths();
        auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config);
        write_blob_datas(blob_store);

        ASSERT_TRUE(check_in_disk_file(test_paths, {1, 2, 3}));

        auto blob_store_check = BlobStore(getCurrentTestName(), file_provider, delegator, config);
        restore_blobs(blob_store_check, {2});

        blob_store_check.blob_stats.restore();

        ASSERT_TRUE(check_in_disk_file(test_paths, {2}));
        for (const auto & path : test_paths)
        {
            DB::tests::TiFlashTestEnv::tryRemovePath(path);
            createIfNotExist(path);
        }
    }

    // Case 4, only recover blob 3
    {
        auto test_paths = delegator->listPaths();
        auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config);
        write_blob_datas(blob_store);

        ASSERT_TRUE(check_in_disk_file(test_paths, {1, 2, 3}));

        auto blob_store_check = BlobStore(getCurrentTestName(), file_provider, delegator, config);
        restore_blobs(blob_store_check, {3});

        blob_store_check.blob_stats.restore();

        ASSERT_TRUE(check_in_disk_file(test_paths, {3}));
        for (const auto & path : test_paths)
        {
            DB::tests::TiFlashTestEnv::tryRemovePath(path);
            createIfNotExist(path);
        }
    }

    // Case 5, recover a not exist blob
    {
        auto test_paths = delegator->listPaths();
        auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config);
        write_blob_datas(blob_store);

        ASSERT_TRUE(check_in_disk_file(test_paths, {1, 2, 3}));

        auto blob_store_check = BlobStore(getCurrentTestName(), file_provider, delegator, config);
        ASSERT_THROW(restore_blobs(blob_store_check, {4}), DB::Exception);
    }
}
CATCH

TEST_F(BlobStoreTest, testWriteRead)
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getContext().getFileProvider();

    PageId page_id = 50;
    size_t buff_nums = 21;
    size_t buff_size = 123;

    auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config);
    char c_buff[buff_size * buff_nums];

    WriteBatch wb;

    for (size_t i = 0; i < buff_nums; ++i)
    {
        for (size_t j = 0; j < buff_size; ++j)
        {
            c_buff[j + i * buff_size] = static_cast<char>((j & 0xff) + i);
        }

        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff + i * buff_size), buff_size);
        wb.putPage(page_id++, /* tag */ 0, buff, buff_size);
    }

    ASSERT_EQ(wb.getTotalDataSize(), buff_nums * buff_size);
    PageEntriesEdit edit = blob_store.write(wb, nullptr);
    ASSERT_EQ(edit.size(), buff_nums);

    char c_buff_read[buff_size * buff_nums];

    size_t index = 0;
    for (const auto & record : edit.getRecords())
    {
        ASSERT_EQ(record.type, EditRecordType::PUT);
        ASSERT_EQ(record.entry.offset, index * buff_size);
        ASSERT_EQ(record.entry.size, buff_size);
        ASSERT_EQ(record.entry.file_id, 1);

        // Read directly from the file
        blob_store.read(buildV3Id(TEST_NAMESPACE_ID, page_id),
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
    const auto file_provider = DB::tests::TiFlashTestEnv::getContext().getFileProvider();

    PageId page_id = 50;
    size_t wb_nums = 5;
    size_t buff_size = 10ul * 1024;
    const size_t rate_target = buff_size - 1;

    auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config);
    char c_buff[wb_nums * buff_size];

    WriteBatch wbs[wb_nums];
    PageEntriesEdit edits[wb_nums];

    for (size_t i = 0; i < wb_nums; ++i)
    {
        for (size_t j = 0; j < buff_size; ++j)
        {
            c_buff[j + i * buff_size] = static_cast<char>((j & 0xff) + i);
        }

        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff + i * buff_size), buff_size);
        wbs[i].putPage(page_id++, /* tag */ 0, buff, buff_size);
    }

    WriteLimiterPtr write_limiter = std::make_shared<WriteLimiter>(rate_target, LimiterType::UNKNOW, 20);

    AtomicStopwatch write_watch;
    for (size_t i = 0; i < wb_nums; ++i)
    {
        edits[i] = blob_store.write(wbs[i], write_limiter);
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
        ReadLimiterPtr read_limiter = std::make_shared<MockReadLimiter>(get_stat,
                                                                        rate_target,
                                                                        LimiterType::UNKNOW);

        AtomicStopwatch read_watch;
        for (size_t i = 0; i < wb_nums; ++i)
        {
            for (const auto & record : edits[i].getRecords())
            {
                blob_store.read(buildV3Id(TEST_NAMESPACE_ID, page_id),
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
        ReadLimiterPtr read_limiter = std::make_shared<MockReadLimiter>(get_stat,
                                                                        rate_target,
                                                                        LimiterType::UNKNOW);

        AtomicStopwatch read_watch;

        // Test `PageMap` read
        blob_store.read(entries, read_limiter);
        auto read_elapsed = read_watch.elapsedSeconds();
        auto read_actual_rate = read_limiter->getTotalBytesThrough() / read_elapsed;
        EXPECT_LE(read_actual_rate / rate_target, 1.30);
    }

    {
        ReadLimiterPtr read_limiter = std::make_shared<MockReadLimiter>(get_stat,
                                                                        rate_target,
                                                                        LimiterType::UNKNOW);

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
    const auto file_provider = DB::tests::TiFlashTestEnv::getContext().getFileProvider();

    PageId page_id1 = 50;
    PageId page_id2 = 51;
    PageId page_id3 = 53;

    size_t buff_size = 120;
    WriteBatch wb;

    auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config);
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
    PageEntriesEdit edit = blob_store.write(wb, nullptr);
    ASSERT_EQ(edit.size(), 3);

    BlobStore::FieldReadInfo read_info1(buildV3Id(TEST_NAMESPACE_ID, page_id1), edit.getRecords()[0].entry, {0, 1, 2, 3});
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
    const auto file_provider = DB::tests::TiFlashTestEnv::getContext().getFileProvider();

    PageId page_id = 50;
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

    auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config);
    char c_buff[buff_size * buff_nums];

    WriteBatch wb;

    for (size_t i = 0; i < buff_nums; ++i)
    {
        for (size_t j = 0; j < buff_size; ++j)
        {
            c_buff[j + i * buff_size] = static_cast<char>((j & 0xff) + i);
        }

        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff + i * buff_size), buff_size);
        wb.putPage(page_id++, /* tag */ 0, buff, buff_size, field_sizes);
    }

    ASSERT_EQ(wb.getTotalDataSize(), buff_nums * buff_size);
    PageEntriesEdit edit = blob_store.write(wb, nullptr);
    ASSERT_EQ(edit.size(), buff_nums);

    char c_buff_read[buff_size * buff_nums];

    size_t index = 0;
    for (const auto & record : edit.getRecords())
    {
        ASSERT_EQ(record.type, EditRecordType::PUT);
        ASSERT_EQ(record.entry.offset, index * buff_size);
        ASSERT_EQ(record.entry.size, buff_size);
        ASSERT_EQ(record.entry.file_id, 1);

        PageFieldSizes check_field_sizes;
        for (const auto & [field_offset, crc] : record.entry.field_offsets)
        {
            check_field_sizes.emplace_back(field_offset);
            ASSERT_TRUE(crc);
        }

        ASSERT_EQ(check_field_sizes, offsets);

        // Read
        blob_store.read(buildV3Id(TEST_NAMESPACE_ID, page_id),
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
    const auto file_provider = DB::tests::TiFlashTestEnv::getContext().getFileProvider();
    auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config);

    PageId page_id = 50;
    const size_t buff_size = 1024;
    WriteBatch wb;
    {
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

        PageEntriesEdit edit = blob_store.write(wb, nullptr);
        ASSERT_EQ(edit.size(), 2);

        auto records = edit.getRecords();
        auto record = records[0];

        ASSERT_EQ(record.type, EditRecordType::PUT);
        ASSERT_EQ(record.page_id.low, page_id);
        ASSERT_EQ(record.entry.offset, 0);
        ASSERT_EQ(record.entry.size, buff_size);
        ASSERT_EQ(record.entry.file_id, 1);

        record = records[1];
        ASSERT_EQ(record.type, EditRecordType::PUT);
        ASSERT_EQ(record.page_id.low, page_id);
        ASSERT_EQ(record.entry.offset, buff_size);
        ASSERT_EQ(record.entry.size, buff_size);
        ASSERT_EQ(record.entry.file_id, 1);
    }


    wb.clear();
    {
        wb.putRefPage(page_id + 1, page_id);
        wb.delPage(page_id + 1);
        wb.delPage(page_id);

        PageEntriesEdit edit = blob_store.write(wb, nullptr);
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

    wb.clear();
    {
        char c_buff[buff_size];

        for (size_t i = 0; i < buff_size; ++i)
        {
            c_buff[i] = i & 0xff;
        }

        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, buff_size);
        wb.putPage(page_id, /*tag*/ 0, buff, buff_size);
        wb.putRefPage(page_id + 1, page_id);
        wb.delPage(page_id);

        PageEntriesEdit edit = blob_store.write(wb, nullptr);
        auto records = edit.getRecords();

        auto record = records[0];
        ASSERT_EQ(record.type, EditRecordType::PUT);
        ASSERT_EQ(record.page_id.low, page_id);
        ASSERT_EQ(record.entry.offset, buff_size * 2);
        ASSERT_EQ(record.entry.size, buff_size);
        ASSERT_EQ(record.entry.file_id, 1);

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
    const auto file_provider = DB::tests::TiFlashTestEnv::getContext().getFileProvider();
    size_t buff_size = 100;

    {
        config.file_limit_size = buff_size - 1;
        auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config);

        WriteBatch wb;
        char c_buff[buff_size];
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff), buff_size);
        wb.putPage(50, /*tag*/ 0, buff, buff_size);

        bool catch_exception = false;
        try
        {
            blob_store.write(wb, nullptr);
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
        auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config);

        WriteBatch wb;
        char c_buff1[buf_size];
        char c_buff2[buf_size];

        ReadBufferPtr buff1 = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff1), buf_size);
        ReadBufferPtr buff2 = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff2), buf_size);

        wb.putPage(50, /*tag*/ 0, buff1, buf_size);

        auto edit = blob_store.write(wb, nullptr);
        ASSERT_EQ(edit.size(), 1);

        auto records = edit.getRecords();
        auto record = records[0];
        ASSERT_EQ(record.type, EditRecordType::PUT);
        ASSERT_EQ(record.page_id.low, 50);
        ASSERT_EQ(record.entry.offset, 0);
        ASSERT_EQ(record.entry.size, buf_size);
        ASSERT_EQ(record.entry.file_id, 1);

        wb.clear();
        wb.putPage(51, /*tag*/ 0, buff2, buf_size);
        edit = blob_store.write(wb, nullptr);
        ASSERT_EQ(edit.size(), 1);

        records = edit.getRecords();
        record = records[0];
        ASSERT_EQ(record.type, EditRecordType::PUT);
        ASSERT_EQ(record.page_id.low, 51);
        ASSERT_EQ(record.entry.offset, 0);
        ASSERT_EQ(record.entry.size, buf_size);
        ASSERT_EQ(record.entry.file_id, 2);
    }
}

TEST_F(BlobStoreTest, testBlobStoreGcStats)
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getContext().getFileProvider();
    size_t buff_size = 1024;
    size_t buff_nums = 10;
    PageId page_id = 50;
    auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config);
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
            ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff + i * buff_size), buff_size);
            wb.putPage(page_id, /* tag */ 0, buff, buff_size);
        }
    }

    auto edit = blob_store.write(wb, nullptr);

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
    ASSERT_EQ(entries_del1.begin()->file_id, 1);

    auto stat = blob_store.blob_stats.blobIdToStat(1);

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
    String path = blob_store.getBlobFile(1)->getPath();
    Poco::File blob_file_in_disk(path);
    ASSERT_EQ(blob_file_in_disk.getSize(), stat->sm_total_size);

    // Clear cache to reproduce https://github.com/pingcap/tiflash/issues/5532
    blob_store.cached_files.reset();
    // Check whether the stat can be totally removed
    stat->changeToReadOnly();
    blob_store.remove(remain_entries);
    ASSERT_EQ(getTotalStatsNum(blob_store.blob_stats.getStats()), 0);
}

TEST_F(BlobStoreTest, testBlobStoreGcStats2)
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getContext().getFileProvider();
    size_t buff_size = 1024;
    size_t buff_nums = 10;
    PageId page_id = 50;
    auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config);
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
            ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff + i * buff_size), buff_size);
            wb.putPage(page_id, /* tag */ 0, buff, buff_size);
        }
    }

    auto edit = blob_store.write(wb, nullptr);

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

    auto stat = blob_store.blob_stats.blobIdToStat(1);

    const auto & gc_stats = blob_store.getGCStats();
    ASSERT_FALSE(gc_stats.empty());

    ASSERT_EQ(stat->sm_valid_rate, 0.2);
    ASSERT_EQ(stat->sm_total_size, buff_size * buff_nums);
    ASSERT_EQ(stat->sm_valid_size, buff_size * 2);

    // Then we must do heavy GC
    ASSERT_EQ(*gc_stats.begin(), 1);
}


TEST_F(BlobStoreTest, GC)
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getContext().getFileProvider();
    PageId page_id = 50;
    size_t buff_nums = 21;
    size_t buff_size = 123;

    auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config);
    char c_buff[buff_size * buff_nums];

    WriteBatch wb;

    for (size_t i = 0; i < buff_nums; ++i)
    {
        for (size_t j = 0; j < buff_size; ++j)
        {
            c_buff[j + i * buff_size] = static_cast<char>((j & 0xff) + i);
        }

        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff + i * buff_size), buff_size);
        wb.putPage(page_id++, /* tag */ 0, buff, buff_size);
    }

    ASSERT_EQ(wb.getTotalDataSize(), buff_nums * buff_size);
    PageEntriesEdit edit = blob_store.write(wb, nullptr);
    ASSERT_EQ(edit.size(), buff_nums);

    PageIdAndVersionedEntries versioned_pageid_entries;
    for (const auto & record : edit.getRecords())
    {
        versioned_pageid_entries.emplace_back(buildV3Id(TEST_NAMESPACE_ID, page_id), 1, record.entry);
    }
    std::map<BlobFileId, PageIdAndVersionedEntries> gc_context;
    gc_context[1] = versioned_pageid_entries;

    // Before we do BlobStore we need change BlobFile0 to Read-Only
    auto stat = blob_store.blob_stats.blobIdToStat(1);
    stat->changeToReadOnly();

    const auto & gc_edit = blob_store.gc(gc_context, static_cast<PageSize>(buff_size * buff_nums));

    // Check copy_list which will apply for Mvcc
    ASSERT_EQ(gc_edit.size(), buff_nums);
    auto it = versioned_pageid_entries.begin();
    for (const auto & record : gc_edit.getRecords())
    {
        ASSERT_EQ(record.page_id.low, page_id);
        auto it_entry = std::get<2>(*it);
        ASSERT_EQ(record.entry.file_id, 2);
        ASSERT_EQ(record.entry.checksum, it_entry.checksum);
        ASSERT_EQ(record.entry.size, it_entry.size);
        it++;
    }

    // Check blobfile1
    Poco::File file1(blob_store.getBlobFile(1)->getPath());
    Poco::File file2(blob_store.getBlobFile(2)->getPath());
    ASSERT_TRUE(file1.exists());
    ASSERT_TRUE(file2.exists());
    ASSERT_EQ(file1.getSize(), file2.getSize());
}


TEST_F(BlobStoreTest, GCMigirateBigData)
try
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getContext().getFileProvider();
    PageId fixed_page_id = 50;
    PageId page_id = fixed_page_id;
    size_t buff_nums = 20;
    size_t buff_size = 20;

    BlobStore::Config config_with_small_file_limit_size;
    config_with_small_file_limit_size.file_limit_size = 100;
    auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config_with_small_file_limit_size);
    char c_buff[buff_size * buff_nums];

    WriteBatch wb;

    std::map<BlobFileId, PageIdAndVersionedEntries> gc_context;

    for (size_t i = 0; i < buff_nums; ++i)
    {
        for (size_t j = 0; j < buff_size; ++j)
        {
            c_buff[j + i * buff_size] = static_cast<char>((j & 0xff) + i);
        }

        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff + i * buff_size), buff_size);
        wb.putPage(page_id, /* tag */ 0, buff, buff_size);
        PageEntriesEdit edit = blob_store.write(wb, nullptr);

        const auto & records = edit.getRecords();
        ASSERT_EQ(records.size(), 1);
        if (gc_context.find(records[0].entry.file_id) == gc_context.end())
        {
            PageIdAndVersionedEntries versioned_pageid_entries;
            versioned_pageid_entries.emplace_back(page_id, 1, records[0].entry);
            gc_context[records[0].entry.file_id] = std::move(versioned_pageid_entries);
        }
        else
        {
            gc_context[records[0].entry.file_id].emplace_back(page_id, 1, records[0].entry);
        }

        page_id++;
        wb.clear();
    }

    const auto & edit = blob_store.gc(gc_context, static_cast<PageSize>(buff_size * buff_nums));
    ASSERT_EQ(edit.size(), buff_nums);
}
CATCH

TEST_F(BlobStoreTest, ReadByFieldReadInfos)
try
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getContext().getFileProvider();
    PageId fixed_page_id = 50;
    PageId page_id = fixed_page_id;
    size_t buff_nums = 20;
    size_t buff_size = 20;

    BlobStore::Config config_with_small_file_limit_size;
    config_with_small_file_limit_size.file_limit_size = 100;
    auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config_with_small_file_limit_size);
    char c_buff[buff_size * buff_nums];

    WriteBatch wb;

    BlobStore::FieldReadInfos read_infos;
    for (size_t i = 0; i < buff_nums; ++i)
    {
        for (size_t j = 0; j < buff_size; ++j)
        {
            c_buff[j + i * buff_size] = static_cast<char>(buildV3Id(TEST_NAMESPACE_ID, page_id + j));
        }

        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff + i * buff_size), buff_size);
        PageFieldSizes field_sizes{1, 2, 4, 8, (buff_size - 1 - 2 - 4 - 8)};
        wb.putPage(page_id, /* tag */ 0, buff, buff_size, field_sizes);
        PageEntriesEdit edit = blob_store.write(wb, nullptr);

        const auto & records = edit.getRecords();
        ASSERT_EQ(records.size(), 1);
        read_infos.emplace_back(BlobStore::FieldReadInfo(buildV3Id(TEST_NAMESPACE_ID, page_id), records[0].entry, {0, 1, 2, 3, 4}));

        page_id++;
        wb.clear();
    }

    auto page_map = blob_store.read(read_infos);
    for (size_t i = 0; i < buff_nums; ++i)
    {
        PageId reading_id = fixed_page_id + i;
        Page page = page_map[reading_id];
        ASSERT_EQ(page.fieldSize(), 5);
    }
}
CATCH

TEST_F(BlobStoreTest, TestBigBlob)
try
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getContext().getFileProvider();
    PageId fixed_page_id = 50;
    PageId page_id = fixed_page_id;

    BlobStore::Config config_with_small_file_limit_size;
    config_with_small_file_limit_size.file_limit_size = 400;
    auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config_with_small_file_limit_size);

    // PUT page_id 50 into blob 1 range [0,200]
    {
        size_t size_200 = 200;
        char c_buff[size_200];

        WriteBatch wb;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff), size_200);
        wb.putPage(page_id, /* tag */ 0, buff, size_200);
        PageEntriesEdit edit = blob_store.write(wb, nullptr);

        const auto & records = edit.getRecords();
        ASSERT_EQ(records.size(), 1);
        ASSERT_EQ(records[0].entry.file_id, 1);
        ASSERT_EQ(records[0].entry.offset, 0);
        ASSERT_EQ(records[0].entry.size, 200);

        const auto & stat = blob_store.blob_stats.blobIdToStat(1);
        ASSERT_TRUE(stat->isNormal());
        ASSERT_EQ(stat->sm_max_caps, 200);
        ASSERT_DOUBLE_EQ(stat->sm_valid_rate, 1.0);
        ASSERT_EQ(stat->sm_valid_size, 200);
        ASSERT_EQ(stat->sm_total_size, 200);

        page_id++;
        wb.clear();
    }

    // PUT page_id 51 into blob 2 range [0,500] , stat will be a BIG_BLOB in mem
    {
        size_t size_500 = 500;
        char c_buff[size_500];

        WriteBatch wb;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff), size_500);
        wb.putPage(page_id, /* tag */ 0, buff, size_500);
        PageEntriesEdit edit = blob_store.write(wb, nullptr);

        const auto & records = edit.getRecords();
        ASSERT_EQ(records.size(), 1);
        ASSERT_EQ(records[0].entry.file_id, 2);
        ASSERT_EQ(records[0].entry.offset, 0);
        ASSERT_EQ(records[0].entry.size, 500);

        // verify blobstat
        const auto & stat = blob_store.blob_stats.blobIdToStat(2);
        ASSERT_TRUE(stat->isBigBlob());
        ASSERT_EQ(stat->sm_max_caps, 0);
        ASSERT_DOUBLE_EQ(stat->sm_valid_rate, 1.0);
        ASSERT_EQ(stat->sm_valid_size, 500);
        ASSERT_EQ(stat->sm_total_size, 500);

        // Verify read
        Page page = blob_store.read(std::make_pair(buildV3Id(TEST_NAMESPACE_ID, page_id), records[0].entry), nullptr);
        ASSERT_TRUE(page.isValid());
        ASSERT_EQ(page.data.size(), size_500);

        page_id++;
        wb.clear();
    }

    // PUT page_id 52 into blob 1 range [200,100]
    {
        size_t size_100 = 100;
        char c_buff[size_100];

        WriteBatch wb;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff), size_100);
        wb.putPage(page_id, /* tag */ 0, buff, size_100);
        PageEntriesEdit edit = blob_store.write(wb, nullptr);

        const auto & records = edit.getRecords();
        ASSERT_EQ(records.size(), 1);
        ASSERT_EQ(records[0].entry.file_id, 1);
        ASSERT_EQ(records[0].entry.offset, 200);
        ASSERT_EQ(records[0].entry.size, 100);

        const auto & stat = blob_store.blob_stats.blobIdToStat(1);
        ASSERT_TRUE(stat->isNormal());
        ASSERT_EQ(stat->sm_max_caps, 100);
        ASSERT_DOUBLE_EQ(stat->sm_valid_rate, 1.0);
        ASSERT_EQ(stat->sm_valid_size, 300);
        ASSERT_EQ(stat->sm_total_size, 300);

        page_id++;
        wb.clear();
    }

    // PUT page_id 53 into blob 3 range [0,300]
    {
        size_t size_300 = 300;
        char c_buff[size_300];

        WriteBatch wb;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff), size_300);
        wb.putPage(page_id, /* tag */ 0, buff, size_300);
        PageEntriesEdit edit = blob_store.write(wb, nullptr);

        const auto & records = edit.getRecords();
        ASSERT_EQ(records.size(), 1);
        ASSERT_EQ(records[0].entry.file_id, 3);
        ASSERT_EQ(records[0].entry.offset, 0);
        ASSERT_EQ(records[0].entry.size, 300);

        const auto & stat = blob_store.blob_stats.blobIdToStat(3);
        ASSERT_TRUE(stat->isNormal());
        ASSERT_EQ(stat->sm_max_caps, 100);
        ASSERT_DOUBLE_EQ(stat->sm_valid_rate, 1.0);
        ASSERT_EQ(stat->sm_valid_size, 300);
        ASSERT_EQ(stat->sm_total_size, 300);

        page_id++;
    }

    // Test mix BigBlob
    {
        char c_buff1[600];
        char c_buff2[10];
        char c_buff3[500];
        char c_buff4[200];

        WriteBatch wb;
        wb.putPage(page_id++, /* tag */ 0, std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff1), sizeof(c_buff1)), sizeof(c_buff1));
        wb.putPage(page_id++, /* tag */ 0, std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff2), sizeof(c_buff2)), sizeof(c_buff2));
        wb.putPage(page_id++, /* tag */ 0, std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff3), sizeof(c_buff3)), sizeof(c_buff3));
        wb.putPage(page_id++, /* tag */ 0, std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff4), sizeof(c_buff4)), sizeof(c_buff4));

        PageEntriesEdit edit = blob_store.write(wb, nullptr);

        const auto & records = edit.getRecords();
        ASSERT_EQ(records.size(), 4);

        // PUT page_id 54 into blob 4 range [0,600]
        ASSERT_EQ(records[0].page_id.low, 54);
        ASSERT_EQ(records[0].entry.file_id, 4);
        ASSERT_EQ(records[0].entry.offset, 0);
        ASSERT_EQ(records[0].entry.size, 600);

        // PUT page_id 55 into blob 1 or 3
        ASSERT_EQ(records[1].page_id.low, 55);
        ASSERT_TRUE(records[1].entry.file_id == 1 || records[1].entry.file_id == 3);

        // PUT page_id 56 into blob 5 range [0,600]
        ASSERT_EQ(records[2].page_id.low, 56);
        ASSERT_EQ(records[2].entry.file_id, 5);
        ASSERT_EQ(records[2].entry.offset, 0);
        ASSERT_EQ(records[2].entry.size, 500);

        // PUT page_id 57 into blob 6 range [0,200]
        ASSERT_EQ(records[3].page_id.low, 57);
        ASSERT_EQ(records[3].entry.file_id, 6);
        ASSERT_EQ(records[3].entry.offset, 0);
        ASSERT_EQ(records[3].entry.size, 200);
    }
}
CATCH


TEST_F(BlobStoreTest, TestBigBlobRemove)
try
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getContext().getFileProvider();
    PageId fixed_page_id = 50;
    PageId page_id = fixed_page_id;

    BlobStore::Config config_with_small_file_limit_size;
    config_with_small_file_limit_size.file_limit_size = 400;
    auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config_with_small_file_limit_size);

    {
        size_t size_500 = 500;
        char c_buff[size_500];

        WriteBatch wb;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff), size_500);
        wb.putPage(page_id, /* tag */ 0, buff, size_500);
        PageEntriesEdit edit = blob_store.write(wb, nullptr);

        const auto & gc_info = blob_store.getGCStats();
        ASSERT_TRUE(gc_info.empty());

        const auto & records = edit.getRecords();
        ASSERT_EQ(records.size(), 1);
        blob_store.remove({records[0].entry});
    }
}
CATCH


TEST_F(BlobStoreTest, TestBigBlobRegisterPath)
try
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getContext().getFileProvider();
    PageId fixed_page_id = 50;
    PageId page_id = fixed_page_id;

    BlobStore::Config config_with_small_file_limit_size;
    config_with_small_file_limit_size.file_limit_size = 400;

    PageEntryV3 entry_from_write;
    {
        auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config_with_small_file_limit_size);
        size_t size_500 = 500;
        char c_buff[size_500];

        WriteBatch wb;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff), size_500);
        wb.putPage(page_id, /* tag */ 0, buff, size_500);
        auto edit = blob_store.write(wb, nullptr);
        const auto & records = edit.getRecords();
        ASSERT_EQ(records.size(), 1);
        entry_from_write = records[0].entry;
    }

    {
        auto blob_store = BlobStore(getCurrentTestName(), file_provider, delegator, config_with_small_file_limit_size);
        blob_store.registerPaths();

        const auto & stat = blob_store.blob_stats.blobIdToStat(1);
        ASSERT_TRUE(stat->isBigBlob());

        blob_store.blob_stats.restoreByEntry(entry_from_write);
        blob_store.blob_stats.restore();
        ASSERT_EQ(stat->sm_max_caps, 0);
        ASSERT_DOUBLE_EQ(stat->sm_valid_rate, 1.0);
        ASSERT_EQ(stat->sm_valid_size, 500);
        ASSERT_EQ(stat->sm_total_size, 500);
    }
}
CATCH

} // namespace DB::PS::V3::tests
