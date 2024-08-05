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
#include <Poco/Logger.h>
#include <Storages/Page/V3/Blob/BlobStat.h>
#include <Storages/Page/V3/PageDefines.h>
#include <TestUtils/MockDiskDelegator.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB::PS::V3::tests
{


class BlobStoreStatsTest : public DB::base::TiFlashStorageTestBasic
{
public:
    static constexpr size_t path_num = 3;

public:
    BlobStoreStatsTest()
        : logger(Logger::get())
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
    BlobConfig config;
    LoggerPtr logger;
    PSDiskDelegatorPtr delegator;
};

static size_t getTotalStatsNum(const BlobStats::StatsMap & stats_map)
{
    size_t total_stats_num = 0;
    for (const auto & iter : stats_map)
    {
        total_stats_num += iter.second.size();
    }
    return total_stats_num;
}

TEST_F(BlobStoreStatsTest, RestoreEmpty)
{
    BlobStats stats(logger, delegator, config);

    stats.restore();

    auto stats_copy = stats.getStats();
    ASSERT_TRUE(stats_copy.empty());

    EXPECT_EQ(stats.cur_max_id, 1);
    EXPECT_NO_THROW(stats.createStat(stats.cur_max_id, config.file_limit_size, stats.lock()));
}

TEST_F(BlobStoreStatsTest, Restore)
try
{
    BlobStats stats(logger, delegator, config);

    BlobFileId file_id1 = 1;
    BlobFileId file_id2 = PageTypeUtils::nextFileID(PageType::Normal, file_id1);
    BlobFileId file_id3 = PageTypeUtils::nextFileID(PageType::RaftData, file_id2);
    BlobFileId file_id4 = PageTypeUtils::nextFileID(PageType::Normal, file_id3);
    ASSERT_EQ(file_id2, 10);
    ASSERT_EQ(file_id3, 21);
    ASSERT_EQ(file_id4, 30);

    {
        const auto & lock = stats.lock();
        stats.createStatNotChecking(file_id1, config.file_limit_size, lock);
        stats.createStatNotChecking(file_id2, config.file_limit_size, lock);
        stats.createStatNotChecking(file_id3, config.file_limit_size, lock);
        stats.createStatNotChecking(file_id4, config.file_limit_size, lock);
    }

    {
        stats.restoreByEntry(PageEntryV3{
            .file_id = file_id1,
            .size = 128,
            .padded_size = 0,
            .tag = 0,
            .offset = 1024,
            .checksum = 0x4567,
        });
        stats.restoreByEntry(PageEntryV3{
            .file_id = file_id1,
            .size = 512,
            .padded_size = 0,
            .tag = 0,
            .offset = 2048,
            .checksum = 0x4567,
        });
        stats.restoreByEntry(PageEntryV3{
            .file_id = file_id2,
            .size = 512,
            .padded_size = 0,
            .tag = 0,
            .offset = 2048,
            .checksum = 0x4567,
        });
        stats.restoreByEntry(PageEntryV3{
            .file_id = file_id3,
            .size = 512,
            .padded_size = 0,
            .tag = 0,
            .offset = 2048,
            .checksum = 0x4567,
        });
        stats.restoreByEntry(PageEntryV3{
            .file_id = file_id4,
            .size = 512,
            .padded_size = 0,
            .tag = 0,
            .offset = 2048,
            .checksum = 0x4567,
        });
        stats.restore();
    }

    auto stats_copy = stats.getStats();

    ASSERT_EQ(stats_copy.size(), std::min(getTotalStatsNum(stats_copy), path_num));
    ASSERT_EQ(getTotalStatsNum(stats_copy), 4);
    EXPECT_EQ(stats.cur_max_id, file_id4);


    auto stat1 = stats.blobIdToStat(file_id1);
    EXPECT_EQ(stat1->sm_total_size, 2048 + 512);
    EXPECT_EQ(stat1->sm_valid_size, 128 + 512);
    auto stat2 = stats.blobIdToStat(file_id2);
    EXPECT_EQ(stat2->sm_total_size, 2048 + 512);
    EXPECT_EQ(stat2->sm_valid_size, 512);
    auto stat3 = stats.blobIdToStat(file_id3);
    EXPECT_EQ(stat2->sm_total_size, 2048 + 512);
    EXPECT_EQ(stat2->sm_valid_size, 512);
    auto stat4 = stats.blobIdToStat(file_id4);
    EXPECT_EQ(stat2->sm_total_size, 2048 + 512);
    EXPECT_EQ(stat2->sm_valid_size, 512);

    EXPECT_ANY_THROW({ stats.createStat(file_id1, config.file_limit_size, stats.lock()); });
    EXPECT_ANY_THROW({ stats.createStat(file_id2, config.file_limit_size, stats.lock()); });
    EXPECT_ANY_THROW({ stats.createStat(file_id3, config.file_limit_size, stats.lock()); });
    EXPECT_ANY_THROW({ stats.createStat(file_id4, config.file_limit_size, stats.lock()); });
}
CATCH

TEST_F(BlobStoreStatsTest, RestoreWithEmptyPageSamePosition)
try
{
    BlobStats stats(logger, delegator, config);

    BlobFileId file_id1 = 11;

    {
        const auto & lock = stats.lock();
        stats.createStatNotChecking(file_id1, config.file_limit_size, lock);
    }

    {
        // one entry before
        stats.restoreByEntry(PageEntryV3{
            .file_id = file_id1,
            .size = 1024,
            .padded_size = 0,
            .tag = 0,
            .offset = 1024,
            .checksum = 0x4567,
        });
        // the entry with the same position
        stats.restoreByEntry(PageEntryV3{
            .file_id = file_id1,
            .size = 512,
            .padded_size = 0,
            .tag = 0,
            .offset = 2048,
            .checksum = 0x4567,
        });
        stats.restoreByEntry(PageEntryV3{
            .file_id = file_id1,
            .size = 0, // empty
            .padded_size = 0,
            .tag = 0,
            .offset = 2048, // an empty page shared the same position
            .checksum = 0x4567,
        });
        stats.restore();
    }

    auto stats_copy = stats.getStats();

    ASSERT_EQ(stats_copy.size(), std::min(getTotalStatsNum(stats_copy), path_num));
    ASSERT_EQ(getTotalStatsNum(stats_copy), 1);
    EXPECT_EQ(stats.cur_max_id, file_id1);

    auto stat = stats.blobIdToStat(file_id1);
    EXPECT_EQ(stat->sm_total_size, 2048 + 512);
    EXPECT_EQ(stat->sm_valid_size, 1024 + 512);

    EXPECT_ANY_THROW({ stats.createStat(file_id1, config.file_limit_size, stats.lock()); });
}
CATCH

TEST_F(BlobStoreStatsTest, RestoreWithEmptyPageSplitSpace)
try
{
    BlobStats stats(logger, delegator, config);

    BlobFileId file_id1 = 11;

    {
        std::lock_guard lock(stats.lock_stats);
        stats.createStatNotChecking(file_id1, config.file_limit_size, lock);
    }

    {
        // an entry at offset=0x15376, size=0
        stats.restoreByEntry(PageEntryV3{
            .file_id = file_id1,
            .size = 0,
            .padded_size = 0,
            .tag = 0,
            .offset = 0x15376,
            .checksum = 0x4567,
        });
        // an entry at offset=0x15373, size=15. offset+size > 0x15376, but should be able to insert
        stats.restoreByEntry(PageEntryV3{
            .file_id = file_id1,
            .size = 15,
            .padded_size = 0,
            .tag = 0,
            .offset = 0x15373,
            .checksum = 0x4567,
        });
        stats.restore();
    }

    auto stats_copy = stats.getStats();

    ASSERT_EQ(stats_copy.size(), std::min(getTotalStatsNum(stats_copy), path_num));
    ASSERT_EQ(getTotalStatsNum(stats_copy), 1);
    EXPECT_EQ(stats.cur_max_id, file_id1);

    auto stat = stats.blobIdToStat(file_id1);
    EXPECT_EQ(stat->sm_total_size, 0x15373 + 15);
    EXPECT_EQ(stat->sm_valid_size, 15);

    EXPECT_ANY_THROW({ stats.createStat(file_id1, config.file_limit_size, stats.lock()); });
}
CATCH

TEST_F(BlobStoreStatsTest, testStats)
{
    BlobStats stats(logger, delegator, config);
    BlobFileId file_id0 = 10;
    auto stat = stats.createStat(file_id0, config.file_limit_size, stats.lock());

    ASSERT_TRUE(stat);
    ASSERT_TRUE(stat->smap);
    BlobFileId file_id1 = PageTypeUtils::nextFileID(PageType::Normal, file_id0);
    BlobFileId file_id2 = PageTypeUtils::nextFileID(PageType::Normal, file_id1);
    {
        auto lock = stats.lock();
        stats.createStat(file_id1, config.file_limit_size, lock);
        stats.createStat(file_id2, config.file_limit_size, lock);
    }


    auto stats_copy = stats.getStats();

    ASSERT_EQ(stats_copy.size(), std::min(getTotalStatsNum(stats_copy), path_num));
    ASSERT_EQ(getTotalStatsNum(stats_copy), 3);

    stats.eraseStat(10, stats.lock());
    stats.eraseStat(20, stats.lock());
    ASSERT_EQ(getTotalStatsNum(stats.getStats()), 1);
}


TEST_F(BlobStoreStatsTest, testStat)
{
    BlobFileId blob_file_id = 0;
    BlobStats::BlobStatPtr stat;

    BlobStats stats(logger, delegator, config);

    std::tie(stat, blob_file_id) = stats.chooseStat(10, PageType::Normal, stats.lock());
    ASSERT_EQ(blob_file_id, 10);
    ASSERT_EQ(stat, nullptr);

    std::tie(stat, blob_file_id) = stats.chooseStat(10, PageType::Normal, stats.lock());
    ASSERT_EQ(blob_file_id, 20);
    ASSERT_EQ(stat, nullptr);

    stats.createStat(0, config.file_limit_size, stats.lock());
    std::tie(stat, blob_file_id) = stats.chooseStat(10, PageType::Normal, stats.lock());
    ASSERT_EQ(blob_file_id, INVALID_BLOBFILE_ID);
    ASSERT_NE(stat, nullptr);

    // PageType::RaftData should not use the same stat with PageType::Normal
    BlobStats::BlobStatPtr raft_stat;
    std::tie(raft_stat, blob_file_id) = stats.chooseStat(10, PageType::RaftData, stats.lock());
    ASSERT_EQ(blob_file_id, 31);
    ASSERT_FALSE(raft_stat);


    auto offset = stat->getPosFromStat(10, stat->lock());
    ASSERT_EQ(offset, 0);

    offset = stat->getPosFromStat(100, stat->lock());
    ASSERT_EQ(offset, 10);

    offset = stat->getPosFromStat(20, stat->lock());
    ASSERT_EQ(offset, 110);

    ASSERT_EQ(stat->sm_total_size, 10 + 100 + 20);
    ASSERT_EQ(stat->sm_valid_size, 10 + 100 + 20);
    ASSERT_EQ(stat->sm_valid_rate, 1);

    stat->removePosFromStat(10, 100, stat->lock());
    ASSERT_EQ(stat->sm_total_size, 10 + 100 + 20);
    ASSERT_EQ(stat->sm_valid_size, 10 + 20);
    ASSERT_LE(stat->sm_valid_rate, 1);

    offset = stat->getPosFromStat(110, stat->lock());
    ASSERT_EQ(offset, 130);
    ASSERT_EQ(stat->sm_total_size, 10 + 100 + 20 + 110);
    ASSERT_EQ(stat->sm_valid_size, 10 + 20 + 110);
    ASSERT_LE(stat->sm_valid_rate, 1);

    offset = stat->getPosFromStat(90, stat->lock());
    ASSERT_EQ(offset, 10);
    ASSERT_EQ(stat->sm_total_size, 10 + 100 + 20 + 110);
    ASSERT_EQ(stat->sm_valid_size, 10 + 20 + 110 + 90);
    ASSERT_LE(stat->sm_valid_rate, 1);

    // Unmark the last range
    stat->removePosFromStat(130, 110, stat->lock());
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
    offset = stat->getPosFromStat(120, stat->lock());
    ASSERT_EQ(offset, 130);
    ASSERT_EQ(stat->sm_total_size, 10 + 100 + 20 + 110 + 10);
    ASSERT_EQ(stat->sm_valid_size, 10 + 20 + 90 + 120);
    ASSERT_LE(stat->sm_valid_rate, 1);
}

TEST_F(BlobStoreStatsTest, StatWithEmptyBlob)
{
    BlobFileId blob_file_id = 0;
    BlobStats::BlobStatPtr stat;

    BlobStats stats(logger, delegator, config);

    stats.createStat(0, config.file_limit_size, stats.lock());
    std::tie(stat, blob_file_id) = stats.chooseStat(10, PageType::Normal, stats.lock());
    ASSERT_EQ(blob_file_id, INVALID_BLOBFILE_ID);
    ASSERT_NE(stat, nullptr);

    auto offset = stat->getPosFromStat(10, stat->lock());
    ASSERT_EQ(offset, 0);

    offset = stat->getPosFromStat(0, stat->lock()); // empty
    ASSERT_EQ(offset, 0); // empty page always "stored" to the beginning of the space
    offset = stat->getPosFromStat(0, stat->lock()); // empty
    ASSERT_EQ(offset, 0); // empty page always "stored" to the beginning of the space
    offset = stat->getPosFromStat(0, stat->lock()); // empty
    ASSERT_EQ(offset, 0); // empty page always "stored" to the beginning of the space

    offset = stat->getPosFromStat(20, stat->lock());
    ASSERT_EQ(offset, 10);

    offset = stat->getPosFromStat(100, stat->lock());
    ASSERT_EQ(offset, 30);

    offset = stat->getPosFromStat(0, stat->lock()); // empty
    ASSERT_EQ(offset, 0); // empty page always "stored" to the beginning of the space

    ASSERT_EQ(stat->sm_total_size, 10 + 20 + 100);
    ASSERT_EQ(stat->sm_valid_size, 10 + 20 + 100);
    ASSERT_EQ(stat->sm_valid_rate, 1);

    stat->removePosFromStat(10, 0, stat->lock());
    ASSERT_EQ(stat->sm_total_size, 10 + 20 + 100);
    ASSERT_EQ(stat->sm_valid_size, 10 + 20 + 100);
    ASSERT_EQ(stat->sm_valid_rate, 1.0);

    stat->removePosFromStat(10, 20, stat->lock());
    ASSERT_EQ(stat->sm_total_size, 10 + 20 + 100);
    ASSERT_EQ(stat->sm_valid_size, 10 + 100);
    ASSERT_LE(stat->sm_valid_rate, 1);
}

TEST_F(BlobStoreStatsTest, testFullStats)
{
    BlobStats stats(logger, delegator, config);

    {
<<<<<<< HEAD
        auto lock = stats.lock();
=======
>>>>>>> dc20fe919f (PageStorage: Fix empty page cause TiFlash failed to start (#9283))
        BlobFileId file_id = 10;
        BlobStats::BlobStatPtr stat = stats.createStat(file_id, config.file_limit_size, stats.lock());
        auto offset = stat->getPosFromStat(BLOBFILE_LIMIT_SIZE - 1, stat->lock());
        ASSERT_EQ(offset, 0);
        stats.cur_max_id = file_id;

        // Can't get pos from a full stat
        offset = stat->getPosFromStat(100, stat->lock());
        ASSERT_EQ(offset, INVALID_BLOBFILE_OFFSET);

        // Stat internal property should not changed
        ASSERT_EQ(stat->sm_total_size, BLOBFILE_LIMIT_SIZE - 1);
        ASSERT_EQ(stat->sm_valid_size, BLOBFILE_LIMIT_SIZE - 1);
        ASSERT_LE(stat->sm_valid_rate, 1);
    }

    // Won't choose full one
    {
        auto [stat, blob_file_id] = stats.chooseStat(100, PageType::Normal, stats.lock());
        ASSERT_EQ(blob_file_id, 20);
        ASSERT_FALSE(stat);
    }

    // A new stat can use
    {
        stats.cur_max_id = PageTypeUtils::nextFileID(PageType::Normal, stats.cur_max_id);
        ASSERT_EQ(stats.cur_max_id, 30);
        auto stat = stats.createStat(stats.cur_max_id, config.file_limit_size, stats.lock());
        ASSERT_EQ(stat->getPosFromStat(100, stat->lock()), 0);

        // Then full the stat which id 2
        auto offset = stat->getPosFromStat(BLOBFILE_LIMIT_SIZE - 100, stat->lock());
        ASSERT_EQ(offset, 100);
    }

    {
        // Then choose stat, it should return a new blob_file_id
        auto [stat, blob_file_id] = stats.chooseStat(100, PageType::Normal, stats.lock());
        ASSERT_EQ(blob_file_id, 40);
        ASSERT_FALSE(stat);
    }
}
} // namespace DB::PS::V3::tests
