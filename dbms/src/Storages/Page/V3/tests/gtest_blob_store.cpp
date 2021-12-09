#include <IO/ReadBufferFromMemory.h>
#include <Poco/Logger.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB::PS::V3::tests
{
using BlobStat = BlobStore::BlobStats::BlobStat;
using BlobStats = BlobStore::BlobStats;

TEST(BlobStatsTest, testStats)
{
    BlobStats stats(&Poco::Logger::get("BlobStatsTest"));

    auto stat = stats.createStat(0);

    ASSERT_TRUE(stat);
    ASSERT_TRUE(stat->smap);
    stats.createStat(1);
    stats.createStat(2);

    ASSERT_EQ(stats.stats_map.size(), 3);
    ASSERT_EQ(stats.roll_id, 3);

    stats.earseStat(0);
    stats.earseStat(1);
    ASSERT_EQ(stats.stats_map.size(), 1);
    ASSERT_EQ(stats.roll_id, 3);
    ASSERT_EQ(stats.old_ids.size(), 2);

    auto old_it = stats.old_ids.begin();

    ASSERT_EQ((*old_it++), 0);
    ASSERT_EQ((*old_it++), 1);
    ASSERT_EQ(old_it, stats.old_ids.end());
}


TEST(BlobStatsTest, testStat)
{
    UInt16 blob_file_id = 0;
    BlobStore::BlobStats::BlobStatPtr stat;

    BlobStats stats(&Poco::Logger::get("BlobStatsTest"));

    std::tie(stat, blob_file_id) = stats.chooseStat(10);
    ASSERT_EQ(blob_file_id, 0);
    ASSERT_FALSE(stat);

    // still 0
    std::tie(stat, blob_file_id) = stats.chooseStat(10);
    ASSERT_EQ(blob_file_id, 0);
    ASSERT_FALSE(stat);

    stats.createStat(0);
    std::tie(stat, blob_file_id) = stats.chooseStat(10);
    ASSERT_EQ(blob_file_id, UINT16_MAX);
    ASSERT_TRUE(stat);

    auto offset = stats.getPosFromStat(stat, 10);
    ASSERT_EQ(offset, 0);

    offset = stats.getPosFromStat(stat, 100);
    ASSERT_EQ(offset, 10);

    offset = stats.getPosFromStat(stat, 20);
    ASSERT_EQ(offset, 110);

    ASSERT_EQ(stat->sm_total_size, 10 + 100 + 20);
    ASSERT_EQ(stat->sm_valid_size, 10 + 100 + 20);
    ASSERT_EQ(stat->sm_valid_rate, 1);

    stats.removePosFromStat(stat, 10, 100);
    ASSERT_EQ(stat->sm_total_size, 10 + 100 + 20);
    ASSERT_EQ(stat->sm_valid_size, 10 + 20);
    ASSERT_LE(stat->sm_valid_rate, 1);

    offset = stats.getPosFromStat(stat, 110);
    ASSERT_EQ(offset, 130);
    ASSERT_EQ(stat->sm_total_size, 10 + 100 + 20 + 110);
    ASSERT_EQ(stat->sm_valid_size, 10 + 20 + 110);
    ASSERT_LE(stat->sm_valid_rate, 1);

    offset = stats.getPosFromStat(stat, 90);
    ASSERT_EQ(offset, 10);
    ASSERT_EQ(stat->sm_total_size, 10 + 100 + 20 + 110);
    ASSERT_EQ(stat->sm_valid_size, 10 + 20 + 110 + 90);
    ASSERT_LE(stat->sm_valid_rate, 1);

    // Unmark the last range
    stats.removePosFromStat(stat, 130, 110);
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
    offset = stats.getPosFromStat(stat, 120);
    ASSERT_EQ(offset, 130);
    ASSERT_EQ(stat->sm_total_size, 10 + 100 + 20 + 110 + 10);
    ASSERT_EQ(stat->sm_valid_size, 10 + 20 + 90 + 120);
    ASSERT_LE(stat->sm_valid_rate, 1);
}

TEST(BlobStatsTest, testFullStats)
{
    UInt16 blob_file_id = 0;
    BlobStore::BlobStats::BlobStatPtr stat;
    UInt64 offset = 0;

    BlobStats stats(&Poco::Logger::get("BlobStatsTest"));

    stat = stats.createStat(0);
    offset = stats.getPosFromStat(stat, BLOBFILE_LIMIT_SIZE - 1);
    ASSERT_EQ(offset, 0);

    // Can't get pos from a full stat
    offset = stats.getPosFromStat(stat, 100);
    ASSERT_EQ(offset, UINT64_MAX);

    // Stat internal property should not changed
    ASSERT_EQ(stat->sm_total_size, BLOBFILE_LIMIT_SIZE - 1);
    ASSERT_EQ(stat->sm_valid_size, BLOBFILE_LIMIT_SIZE - 1);
    ASSERT_LE(stat->sm_valid_rate, 1);

    // Won't choose full one
    std::tie(stat, blob_file_id) = stats.chooseStat(100);
    ASSERT_EQ(blob_file_id, 1);
    ASSERT_FALSE(stat);

    // A new stat can use
    stat = stats.createStat(blob_file_id);
    offset = stats.getPosFromStat(stat, 100);
    ASSERT_EQ(offset, 0);

    // Remove the stat which id is 0 , now remain the stat which id is 1
    stats.earseStat(0);

    // Then full the stat which id 1
    offset = stats.getPosFromStat(stat, BLOBFILE_LIMIT_SIZE - 100);
    ASSERT_EQ(offset, 100);

    // Then choose stat , it should return the stat id 0
    // cause in this time , stat which id is 1 have been earsed,
    // and stat which id is 1 is full.
    std::tie(stat, blob_file_id) = stats.chooseStat(100);
    ASSERT_EQ(blob_file_id, 0);
    ASSERT_FALSE(stat);
}

TEST(BlobStatsTest, testWriteRead)
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getContext().getFileProvider();

    PageId page_id = 50;
    size_t buff_size = 20;
    size_t buff_nums = 5;

    auto blob_store = BlobStore(file_provider);
    char c_buff[buff_size * buff_nums];

    WriteBatch wb;

    for (size_t i = 0; i < buff_nums; ++i)
    {
        for (size_t j = 0; j < buff_size; ++j)
        {
            c_buff[j + i * buff_size] = j & 0xff;
        }

        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>((const char *)(c_buff + i * buff_size), buff_size);
        wb.putPage(page_id++, /* tag */ 0, buff, buff_size);
    }

    ASSERT_EQ(wb.getTotalDataSize(), 100);
    PageEntriesEdit edit = blob_store.write(wb, nullptr);
    ASSERT_EQ(edit.size(), buff_nums);

    char c_buff_read[buff_size * buff_nums];

    size_t index = 0;
    for (auto & record : edit.getRecords())
    {
        ASSERT_EQ(record.type, WriteBatch::WriteType::PUT);
        ASSERT_EQ(record.entry.offset, index * buff_size);
        ASSERT_EQ(record.entry.size, buff_size);
        ASSERT_EQ(record.entry.file_id, 0);

        // Read
        blob_store.read(record.entry.file_id,
                        record.entry.offset,
                        c_buff_read + index * buff_size,
                        record.entry.size,
                        /* ReadLimiterPtr */ nullptr);

        ASSERT_EQ(strncmp(c_buff, c_buff_read, record.entry.size), 0);
        index++;
    }
    ASSERT_EQ(index, buff_nums);

    index = 0;
    std::vector<std::tuple<BlobStore::BlobFileId, UInt64, size_t>> read_reqs;
    std::vector<char *> buffers_read;
    for (auto & record : edit.getRecords())
    {
        buffers_read.emplace_back(c_buff_read + index++ * buff_size);
        read_reqs.emplace_back(std::make_tuple(
            record.entry.file_id,
            record.entry.offset,
            record.entry.size));
    }

    blob_store.read(read_reqs,
                    buffers_read,
                    /* ReadLimiterPtr */ nullptr);

    ASSERT_EQ(strncmp(c_buff, c_buff_read, sizeof(c_buff)), 0);
}


TEST(BlobStatsTest, testFeildOffsetWriteRead)
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

    auto blob_store = BlobStore(file_provider);
    char c_buff[buff_size * buff_nums];

    WriteBatch wb;

    for (size_t i = 0; i < buff_nums; ++i)
    {
        for (size_t j = 0; j < buff_size; ++j)
        {
            c_buff[j + i * buff_size] = j & 0xff;
        }

        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>((const char *)(c_buff + i * buff_size), buff_size);
        wb.putPage(page_id++, /* tag */ 0, buff, buff_size, field_sizes);
    }

    ASSERT_EQ(wb.getTotalDataSize(), 100);
    PageEntriesEdit edit = blob_store.write(wb, nullptr);
    ASSERT_EQ(edit.size(), buff_nums);

    char c_buff_read[buff_size * buff_nums];

    size_t index = 0;
    for (auto & record : edit.getRecords())
    {
        ASSERT_EQ(record.type, WriteBatch::WriteType::PUT);
        ASSERT_EQ(record.entry.offset, index * buff_size);
        ASSERT_EQ(record.entry.size, buff_size);
        ASSERT_EQ(record.entry.file_id, 0);


        PageFieldSizes check_field_sizes;
        for (auto & [field_offset, crc] : record.entry.field_offsets)
        {
            check_field_sizes.emplace_back(field_offset);
            ASSERT_TRUE(crc);
        }

        ASSERT_EQ(check_field_sizes, offsets);

        // Read
        blob_store.read(record.entry.file_id,
                        record.entry.offset,
                        c_buff_read + index * buff_size,
                        record.entry.size,
                        /* ReadLimiterPtr */ nullptr);

        ASSERT_EQ(strncmp(c_buff, c_buff_read, record.entry.size), 0);
        index++;
    }
    ASSERT_EQ(index, buff_nums);
}


} // namespace DB::PS::V3::tests
