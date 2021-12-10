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
    size_t buff_nums = 21;
    size_t buff_size = 123;

    auto blob_store = BlobStore(file_provider);
    char c_buff[buff_size * buff_nums];

    WriteBatch wb;

    for (size_t i = 0; i < buff_nums; ++i)
    {
        for (size_t j = 0; j < buff_size; ++j)
        {
            c_buff[j + i * buff_size] = (char)((j & 0xff) + i);
        }

        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>((const char *)(c_buff + i * buff_size), buff_size);
        wb.putPage(page_id++, /* tag */ 0, buff, buff_size);
    }

    ASSERT_EQ(wb.getTotalDataSize(), buff_nums * buff_size);
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

        // Read directly from the file
        blob_store.read(record.entry.file_id,
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

    for (auto & record : edit.getRecords())
    {
        entries.emplace_back(std::make_pair(page_id++, record.entry));
    }

    // Test `PageMap` read
    page_id = 50;
    index = 0;
    auto page_map = blob_store.read(entries, /* ReadLimiterPtr */ nullptr);
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
        auto page = blob_store.read(entry, /* ReadLimiterPtr */ nullptr);
        ASSERT_EQ(page.data.size(), buff_size);
        ASSERT_EQ(strncmp(c_buff + index * buff_size, page.data.begin(), page.data.size()), 0);
        index++;
    }
    ASSERT_EQ(index, buff_nums);
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
            c_buff[j + i * buff_size] = (char)((j & 0xff) + i);
        }

        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>((const char *)(c_buff + i * buff_size), buff_size);
        wb.putPage(page_id++, /* tag */ 0, buff, buff_size, field_sizes);
    }

    ASSERT_EQ(wb.getTotalDataSize(), buff_nums * buff_size);
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

        ASSERT_EQ(strncmp(c_buff + index * buff_size, c_buff_read + index * buff_size, record.entry.size), 0);
        index++;
    }
    ASSERT_EQ(index, buff_nums);
}

TEST(BlobStatsTest, testWrite)
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getContext().getFileProvider();
    auto blob_store = BlobStore(file_provider);

    PageId page_id = 50;
    const size_t buff_size = 1024;
    WriteBatch wb;
    {
        char c_buff1[buff_size];
        char c_buff2[buff_size];

        for (size_t i = 0; i < buff_size; ++i)
        {
            c_buff1[i] = i & 0xff;
            c_buff2[i] = (char)((i & 0xff) + 1);
        }

        ReadBufferPtr buff1 = std::make_shared<ReadBufferFromMemory>(c_buff1, buff_size);
        ReadBufferPtr buff2 = std::make_shared<ReadBufferFromMemory>(c_buff2, buff_size);

        wb.putPage(page_id, /*tag*/ 0, buff1, buff_size);
        wb.upsertPage(page_id,
                      /*tag*/ 0,
                      /*PageFileIdAndLevel*/ {},
                      buff2,
                      buff_size,
                      /*PageFieldOffsetChecksums*/ {});

        PageEntriesEdit edit = blob_store.write(wb, nullptr);
        ASSERT_EQ(edit.size(), 2);

        auto records = edit.getRecords();
        auto record = records[0];

        ASSERT_EQ(record.type, WriteBatch::WriteType::PUT);
        ASSERT_EQ(record.page_id, page_id);
        ASSERT_EQ(record.entry.offset, 0);
        ASSERT_EQ(record.entry.size, buff_size);
        ASSERT_EQ(record.entry.file_id, 0);

        record = records[1];
        ASSERT_EQ(record.type, WriteBatch::WriteType::UPSERT);
        ASSERT_EQ(record.page_id, page_id);
        ASSERT_EQ(record.entry.offset, buff_size);
        ASSERT_EQ(record.entry.size, buff_size);
        ASSERT_EQ(record.entry.file_id, 0);
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

        ASSERT_EQ(record.type, WriteBatch::WriteType::REF);
        ASSERT_EQ(record.page_id, page_id + 1);
        ASSERT_EQ(record.ori_page_id, page_id);

        record = records[1];
        ASSERT_EQ(record.type, WriteBatch::WriteType::DEL);
        ASSERT_EQ(record.page_id, page_id + 1);

        record = records[2];
        ASSERT_EQ(record.type, WriteBatch::WriteType::DEL);
        ASSERT_EQ(record.page_id, page_id);
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
        ASSERT_EQ(record.type, WriteBatch::WriteType::PUT);
        ASSERT_EQ(record.page_id, page_id);
        ASSERT_EQ(record.entry.offset, buff_size * 2);
        ASSERT_EQ(record.entry.size, buff_size);
        ASSERT_EQ(record.entry.file_id, 0);

        record = records[1];
        ASSERT_EQ(record.type, WriteBatch::WriteType::REF);
        ASSERT_EQ(record.page_id, page_id + 1);
        ASSERT_EQ(record.ori_page_id, page_id);

        record = records[2];
        ASSERT_EQ(record.type, WriteBatch::WriteType::DEL);
        ASSERT_EQ(record.page_id, page_id);
    }
}

} // namespace DB::PS::V3::tests
