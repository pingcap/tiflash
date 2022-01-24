#include <IO/ReadBufferFromMemory.h>
#include <Poco/Logger.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/tests/entries_helper.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB::PS::V3::tests
{
using BlobStat = BlobStore::BlobStats::BlobStat;
using BlobStats = BlobStore::BlobStats;

class BlobStoreTest : public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override
    {
        path = getTemporaryPath();
        DB::tests::TiFlashTestEnv::tryRemovePath(path);

        Poco::File file(path);
        if (!file.exists())
        {
            file.createDirectories();
        }
    }

protected:
    BlobStore::Config config;
    String path{};
};

TEST_F(BlobStoreTest, testStats)
{
    BlobStats stats(&Poco::Logger::get("BlobStoreTest"), config);


    auto stat = stats.createStat(0, stats.lock());

    ASSERT_TRUE(stat);
    ASSERT_TRUE(stat->smap);
    stats.createStat(1, stats.lock());
    stats.createStat(2, stats.lock());

    ASSERT_EQ(stats.stats_map.size(), 3);
    ASSERT_EQ(stats.roll_id, 3);

    stats.eraseStat(0, stats.lock());
    stats.eraseStat(1, stats.lock());
    ASSERT_EQ(stats.stats_map.size(), 1);
    ASSERT_EQ(stats.roll_id, 3);
    ASSERT_EQ(stats.old_ids.size(), 2);

    auto old_it = stats.old_ids.begin();

    ASSERT_EQ((*old_it++), 0);
    ASSERT_EQ((*old_it++), 1);
    ASSERT_EQ(old_it, stats.old_ids.end());
}


TEST_F(BlobStoreTest, testStat)
{
    BlobFileId blob_file_id = 0;
    BlobStore::BlobStats::BlobStatPtr stat;

    BlobStats stats(&Poco::Logger::get("BlobStoreTest"), config);

    std::tie(stat, blob_file_id) = stats.chooseStat(10, BLOBFILE_LIMIT_SIZE, stats.lock());
    ASSERT_EQ(blob_file_id, 1);
    ASSERT_FALSE(stat);

    // still 0
    std::tie(stat, blob_file_id) = stats.chooseStat(10, BLOBFILE_LIMIT_SIZE, stats.lock());
    ASSERT_EQ(blob_file_id, 1);
    ASSERT_FALSE(stat);

    stats.createStat(0, stats.lock());
    std::tie(stat, blob_file_id) = stats.chooseStat(10, BLOBFILE_LIMIT_SIZE, stats.lock());
    ASSERT_EQ(blob_file_id, INVALID_BLOBFILE_ID);
    ASSERT_TRUE(stat);

    auto offset = stat->getPosFromStat(10);
    ASSERT_EQ(offset, 0);

    offset = stat->getPosFromStat(100);
    ASSERT_EQ(offset, 10);

    offset = stat->getPosFromStat(20);
    ASSERT_EQ(offset, 110);

    ASSERT_EQ(stat->sm_total_size, 10 + 100 + 20);
    ASSERT_EQ(stat->sm_valid_size, 10 + 100 + 20);
    ASSERT_EQ(stat->sm_valid_rate, 1);

    stat->removePosFromStat(10, 100);
    ASSERT_EQ(stat->sm_total_size, 10 + 100 + 20);
    ASSERT_EQ(stat->sm_valid_size, 10 + 20);
    ASSERT_LE(stat->sm_valid_rate, 1);

    offset = stat->getPosFromStat(110);
    ASSERT_EQ(offset, 130);
    ASSERT_EQ(stat->sm_total_size, 10 + 100 + 20 + 110);
    ASSERT_EQ(stat->sm_valid_size, 10 + 20 + 110);
    ASSERT_LE(stat->sm_valid_rate, 1);

    offset = stat->getPosFromStat(90);
    ASSERT_EQ(offset, 10);
    ASSERT_EQ(stat->sm_total_size, 10 + 100 + 20 + 110);
    ASSERT_EQ(stat->sm_valid_size, 10 + 20 + 110 + 90);
    ASSERT_LE(stat->sm_valid_rate, 1);

    // Unmark the last range
    stat->removePosFromStat(130, 110);
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
    offset = stat->getPosFromStat(120);
    ASSERT_EQ(offset, 130);
    ASSERT_EQ(stat->sm_total_size, 10 + 100 + 20 + 110 + 10);
    ASSERT_EQ(stat->sm_valid_size, 10 + 20 + 90 + 120);
    ASSERT_LE(stat->sm_valid_rate, 1);
}

TEST_F(BlobStoreTest, testFullStats)
{
    BlobFileId blob_file_id = 0;
    BlobStore::BlobStats::BlobStatPtr stat;
    BlobFileOffset offset = 0;

    BlobStats stats(&Poco::Logger::get("BlobStoreTest"), config);

    stat = stats.createStat(1, stats.lock());
    offset = stat->getPosFromStat(BLOBFILE_LIMIT_SIZE - 1);
    ASSERT_EQ(offset, 0);

    // Can't get pos from a full stat
    offset = stat->getPosFromStat(100);
    ASSERT_EQ(offset, INVALID_BLOBFILE_OFFSET);

    // Stat internal property should not changed
    ASSERT_EQ(stat->sm_total_size, BLOBFILE_LIMIT_SIZE - 1);
    ASSERT_EQ(stat->sm_valid_size, BLOBFILE_LIMIT_SIZE - 1);
    ASSERT_LE(stat->sm_valid_rate, 1);

    // Won't choose full one
    std::tie(stat, blob_file_id) = stats.chooseStat(100, BLOBFILE_LIMIT_SIZE, stats.lock());
    ASSERT_EQ(blob_file_id, 2);
    ASSERT_FALSE(stat);

    // A new stat can use
    stat = stats.createStat(blob_file_id, stats.lock());
    offset = stat->getPosFromStat(100);
    ASSERT_EQ(offset, 0);

    // Remove the stat which id is 0 , now remain the stat which id is 1
    stats.eraseStat(1, stats.lock());

    // Then full the stat which id 2
    offset = stat->getPosFromStat(BLOBFILE_LIMIT_SIZE - 100);
    ASSERT_EQ(offset, 100);

    // Then choose stat , it should return the stat id 1
    // cause in this time , stat which id is 1 have been earsed,
    // and stat which id is 2 is full.
    std::tie(stat, blob_file_id) = stats.chooseStat(100, BLOBFILE_LIMIT_SIZE, stats.lock());
    ASSERT_EQ(blob_file_id, 1);
    ASSERT_FALSE(stat);
}

TEST_F(BlobStoreTest, testWriteRead)
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getContext().getFileProvider();

    PageId page_id = 50;
    size_t buff_nums = 21;
    size_t buff_size = 123;

    auto blob_store = BlobStore(file_provider, path, config);
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
        ASSERT_EQ(record.type, WriteBatch::WriteType::PUT);
        ASSERT_EQ(record.entry.offset, index * buff_size);
        ASSERT_EQ(record.entry.size, buff_size);
        ASSERT_EQ(record.entry.file_id, 1);

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

    auto blob_store = BlobStore(file_provider, path, config);
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
        ASSERT_EQ(record.type, WriteBatch::WriteType::PUT);
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

TEST_F(BlobStoreTest, testWrite)
try
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getContext().getFileProvider();
    auto blob_store = BlobStore(file_provider, path, config);

    PageId page_id = 50;
    const size_t buff_size = 1024;
    WriteBatch wb;
    {
        char c_buff1[buff_size];
        char c_buff2[buff_size];

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

        ASSERT_EQ(record.type, WriteBatch::WriteType::PUT);
        ASSERT_EQ(record.page_id, page_id);
        ASSERT_EQ(record.entry.offset, 0);
        ASSERT_EQ(record.entry.size, buff_size);
        ASSERT_EQ(record.entry.file_id, 1);

        record = records[1];
        ASSERT_EQ(record.type, WriteBatch::WriteType::PUT);
        ASSERT_EQ(record.page_id, page_id);
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
        ASSERT_EQ(record.entry.file_id, 1);

        record = records[1];
        ASSERT_EQ(record.type, WriteBatch::WriteType::REF);
        ASSERT_EQ(record.page_id, page_id + 1);
        ASSERT_EQ(record.ori_page_id, page_id);

        record = records[2];
        ASSERT_EQ(record.type, WriteBatch::WriteType::DEL);
        ASSERT_EQ(record.page_id, page_id);
    }
}
CATCH

TEST_F(BlobStoreTest, testWriteOutOfLimitSize)
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getContext().getFileProvider();
    size_t buff_size = 100;

    {
        config.file_limit_size = buff_size - 1;
        auto blob_store = BlobStore(file_provider, path, config);

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
    for (auto & buf_size : buffer_sizes)
    {
        auto blob_store = BlobStore(file_provider, path, config);

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
        ASSERT_EQ(record.type, WriteBatch::WriteType::PUT);
        ASSERT_EQ(record.page_id, 50);
        ASSERT_EQ(record.entry.offset, 0);
        ASSERT_EQ(record.entry.size, buf_size);
        ASSERT_EQ(record.entry.file_id, 1);

        wb.clear();
        wb.putPage(51, /*tag*/ 0, buff2, buf_size);
        edit = blob_store.write(wb, nullptr);
        ASSERT_EQ(edit.size(), 1);

        records = edit.getRecords();
        record = records[0];
        ASSERT_EQ(record.type, WriteBatch::WriteType::PUT);
        ASSERT_EQ(record.page_id, 51);
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
    auto blob_store = BlobStore(file_provider, path, config);
    std::list<size_t> remove_entries_idx1 = {1, 3, 4, 7, 9};
    std::list<size_t> remove_entries_idx2 = {6, 8};

    WriteBatch wb;
    {
        char c_buff[buff_size * buff_nums];
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
    PageEntriesV3 entries_del1, entries_del2;
    for (auto & record : edit.getRecords())
    {
        for (size_t index : remove_entries_idx1)
        {
            if (idx == index)
            {
                entries_del1.emplace_back(record.entry);
                break;
            }
        }

        for (size_t index : remove_entries_idx2)
        {
            if (idx == index)
            {
                entries_del2.emplace_back(record.entry);
                break;
            }
        }

        idx++;
    }

    // After remove `entries_del1`.
    // Remain entries index [0, 2, 5, 6, 8]
    blob_store.remove(entries_del1);
    ASSERT_EQ(entries_del1.begin()->file_id, 1);

    auto stat = blob_store.blob_stats.fileIdToStat(1);

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
    String path = blob_store.getBlobFilePath(1);
    Poco::File blob_file_in_disk(path);
    ASSERT_EQ(blob_file_in_disk.getSize(), stat->sm_total_size);
}

TEST_F(BlobStoreTest, testBlobStoreGcStats2)
{
    const auto file_provider = DB::tests::TiFlashTestEnv::getContext().getFileProvider();
    size_t buff_size = 1024;
    size_t buff_nums = 10;
    PageId page_id = 50;
    auto blob_store = BlobStore(file_provider, path, config);
    std::list<size_t> remove_entries_idx = {0, 1, 2, 3, 4, 5, 6, 7};

    WriteBatch wb;
    {
        char c_buff[buff_size * buff_nums];
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
    for (auto & record : edit.getRecords())
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

    auto stat = blob_store.blob_stats.fileIdToStat(1);

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

    auto blob_store = BlobStore(file_provider, path, config);
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

    VersionedEntries versioned_entries;
    for (const auto & record : edit.getRecords())
    {
        versioned_entries.emplace_back(1, record.entry);
    }

    PageIdAndVersionedEntries versioned_pageid_entries;
    versioned_pageid_entries.emplace_back(std::make_pair(page_id, versioned_entries));
    std::map<BlobFileId, PageIdAndVersionedEntries> gc_context;
    gc_context[1] = versioned_pageid_entries;

    // Before we do BlobStore we need change BlobFile0 to Read-Only
    auto stat = blob_store.blob_stats.fileIdToStat(1);
    stat->changeToReadOnly();

    const auto & gc_edit = blob_store.gc(gc_context, static_cast<PageSize>(buff_size * buff_nums));

    // Check copy_list which will apply fo Mvcc
    ASSERT_EQ(gc_edit.size(), buff_nums);
    auto it = versioned_entries.begin();
    for (const auto & record : gc_edit.getRecords())
    {
        ASSERT_EQ(record.page_id, page_id);
        ASSERT_EQ(record.entry.file_id, 2);
        ASSERT_EQ(record.entry.checksum, it->second.checksum);
        ASSERT_EQ(record.entry.size, it->second.size);
        it++;
    }

    // Check blobfile1
    Poco::File file1(blob_store.getBlobFilePath(1));
    Poco::File file2(blob_store.getBlobFilePath(2));
    ASSERT_TRUE(file1.exists());
    ASSERT_TRUE(file2.exists());
    ASSERT_EQ(file1.getSize(), file2.getSize());
}


} // namespace DB::PS::V3::tests