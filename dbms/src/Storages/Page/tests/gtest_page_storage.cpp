#include "gtest/gtest.h"

#include <IO/ReadBufferFromMemory.h>
#include <Poco/File.h>

#define private public
#include <Storages/Page/PageStorage.h>
#undef private

namespace DB
{
namespace tests
{

class PageStorage_test : public ::testing::Test
{
public:
    PageStorage_test() : path("./t"), storage() {}

protected:
    void SetUp() override
    {
        // drop dir if exists
        Poco::File file(path);
        if (file.exists())
        {
            file.remove(true);
        }
        config.file_roll_size               = 512;
        config.merge_hint_low_used_file_num = 1;

        storage = std::make_shared<PageStorage>(path, config);
    }

protected:
    String                       path;
    PageStorage::Config          config;
    std::shared_ptr<PageStorage> storage;
};

TEST_F(PageStorage_test, WriteRead)
{
    const UInt64 tag    = 0;
    const size_t buf_sz = 1024;
    char         c_buff[buf_sz];
    for (size_t i = 0; i < buf_sz; ++i)
    {
        c_buff[i] = i % 0xff;
    }

    {
        WriteBatch    batch;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(0, tag, buff, buf_sz);
        buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(1, tag, buff, buf_sz);
        storage->write(batch);
    }

    Page page0 = storage->read(0);
    ASSERT_EQ(page0.data.size(), buf_sz);
    ASSERT_EQ(page0.page_id, 0UL);
    for (size_t i = 0; i < buf_sz; ++i)
    {
        EXPECT_EQ(*(page0.data.begin() + i), static_cast<char>(i % 0xff));
    }
    Page page1 = storage->read(1);
    ASSERT_EQ(page1.data.size(), buf_sz);
    ASSERT_EQ(page1.page_id, 1UL);
    for (size_t i = 0; i < buf_sz; ++i)
    {
        EXPECT_EQ(*(page1.data.begin() + i), static_cast<char>(i % 0xff));
    }
}

TEST_F(PageStorage_test, WriteReadGc)
{
    const size_t buf_sz = 256;
    char         c_buff[buf_sz];

    const size_t num_repeat = 10;
    PageId       pid        = 1;
    const char   page0_byte = 0x3f;
    {
        // put page0
        WriteBatch batch;
        memset(c_buff, page0_byte, buf_sz);
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(0, 0, buff, buf_sz);
        storage->write(batch);
    }
    // repeated put page1
    for (size_t n = 1; n <= num_repeat; ++n)
    {
        WriteBatch batch;
        memset(c_buff, n, buf_sz);
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(pid, 0, buff, buf_sz);
        storage->write(batch);
    }

    {
        Page page0 = storage->read(0);
        ASSERT_EQ(page0.data.size(), buf_sz);
        ASSERT_EQ(page0.page_id, 0UL);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page0.data.begin() + i), page0_byte);
        }

        Page page1 = storage->read(pid);
        ASSERT_EQ(page1.data.size(), buf_sz);
        ASSERT_EQ(page1.page_id, pid);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page1.data.begin() + i), static_cast<char>(num_repeat % 0xff));
        }
    }

    storage->gc();

    {
        Page page0 = storage->read(0);
        ASSERT_EQ(page0.data.size(), buf_sz);
        ASSERT_EQ(page0.page_id, 0UL);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page0.data.begin() + i), page0_byte);
        }

        Page page1 = storage->read(pid);
        ASSERT_EQ(page1.data.size(), buf_sz);
        ASSERT_EQ(page1.page_id, pid);
        for (size_t i = 0; i < buf_sz; ++i)
        {
            EXPECT_EQ(*(page1.data.begin() + i), static_cast<char>(num_repeat % 0xff));
        }
    }
}

TEST_F(PageStorage_test, GcConcurrencyDelPage)
{
    PageId pid = 0;
    // gc move Page0 -> PageFile{5,1}
    PageEntryMap map;
    map.put(pid, PageEntry{.file_id = 1, .level = 0});
    // write thread del Page0 in page_map before gc thread get unique_lock of `read_mutex`
    storage->page_entry_map.clear();
    // gc continue
    storage->gcUpdatePageMap(map);
    // page0 don't update to page_map
    const PageEntry entry = storage->getEntry(pid);
    ASSERT_FALSE(entry.isValid());
}

static void EXPECT_PagePos_LT(PageFileIdAndLevel p0, PageFileIdAndLevel p1)
{
    EXPECT_LT(p0, p1);
}

TEST_F(PageStorage_test, GcPageMove)
{
    EXPECT_PagePos_LT({4, 0}, {5, 1});
    EXPECT_PagePos_LT({5, 0}, {5, 1});
    EXPECT_PagePos_LT({5, 1}, {6, 1});
    EXPECT_PagePos_LT({5, 2}, {6, 1});

    const PageId pid = 0;
    // old Page0 is in PageFile{5, 0}
    storage->page_entry_map.put(pid,
                                PageEntry{
                                    .file_id = 5,
                                    .level   = 0,
                                });
    // gc move Page0 -> PageFile{5,1}
    PageEntryMap map;
    map.put(pid,
            PageEntry{
                .file_id = 5,
                .level   = 1,
            });
    storage->gcUpdatePageMap(map);
    // page_map get updated
    const PageEntry entry = storage->getEntry(pid);
    ASSERT_TRUE(entry.isValid());
    ASSERT_EQ(entry.file_id, 5);
    ASSERT_EQ(entry.level, 1);
}

TEST_F(PageStorage_test, GcConcurrencySetPage)
{
    const PageId pid = 0;
    // gc move Page0 -> PageFile{5,1}
    PageEntryMap map;
    map.put(pid,
            PageEntry{
                .file_id = 5,
                .level   = 1,
            });
    // write thread insert newer Page0 before gc thread get unique_lock on `read_mutex`
    storage->page_entry_map.put(pid,
                                PageEntry{
                                    .file_id = 6,
                                    .level   = 0,
                                });
    // gc continue
    storage->gcUpdatePageMap(map);
    // read
    const PageEntry entry = storage->getEntry(pid);
    ASSERT_TRUE(entry.isValid());
    ASSERT_EQ(entry.file_id, 6);
    ASSERT_EQ(entry.level, 0);
}

class PageStorageWith2Pages_test : public PageStorage_test
{
public:
    PageStorageWith2Pages_test() : PageStorage_test() {}

protected:
    void SetUp() override
    {
        PageStorage_test::SetUp();
        // put predefine Page1, Page2
        const size_t buf_sz = 1024;
        char         buf[buf_sz];
        {
            WriteBatch wb;
            memset(buf, 0x01, buf_sz);
            wb.putPage(1, 0, std::make_shared<ReadBufferFromMemory>(buf, buf_sz), buf_sz);
            storage->write(wb);
        }
        {
            WriteBatch wb;
            memset(buf, 0x02, buf_sz);
            wb.putPage(2, 0, std::make_shared<ReadBufferFromMemory>(buf, buf_sz), buf_sz);
            storage->write(wb);
        }
    }
};

TEST_F(PageStorageWith2Pages_test, UpdateRefPages)
{
    const UInt64 tag = 0;
    // put ref page: RefPage3 -> Page2
    {
        WriteBatch batch;
        batch.putRefPage(3, 2, tag);
        storage->write(batch);
    }
    const size_t buf_sz = 1024;
    char         buf[buf_sz];
    // if update PageId == 3 or PageId == 2, both RefPage3 && Page2 get updated
    {
        // update RefPage3
        WriteBatch batch;
        char       ch_to_update = 0x0f;
        memset(buf, ch_to_update, buf_sz);
        batch.putPage(3, tag, std::make_shared<ReadBufferFromMemory>(buf, buf_sz), buf_sz);
        storage->write(batch);

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
        char       ch_to_update = 0xef;
        memset(buf, ch_to_update, buf_sz);
        batch.putPage(2, tag, std::make_shared<ReadBufferFromMemory>(buf, buf_sz), buf_sz);
        storage->write(batch);

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

TEST_F(PageStorageWith2Pages_test, DeleteRefPages)
{
    const UInt64 tag = 0;
    // put ref page: RefPage3 -> Page2, RefPage4 -> Page2
    {
        WriteBatch batch;
        batch.putRefPage(3, 2, tag);
        batch.putRefPage(4, 2, tag);
        storage->write(batch);
    }
    { // tests for delete Page
        // delete RefPage3, RefPage4 don't get deleted
        {
            WriteBatch batch;
            batch.delPage(3);
            storage->write(batch);
            EXPECT_FALSE(storage->getEntry(3).isValid());
            EXPECT_TRUE(storage->getEntry(4).isValid());
        }
        // delete RefPage4
        {
            WriteBatch batch;
            batch.delPage(4);
            storage->write(batch);
            EXPECT_FALSE(storage->getEntry(4).isValid());
        }
    }
}

TEST_F(PageStorageWith2Pages_test, PutRefPagesOverRefPages)
{
    /// put ref page to ref page, ref path collapse to normal page
    const UInt64 tag = 0;

    {
        WriteBatch batch;
        // RefPage3 -> Page1
        batch.putRefPage(3, 1, tag);
        // RefPage4 -> RefPage3 -> Page1
        batch.putRefPage(4, 3, tag);
        storage->write(batch);
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

} // namespace tests
} // namespace DB
