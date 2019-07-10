#include "gtest/gtest.h"

#include <IO/ReadBufferFromMemory.h>
#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/File.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/PatternFormatter.h>
#include <common/logger_useful.h>

#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/PageEntryMap.h>
#include <Storages/Page/PageFile.h>
#include <Storages/Page/WriteBatch.h>

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
    static void SetUpTestCase()
    {
        Poco::AutoPtr<Poco::ConsoleChannel>   channel = new Poco::ConsoleChannel(std::cerr);
        Poco::AutoPtr<Poco::PatternFormatter> formatter(new Poco::PatternFormatter);
        formatter->setProperty("pattern", "%L%Y-%m-%d %H:%M:%S.%i <%p> %s: %t");
        Poco::AutoPtr<Poco::FormattingChannel> formatting_channel(new Poco::FormattingChannel(formatter, channel));
        Logger::root().setChannel(formatting_channel);
        Logger::root().setLevel("trace");
    }

    void SetUp() override
    {
        // drop dir if exists
        Poco::File file(path);
        if (file.exists())
        {
            file.remove(true);
        }
        // default test config
        config.file_roll_size               = 512;
        config.merge_hint_low_used_file_num = 1;

        storage = reopenWithConfig(config);
    }

    std::shared_ptr<PageStorage> reopenWithConfig(const PageStorage::Config & config_)
    {
        return std::make_shared<PageStorage>(path, config_);
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

TEST_F(PageStorage_test, WriteMultipleBatchRead)
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
        storage->write(batch);
    }
    {
        WriteBatch    batch;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
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

TEST_F(PageStorage_test, WriteReadAfterGc)
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

TEST_F(PageStorage_test, GcMigrateValidRefPages)
{
    const size_t buf_sz      = 1024;
    char         buf[buf_sz] = {0};
    const PageId ref_id      = 1024;
    const PageId page_id     = 32;

    const PageId placeholder_page_id = 33;
    const PageId deleted_ref_id      = 1025;

    {
        // prepare ref page record without any valid pages
        PageStorage::Config tmp_config(config);
        tmp_config.file_roll_size = 1;
        storage                   = reopenWithConfig(tmp_config);
        {
            // this batch is written to PageFile{1,0}
            WriteBatch batch;
            batch.putPage(page_id, 0, std::make_shared<ReadBufferFromMemory>(buf, buf_sz), buf_sz);
            storage->write(batch);
        }
        {
            // this batch is written to PageFile{2,0}
            WriteBatch batch;
            batch.putRefPage(ref_id, page_id);
            batch.delPage(page_id);
            // deleted ref pages will not migrate
            batch.putRefPage(deleted_ref_id, page_id);
            batch.putPage(placeholder_page_id, 0, std::make_shared<ReadBufferFromMemory>(buf, buf_sz), buf_sz);
            storage->write(batch);
        }
        {
            // this batch is written to PageFile{3,0}
            WriteBatch batch;
            batch.delPage(deleted_ref_id);
            storage->write(batch);
        }
        const PageEntry entry2 = storage->getEntry(ref_id);
        ASSERT_TRUE(entry2.isValid());
    }

    const PageStorage::GcLivesPages lives_pages;
    PageStorage::GcCandidates       candidates;
    PageStorage::SnapshotPtr        snapshot = storage->getSnapshot();
    //candidates.insert(PageFileIdAndLevel{1, 0});
    candidates.insert(PageFileIdAndLevel{2, 0});
    const PageEntriesEdit gc_file_edit = storage->gcMigratePages(snapshot, lives_pages, candidates);
    ASSERT_FALSE(gc_file_edit.empty());
    // check the ref is migrated.
    // check the deleted ref is not migrated.
    bool is_deleted_ref_id_exists = false;
    for (const auto & rec : gc_file_edit.getRecords())
    {
        if (rec.type == WriteBatch::WriteType::REF)
        {
            if (rec.page_id == ref_id)
            {
                ASSERT_EQ(rec.ori_page_id, page_id);
            }
            if (rec.page_id == deleted_ref_id)
            {
                ASSERT_NE(rec.ori_page_id, page_id);
                is_deleted_ref_id_exists = true;
            }
        }
    }
    ASSERT_FALSE(is_deleted_ref_id_exists);
}

/**
 * PageStorage tests with predefine Page1 && Page2
 */
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
    /// update on RefPage, all references get updated.
    const UInt64 tag = 0;
    // put ref page: RefPage3 -> Page2
    {
        WriteBatch batch;
        batch.putRefPage(3, 2);
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
    // put ref page: RefPage3 -> Page2, RefPage4 -> Page2
    {
        WriteBatch batch;
        batch.putRefPage(3, 2);
        batch.putRefPage(4, 2);
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
    {
        WriteBatch batch;
        // RefPage3 -> Page1
        batch.putRefPage(3, 1);
        // RefPage4 -> RefPage3 -> Page1
        batch.putRefPage(4, 3);
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

TEST_F(PageStorageWith2Pages_test, PutDuplicateRefPages)
{
    /// put duplicated RefPages in different WriteBatch
    {
        WriteBatch batch;
        batch.putRefPage(3, 1);
        storage->write(batch);

        WriteBatch batch2;
        batch2.putRefPage(3, 1);
        storage->write(batch);
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
        storage->write(batch);
        PageEntry entry_after_del1 = storage->getEntry(3);
        ASSERT_TRUE(entry_after_del1.isValid());
        EXPECT_EQ(entry1.fileIdLevel(), entry_after_del1.fileIdLevel());
        EXPECT_EQ(entry1.offset, entry_after_del1.offset);
        EXPECT_EQ(entry1.size, entry_after_del1.size);
        EXPECT_EQ(entry1.checksum, entry_after_del1.checksum);

        WriteBatch batch2;
        batch2.delPage(3);
        storage->write(batch2);
        PageEntry entry_after_del2 = storage->getEntry(3);
        ASSERT_FALSE(entry_after_del2.isValid());
    }
}

TEST_F(PageStorageWith2Pages_test, PutCollapseDuplicatedRefPages)
{
    /// put duplicated RefPages due to ref-path-collapse
    {
        WriteBatch batch;
        // RefPage3 -> Page1
        batch.putRefPage(3, 1);
        // RefPage4 -> RefPage3, collapse to RefPage4 -> Page1
        batch.putRefPage(4, 3);
        storage->write(batch);

        WriteBatch batch2;
        // RefPage4 -> Page1, duplicated due to ref-path-collapse
        batch2.putRefPage(4, 1);
        storage->write(batch);
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
        storage->write(batch);
        PageEntry entry_after_del2 = storage->getEntry(3);
        ASSERT_TRUE(entry_after_del2.isValid());
        EXPECT_EQ(entry1.fileIdLevel(), entry_after_del2.fileIdLevel());
        EXPECT_EQ(entry1.offset, entry_after_del2.offset);
        EXPECT_EQ(entry1.size, entry_after_del2.size);
        EXPECT_EQ(entry1.checksum, entry_after_del2.checksum);

        WriteBatch batch2;
        batch2.delPage(3);
        storage->write(batch2);
        PageEntry entry_after_del3 = storage->getEntry(3);
        ASSERT_FALSE(entry_after_del3.isValid());
    }
}

TEST_F(PageStorageWith2Pages_test, AddRefPageToNonExistPage)
{
    {
        WriteBatch batch;
        // RefPage3 -> non-exist Page999
        batch.putRefPage(3, 999);
        ASSERT_NO_THROW(storage->write(batch));
    }

    ASSERT_FALSE(storage->getEntry(3).isValid());
    ASSERT_THROW(storage->read(3), DB::Exception);

    // Invalid Pages is filtered after reopen PageStorage
    ASSERT_NO_THROW(reopenWithConfig(config));
    ASSERT_FALSE(storage->getEntry(3).isValid());
}

TEST_F(PageStorageWith2Pages_test, SnapshotReadSnapshotVersion)
{
    char      ch_before         = 0x01;
    char      ch_update         = 0xFF;
    auto      snapshot          = storage->getSnapshot();
    PageEntry p1_snapshot_entry = storage->getEntry(1, snapshot);

    {
        // write new version of Page1
        const size_t buf_sz = 1024;
        char         buf[buf_sz];
        {
            WriteBatch wb;
            memset(buf, ch_update, buf_sz);
            wb.putPage(1, 0, std::make_shared<ReadBufferFromMemory>(buf, buf_sz), buf_sz);
            wb.putPage(3, 0, std::make_shared<ReadBufferFromMemory>(buf, buf_sz), buf_sz);
            storage->write(wb);
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
        Page page1 = storage->read(1, snapshot);
        ASSERT_EQ(*page1.data.begin(), ch_before);

        // read(vec<PageId>) with snapshot
        PageIds ids{
            1,
        };
        auto pages = storage->read(ids, snapshot);
        ASSERT_EQ(pages.count(1), 1UL);
        ASSERT_EQ(*pages[1].data.begin(), ch_before);
        // TODO read(vec<PageId>, callback) with snapshot

        // new page do appear while read with snapshot
        PageEntry p3_entry = storage->getEntry(3, snapshot);
        ASSERT_FALSE(p3_entry.isValid());
        ASSERT_THROW({ storage->read(3, snapshot); }, DB::Exception);
    }
}

TEST_F(PageStorageWith2Pages_test, GetIdenticalSnapshots)
{
    char      ch_before         = 0x01;
    char      ch_update         = 0xFF;
    PageEntry p1_snapshot_entry = storage->getEntry(1);
    auto      s1                = storage->getSnapshot();
    auto      s2                = storage->getSnapshot();
    auto      s3                = storage->getSnapshot();

    {
        // write new version of Page1
        const size_t buf_sz = 1024;
        char         buf[buf_sz];
        {
            WriteBatch wb;
            memset(buf, ch_update, buf_sz);
            wb.putPage(1, 0, std::make_shared<ReadBufferFromMemory>(buf, buf_sz), buf_sz);
            wb.putPage(3, 0, std::make_shared<ReadBufferFromMemory>(buf, buf_sz), buf_sz);
            storage->write(wb);
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
    Page page1 = storage->read(1, s1);
    ASSERT_EQ(*page1.data.begin(), ch_before);
    page1 = storage->read(1, s2);
    ASSERT_EQ(*page1.data.begin(), ch_before);
    page1 = storage->read(1, s3);
    ASSERT_EQ(*page1.data.begin(), ch_before);
    // read(vec<PageId>) with snapshot
    auto pages = storage->read(ids, s1);
    ASSERT_EQ(pages.count(1), 1UL);
    ASSERT_EQ(*pages[1].data.begin(), ch_before);
    pages = storage->read(ids, s2);
    ASSERT_EQ(pages.count(1), 1UL);
    ASSERT_EQ(*pages[1].data.begin(), ch_before);
    pages = storage->read(ids, s3);
    ASSERT_EQ(pages.count(1), 1UL);
    ASSERT_EQ(*pages[1].data.begin(), ch_before);
    // TODO read(vec<PageId>, callback) with snapshot
    // without snapshot
    p1_entry = storage->getEntry(1);
    ASSERT_NE(p1_entry.checksum, p1_snapshot_entry.checksum);

    s1.reset(); /// free snapshot 1

    // getEntry with snapshot
    p1_entry = storage->getEntry(1, s2);
    ASSERT_EQ(p1_entry.checksum, p1_snapshot_entry.checksum);
    p1_entry = storage->getEntry(1, s3);
    ASSERT_EQ(p1_entry.checksum, p1_snapshot_entry.checksum);
    // read(PageId) with snapshot
    page1 = storage->read(1, s2);
    ASSERT_EQ(*page1.data.begin(), ch_before);
    page1 = storage->read(1, s3);
    ASSERT_EQ(*page1.data.begin(), ch_before);
    // read(vec<PageId>) with snapshot
    ASSERT_EQ(*pages[1].data.begin(), ch_before);
    pages = storage->read(ids, s2);
    ASSERT_EQ(pages.count(1), 1UL);
    ASSERT_EQ(*pages[1].data.begin(), ch_before);
    pages = storage->read(ids, s3);
    ASSERT_EQ(pages.count(1), 1UL);
    ASSERT_EQ(*pages[1].data.begin(), ch_before);
    // TODO read(vec<PageId>, callback) with snapshot
    // without snapshot
    p1_entry = storage->getEntry(1);
    ASSERT_NE(p1_entry.checksum, p1_snapshot_entry.checksum);

    s2.reset(); /// free snapshot 2

    // getEntry with snapshot
    p1_entry = storage->getEntry(1, s3);
    ASSERT_EQ(p1_entry.checksum, p1_snapshot_entry.checksum);
    // read(PageId) with snapshot
    page1 = storage->read(1, s3);
    ASSERT_EQ(*page1.data.begin(), ch_before);
    // read(vec<PageId>) with snapshot
    pages = storage->read(ids, s3);
    ASSERT_EQ(pages.count(1), 1UL);
    ASSERT_EQ(*pages[1].data.begin(), ch_before);
    // TODO read(vec<PageId>, callback) with snapshot
    // without snapshot
    p1_entry = storage->getEntry(1);
    ASSERT_NE(p1_entry.checksum, p1_snapshot_entry.checksum);

    s3.reset(); /// free snapshot 3

    // without snapshot
    p1_entry = storage->getEntry(1);
    ASSERT_NE(p1_entry.checksum, p1_snapshot_entry.checksum);
}

} // namespace tests
} // namespace DB
