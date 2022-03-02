#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/PageStorageImpl.h>
#include <Storages/Page/V3/tests/entries_helper.h>
#include <Storages/PathPool.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/MockDiskDelegator.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace FailPoints
{
extern const char exception_before_page_file_write_sync[];
extern const char force_set_page_file_write_errno[];
} // namespace FailPoints

namespace PS::V3::tests
{
class PageStorageTest : public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        auto path = getTemporaryPath();
        createIfNotExist(path);
        file_provider = DB::tests::TiFlashTestEnv::getContext().getFileProvider();
        auto delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        page_storage = std::make_shared<PageStorageImpl>("test.t", delegator, config, file_provider);
        page_storage->restore();
    }

    std::shared_ptr<PageStorageImpl> reopenWithConfig(const PageStorage::Config & config_)
    {
        auto path = getTemporaryPath();
        auto delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        auto storage = std::make_shared<PageStorageImpl>("test.t", delegator, config_, file_provider);
        storage->restore();
        return storage;
    }

protected:
    FileProviderPtr file_provider;
    std::unique_ptr<StoragePathPool> path_pool;
    PageStorage::Config config;
    std::shared_ptr<PageStorageImpl> page_storage;

    std::list<PageDirectorySnapshotPtr> snapshots_holder;
    size_t fixed_test_buff_size = 1024;

    size_t epoch_offset = 0;
};

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

TEST_F(PageStorageTest, WriteMultipleBatchRead1)
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
        page_storage->write(std::move(batch));
    }
    {
        WriteBatch batch;
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
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
    for (DB::PageId page_id = 0; page_id <= page_id_max; ++page_id)
    {
        std::mt19937 size_gen;
        size_gen.seed(time(nullptr));
        std::uniform_int_distribution<> dist(0, 3000);

        const size_t buff_sz = 2 * DB::MB + dist(size_gen);
        char * buff = static_cast<char *>(malloc(buff_sz));
        if (buff == nullptr)
        {
            throw DB::Exception("Alloc fix memory failed.", DB::ErrorCodes::LOGICAL_ERROR);
        }

        const char buff_ch = page_id % 0xFF;
        memset(buff, buff_ch, buff_sz);
        DB::MemHolder holder = DB::createMemHolder(buff, [&](char * p) { free(p); });

        auto read_buff = std::make_shared<DB::ReadBufferFromMemory>(const_cast<char *>(buff), buff_sz);

        DB::WriteBatch wb;
        wb.putPage(page_id, 0, read_buff, read_buff->buffer().size());
        page_storage->write(std::move(wb));
    }

    for (DB::PageId page_id = 0; page_id <= page_id_max; ++page_id)
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
    PageId pid = 1;
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
        batch.putPage(1, 0, std::make_shared<ReadBufferFromMemory>(buf, buf_sz), buf_sz, PageFieldSizes{{32, 64, 79, 128, 196, 256, 269}});
        batch.putPage(2, 0, std::make_shared<ReadBufferFromMemory>(buf, buf_sz), buf_sz, PageFieldSizes{{64, 79, 128, 196, 256, 301}});
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
    page_storage = reopenWithConfig(PageStorage::Config{});

    {
        size_t num_pages = 0;
        page_storage->traverse([&num_pages](const DB::Page &) { num_pages += 1; });
        ASSERT_EQ(num_pages, 0);
    }

    // Continue to write some pages
    {
        WriteBatch batch;
        memset(buf, 0x02, buf_sz);
        batch.putPage(1,
                      0,
                      std::make_shared<ReadBufferFromMemory>(buf, buf_sz),
                      buf_sz, //
                      PageFieldSizes{{32, 128, 196, 256, 12, 99, 1, 300}});
        page_storage->write(std::move(batch));

        auto page1 = page_storage->read(1);
        ASSERT_EQ(page1.data.size(), buf_sz);
        for (size_t i = 0; i < page1.data.size(); ++i)
        {
            auto * p = page1.data.begin();
            EXPECT_EQ(*p, 0x02);
        }
    }

    // Restore again, we should be able to read page 1
    page_storage = reopenWithConfig(PageStorage::Config{});

    {
        size_t num_pages = 0;
        page_storage->traverse([&num_pages](const Page &) { num_pages += 1; });
        ASSERT_EQ(num_pages, 1);

        auto page1 = page_storage->read(1);
        ASSERT_EQ(page1.data.size(), buf_sz);
        for (size_t i = 0; i < page1.data.size(); ++i)
        {
            auto * p = page1.data.begin();
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
        batch.putPage(1, 0, std::make_shared<ReadBufferFromMemory>(buf, buf_sz), buf_sz, PageFieldSizes{{32, 64, 79, 128, 196, 256, 269}});
        batch.putPage(2, 0, std::make_shared<ReadBufferFromMemory>(buf, buf_sz), buf_sz, PageFieldSizes{{64, 79, 128, 196, 256, 301}});
        batch.putRefPage(3, 2);
        batch.putRefPage(4, 2);
        try
        {
            FailPointHelper::enableFailPoint(FailPoints::force_set_page_file_write_errno);
            page_storage->write(std::move(batch));
        }
        catch (DB::Exception & e)
        {
            // Mock to catch and ignore the exception in background thread
            if (e.code() != ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR)
                throw;
        }
    }

    FailPointHelper::disableFailPoint(FailPoints::force_set_page_file_write_errno);
    {
        size_t num_pages = 0;
        page_storage->traverse([&num_pages](const Page &) { num_pages += 1; });
        ASSERT_EQ(num_pages, 0);
    }

    // Continue to write some pages
    {
        WriteBatch batch;
        memset(buf, 0x02, buf_sz);
        batch.putPage(1,
                      0,
                      std::make_shared<ReadBufferFromMemory>(buf, buf_sz),
                      buf_sz, //
                      PageFieldSizes{{32, 128, 196, 256, 12, 99, 1, 300}});
        page_storage->write(std::move(batch));

        auto page1 = page_storage->read(1);
        ASSERT_EQ(page1.data.size(), buf_sz);
        for (size_t i = 0; i < page1.data.size(); ++i)
        {
            auto * p = page1.data.begin();
            EXPECT_EQ(*p, 0x02);
        }
    }

    // Restore again, we should be able to read page 1
    page_storage = reopenWithConfig(PageStorage::Config{});

    {
        size_t num_pages = 0;
        page_storage->traverse([&num_pages](const Page &) { num_pages += 1; });
        ASSERT_EQ(num_pages, 1);

        auto page1 = page_storage->read(1);
        ASSERT_EQ(page1.data.size(), buf_sz);
        for (size_t i = 0; i < page1.data.size(); ++i)
        {
            auto * p = page1.data.begin();
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
        page_storage->write(std::move(batch));
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
        page_storage->write(std::move(batch));
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

    ExternalPageCallbacks callbacks;
    callbacks.scanner = []() -> ExternalPageCallbacks::PathAndIdsVec {
        return {};
    };
    callbacks.remover = [&times_remover_called](const ExternalPageCallbacks::PathAndIdsVec &, const std::set<PageId> & living_page_ids) -> void {
        times_remover_called += 1;
        // 0, 1024 are still alive
        EXPECT_EQ(living_page_ids.size(), 2);
        EXPECT_GT(living_page_ids.count(0), 0);
        EXPECT_GT(living_page_ids.count(1024), 0);
    };
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
        auto ori_id_0 = page_storage->getNormalPageId(0, nullptr);
        ASSERT_EQ(ori_id_0, 0);
        auto ori_id_2 = page_storage->getNormalPageId(2, nullptr);
        ASSERT_EQ(ori_id_2, 0);
        ASSERT_EQ(1024, page_storage->getNormalPageId(1024, snapshot));
        ASSERT_EQ(0, page_storage->getNormalPageId(1, snapshot));
        ASSERT_ANY_THROW(page_storage->getNormalPageId(1024, nullptr));
        ASSERT_ANY_THROW(page_storage->getNormalPageId(1, nullptr));
    }

    /// After `snapshot` released, 1024 should be removed from `living`
    snapshot.reset();
    callbacks.remover = [&times_remover_called](const ExternalPageCallbacks::PathAndIdsVec &, const std::set<PageId> & living_page_ids) -> void {
        times_remover_called += 1;
        EXPECT_EQ(living_page_ids.size(), 1);
        EXPECT_GT(living_page_ids.count(0), 0);
    };
    page_storage->registerExternalPagesCallbacks(callbacks);
    {
        SCOPED_TRACE("gc with snapshot released");
        page_storage->gc();
        EXPECT_EQ(times_remover_called, 3);
    }
}
CATCH


} // namespace PS::V3::tests
} // namespace DB
