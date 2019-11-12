#include "gtest/gtest.h"

#include <Common/CurrentMetrics.h>
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
        return std::make_shared<PageStorage>("test.t", path, config_);
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

TEST_F(PageStorage_test, WriteReadGcExternalPage)
try
{
    PageStorage::Config tmp_config(config);
    tmp_config.merge_hint_low_used_file_num = 0; // each time will run gc
    storage                                 = reopenWithConfig(tmp_config);

    {
        WriteBatch batch;
        batch.putExternal(0, 0);
        batch.putRefPage(1, 0);
        batch.putExternal(1024, 0);
        storage->write(batch);
    }

    PageStorage::ExternalPagesScanner scanner = []() -> std::set<PageId> { return {}; };
    PageStorage::ExternalPagesRemover remover = [](const std::set<PageId> &, const std::set<PageId> & normal_page_ids) -> void {
        ASSERT_EQ(normal_page_ids.size(), 2UL);
        EXPECT_GT(normal_page_ids.count(0), 0UL);
        EXPECT_GT(normal_page_ids.count(1024), 0UL);
    };
    storage->registerExternalPagesCallbacks(scanner, remover);
    {
        SCOPED_TRACE("fist gc");
        storage->gc();
    }

    auto snapshot = storage->getSnapshot();

    {
        WriteBatch batch;
        batch.putRefPage(2, 1); // ref 2 -> 1 -> 0
        batch.delPage(1);       // free ref 1 -> 0
        batch.delPage(1024);    // free normal page 1024
        storage->write(batch);
    }

    {
        SCOPED_TRACE("gc with snapshot");
        storage->gc();
    }

    {
        Page page0 = storage->read(0);
        ASSERT_EQ(page0.data.size(), 0UL);
        ASSERT_EQ(page0.page_id, 0UL);

        Page page2 = storage->read(2);
        ASSERT_EQ(page2.data.size(), 0UL);
        ASSERT_EQ(page2.page_id, 2UL);
    }

    snapshot.reset();
    remover = [](const std::set<PageId> &, const std::set<PageId> & normal_page_ids) -> void {
        ASSERT_EQ(normal_page_ids.size(), 1UL);
        EXPECT_GT(normal_page_ids.count(0), 0UL);
    };
    storage->registerExternalPagesCallbacks(scanner, remover);
    {
        SCOPED_TRACE("gc with snapshot released");
        storage->gc();
    }
}
catch (const Exception & e)
{
    std::string text = e.displayText();

    auto embedded_stack_trace_pos = text.find("Stack trace");
    std::cerr << "Code: " << e.code() << ". " << text << std::endl << std::endl;
    if (std::string::npos == embedded_stack_trace_pos)
        std::cerr << "Stack trace:" << std::endl << e.getStackTrace().toString() << std::endl;

    throw;
}

TEST_F(PageStorage_test, IdempotentDelAndRef)
{
    const size_t buf_sz = 1024;
    char         c_buff[buf_sz];

    {
        // Page1 should be written to PageFile{1, 0}
        WriteBatch batch;
        memset(c_buff, 0xf, buf_sz);
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(1, 0, buff, buf_sz);

        storage->write(batch);
    }

    {
        // RefPage 2 -> 1, Del Page 1 should be written to PageFile{2, 0}
        WriteBatch batch;
        batch.putRefPage(2, 1);
        batch.delPage(1);
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(1000, 0, buff, buf_sz);

        storage->write(batch);
    }

    {
        // Another RefPage 2 -> 1, Del Page 1 should be written to PageFile{3, 0}
        WriteBatch batch;
        batch.putRefPage(2, 1);
        batch.delPage(1);

        storage->write(batch);
    }

    {
        auto snap      = storage->getSnapshot();
        auto ref_entry = snap->version()->find(1);
        ASSERT_FALSE(ref_entry);

        ref_entry = snap->version()->find(2);
        ASSERT_TRUE(ref_entry);
        ASSERT_EQ(ref_entry->file_id, 1UL);
        ASSERT_EQ(ref_entry->ref, 1UL);

        auto normal_entry = snap->version()->findNormalPageEntry(1);
        ASSERT_TRUE(normal_entry);
        ASSERT_EQ(normal_entry->file_id, 1UL);
        ASSERT_EQ(normal_entry->ref, 1UL);

        // Point to the same entry
        ASSERT_EQ(ref_entry->offset, normal_entry->offset);
    }

    storage = reopenWithConfig(config);

    {
        auto snap      = storage->getSnapshot();
        auto ref_entry = snap->version()->find(1);
        ASSERT_FALSE(ref_entry);

        ref_entry = snap->version()->find(2);
        ASSERT_TRUE(ref_entry);
        ASSERT_EQ(ref_entry->file_id, 1UL);
        ASSERT_EQ(ref_entry->ref, 1UL);

        auto normal_entry = snap->version()->findNormalPageEntry(1);
        ASSERT_TRUE(normal_entry);
        ASSERT_EQ(normal_entry->file_id, 1UL);
        ASSERT_EQ(normal_entry->ref, 1UL);

        // Point to the same entry
        ASSERT_EQ(ref_entry->offset, normal_entry->offset);
    }
}

TEST_F(PageStorage_test, DISABLED_GcMigrateValidRefPages)
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
    const PageEntriesEdit gc_file_edit = storage->gcMigratePages(snapshot, lives_pages, candidates, 3);
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

TEST_F(PageStorage_test, GcMoveNormalPage)
{
    const size_t buf_sz = 256;
    char         c_buff[buf_sz];

    {
        WriteBatch batch;
        memset(c_buff, 0xf, buf_sz);
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        // Page1, RefPage2 -> 1, RefPage3 -> 2
        batch.putPage(1, 0, buff, buf_sz);
        batch.putRefPage(2, 1);
        batch.putRefPage(3, 2);
        // DelPage1
        batch.delPage(1);

        storage->write(batch);
    }

    PageFileIdAndLevel        id_and_lvl = {1, 0}; // PageFile{1, 0} is ready to be migrated by gc
    PageStorage::GcLivesPages livesPages{{id_and_lvl,
                                          {buf_sz,
                                           {
                                               1,
                                           }}}};
    PageStorage::GcCandidates candidates{
        id_and_lvl,
    };
    auto            s0         = storage->getSnapshot();
    auto            page_files = PageStorage::listAllPageFiles(storage->storage_path, true, true, storage->page_file_log);
    PageEntriesEdit edit       = storage->gcMigratePages(s0, livesPages, candidates, 1);
    s0.reset();
    auto [live_files, live_normal_pages] = storage->versioned_page_entries.gcApply(edit);
    ASSERT_EQ(live_normal_pages.size(), 1UL);
    ASSERT_GT(live_normal_pages.count(1), 0UL);
    ASSERT_EQ(live_files.size(), 1UL);
    ASSERT_EQ(live_files.count(id_and_lvl), 0UL);
    storage->gcRemoveObsoleteData(page_files, {2, 0}, live_files);

    // reopen PageStorage, RefPage 3 -> 1 is still valid
    storage = reopenWithConfig(config);
    auto s1 = storage->getSnapshot();

    auto [is_ref, normal_page_id] = s1->version()->isRefId(3);
    ASSERT_TRUE(is_ref);
    ASSERT_EQ(normal_page_id, 1UL);

    std::tie(is_ref, normal_page_id) = s1->version()->isRefId(2);
    ASSERT_TRUE(is_ref);
    ASSERT_EQ(normal_page_id, 1UL);

    // Page 1 is deleted.
    auto entry1 = s1->version()->find(1);
    ASSERT_FALSE(entry1);

    // Normal page 1 is moved to PageFile_1_1
    entry1 = s1->version()->findNormalPageEntry(normal_page_id);
    ASSERT_TRUE(entry1);
    ASSERT_EQ(entry1->fileIdLevel().first, 1UL);
    ASSERT_EQ(entry1->fileIdLevel().second, 1UL);

    Page page = storage->read(3, s1);
    ASSERT_EQ(page.data.size(), buf_sz);

    page = storage->read(2, s1);
    ASSERT_EQ(page.data.size(), buf_sz);
}

TEST_F(PageStorage_test, GcMoveRefPage)
{
    const size_t buf_sz = 256;
    char         c_buff[buf_sz];

    {
        WriteBatch batch;
        memset(c_buff, 0xf, buf_sz);
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(1, 0, buff, buf_sz);
        batch.putRefPage(2, 1);
        batch.putRefPage(3, 2);

        batch.delPage(2);

        storage->write(batch);
    }

    PageFileIdAndLevel        id_and_lvl = {1, 0}; // PageFile{1, 0} is ready to be migrated by gc
    PageStorage::GcLivesPages livesPages{{id_and_lvl,
                                          {buf_sz,
                                           {
                                               1,
                                           }}}};
    PageStorage::GcCandidates candidates{
        id_and_lvl,
    };
    auto            s0   = storage->getSnapshot();
    PageEntriesEdit edit = storage->gcMigratePages(s0, livesPages, candidates, 2);
    s0.reset();

    // reopen PageStorage, RefPage 3 -> 1 is still valid
    storage                       = reopenWithConfig(config);
    auto s1                       = storage->getSnapshot();
    auto [is_ref, normal_page_id] = s1->version()->isRefId(3);
    ASSERT_TRUE(is_ref);
    ASSERT_EQ(normal_page_id, 1UL);
}

TEST_F(PageStorage_test, GcMovePageDelMeta)
{
    const size_t buf_sz = 256;
    char         c_buff[buf_sz];

    {
        // Page1 should be written to PageFile{1, 0}
        WriteBatch batch;
        memset(c_buff, 0xf, buf_sz);
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(1, 0, buff, buf_sz);
        buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(2, 0, buff, buf_sz);
        buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(3, 0, buff, buf_sz);

        storage->write(batch);
    }

    {
        // DelPage1 should be written to PageFile{2, 0}
        WriteBatch batch;
        batch.delPage(1);

        storage->write(batch);
    }

    PageFileIdAndLevel        id_and_lvl = {2, 0}; // PageFile{2, 0} is ready to be migrated by gc
    PageStorage::GcLivesPages livesPages{{id_and_lvl, {0, {}}}};
    PageStorage::GcCandidates candidates{
        id_and_lvl,
    };
    auto            page_files = PageStorage::listAllPageFiles(storage->storage_path, true, true, storage->page_file_log);
    auto            s0         = storage->getSnapshot();
    PageEntriesEdit edit       = storage->gcMigratePages(s0, livesPages, candidates, 2);
    s0.reset();

    auto [live_files, live_normal_pages] = storage->versioned_page_entries.gcApply(edit);
    EXPECT_EQ(live_files.find(id_and_lvl), live_files.end());
    EXPECT_EQ(live_normal_pages.size(), 2UL);
    EXPECT_GT(live_normal_pages.count(2), 0UL);
    EXPECT_GT(live_normal_pages.count(3), 0UL);
    storage->gcRemoveObsoleteData(/* page_files= */ page_files, /* writing_file_id_level= */ {3, 0}, live_files);

    // reopen PageStorage, Page 1 should be deleted
    storage = reopenWithConfig(config);
    auto s1 = storage->getSnapshot();
    ASSERT_EQ(s1->version()->find(1), std::nullopt);
}

TEST_F(PageStorage_test, GcMigrateRefPageToRefPage)
try
{
    PageId page_id = 1;

    const size_t buf_sz = 256;
    char         c_buff[buf_sz];

    {
        // Page1 should be written to PageFile{1, 0}
        WriteBatch batch;
        memset(c_buff, 0xf, buf_sz);
        ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
        batch.putPage(page_id, 0, buff, buf_sz);
        // RefPage2 -> Page1, RefPage3 -> Page1, then del Page1
        batch.putRefPage(2, page_id);
        batch.putRefPage(3, page_id);
        batch.delPage(page_id);
        // RefPage4 -> 2 -> Page1, RefPage5 -> 3 -> Page 1, then del Page2, Page3
        batch.putRefPage(4, 2);
        batch.putRefPage(5, 3);
        batch.delPage(2);
        batch.delPage(3);

        storage->write(batch);
    }

    PageFileIdAndLevel id_and_lvl = {1, 0}; // PageFile{1, 0} is ready to be migrated by gc

    auto            page_files = PageStorage::listAllPageFiles(storage->storage_path, true, true, storage->page_file_log);
    PageEntriesEdit edit;
    {
        // migrate PageFile{1, 0} -> PageFile{1, 1}
        auto snapshot = storage->getSnapshot();

        PageStorage::GcLivesPages livesPages{{id_and_lvl, {256, {page_id}}}};
        PageStorage::GcCandidates candidates{id_and_lvl};
        edit = storage->gcMigratePages(snapshot, livesPages, candidates, 0);
    }

    {
        // check that now only PageFile{1, 1} is live and remove PageFile{1, 0}
        auto [live_files, live_normal_pages] = storage->versioned_page_entries.gcApply(edit);
        ASSERT_EQ(live_files.size(), 1UL);
        ASSERT_EQ(*live_files.begin(), PageFileIdAndLevel(1, 1));
        EXPECT_EQ(live_normal_pages.size(), 1UL);
        EXPECT_GT(live_normal_pages.count(1), 0UL);

        storage->gcRemoveObsoleteData(page_files, {2, 0}, live_files);
    }

    // reload to check if there is any exception
    EXPECT_NO_THROW(storage = reopenWithConfig(config));
    storage = reopenWithConfig(config);
}
catch (const Exception & e)
{
    std::string text = e.displayText();

    auto embedded_stack_trace_pos = text.find("Stack trace");
    std::cerr << "Code: " << e.code() << ". " << text << std::endl << std::endl;
    if (std::string::npos == embedded_stack_trace_pos)
        std::cerr << "Stack trace:" << std::endl << e.getStackTrace().toString() << std::endl;

    throw;
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

namespace
{

CurrentMetrics::Value getPSMVCCNumSnapshots()
{
    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
    {
        if (i == CurrentMetrics::PSMVCCNumSnapshots)
        {
            return CurrentMetrics::values[i].load(std::memory_order_relaxed);
        }
    }
    throw Exception(std::string(CurrentMetrics::getDescription(CurrentMetrics::PSMVCCNumSnapshots)) + " not found.");
}

} // namespace


TEST_F(PageStorageWith2Pages_test, SnapshotReadSnapshotVersion)
{
    char ch_before = 0x01;
    char ch_update = 0xFF;

    EXPECT_EQ(getPSMVCCNumSnapshots(), 0);
    auto snapshot = storage->getSnapshot();
    EXPECT_EQ(getPSMVCCNumSnapshots(), 1);
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
    EXPECT_EQ(getPSMVCCNumSnapshots(), 0);
    auto s1 = storage->getSnapshot();
    EXPECT_EQ(getPSMVCCNumSnapshots(), 1);
    auto s2 = storage->getSnapshot();
    EXPECT_EQ(getPSMVCCNumSnapshots(), 2);
    auto s3 = storage->getSnapshot();
    EXPECT_EQ(getPSMVCCNumSnapshots(), 3);

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
    EXPECT_EQ(getPSMVCCNumSnapshots(), 2);

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
    EXPECT_EQ(getPSMVCCNumSnapshots(), 1);

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
    EXPECT_EQ(getPSMVCCNumSnapshots(), 0);

    // without snapshot
    p1_entry = storage->getEntry(1);
    ASSERT_NE(p1_entry.checksum, p1_snapshot_entry.checksum);
}

} // namespace tests
} // namespace DB
