#ifndef NDEBUG

#include <IO/WriteHelpers.h>

#define private public
#include <Storages/Page/gc/DataCompactor.h>
#undef private

#include <Storages/Page/PageStorage.h>
#include <Storages/Page/mock/MockUtils.h>
#include <test_utils/TiflashTestBasic.h>

namespace DB
{
namespace tests
{


TEST(DataCompactor_test, MigratePages)
try
{
    TiFlashTestEnv::setupLogger();

    CHECK_TESTS_WITH_DATA_ENABLED;

    PageStorage::Config config;
    config.num_write_slots              = 2;
    const FileProviderPtr file_provider = TiFlashTestEnv::getContext().getFileProvider();
#if 1
    const String test_path = TiFlashTestEnv::findTestDataPath("page_storage_compactor_migrate")[0];
    PageStorage  storage("data_compact_test", test_path, config, file_provider);
#else
    const String test_path = TiFlashTestEnv::getTemporaryPath() + "/data_compactor_test";
    PageStorage  storage("data_compact_test", test_path, config, file_provider);
    storage.restore();
    // Created by these write batches:
    {
        char i               = 0;
        char buf[1024]       = {'\0'};
        auto create_buff_ptr = [&buf, &i](size_t sz) -> ReadBufferPtr {
            buf[0] = i++;
            return std::make_shared<ReadBufferFromMemory>(buf, sz);
        };

        const size_t page_size = 1;
        {
            // This is written to PageFile{1, 0}
            WriteBatch wb;
            wb.putPage(1, 0, create_buff_ptr(page_size), page_size); // page 1, data 0
            storage.write(std::move(wb));
        }
        {
            // This is written to PageFile{2, 0}
            WriteBatch wb;
            wb.putPage(1, 1, create_buff_ptr(page_size), page_size); // new version of page 1, data 1
            wb.putPage(2, 0, create_buff_ptr(page_size), page_size); // page 2, data 2
            wb.putRefPage(3, 2);
            wb.putPage(4, 0, create_buff_ptr(page_size), page_size); // page 4, data 3
            storage.write(std::move(wb));
        }
        {
            // This is written to PageFile{1, 0}
            WriteBatch wb;
            wb.putPage(1, 2, create_buff_ptr(page_size), page_size); // new version of page 1, data 4
            wb.delPage(4);
            wb.putRefPage(5, 3);
            wb.delPage(3);
            wb.putPage(6, 0, create_buff_ptr(page_size), page_size); // page 6, data 5
            storage.write(std::move(wb));
        }
        return;
    }
#endif

    // snapshot contains {1, 2, 6}
    // Not contains 4 since it's deleted.
    auto      snapshot = std::make_shared<MockSnapshot>();
    PageEntry entry;
    entry.file_id = 1;
    entry.tag     = 2;
    snapshot->version()->put(1, entry);
    entry.file_id = 2;
    entry.tag     = 0;
    snapshot->version()->put(2, entry);
    entry.file_id = 1;
    entry.tag     = 0;
    snapshot->version()->put(6, entry);

    // valid_pages
    DataCompactor<MockSnapshotPtr> compactor(storage);
    auto                           valid_pages = DataCompactor<MockSnapshotPtr>::collectValidPagesInPageFile(snapshot);
    ASSERT_EQ(valid_pages.size(), 2UL);

    auto candidates             = PageStorage::listAllPageFiles(test_path, file_provider, storage.page_file_log);
    auto [edits, bytes_written] = compactor.migratePages(snapshot, valid_pages, candidates, 0);
    std::ignore                 = bytes_written;
    ASSERT_EQ(edits.size(), 3UL); // 1, 2, 6
    const PageFileIdAndLevel target_id_lvl{2, 1};
    auto &                   records = edits.getRecords();
    for (size_t i = 0; i < records.size(); ++i)
    {
        const auto & rec = records[i];
        EXPECT_EQ(rec.type, WriteBatch::WriteType::UPSERT);
        // Page 1, 2, 6 is moved to PageFile{2,1}
        if (rec.page_id == 1 || rec.page_id == 2 || rec.page_id == 6)
        {
            EXPECT_EQ(rec.entry.fileIdLevel(), target_id_lvl);
        }
        else
            GTEST_FAIL() << "unknown page_id: " << rec.page_id;
    }

    {
        // Try to recover from disk
        PageStorage ps("data_compact_test", test_path, config, file_provider);
        ps.restore();
        PageEntry entry = ps.getEntry(1);
        EXPECT_EQ(entry.fileIdLevel(), target_id_lvl);
        EXPECT_EQ(entry.tag, 2UL);

        entry = ps.getEntry(2);
        EXPECT_EQ(entry.fileIdLevel(), target_id_lvl);
        EXPECT_EQ(entry.tag, 0UL);

        auto entry5 = ps.getEntry(5);
        EXPECT_EQ(entry5, entry);

        entry = ps.getEntry(3);
        ASSERT_FALSE(entry.isValid());

        entry = ps.getEntry(4);
        ASSERT_FALSE(entry.isValid());

        entry = ps.getEntry(6);
        EXPECT_EQ(entry.fileIdLevel(), target_id_lvl);
        EXPECT_EQ(entry.tag, 0UL);
    }
}
CATCH

} // namespace tests
} // namespace DB

#endif // NDEBUG
