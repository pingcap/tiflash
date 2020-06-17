#ifndef NDEBUG

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

    PageStorage::Config config;
    config.num_write_slots = 2;
#if 1
    const String        test_path = TiFlashTestEnv::findTestDataPath("page_storage_compactor_migrate")[0];
    PageStorage storage("data_compact_test", test_path, config);
#else
    const String test_path = TiFlashTestEnv::getTemporaryPath() + "/data_compactor_test";
    PageStorage  storage("data_compact_test", test_path, config);
    storage.restore();
    // Created by these write batches:
    {
        char buf[1024] = {'\0'};
        {
            WriteBatch wb;
            wb.putPage(1, 0, std::make_shared<ReadBufferFromMemory>(buf, 0), 0);
            storage.write(std::move(wb));
        }
        {
            WriteBatch wb;
            wb.putPage(2, 0, std::make_shared<ReadBufferFromMemory>(buf, 0), 0);
            wb.putRefPage(3, 2);
            wb.putPage(4, 0, std::make_shared<ReadBufferFromMemory>(buf, 0), 0);
            storage.write(std::move(wb));
        }
        {
            WriteBatch wb;
            wb.delPage(4);
            wb.putRefPage(5, 3);
            wb.delPage(3);
            wb.putPage(6, 0, std::make_shared<ReadBufferFromMemory>(buf, 0), 0);
            storage.write(std::move(wb));
        }
        return;
    }
#endif

    // snapshot
    auto snapshot = std::make_shared<MockSnapshot>();
    snapshot->version()->put(1, PageEntry{.file_id = 1});
    snapshot->version()->put(2, PageEntry{.file_id = 2});
    snapshot->version()->put(6, PageEntry{.file_id = 1});


    // valid_pages
    DataCompactor<MockSnapshotPtr> compactor(storage);
    auto                           valid_pages = DataCompactor<MockSnapshotPtr>::collectValidPagesInPageFile(snapshot);
    ASSERT_EQ(valid_pages.size(), 2UL);

    auto candidates             = PageStorage::listAllPageFiles(test_path, storage.page_file_log);
    auto [edits, bytes_written] = compactor.migratePages(snapshot, valid_pages, candidates, 0);
    ASSERT_EQ(edits.size(), 3UL); // 1, 2, 6
    auto & records = edits.getRecords();
    for (size_t i = 0; i < records.size(); ++i)
    {
        const auto & rec = records[i];
        EXPECT_EQ(rec.type, WriteBatch::WriteType::UPSERT);
        // Page 1, 2, 6 is moved to PageFile{2,1}
        if (rec.page_id == 1 || rec.page_id == 2 || rec.page_id == 6)
        {
            EXPECT_EQ(rec.entry.file_id, 2UL);
            EXPECT_EQ(rec.entry.level, 1U);
        }
        else
            GTEST_FAIL() << "unknown page_id: " << rec.page_id;
    }
}
CATCH

} // namespace tests
} // namespace DB

#endif // NDEBUG
