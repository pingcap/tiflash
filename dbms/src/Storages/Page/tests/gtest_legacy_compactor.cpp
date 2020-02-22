#include <Common/CurrentMetrics.h>
#include <IO/ReadBufferFromMemory.h>
#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/File.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/PatternFormatter.h>
#include <Storages/DeltaMerge/tests/dm_basic_include.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/PageFile.h>
#include <Storages/Page/WriteBatch.h>
#include <common/logger_useful.h>
#include <test_utils/TiflashTestBasic.h>

#define private public
#include <Storages/Page/gc/LegacyCompactor.h>
#undef private

namespace DB
{
namespace tests
{

TEST(LegacyCompactor_test, WriteMultipleBatchRead)
try
{
    PageStorage::Config config;
    Poco::Logger *      log = &Poco::Logger::get("LegacyCompactor_test");

    PageEntriesVersionSetWithDelta original_version(config.version_set_config, log);

    // Prepare a simple version
    {
        PageEntry entry1, entry2;
        // Specify magic checksum for test
        entry1.checksum = 0x123;
        entry2.checksum = 0x321;

        PageEntriesEdit edit;

        edit.put(1, entry1);
        edit.put(2, entry2);
        edit.ref(3, 1);
        edit.del(1);

        // Expected version is:
        //   Page{3} -> NormalPage{1}
        //   Page{2} -> NormalPage{2}
        original_version.apply(edit);
    }

    PageEntriesVersionSetWithDelta version_restored_with_snapshot(config.version_set_config, log);
    // Restore a new version set with snapshot WriteBatch
    {
        auto       snapshot = original_version.getSnapshot();
        WriteBatch wb       = LegacyCompactor::prepareCheckpointWriteBatch(snapshot, 0);

        PageEntriesEdit edit;

        auto writes = wb.getWrites();
        for (auto w : writes)
        {
            if (w.type == WriteBatch::WriteType::UPSERT)
            {
                auto entry = snapshot->version()->findNormalPageEntry(w.page_id);
                if (entry)
                    edit.upsertPage(w.page_id, *entry);
                else
                    FAIL() << "Cannot find specified page";
            }
            else if (w.type == WriteBatch::WriteType::REF)
                edit.ref(w.page_id, w.ori_page_id);
            else
                FAIL() << "Snapshot writes should only contain UPSERT and REF";
        }

        version_restored_with_snapshot.apply(edit);
    }

    // Compare the two versions above
    {
        auto original_snapshot = original_version.getSnapshot();
        auto original          = original_snapshot->version();
        auto restored_snapshot = version_restored_with_snapshot.getSnapshot();
        auto restored          = restored_snapshot->version();

        auto original_normal_page_ids = original->validNormalPageIds();
        auto restored_normal_page_ids = restored->validNormalPageIds();

        ASSERT_EQ(original_normal_page_ids.size(), restored_normal_page_ids.size());

        for (auto id : original_normal_page_ids)
        {
            EXPECT_TRUE(restored_normal_page_ids.find(id) != restored_normal_page_ids.end());

            auto original_page = original->findNormalPageEntry(id);
            auto restored_page = restored->findNormalPageEntry(id);

            ASSERT_TRUE(original_page);
            ASSERT_TRUE(restored_page);

            ASSERT_EQ(original_page->ref, restored_page->ref);

            // Use specified checksum to identify page_entry
            ASSERT_EQ(original_page->checksum, restored_page->checksum);
            if (id == 1)
                ASSERT_EQ(original_page->checksum, 0x123UL);
            else if (id == 2)
                ASSERT_EQ(original_page->checksum, 0x321UL);
            else
                FAIL() << "Invalid normal page id";
        }

        auto original_ref_page_ids = original->validPageIds();
        auto restored_ref_page_ids = restored->validPageIds();

        ASSERT_EQ(original_ref_page_ids.size(), restored_ref_page_ids.size());

        for (auto id : original_ref_page_ids)
        {
            EXPECT_TRUE(restored_ref_page_ids.find(id) != restored_ref_page_ids.end());
        }
    }
}
CATCH

} // namespace tests
} // namespace DB
