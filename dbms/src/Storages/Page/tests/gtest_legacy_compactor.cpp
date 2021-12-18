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
#include <Storages/PathPool.h>
#include <common/logger_useful.h>
#include <TestUtils/TiFlashTestBasic.h>

#define private public
#include <Storages/Page/gc/LegacyCompactor.h>
#undef private

#include <Storages/Page/gc/restoreFromCheckpoints.h>

namespace DB
{
namespace tests
{

TEST(LegacyCompactor_test, WriteMultipleBatchRead)
try
{
    PageStorage::Config config;
    Poco::Logger *      log = &Poco::Logger::get("LegacyCompactor_test");

    PageEntriesVersionSetWithDelta original_version("test", config.version_set_config, log);

    // Prepare a simple version
    {
        PageEntry entry1, entry2;
        // Specify magic checksum for test
        entry1.checksum = 0x123;
        entry1.offset   = 0x111;
        entry2.checksum = 0x321;
        entry2.offset   = 0x222;

        entry1.field_offsets = {{0, 0x11}, {255, 0xff}};
        entry1.size          = 1024;

        entry2.field_offsets = {{0, 0xdd}, {16, 0xaa}, {77, 0x77}};
        entry2.size          = 1010;

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

    PageEntriesVersionSetWithDelta version_restored_with_snapshot("test", config.version_set_config, log);
    // Restore a new version set with snapshot WriteBatch
    WriteBatch::SequenceID seq_write = 0x1234;
    {
        auto       snapshot = original_version.getSnapshot();
        WriteBatch wb       = LegacyCompactor::prepareCheckpointWriteBatch(snapshot, seq_write);
        EXPECT_EQ(wb.getSequence(), seq_write);

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
            {
                ASSERT_EQ(original_page->checksum, 0x123UL);
                ASSERT_EQ(original_page->offset, 0x111UL);
                ASSERT_EQ(original_page->field_offsets.size(), 2UL);
                ASSERT_EQ(original_page->field_offsets[0].first, 0UL);
                ASSERT_EQ(original_page->field_offsets[0].second, 0x11UL);
                ASSERT_EQ(original_page->field_offsets[1].first, 255UL);
                ASSERT_EQ(original_page->field_offsets[1].second, 0xffUL);
                ASSERT_EQ(original_page->size, 1024UL);
            }
            else if (id == 2)
            {
                ASSERT_EQ(original_page->checksum, 0x321UL);
                ASSERT_EQ(original_page->offset, 0x222UL);
                ASSERT_EQ(original_page->field_offsets.size(), 3UL);
                ASSERT_EQ(original_page->field_offsets[0].first, 0UL);
                ASSERT_EQ(original_page->field_offsets[0].second, 0xddUL);
                ASSERT_EQ(original_page->field_offsets[1].first, 16UL);
                ASSERT_EQ(original_page->field_offsets[1].second, 0xaaUL);
                ASSERT_EQ(original_page->field_offsets[2].first, 77UL);
                ASSERT_EQ(original_page->field_offsets[2].second, 0x77UL);
                ASSERT_EQ(original_page->size, 1010UL);
            }
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

// TODO: enable this test
TEST(LegacyCompactor_test, DISABLED_CompactAndRestore)
try
{
    auto                  ctx           = TiFlashTestEnv::getContext();
    const FileProviderPtr file_provider = ctx.getFileProvider();
    StoragePathPool       spool         = ctx.getPathPool().withTable("test", "t", false);
    auto                  delegator     = spool.getPSDiskDelegatorSingle("meta");
    PageStorage           storage("compact_test", delegator, PageStorage::Config{}, file_provider);

    PageStorage::ListPageFilesOption opt;
    opt.ignore_checkpoint = false;
    opt.ignore_legacy     = false;
    opt.remove_tmp_files  = false;
    auto page_files       = PageStorage::listAllPageFiles(file_provider, delegator, storage.page_file_log, opt);

    LegacyCompactor compactor(storage);
    auto && [page_files_left, page_files_compacted, bytes_written] = compactor.tryCompact(std::move(page_files), {});
    (void)page_files_left;
    (void)bytes_written;
    ASSERT_EQ(page_files_compacted.size(), 4UL);

    // TODO:
    PageFile page_file
        = PageFile::openPageFileForRead(7, 0, delegator->defaultPath(), file_provider, PageFile::Type::Checkpoint, storage.page_file_log);
    ASSERT_TRUE(page_file.isExist());

    PageStorage::MetaMergingQueue mergine_queue;
    {
        if (auto reader = PageFile::MetaMergingReader::createFrom(page_file); //
            reader->hasNext())
        {
            reader->moveNext();
            mergine_queue.push(std::move(reader));
        }
    }

    DB::PageStorage::StatisticsInfo   debug_info;
    PageStorage::VersionedPageEntries vset_restored("restore_vset", storage.config.version_set_config, storage.log);
    auto && [old_checkpoint_file, old_checkpoint_sequence, page_files_to_remove]
        = restoreFromCheckpoints(mergine_queue, vset_restored, debug_info, "restore_test", storage.log);
    (void)old_checkpoint_file;
    (void)old_checkpoint_sequence;
    (void)page_files_to_remove;

    {
        auto s0 = compactor.version_set.getSnapshot();
        auto s1 = vset_restored.getSnapshot();
        ASSERT_EQ(s0->version()->numPages(), s1->version()->numPages());
        ASSERT_EQ(s0->version()->numNormalPages(), s1->version()->numNormalPages());

        auto   page_ids  = s0->version()->validPageIds();
        size_t num_pages = 0;
        for (auto page_id : page_ids)
        {
            auto entry0 = s0->version()->find(page_id);
            ASSERT_TRUE(entry0);
            auto entry1 = s1->version()->find(page_id);
            ASSERT_TRUE(entry1);
            ASSERT_EQ(entry0->fileIdLevel(), entry1->fileIdLevel());
            ASSERT_EQ(entry0->offset, entry1->offset);
            ASSERT_EQ(entry0->size, entry1->size);
            ASSERT_EQ(entry0->tag, entry1->tag);
            // TODO: compare
            // entry0->field_offsets

            num_pages += 1;
        }

        LOG_INFO(storage.log, "All " << num_pages << " are consist.");
    }
}
CATCH

} // namespace tests
} // namespace DB
