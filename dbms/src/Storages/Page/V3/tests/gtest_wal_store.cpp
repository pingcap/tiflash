#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/WAL/WALReader.h>
#include <Storages/Page/V3/WALStore.h>
#include <Storages/Page/V3/tests/entries_helper.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/MockDiskDelegator.h>
#include <TestUtils/TiFlashTestEnv.h>

namespace DB::PS::V3::tests
{
TEST(WALSeriTest, AllPuts)
{
    PageEntryV3 entry_p1{.file_id = 1, .size = 1, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry_p2{.file_id = 1, .size = 2, .offset = 0x123, .checksum = 0x4567};
    PageVersionType ver20(/*seq=*/20);
    PageEntriesEdit edit;
    edit.put(1, entry_p1);
    edit.put(2, entry_p2);

    for (auto & rec : edit.getMutRecords())
        rec.version = ver20;

    auto deseri_edit = DB::PS::V3::ser::deserializeFrom(DB::PS::V3::ser::serializeTo(edit));
    ASSERT_EQ(deseri_edit.size(), 2);
    auto iter = edit.getRecords().begin();
    EXPECT_EQ(iter->page_id, 1);
    EXPECT_EQ(iter->version, ver20);
    EXPECT_TRUE(isSameEntry(iter->entry, entry_p1));
}

TEST(WALSeriTest, PutsAndRefsAndDels)
try
{
    PageEntryV3 entry_p3{.file_id = 1, .size = 3, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry_p5{.file_id = 1, .size = 5, .offset = 0x123, .checksum = 0x4567};
    PageVersionType ver21(/*seq=*/21);
    PageEntriesEdit edit;
    edit.put(3, entry_p3);
    // Mock for edit.ref(4, 3);
    edit.appendRecord(PageEntriesEdit::EditRecord{.type = WriteBatch::WriteType::REF, .page_id = 4, .ori_page_id = 3, .entry = entry_p3});
    edit.put(5, entry_p5);
    edit.del(2);
    edit.del(1);
    edit.del(987);

    for (auto & rec : edit.getMutRecords())
        rec.version = ver21;

    auto deseri_edit = DB::PS::V3::ser::deserializeFrom(DB::PS::V3::ser::serializeTo(edit));
    ASSERT_EQ(deseri_edit.size(), 6);
    auto iter = edit.getRecords().begin();
    EXPECT_EQ(iter->page_id, 3);
    EXPECT_EQ(iter->version, ver21);
    EXPECT_TRUE(isSameEntry(iter->entry, entry_p3));
    iter++;
    EXPECT_EQ(iter->page_id, 4);
    EXPECT_EQ(iter->version, ver21);
    EXPECT_TRUE(isSameEntry(iter->entry, entry_p3));
    iter++;
    EXPECT_EQ(iter->page_id, 5);
    EXPECT_EQ(iter->version, ver21);
    EXPECT_TRUE(isSameEntry(iter->entry, entry_p5));
    iter++;
    EXPECT_EQ(iter->type, WriteBatch::WriteType::DEL);
    EXPECT_EQ(iter->page_id, 2);
    EXPECT_EQ(iter->version, ver21);
    iter++;
    EXPECT_EQ(iter->type, WriteBatch::WriteType::DEL);
    EXPECT_EQ(iter->page_id, 1);
    EXPECT_EQ(iter->version, ver21);
    iter++;
    EXPECT_EQ(iter->type, WriteBatch::WriteType::DEL);
    EXPECT_EQ(iter->page_id, 987);
    EXPECT_EQ(iter->version, ver21);
}
CATCH

TEST(WALSeriTest, Upserts)
{
    PageEntryV3 entry_p1_2{.file_id = 2, .size = 1, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry_p3_2{.file_id = 2, .size = 3, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry_p5_2{.file_id = 2, .size = 5, .offset = 0x123, .checksum = 0x4567};
    PageVersionType ver20_1(/*seq=*/20, /*epoch*/ 1);
    PageVersionType ver21_1(/*seq=*/21, /*epoch*/ 1);
    PageEntriesEdit edit;
    edit.upsertPage(1, ver20_1, entry_p1_2);
    edit.upsertPage(3, ver21_1, entry_p3_2);
    edit.upsertPage(5, ver21_1, entry_p5_2);

    auto deseri_edit = DB::PS::V3::ser::deserializeFrom(DB::PS::V3::ser::serializeTo(edit));
    ASSERT_EQ(deseri_edit.size(), 3);
    auto iter = edit.getRecords().begin();
    EXPECT_EQ(iter->page_id, 1);
    EXPECT_EQ(iter->version, ver20_1);
    EXPECT_TRUE(isSameEntry(iter->entry, entry_p1_2));
    iter++;
    EXPECT_EQ(iter->page_id, 3);
    EXPECT_EQ(iter->version, ver21_1);
    EXPECT_TRUE(isSameEntry(iter->entry, entry_p3_2));
    iter++;
    EXPECT_EQ(iter->page_id, 3);
    EXPECT_EQ(iter->version, ver21_1);
    EXPECT_TRUE(isSameEntry(iter->entry, entry_p5_2));
}


class WALStoreTest : public DB::base::TiFlashStorageTestBasic
{
    void SetUp() override
    {
        auto path = getTemporaryPath();
        dropDataOnDisk(path);

        // TODO: multi-path
        delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(getTemporaryPath());
    }

protected:
    PSDiskDelegatorPtr delegator;
};

TEST_F(WALStoreTest, Empty)
{
    auto ctx = DB::tests::TiFlashTestEnv::getContext();
    auto provider = ctx.getFileProvider();
    auto path = getTemporaryPath();
    size_t num_callback_called = 0;
    auto wal = WALStore::create(
        [&](PageEntriesEdit &&) {
            num_callback_called += 1;
        },
        provider,
        delegator);
    ASSERT_NE(wal, nullptr);
    ASSERT_EQ(num_callback_called, 0);
}

TEST_F(WALStoreTest, ReadWriteRestore)
try
{
    auto ctx = DB::tests::TiFlashTestEnv::getContext();
    auto provider = ctx.getFileProvider();
    auto path = getTemporaryPath();

    // Stage 1. empty
    {
        size_t num_applied_edit = 0;
        auto reader = WALStoreReader::create(provider, delegator);
        for (; reader->remained(); reader->next())
        {
            num_applied_edit += 1;
        }
        EXPECT_EQ(num_applied_edit, 0);
        EXPECT_EQ(reader->logNum(), 0);
    }

    auto wal = WALStore::create([](PageEntriesEdit &&) {}, provider, delegator);
    ASSERT_NE(wal, nullptr);

    // Stage 2. Apply with only puts
    PageEntryV3 entry_p1{.file_id = 1, .size = 1, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry_p2{.file_id = 1, .size = 2, .offset = 0x123, .checksum = 0x4567};
    PageVersionType ver20(/*seq=*/20);
    {
        PageEntriesEdit edit;
        edit.put(1, entry_p1);
        edit.put(2, entry_p2);
        wal->apply(edit, ver20);
    }

    wal.reset();

    {
        size_t num_applied_edit = 0;
        auto reader = WALStoreReader::create(provider, delegator);
        while (reader->remained())
        {
            const auto & [ok, edit] = reader->next();
            ASSERT_TRUE(ok);
            num_applied_edit += 1;
            // Details of each edit is verified in `WALSeriTest`
            ASSERT_EQ(edit.size(), 2);
        }
        EXPECT_EQ(num_applied_edit, 1);
    }

    wal = WALStore::create([](PageEntriesEdit &&) {}, provider, delegator);

    // Stage 3. Apply with puts and refs
    PageEntryV3 entry_p3{.file_id = 1, .size = 3, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry_p5{.file_id = 1, .size = 5, .offset = 0x123, .checksum = 0x4567};
    PageVersionType ver21(/*seq=*/21);
    {
        PageEntriesEdit edit;
        edit.put(3, entry_p3);
        // Mock for edit.ref(4, 3);
        edit.appendRecord(PageEntriesEdit::EditRecord{.type = WriteBatch::WriteType::REF, .page_id = 4, .ori_page_id = 3, .entry = entry_p3});
        edit.put(5, entry_p5);
        edit.del(2);
        wal->apply(edit, ver21);
    }

    wal.reset();

    {
        size_t num_applied_edit = 0;
        auto reader = WALStoreReader::create(provider, delegator);
        while (reader->remained())
        {
            const auto & [ok, edit] = reader->next();
            ASSERT_TRUE(ok);
            num_applied_edit += 1;
            // Details of each edit is verified in `WALSeriTest`
            if (num_applied_edit == 1)
            {
                ASSERT_EQ(edit.size(), 2);
            }
            else if (num_applied_edit == 2)
            {
                ASSERT_EQ(edit.size(), 4);
            }
        }
        EXPECT_EQ(num_applied_edit, 2);
    }

    wal = WALStore::create([](PageEntriesEdit &&) {}, provider, delegator);

    // Stage 4. Apply with delete and upsert
    PageEntryV3 entry_p1_2{.file_id = 2, .size = 1, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry_p3_2{.file_id = 2, .size = 3, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry_p5_2{.file_id = 2, .size = 5, .offset = 0x123, .checksum = 0x4567};
    PageVersionType ver20_1(/*seq=*/20, /*epoch*/ 1);
    PageVersionType ver21_1(/*seq=*/21, /*epoch*/ 1);
    {
        PageEntriesEdit edit;
        edit.upsertPage(1, ver20_1, entry_p1_2);
        edit.upsertPage(3, ver21_1, entry_p3_2);
        edit.upsertPage(5, ver21_1, entry_p5_2);
        wal->apply(edit);
    }

    wal.reset();

    {
        size_t num_applied_edit = 0;
        auto reader = WALStoreReader::create(provider, delegator);
        while (reader->remained())
        {
            const auto & [ok, edit] = reader->next();
            ASSERT_TRUE(ok);
            num_applied_edit += 1;
            // Details of each edit is verified in `WALSeriTest`
            if (num_applied_edit == 1)
            {
                ASSERT_EQ(edit.size(), 2);
            }
            else if (num_applied_edit == 2)
            {
                ASSERT_EQ(edit.size(), 4);
            }
            else if (num_applied_edit == 3)
            {
                ASSERT_EQ(edit.size(), 3);
            }
        }
        EXPECT_EQ(num_applied_edit, 3);
    }
}
CATCH

} // namespace DB::PS::V3::tests
