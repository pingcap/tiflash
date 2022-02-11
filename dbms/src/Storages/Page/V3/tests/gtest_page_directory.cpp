#include <Common/Exception.h>
#include <Common/FmtUtils.h>
#include <Encryption/FileProvider.h>
#include <IO/WriteHelpers.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/WALStore.h>
#include <Storages/Page/V3/tests/entries_helper.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/MockDiskDelegator.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <common/types.h>
#include <fmt/format.h>

#include <memory>

namespace DB
{
namespace PS::V3::tests
{
TEST(ExternalPageHolderTest, Visible)
{
    ExternalPageHolder h(10, 3);
    EXPECT_EQ(h.deleted_sequence, 0);
    EXPECT_FALSE(h.isVisible(1));
    EXPECT_FALSE(h.isVisible(2));
    EXPECT_TRUE(h.isVisible(3));
    EXPECT_TRUE(h.isVisible(4));
    EXPECT_TRUE(h.isVisible(5));
    EXPECT_TRUE(h.isVisible(6));
    EXPECT_TRUE(h.isVisible(0xFFFFFFFF));

    h.deleted_sequence = 5;
    EXPECT_FALSE(h.isVisible(1));
    EXPECT_FALSE(h.isVisible(2));
    EXPECT_TRUE(h.isVisible(3));
    EXPECT_TRUE(h.isVisible(4));
    EXPECT_FALSE(h.isVisible(5));
    EXPECT_FALSE(h.isVisible(6));
    EXPECT_FALSE(h.isVisible(0xFFFFFFFF));
}

TEST(ExternalMapTest, CreateDelete)
{
    ExternalMap m;
    PageId page_id = 10;
    EXPECT_ANY_THROW(m.getNormalPageId(page_id, 50));

    EXPECT_TRUE(m.createExternal(page_id, 50));
    EXPECT_ANY_THROW(m.getNormalPageId(page_id, 49));
    EXPECT_EQ(page_id, m.getNormalPageId(page_id, 50));
    EXPECT_EQ(page_id, m.getNormalPageId(page_id, 0xFFFFFFFF));

    EXPECT_TRUE(m.tryCreateDel(page_id, 51));
    EXPECT_EQ(page_id, m.getNormalPageId(page_id, 50));
    EXPECT_ANY_THROW(m.getNormalPageId(page_id, 52));

    EXPECT_FALSE(m.tryCreateDel(page_id + 1, 51));

    auto ids = m.getPendingRemoveIDs();
    ASSERT_EQ(1, ids.size());
    EXPECT_EQ(page_id, *ids.begin());

    // again, should remove nothing
    ids = m.getPendingRemoveIDs();
    ASSERT_TRUE(ids.empty());

    EXPECT_EQ(m.numPages(), 1);
    m.cleanUpHolders(49);
    EXPECT_EQ(m.numPages(), 1);

    m.cleanUpHolders(50);
    EXPECT_EQ(m.numPages(), 1);

    m.cleanUpHolders(51);
    EXPECT_EQ(m.numPages(), 0);
}

TEST(ExternalMapTest, CreateDelete2)
{
    ExternalMap m;
    PageId page_id = 10;
    PageId page_id2 = 11;
    EXPECT_ANY_THROW(m.getNormalPageId(page_id, 50));
    EXPECT_ANY_THROW(m.getNormalPageId(page_id2, 50));

    EXPECT_TRUE(m.createExternal(page_id, 50));
    EXPECT_ANY_THROW(m.getNormalPageId(page_id, 49));
    EXPECT_EQ(page_id, m.getNormalPageId(page_id, 50));
    EXPECT_EQ(page_id, m.getNormalPageId(page_id, 0xFFFFFFFF));

    EXPECT_TRUE(m.createExternal(page_id2, 51));
    EXPECT_ANY_THROW(m.getNormalPageId(page_id2, 50));
    EXPECT_EQ(page_id2, m.getNormalPageId(page_id2, 51));
    EXPECT_EQ(page_id2, m.getNormalPageId(page_id2, 0xFFFFFFFF));

    EXPECT_TRUE(m.tryCreateDel(page_id, 51));
    EXPECT_EQ(page_id, m.getNormalPageId(page_id, 50));
    EXPECT_ANY_THROW(m.getNormalPageId(page_id, 52));

    EXPECT_FALSE(m.tryCreateDel(page_id + 2, 51));

    auto ids = m.getPendingRemoveIDs();
    ASSERT_EQ(1, ids.size());
    EXPECT_EQ(page_id, *ids.begin());

    // again
    ids = m.getPendingRemoveIDs();
    ASSERT_TRUE(ids.empty());

    EXPECT_EQ(m.numPages(), 2);
    m.cleanUpHolders(49);
    EXPECT_EQ(m.numPages(), 2);

    m.cleanUpHolders(50);
    EXPECT_EQ(m.numPages(), 2);

    m.cleanUpHolders(51);
    EXPECT_EQ(m.numPages(), 1);
}

TEST(ExternalMapTest, CreateRefDelete)
{
    ExternalMap m;
    PageId page_id = 10;
    PageId ref_id1 = 11;
    PageId ref_id2 = 12;
    EXPECT_TRUE(m.createExternal(page_id, 50));
    // can not create ref on non-exist external page
    EXPECT_FALSE(m.tryCreateRef(ref_id1, 999, 51));

    EXPECT_TRUE(m.tryCreateRef(ref_id1, page_id, 51));
    EXPECT_TRUE(m.tryCreateRef(ref_id2, page_id, 51));
    EXPECT_EQ(page_id, m.getNormalPageId(ref_id1, 51));
    EXPECT_EQ(page_id, m.getNormalPageId(ref_id2, 51));

    EXPECT_TRUE(m.tryCreateDel(page_id, 51));
    EXPECT_EQ(page_id, m.getNormalPageId(ref_id1, 51));
    EXPECT_EQ(page_id, m.getNormalPageId(ref_id2, 51));

    auto ids = m.getPendingRemoveIDs();
    EXPECT_TRUE(ids.empty());
    EXPECT_EQ(m.numPages(), 3);
    m.cleanUpHolders(51);
    EXPECT_EQ(m.numPages(), 2);

    EXPECT_TRUE(m.tryCreateDel(ref_id1, 52));
    EXPECT_FALSE(m.tryCreateDel(ref_id1, 52)); // delete ref again, should be ok
    ids = m.getPendingRemoveIDs();
    EXPECT_TRUE(ids.empty());
    EXPECT_EQ(m.numPages(), 2);
    m.cleanUpHolders(52);
    EXPECT_EQ(m.numPages(), 1);

    EXPECT_TRUE(m.tryCreateDel(ref_id2, 53));
    EXPECT_FALSE(m.tryCreateDel(ref_id2, 54)); // delete ref again, should be ok
    ids = m.getPendingRemoveIDs();
    ASSERT_EQ(1, ids.size());
    ASSERT_EQ(page_id, *ids.begin());
    ids = m.getPendingRemoveIDs(); // clean again, should be empty
    EXPECT_TRUE(ids.empty());
    EXPECT_EQ(m.numPages(), 1);
    m.cleanUpHolders(53);
    EXPECT_EQ(m.numPages(), 0);
}

TEST(ExternalMapTest, CreateAgainAfterRef)
{
    ExternalMap m;
    PageId page_id = 10;
    PageId ref_id1 = 11;
    PageId ref_id2 = 12;
    EXPECT_TRUE(m.createExternal(page_id, 50));

    EXPECT_TRUE(m.tryCreateRef(ref_id1, page_id, 51));
    EXPECT_TRUE(m.tryCreateRef(ref_id2, page_id, 51));
    EXPECT_EQ(page_id, m.getNormalPageId(ref_id1, 51));
    EXPECT_EQ(page_id, m.getNormalPageId(ref_id2, 51));

    EXPECT_FALSE(m.createExternal(page_id, 51));

    EXPECT_TRUE(m.tryCreateDel(page_id, 52));
    EXPECT_EQ(page_id, m.getNormalPageId(ref_id1, 52));
    EXPECT_EQ(page_id, m.getNormalPageId(ref_id2, 52));

    EXPECT_FALSE(m.createExternal(page_id, 52)); // should be fail

    auto ids = m.getPendingRemoveIDs();
    EXPECT_TRUE(ids.empty());
    EXPECT_EQ(m.numPages(), 3);
    m.cleanUpHolders(52);
    EXPECT_EQ(m.numPages(), 2);

    EXPECT_FALSE(m.createExternal(page_id, 52)); // should be fail tought we cleanup

    EXPECT_TRUE(m.tryCreateDel(ref_id1, 53));
    EXPECT_FALSE(m.tryCreateDel(ref_id1, 53)); // delete ref again(same seq), should be ok
    ids = m.getPendingRemoveIDs();
    EXPECT_TRUE(ids.empty());
    EXPECT_EQ(m.numPages(), 2);
    m.cleanUpHolders(53);
    EXPECT_EQ(m.numPages(), 1);

    EXPECT_TRUE(m.tryCreateDel(ref_id2, 54));
    EXPECT_FALSE(m.tryCreateDel(ref_id2, 55)); // delete ref again(incr seq), should be ok
    ids = m.getPendingRemoveIDs(); // clean the page_id since its ref-count is down to 0
    ASSERT_EQ(1, ids.size());
    ASSERT_EQ(page_id, *ids.begin());
    ids = m.getPendingRemoveIDs(); // clean again, should be empty
    EXPECT_TRUE(ids.empty());
    EXPECT_EQ(m.numPages(), 1);
    m.cleanUpHolders(54);
    EXPECT_EQ(m.numPages(), 0);

    EXPECT_TRUE(m.createExternal(page_id, 55)); // this is ok after all old external and old ref cleaned.
    EXPECT_TRUE(m.tryCreateDel(page_id, 55));
    ids = m.getPendingRemoveIDs();
    ASSERT_EQ(1, ids.size());
    ASSERT_EQ(page_id, *ids.begin());
    m.cleanUpHolders(55);
    EXPECT_EQ(m.numPages(), 0);
}

TEST(ExternalMapTest, CreateRefOnRef)
{
    ExternalMap m;
    PageId page_id = 10;
    PageId ref_id1 = 11;
    PageId ref_id2 = 12;
    EXPECT_TRUE(m.createExternal(page_id, 50));
    EXPECT_TRUE(m.tryCreateRef(ref_id1, page_id, 51));
    EXPECT_TRUE(m.tryCreateRef(ref_id2, page_id, 51));
    EXPECT_EQ(page_id, m.getNormalPageId(ref_id1, 51));
    EXPECT_EQ(page_id, m.getNormalPageId(ref_id2, 51));

    // 13 -> 11, 11 -> 10 ==> 13 -> 10
    PageId ref_on_ref_id3 = 13;
    EXPECT_TRUE(m.tryCreateRef(ref_on_ref_id3, ref_id1, 52));
    EXPECT_EQ(page_id, m.getNormalPageId(ref_on_ref_id3, 52));

    // remove to check the ref count
    EXPECT_TRUE(m.tryCreateDel(page_id, 54));
    EXPECT_TRUE(m.tryCreateDel(ref_id1, 54));
    EXPECT_TRUE(m.tryCreateDel(ref_id2, 54));

    auto ids = m.getPendingRemoveIDs();
    ASSERT_TRUE(ids.empty());
    m.cleanUpHolders(54);
    EXPECT_EQ(m.numPages(), 1);

    EXPECT_TRUE(m.tryCreateDel(ref_on_ref_id3, 55));
    ids = m.getPendingRemoveIDs();
    ASSERT_EQ(1, ids.size());
    EXPECT_EQ(page_id, *ids.begin());
    m.cleanUpHolders(55);
    EXPECT_EQ(m.numPages(), 0);
}

TEST(ExternalMapTest, CreateInvaildRef)
{
    ExternalMap m;
    PageId page_id = 10;
    PageId ref_id1 = 11;
    PageId ref_id2 = 12;
    EXPECT_TRUE(m.createExternal(page_id, 50));
    EXPECT_TRUE(m.tryCreateRef(ref_id1, page_id, 51));
    EXPECT_TRUE(m.tryCreateRef(ref_id2, page_id, 51));
    EXPECT_EQ(page_id, m.getNormalPageId(ref_id1, 51));
    EXPECT_EQ(page_id, m.getNormalPageId(ref_id2, 51));

    EXPECT_FALSE(m.tryCreateRef(ref_id1, page_id, 53));

    // remove to check the ref count
    EXPECT_TRUE(m.tryCreateDel(page_id, 54));
    EXPECT_TRUE(m.tryCreateDel(ref_id1, 54));
    EXPECT_TRUE(m.tryCreateDel(ref_id2, 54));

    auto ids = m.getPendingRemoveIDs();
    ASSERT_EQ(1, ids.size());
    ASSERT_EQ(page_id, *ids.begin());

    EXPECT_EQ(m.numPages(), 3);
    m.cleanUpHolders(54);
    EXPECT_EQ(m.numPages(), 0);
}

class PageDirectoryTest : public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override
    {
        auto path = getTemporaryPath();
        dropDataOnDisk(path);

        auto ctx = DB::tests::TiFlashTestEnv::getContext();
        FileProviderPtr provider = ctx.getFileProvider();
        PSDiskDelegatorPtr delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        CollapsingPageDirectory collapsed_state;
        auto wal = WALStore::create(nullptr, provider, delegator);
        dir = PageDirectory::create(collapsed_state, std::move(wal));
    }

    void TearDown() override
    {
    }

protected:
    PageDirectory dir;
};

TEST_F(PageDirectoryTest, ApplyPutRead)
try
{
    auto snap0 = dir.createSnapshot();
    EXPECT_ENTRY_NOT_EXIST(dir, 1, snap0);

    PageEntryV3 entry1{.file_id = 1, .size = 1024, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(1, entry1);
        dir.apply(std::move(edit));
    }

    auto snap1 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry1, dir, 1, snap1);

    PageEntryV3 entry2{.file_id = 2, .size = 1024, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(2, entry2);
        dir.apply(std::move(edit));
    }

    auto snap2 = dir.createSnapshot();
    EXPECT_ENTRY_NOT_EXIST(dir, 2, snap1); // creating snap2 won't affect the result of snap1
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap2);
    EXPECT_ENTRY_EQ(entry1, dir, 1, snap2);
    {
        PageIds ids{1, 2};
        PageIDAndEntriesV3 expected_entries{{1, entry1}, {2, entry2}};
        EXPECT_ENTRIES_EQ(expected_entries, dir, ids, snap2);
    }
}
CATCH

TEST_F(PageDirectoryTest, ApplyPutWithIdenticalPages)
try
{
    // Put identical page in different `edit`
    PageId page_id = 50;

    auto snap0 = dir.createSnapshot();
    EXPECT_ENTRY_NOT_EXIST(dir, page_id, snap0);

    PageEntryV3 entry1{.file_id = 1, .size = 1024, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(page_id, entry1);
        dir.apply(std::move(edit));
    }

    auto snap1 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry1, dir, page_id, snap1);

    PageEntryV3 entry2{.file_id = 1, .size = 1024, .tag = 0, .offset = 0x1234, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(page_id, entry2);
        dir.apply(std::move(edit));
    }

    auto snap2 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry1, dir, page_id, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, page_id, snap2);
    {
        PageIds ids{page_id};
        PageIDAndEntriesV3 expected_entries{{page_id, entry2}};
        EXPECT_ENTRIES_EQ(expected_entries, dir, ids, snap2);
    }

    // Put identical page within one `edit`
    page_id++;
    PageEntryV3 entry3{.file_id = 1, .size = 1024, .tag = 0, .offset = 0x12345, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(page_id, entry1);
        edit.put(page_id, entry2);
        edit.put(page_id, entry3);

        // Should not be dead-lock
        dir.apply(std::move(edit));
    }
    auto snap3 = dir.createSnapshot();

    PageIds ids{page_id};
    PageIDAndEntriesV3 expected_entries{{page_id, entry3}};
    EXPECT_ENTRIES_EQ(expected_entries, dir, ids, snap3);
}
CATCH

TEST_F(PageDirectoryTest, ApplyPutDelRead)
try
{
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry2{.file_id = 2, .size = 1024, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(1, entry1);
        edit.put(2, entry2);
        dir.apply(std::move(edit));
    }

    auto snap1 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry1, dir, 1, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);

    PageEntryV3 entry3{.file_id = 3, .size = 1024, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry4{.file_id = 4, .size = 1024, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.del(2);
        edit.put(3, entry3);
        edit.put(4, entry4);
        dir.apply(std::move(edit));
    }

    auto snap2 = dir.createSnapshot();
    // sanity check for snap1
    EXPECT_ENTRY_EQ(entry1, dir, 1, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_NOT_EXIST(dir, 3, snap1);
    EXPECT_ENTRY_NOT_EXIST(dir, 4, snap1);
    // check for snap2
    EXPECT_ENTRY_NOT_EXIST(dir, 2, snap2); // deleted
    EXPECT_ENTRY_EQ(entry1, dir, 1, snap2);
    EXPECT_ENTRY_EQ(entry3, dir, 3, snap2);
    EXPECT_ENTRY_EQ(entry4, dir, 4, snap2);
    {
        PageIds ids{1, 3, 4};
        PageIDAndEntriesV3 expected_entries{{1, entry1}, {3, entry3}, {4, entry4}};
        EXPECT_ENTRIES_EQ(expected_entries, dir, ids, snap2);
    }
}
CATCH

TEST_F(PageDirectoryTest, ApplyUpdateOnRefEntries)
try
{
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry2{.file_id = 2, .size = 1024, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(1, entry1);
        edit.put(2, entry2);
        dir.apply(std::move(edit));
    }

    { // Ref 3->2
        PageEntriesEdit edit;
        edit.ref(3, 2);
        dir.apply(std::move(edit));
    }
    auto snap1 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);

    // Update 3, 2 won't get updated. Update 2, 3 won't get updated.
    // Note that users should not rely on this behavior
    PageEntryV3 entry_updated{.file_id = 999, .size = 16, .tag = 0, .offset = 0x123, .checksum = 0x123};
    {
        PageEntriesEdit edit;
        edit.put(3, entry_updated);
        dir.apply(std::move(edit));
    }
    auto snap2 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap2);
    EXPECT_ENTRY_EQ(entry_updated, dir, 3, snap2);

    PageEntryV3 entry_updated2{.file_id = 777, .size = 16, .tag = 0, .offset = 0x123, .checksum = 0x123};
    {
        PageEntriesEdit edit;
        edit.put(2, entry_updated2);
        dir.apply(std::move(edit));
    }
    auto snap3 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap2);
    EXPECT_ENTRY_EQ(entry_updated, dir, 3, snap2);
    EXPECT_ENTRY_EQ(entry_updated2, dir, 2, snap3);
    EXPECT_ENTRY_EQ(entry_updated, dir, 3, snap3);
}
CATCH

TEST_F(PageDirectoryTest, ApplyDeleteOnRefEntries)
try
{
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry2{.file_id = 2, .size = 1024, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(1, entry1);
        edit.put(2, entry2);
        dir.apply(std::move(edit));
    }

    { // Ref 3->2
        PageEntriesEdit edit;
        edit.ref(3, 2);
        dir.apply(std::move(edit));
    }
    auto snap1 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);

    // Delete 3, 2 won't get deleted.
    {
        PageEntriesEdit edit;
        edit.del(3);
        dir.apply(std::move(edit));
    }
    auto snap2 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap2);
    EXPECT_ENTRY_NOT_EXIST(dir, 3, snap2);

    // Delete 2, 3 won't get deleted.
    {
        PageEntriesEdit edit;
        edit.del(2);
        dir.apply(std::move(edit));
    }
    auto snap3 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap2);
    EXPECT_ENTRY_NOT_EXIST(dir, 3, snap2);
    EXPECT_ENTRY_NOT_EXIST(dir, 2, snap3);
    EXPECT_ENTRY_NOT_EXIST(dir, 3, snap3);
}
CATCH

/// Put ref page to ref page, ref path collapse to normal page
TEST_F(PageDirectoryTest, ApplyRefOnRefEntries)
try
{
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry2{.file_id = 2, .size = 1024, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(1, entry1);
        edit.put(2, entry2);
        dir.apply(std::move(edit));
    }

    { // Ref 3->2
        PageEntriesEdit edit;
        edit.ref(3, 2);
        dir.apply(std::move(edit));
    }
    auto snap1 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);

    // Ref 4 -> 3
    {
        PageEntriesEdit edit;
        edit.ref(4, 3);
        dir.apply(std::move(edit));
    }
    auto snap2 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);
    EXPECT_ENTRY_NOT_EXIST(dir, 4, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap2);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap2);
    EXPECT_ENTRY_EQ(entry2, dir, 4, snap2);
}
CATCH

/// Put duplicated RefPages in different WriteBatch
TEST_F(PageDirectoryTest, ApplyDuplicatedRefEntries)
try
{
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry2{.file_id = 2, .size = 1024, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(1, entry1);
        edit.put(2, entry2);
        dir.apply(std::move(edit));
    }

    { // Ref 3->2
        PageEntriesEdit edit;
        edit.ref(3, 2);
        dir.apply(std::move(edit));
    }
    auto snap1 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);

    // Ref 3 -> 2
    {
        PageEntriesEdit edit;
        edit.ref(3, 2);
        dir.apply(std::move(edit));
    }
    auto snap2 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap2);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap2);
}
CATCH

/// Put duplicated RefPages due to ref-path-collapse
TEST_F(PageDirectoryTest, ApplyCollapseDuplicatedRefEntries)
try
{
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry2{.file_id = 2, .size = 1024, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(1, entry1);
        edit.put(2, entry2);
        dir.apply(std::move(edit));
    }

    { // Ref 3->2
        PageEntriesEdit edit;
        edit.ref(3, 2);
        dir.apply(std::move(edit));
    }
    auto snap1 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);


    { // Ref 4 -> 3, collapse to 4 -> 2
        PageEntriesEdit edit;
        edit.ref(4, 3);
        dir.apply(std::move(edit));
    }
    auto snap2 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);
    EXPECT_ENTRY_NOT_EXIST(dir, 4, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap2);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap2);
    EXPECT_ENTRY_EQ(entry2, dir, 4, snap2);
}
CATCH

TEST_F(PageDirectoryTest, ApplyRefToNotExistEntry)
try
{
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry2{.file_id = 2, .size = 1024, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry3{.file_id = 3, .size = 1024, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(1, entry1);
        edit.put(2, entry2);
        dir.apply(std::move(edit));
    }

    { // Ref 4-> 999
        PageEntriesEdit edit;
        edit.put(3, entry3);
        edit.ref(4, 999);
        dir.apply(std::move(edit));
    }
    auto snap1 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry1, dir, 1, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry3, dir, 3, snap1);
    EXPECT_ENTRY_NOT_EXIST(dir, 4, snap1);

    // TODO: restore, invalid ref page is filtered
}
CATCH

TEST_F(PageDirectoryTest, TestRefWontDeadLock)
try
{
    PageEntriesEdit edit;
    {
        // 1. batch.putExternal(0, 0);
        edit.putExternal(0);

        // 2. batch.putRefPage(1, 0);
        edit.ref(1, 0);
    }

    dir.apply(std::move(edit));

    PageEntriesEdit edit2;
    {
        // 1. batch.putRefPage(2, 1); // ref 2 -> 1 -> 0
        edit2.ref(2, 1);

        // 2. batch.delPage(1); // free ref 1 -> 0
        edit2.del(1);
    }

    dir.apply(std::move(edit2));
}
CATCH

TEST_F(PageDirectoryTest, NormalPageId)
try
{
    {
        PageEntriesEdit edit;
        edit.put(9, PageEntryV3{});
        edit.putExternal(10);
        dir.apply(std::move(edit));
    }
    auto s0 = dir.createSnapshot();
    // calling getNormalPageId on non-external-page is not acceptable
    EXPECT_ANY_THROW(dir.getNormalPageId(9, s0));
    EXPECT_EQ(10, dir.getNormalPageId(10, s0));
    EXPECT_ANY_THROW(dir.getNormalPageId(11, s0));
    EXPECT_ANY_THROW(dir.getNormalPageId(12, s0));

    {
        PageEntriesEdit edit;
        edit.ref(11, 10);
        edit.ref(12, 10);
        edit.del(10);
        dir.apply(std::move(edit));
    }
    auto s1 = dir.createSnapshot();
    EXPECT_ANY_THROW(dir.getNormalPageId(10, s1));
    EXPECT_EQ(10, dir.getNormalPageId(11, s1));
    EXPECT_EQ(10, dir.getNormalPageId(12, s1));

    {
        PageEntriesEdit edit;
        edit.del(11);
        dir.apply(std::move(edit));
    }
    auto s2 = dir.createSnapshot();
    EXPECT_ANY_THROW(dir.getNormalPageId(10, s2));
    EXPECT_ANY_THROW(dir.getNormalPageId(11, s2));
    EXPECT_EQ(10, dir.getNormalPageId(12, s2));

    {
        PageEntriesEdit edit;
        edit.del(12);
        dir.apply(std::move(edit));
    }
    auto s3 = dir.createSnapshot();
    EXPECT_ANY_THROW(dir.getNormalPageId(10, s3));
    EXPECT_ANY_THROW(dir.getNormalPageId(11, s3));
    EXPECT_ANY_THROW(dir.getNormalPageId(12, s3));
}
CATCH

#define INSERT_BLOBID_ENTRY(BLOBID, VERSION)                                                                             \
    PageEntryV3 entry_v##VERSION{.file_id = (BLOBID), .size = (VERSION), .tag = 0, .offset = 0x123, .checksum = 0x4567}; \
    entries.createNewVersion((VERSION), entry_v##VERSION);
#define INSERT_ENTRY(VERSION) INSERT_BLOBID_ENTRY(1, VERSION)
#define INSERT_GC_ENTRY(VERSION, EPOCH)                                                                                        \
    PageEntryV3 entry_gc_v##VERSION##_##EPOCH{.file_id = 2, .size = (VERSION), .tag = 0, .offset = 0x234, .checksum = 0x5678}; \
    entries.createNewVersion((VERSION), (EPOCH), entry_gc_v##VERSION##_##EPOCH);

TEST(VersionedEntriesTest, InsertGet)
{
    VersionedPageEntries entries;
    INSERT_ENTRY(2);
    INSERT_ENTRY(5);
    INSERT_ENTRY(10);

    // Insert some entries with version
    ASSERT_FALSE(entries.getEntry(1).has_value());
    ASSERT_TRUE(isSameEntry(*entries.getEntry(2), entry_v2));
    ASSERT_TRUE(isSameEntry(*entries.getEntry(3), entry_v2));
    ASSERT_TRUE(isSameEntry(*entries.getEntry(4), entry_v2));
    for (UInt64 seq = 5; seq < 10; ++seq)
    {
        ASSERT_TRUE(isSameEntry(*entries.getEntry(seq), entry_v5));
    }
    for (UInt64 seq = 10; seq < 20; ++seq)
    {
        ASSERT_TRUE(isSameEntry(*entries.getEntry(seq), entry_v10));
    }

    // Insert some entries with version && gc epoch
    INSERT_GC_ENTRY(2, 1);
    INSERT_GC_ENTRY(5, 1);
    INSERT_GC_ENTRY(5, 2);
    ASSERT_FALSE(entries.getEntry(1).has_value());
    ASSERT_TRUE(isSameEntry(*entries.getEntry(2), entry_gc_v2_1));
    ASSERT_TRUE(isSameEntry(*entries.getEntry(3), entry_gc_v2_1));
    ASSERT_TRUE(isSameEntry(*entries.getEntry(4), entry_gc_v2_1));
    for (UInt64 seq = 5; seq < 10; ++seq)
    {
        ASSERT_TRUE(isSameEntry(*entries.getEntry(seq), entry_gc_v5_2));
    }
    for (UInt64 seq = 10; seq < 20; ++seq)
    {
        ASSERT_TRUE(isSameEntry(*entries.getEntry(seq), entry_v10));
    }

    // Insert delete. Can not get entry with seq >= delete_version.
    // But it won't affect reading with old seq.
    entries.createDelete(15);
    ASSERT_FALSE(entries.getEntry(1).has_value());
    ASSERT_TRUE(isSameEntry(*entries.getEntry(2), entry_gc_v2_1));
    ASSERT_TRUE(isSameEntry(*entries.getEntry(3), entry_gc_v2_1));
    ASSERT_TRUE(isSameEntry(*entries.getEntry(4), entry_gc_v2_1));
    for (UInt64 seq = 5; seq < 10; ++seq)
    {
        ASSERT_TRUE(isSameEntry(*entries.getEntry(seq), entry_gc_v5_2));
    }
    for (UInt64 seq = 10; seq < 15; ++seq)
    {
        ASSERT_TRUE(isSameEntry(*entries.getEntry(seq), entry_v10));
    }
    for (UInt64 seq = 15; seq < 20; ++seq)
    {
        ASSERT_FALSE(entries.getEntry(seq).has_value());
    }
}

TEST(VersionedEntriesTest, GC)
try
{
    VersionedPageEntries entries;
    INSERT_ENTRY(2);
    INSERT_GC_ENTRY(2, 1);
    INSERT_ENTRY(5);
    INSERT_GC_ENTRY(5, 1);
    INSERT_GC_ENTRY(5, 2);
    INSERT_ENTRY(10);
    INSERT_ENTRY(11);
    entries.createDelete(15);

    // noting to be removed
    auto removed_entries = entries.deleteAndGC(1);
    ASSERT_FALSE(removed_entries.second);
    ASSERT_EQ(removed_entries.first.size(), 0);

    // <2,0> get removed.
    removed_entries = entries.deleteAndGC(2);
    ASSERT_FALSE(removed_entries.second);
    ASSERT_EQ(removed_entries.first.size(), 1);
    auto iter = removed_entries.first.begin();
    ASSERT_TRUE(isSameEntry(entry_v2, *iter));

    // nothing get removed.
    removed_entries = entries.deleteAndGC(4);
    ASSERT_FALSE(removed_entries.second);
    ASSERT_EQ(removed_entries.first.size(), 0);

    // <2,1>, <5,0>, <5,1>, <5,2>, <10,0> get removed.
    removed_entries = entries.deleteAndGC(11);
    ASSERT_FALSE(removed_entries.second);
    ASSERT_EQ(removed_entries.first.size(), 5);
    iter = removed_entries.first.begin();
    ASSERT_TRUE(isSameEntry(entry_v10, *iter));
    iter++;
    ASSERT_TRUE(isSameEntry(entry_gc_v5_2, *iter));
    iter++;
    ASSERT_TRUE(isSameEntry(entry_gc_v5_1, *iter));
    iter++;
    ASSERT_TRUE(isSameEntry(entry_v5, *iter));
    iter++;
    ASSERT_TRUE(isSameEntry(entry_gc_v2_1, *iter)) << toString(*iter);

    // <11,0> get removed, all cleared.
    removed_entries = entries.deleteAndGC(20);
    ASSERT_TRUE(removed_entries.second);
    ASSERT_EQ(removed_entries.first.size(), 1);
}
CATCH

TEST(VersionedEntriesTest, ReadAfterGcApplied)
try
{
    VersionedPageEntries entries;
    INSERT_ENTRY(2);
    INSERT_ENTRY(3);
    INSERT_ENTRY(5);

    // Read with snapshot seq=2
    ASSERT_TRUE(isSameEntry(entry_v2, *entries.getEntry(2)));

    // Mock that gc applied and insert <2, 1>
    INSERT_GC_ENTRY(2, 1);

    // Now we should read the entry <2, 1> with seq=2
    ASSERT_TRUE(isSameEntry(entry_gc_v2_1, *entries.getEntry(2)));

    // <2,0> get removed
    auto removed_entries = entries.deleteAndGC(2);
    ASSERT_EQ(removed_entries.first.size(), 1);
}
CATCH

TEST(VersionedEntriesTest, getEntriesByBlobId)
{
    VersionedPageEntries entries;

    INSERT_BLOBID_ENTRY(1, 1);
    INSERT_BLOBID_ENTRY(1, 2);
    INSERT_BLOBID_ENTRY(2, 3);
    INSERT_BLOBID_ENTRY(2, 4);
    INSERT_BLOBID_ENTRY(1, 5);
    INSERT_BLOBID_ENTRY(3, 6);
    INSERT_BLOBID_ENTRY(3, 8);
    INSERT_BLOBID_ENTRY(1, 11);

    const auto & [versioned_entries1, total_size1] = entries.getEntriesByBlobId(1);

    ASSERT_EQ(versioned_entries1.size(), 4);
    ASSERT_EQ(total_size1, 1 + 2 + 5 + 11);
    auto it = versioned_entries1.begin();

    ASSERT_EQ(it->first.sequence, 1);
    ASSERT_TRUE(isSameEntry(it->second, entry_v1));

    it++;
    ASSERT_EQ(it->first.sequence, 2);
    ASSERT_TRUE(isSameEntry(it->second, entry_v2));

    it++;
    ASSERT_EQ(it->first.sequence, 5);
    ASSERT_TRUE(isSameEntry(it->second, entry_v5));

    it++;
    ASSERT_EQ(it->first.sequence, 11);
    ASSERT_TRUE(isSameEntry(it->second, entry_v11));

    const auto & [versioned_entries2, total_size2] = entries.getEntriesByBlobId(2);

    ASSERT_EQ(versioned_entries2.size(), 2);
    ASSERT_EQ(total_size2, 3 + 4);
    it = versioned_entries2.begin();

    ASSERT_EQ(it->first.sequence, 3);
    ASSERT_TRUE(isSameEntry(it->second, entry_v3));

    it++;
    ASSERT_EQ(it->first.sequence, 4);
    ASSERT_TRUE(isSameEntry(it->second, entry_v4));

    const auto & [versioned_entries3, total_size3] = entries.getEntriesByBlobId(3);

    ASSERT_EQ(versioned_entries3.size(), 2);
    ASSERT_EQ(total_size3, 6 + 8);
    it = versioned_entries3.begin();

    ASSERT_EQ(it->first.sequence, 6);
    ASSERT_TRUE(isSameEntry(it->second, entry_v6));

    it++;
    ASSERT_EQ(it->first.sequence, 8);
    ASSERT_TRUE(isSameEntry(it->second, entry_v8));
}

#undef INSERT_BLOBID_ENTRY
#undef INSERT_ENTRY
#undef INSERT_GC_ENTRY
// end of testing `VersionedEntriesTest`

class PageDirectoryGCTest : public PageDirectoryTest
{
};

#define INSERT_ENTRY_TO(PAGE_ID, VERSION, BLOB_FILE_ID)                                                                        \
    PageEntryV3 entry_v##VERSION{.file_id = (BLOB_FILE_ID), .size = (VERSION), .tag = 0, .offset = 0x123, .checksum = 0x4567}; \
    {                                                                                                                          \
        PageEntriesEdit edit;                                                                                                  \
        edit.put((PAGE_ID), entry_v##VERSION);                                                                                 \
        dir.apply(std::move(edit));                                                                                            \
    }
// Insert an entry into mvcc directory
#define INSERT_ENTRY(PAGE_ID, VERSION) INSERT_ENTRY_TO(PAGE_ID, VERSION, 1)
// Insert an entry into mvcc directory, and acquire a snapshot
#define INSERT_ENTRY_ACQ_SNAP(PAGE_ID, VERSION) \
    INSERT_ENTRY(PAGE_ID, VERSION)              \
    auto snapshot##VERSION = dir.createSnapshot();
#define INSERT_DELETE(PAGE_ID)      \
    {                               \
        PageEntriesEdit edit;       \
        edit.del((PAGE_ID));        \
        dir.apply(std::move(edit)); \
    }

static size_t getNumEntries(const std::vector<PageEntriesV3> & entries)
{
    size_t num = 0;
    for (const auto & es : entries)
        num += es.size();
    return num;
}

TEST_F(PageDirectoryGCTest, GCPushForward)
try
{
    PageId page_id = 50;

    /**
     * before GC => {
     *     50  -> [v1...v5]
     *   }
     *   snapshot remain: [v3,v5]
     */
    INSERT_ENTRY(page_id, 1);
    INSERT_ENTRY(page_id, 2);
    INSERT_ENTRY_ACQ_SNAP(page_id, 3);
    INSERT_ENTRY(page_id, 4);
    INSERT_ENTRY_ACQ_SNAP(page_id, 5);

    EXPECT_ENTRY_EQ(entry_v3, dir, page_id, snapshot3);
    EXPECT_ENTRY_EQ(entry_v5, dir, page_id, snapshot5);

    /**
     * after GC =>  {
     *     50  -> [v3,v4,v5]
     *   }
     *   snapshot remain: [v3,v5]
     */
    std::vector<PageEntriesV3> del_entries;
    std::set<PageId> pending_ids;
    std::tie(del_entries, pending_ids) = dir.gc();
    // TODO: check pending_ids
    (void)pending_ids;
    EXPECT_EQ(del_entries.size(), 1);
    // v1, v2 have been removed.
    ASSERT_EQ(getNumEntries(del_entries), 2);

    EXPECT_ENTRY_EQ(entry_v3, dir, page_id, snapshot3);
    EXPECT_ENTRY_EQ(entry_v5, dir, page_id, snapshot5);

    // Release all snapshots and run gc again, (min gc version get pushed forward and)
    // all versions get compacted.
    snapshot3.reset();
    snapshot5.reset();
    std::tie(del_entries, pending_ids) = dir.gc();
    ASSERT_EQ(getNumEntries(del_entries), 2);

    auto snapshot_after_gc = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry_v5, dir, page_id, snapshot_after_gc);
}
CATCH

TEST_F(PageDirectoryGCTest, GCPushForward2)
try
{
    // GC push forward with 2 page entry list
    /**
     * case 2
     * before GC => {
     *     50  -> [v2,v5,v10]
     *     512 -> [v1,v3,v4,v6,v7,v8,v9]
     *   }
     *   snapshot remain: [v3, v5]
     */
    PageId page_id = 50;
    PageId another_page_id = 512;

    INSERT_ENTRY(another_page_id, 1);
    INSERT_ENTRY(page_id, 2);
    INSERT_ENTRY_ACQ_SNAP(another_page_id, 3);
    INSERT_ENTRY(another_page_id, 4);
    INSERT_ENTRY_ACQ_SNAP(page_id, 5);
    INSERT_ENTRY(another_page_id, 6);
    INSERT_ENTRY(another_page_id, 7);
    INSERT_ENTRY(another_page_id, 8);
    INSERT_ENTRY(another_page_id, 9);
    INSERT_ENTRY(page_id, 10);

    {
        /**
         * after GC => {
         *     50  -> [v5,v10]
         *     512 -> [v3,v4,v6,v7,v8,v9]
         *   }
         *   snapshot remain: [v3, v5]
         */
        std::vector<PageEntriesV3> del_entries;
        std::set<PageId> pending_ids;
        std::tie(del_entries, pending_ids) = dir.gc();
        // page_id: []; another_page_id: v1 have been removed.
        EXPECT_EQ(getNumEntries(del_entries), 1);
    }

    {
        snapshot3.reset();
        /**
         * after GC => {
         *     50  -> [v5,v10]
         *     512 -> [v6,v7,v8,v9]
         *   }
         *   snapshot remain: [v5]
         */
        std::vector<PageEntriesV3> del_entries;
        std::set<PageId> pending_ids;
        std::tie(del_entries, pending_ids) = dir.gc();
        // page_id: v2; another_page_id: v3 have been removed.
        EXPECT_EQ(getNumEntries(del_entries), 2);
    }

    {
        snapshot5.reset();
        /**
         * after GC => {
         *     50  -> [v10]
         *     512 -> [v9]
         *   }
         *   snapshot remain: []
         */
        std::vector<PageEntriesV3> del_entries;
        std::set<PageId> pending_ids;
        std::tie(del_entries, pending_ids) = dir.gc();
        // page_id: v5; another_page_id: v6,v7,v8 have been removed.
        EXPECT_EQ(getNumEntries(del_entries), 5);
    }
}
CATCH

TEST_F(PageDirectoryGCTest, GCBlockedByOldSnap)
try
{
    /**
     * case 1
     * before GC => {
     *     50  -> [v3,v5,v10]
     *     512 -> [v1,v2,v4,v6,v7,v8,v9]
     *   }
     *   snapshot remain: [v1, v3, v5, v10]
     */
    PageId page_id = 50;
    PageId another_page_id = 512;

    // Push entries
    INSERT_ENTRY_ACQ_SNAP(another_page_id, 1);
    INSERT_ENTRY(another_page_id, 2);
    INSERT_ENTRY_ACQ_SNAP(page_id, 3);
    INSERT_ENTRY(another_page_id, 4);
    INSERT_ENTRY_ACQ_SNAP(page_id, 5);
    INSERT_ENTRY(another_page_id, 6);
    INSERT_ENTRY(another_page_id, 7);
    INSERT_ENTRY(another_page_id, 8);
    INSERT_ENTRY(another_page_id, 9);
    INSERT_ENTRY_ACQ_SNAP(page_id, 10);

    EXPECT_ENTRY_EQ(entry_v1, dir, another_page_id, snapshot1);
    EXPECT_ENTRY_NOT_EXIST(dir, page_id, snapshot1);
    EXPECT_ENTRY_EQ(entry_v3, dir, page_id, snapshot3);
    EXPECT_ENTRY_EQ(entry_v5, dir, page_id, snapshot5);

    {
        /**
         * after GC => [
         *     50  -> [v3,v5,v10]
         *     512 -> [v1,v2,v4,v6,v7,v8,v9]
         *   }
         *   snapshot remain: [v1,v3,v5,v10]
         */
        std::vector<PageEntriesV3> removed_entries;
        std::set<PageId> pending_ids;
        std::tie(removed_entries, pending_ids) = dir.gc(); // The GC on page_id=50 is blocked by previous `snapshot1`
        EXPECT_EQ(getNumEntries(removed_entries), 0);
        EXPECT_ENTRY_EQ(entry_v1, dir, another_page_id, snapshot1);
        EXPECT_ENTRY_NOT_EXIST(dir, page_id, snapshot1);
        EXPECT_ENTRY_EQ(entry_v3, dir, page_id, snapshot3);
        EXPECT_ENTRY_EQ(entry_v5, dir, page_id, snapshot5);
    }

    {
        /**
         * after GC => [
         *     50  -> [v3,v5,v10]
         *     512 -> [v1,v2,v4,v6,v7,v8,v9]
         *   }
         *   snapshot remain: [v1]
         */
        // The GC on page_id=50 is blocked by previous `snapshot1`,
        // release other snapshot won't change the result from `snapshot1`
        snapshot3.reset();
        snapshot5.reset();
        snapshot10.reset();
        std::vector<PageEntriesV3> removed_entries;
        std::set<PageId> pending_ids;
        std::tie(removed_entries, pending_ids) = dir.gc();
        EXPECT_EQ(getNumEntries(removed_entries), 0);
        EXPECT_ENTRY_EQ(entry_v1, dir, another_page_id, snapshot1);
        EXPECT_ENTRY_NOT_EXIST(dir, page_id, snapshot1);
    }

    {
        /**
         * after GC => [
         *     50  -> [v10]
         *     512 -> [v9]
         *   }
         *   snapshot remain: []
         */
        snapshot1.reset();
        std::vector<PageEntriesV3> removed_entries;
        std::set<PageId> pending_ids;
        std::tie(removed_entries, pending_ids) = dir.gc(); // this will compact all versions
        // page_id: v3,v5; another_page_id: v1,v2,v4,v6,v7,v8 get removed
        EXPECT_EQ(getNumEntries(removed_entries), 8);

        auto snap_after_gc = dir.createSnapshot();
        EXPECT_ENTRY_EQ(entry_v10, dir, page_id, snap_after_gc);
        EXPECT_ENTRY_EQ(entry_v9, dir, another_page_id, snap_after_gc);
    }
}
CATCH

TEST_F(PageDirectoryGCTest, GCCleanUPDeletedPage)
try
{
    /**
     * before GC => {
     *     50  -> [v3,v5,v8(delete)]
     *     512 -> [v1,v2,v4,v6,v7,v8,v9,v10(delete)]
     *   }
     *   snapshot remain: [v5, v8, v9]
     */
    PageId page_id = 50;
    PageId another_page_id = 512;

    // Push entries
    INSERT_ENTRY(another_page_id, 1);
    INSERT_ENTRY(another_page_id, 2);
    INSERT_ENTRY(page_id, 3);
    INSERT_ENTRY(another_page_id, 4);
    INSERT_ENTRY_ACQ_SNAP(page_id, 5);
    INSERT_ENTRY(another_page_id, 6);
    INSERT_ENTRY(another_page_id, 7);
    PageEntryV3 entry_v8{.file_id = 1, .size = 8, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.del(page_id);
        edit.put(another_page_id, entry_v8);
        dir.apply(std::move(edit));
    }
    auto snapshot8 = dir.createSnapshot();
    INSERT_ENTRY_ACQ_SNAP(another_page_id, 9);
    INSERT_DELETE(another_page_id);

    {
        EXPECT_ENTRY_EQ(entry_v5, dir, page_id, snapshot5);
        EXPECT_ENTRY_EQ(entry_v4, dir, another_page_id, snapshot5);
        EXPECT_ENTRY_NOT_EXIST(dir, page_id, snapshot8);
        EXPECT_ENTRY_EQ(entry_v8, dir, another_page_id, snapshot8);
        EXPECT_ENTRY_NOT_EXIST(dir, page_id, snapshot9);
        EXPECT_ENTRY_EQ(entry_v9, dir, another_page_id, snapshot9);
        /**
         * after GC => [
         *     50  -> [v5,v8(delete)]
         *     512 -> [v4,v6,v8,v9,v10(delete)]
         *   }
         *   snapshot remain: [v5,v8,v9]
         */
        std::vector<PageEntriesV3> del_entries;
        std::set<PageId> pending_ids;
        std::tie(del_entries, pending_ids) = dir.gc();
        // page_id: v3; another_page_id: v1,v2 have been removed.
        EXPECT_EQ(getNumEntries(del_entries), 3);
        ASSERT_EQ(dir.numPages(), 2);
    }

    {
        /**
         * after GC => [
         *     512 -> [v8,v9,v10(delete)]
         *   }
         *   snapshot remain: [v8,v9]
         */
        snapshot5.reset();
        EXPECT_ENTRY_NOT_EXIST(dir, page_id, snapshot8);
        EXPECT_ENTRY_EQ(entry_v8, dir, another_page_id, snapshot8);
        EXPECT_ENTRY_NOT_EXIST(dir, page_id, snapshot9);
        EXPECT_ENTRY_EQ(entry_v9, dir, another_page_id, snapshot9);
        std::vector<PageEntriesV3> del_entries;
        std::set<PageId> pending_ids;
        std::tie(del_entries, pending_ids) = dir.gc();
        // page_id: v5; another_page_id: v4,v6,v7 have been removed.
        EXPECT_EQ(getNumEntries(del_entries), 4);
        ASSERT_EQ(dir.numPages(), 1); // page_id should be removed.
    }

    {
        /**
         * after GC => [
         *     512 -> [v9, v10(delete)]
         *   }
         *   snapshot remain: [v9]
         */
        snapshot8.reset();
        EXPECT_ENTRY_NOT_EXIST(dir, page_id, snapshot9);
        EXPECT_ENTRY_EQ(entry_v9, dir, another_page_id, snapshot9);
        std::vector<PageEntriesV3> del_entries;
        std::set<PageId> pending_ids;
        std::tie(del_entries, pending_ids) = dir.gc();
        // another_page_id: v8 have been removed.
        EXPECT_EQ(getNumEntries(del_entries), 1);
    }

    {
        /**
         * after GC => { empty }
         *   snapshot remain: []
         */
        snapshot9.reset();
        std::vector<PageEntriesV3> del_entries;
        std::set<PageId> pending_ids;
        std::tie(del_entries, pending_ids) = dir.gc();
        // another_page_id: v9 have been removed.
        EXPECT_EQ(getNumEntries(del_entries), 1);
        ASSERT_EQ(dir.numPages(), 0); // all should be removed.
    }

    auto snapshot_after_all = dir.createSnapshot();
    EXPECT_ENTRY_NOT_EXIST(dir, page_id, snapshot_after_all);
    EXPECT_ENTRY_NOT_EXIST(dir, another_page_id, snapshot_after_all);
}
CATCH

TEST_F(PageDirectoryGCTest, FullGCApply)
try
{
    PageId page_id = 50;
    PageId another_page_id = 512;
    INSERT_ENTRY_TO(page_id, 1, 1);
    INSERT_ENTRY_TO(page_id, 2, 2);
    INSERT_ENTRY_TO(another_page_id, 3, 2);
    INSERT_ENTRY_TO(page_id, 4, 1);
    INSERT_ENTRY_TO(page_id, 5, 3);
    INSERT_ENTRY_TO(another_page_id, 6, 1);

    // FIXME: This will copy many outdate pages
    // Full GC get entries
    auto candidate_entries_1 = dir.getEntriesByBlobIds({1});
    EXPECT_EQ(candidate_entries_1.first.size(), 1);
    EXPECT_EQ(candidate_entries_1.first[1].size(), 2); // 2 page entries list

    auto candidate_entries_2_3 = dir.getEntriesByBlobIds({2, 3});
    EXPECT_EQ(candidate_entries_2_3.first.size(), 2);
    const auto & entries_in_file2 = candidate_entries_2_3.first[2];
    const auto & entries_in_file3 = candidate_entries_2_3.first[3];
    EXPECT_EQ(entries_in_file2.size(), 2); // 2 page entries list
    EXPECT_EQ(entries_in_file3.size(), 1); // 1 page entries list

    PageEntriesEdit gc_migrate_entries;
    for (const auto & [file_id, entries] : candidate_entries_1.first)
    {
        (void)file_id;
        for (const auto & [page_id, version_entries] : entries)
        {
            for (const auto & [ver, entry] : version_entries)
            {
                gc_migrate_entries.upsertPage(page_id, ver, entry);
            }
        }
    }
    for (const auto & [file_id, entries] : candidate_entries_2_3.first)
    {
        (void)file_id;
        for (const auto & [page_id, version_entries] : entries)
        {
            for (const auto & [ver, entry] : version_entries)
            {
                gc_migrate_entries.upsertPage(page_id, ver, entry);
            }
        }
    }

    // Full GC execute apply
    dir.gcApply(std::move(gc_migrate_entries));
}
CATCH

TEST_F(PageDirectoryGCTest, MVCCAndFullGCInConcurrent)
try
{
    PageId page_id = 50;
    PageId another_page_id = 512;
    INSERT_ENTRY_TO(page_id, 1, 1);
    INSERT_ENTRY_TO(page_id, 2, 2);
    INSERT_ENTRY_TO(page_id, 3, 2);
    INSERT_ENTRY_TO(page_id, 4, 1);
    INSERT_ENTRY_TO(page_id, 5, 3);
    INSERT_ENTRY_TO(another_page_id, 6, 1);
    INSERT_DELETE(page_id);

    EXPECT_EQ(dir.numPages(), 2);

    // 1.1 Full GC get entries
    auto candidate_entries_1 = dir.getEntriesByBlobIds({1});
    EXPECT_EQ(candidate_entries_1.first.size(), 1);
    EXPECT_EQ(candidate_entries_1.first[1].size(), 2);

    auto candidate_entries_2_3 = dir.getEntriesByBlobIds({2, 3});
    EXPECT_EQ(candidate_entries_2_3.first.size(), 2);
    const auto & entries_in_file2 = candidate_entries_2_3.first[2];
    const auto & entries_in_file3 = candidate_entries_2_3.first[3];
    EXPECT_EQ(entries_in_file2.size(), 1);
    EXPECT_EQ(entries_in_file3.size(), 1);

    // 2.1 Execute GC
    dir.gc();
    // `page_id` get removed
    EXPECT_EQ(dir.numPages(), 1);

    PageEntriesEdit gc_migrate_entries;
    for (const auto & [file_id, entries] : candidate_entries_1.first)
    {
        (void)file_id;
        for (const auto & [page_id, version_entries] : entries)
        {
            for (const auto & [ver, entry] : version_entries)
            {
                gc_migrate_entries.upsertPage(page_id, ver, entry);
            }
        }
    }
    for (const auto & [file_id, entries] : candidate_entries_2_3.first)
    {
        (void)file_id;
        for (const auto & [page_id, version_entries] : entries)
        {
            for (const auto & [ver, entry] : version_entries)
            {
                gc_migrate_entries.upsertPage(page_id, ver, entry);
            }
        }
    }

    // 1.2 Full GC execute apply
    ASSERT_THROW({ dir.gcApply(std::move(gc_migrate_entries)); }, DB::Exception);
}
CATCH

#undef INSERT_ENTRY_TO
#undef INSERT_ENTRY
#undef INSERT_ENTRY_ACQ_SNAP
#undef INSERT_DELETE

} // namespace PS::V3::tests
} // namespace DB
