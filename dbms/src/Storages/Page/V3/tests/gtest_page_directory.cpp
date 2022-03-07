#include <Common/Exception.h>
#include <Common/FmtUtils.h>
#include <Encryption/FileProvider.h>
#include <IO/WriteHelpers.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageDirectoryFactory.h>
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
        PageDirectoryFactory factory;
        dir = factory.create(provider, delegator);
    }

    void TearDown() override
    {
    }

protected:
    PageDirectoryPtr dir;
};

TEST_F(PageDirectoryTest, ApplyPutRead)
try
{
    auto snap0 = dir->createSnapshot();
    EXPECT_ENTRY_NOT_EXIST(dir, 1, snap0);

    PageEntryV3 entry1{.file_id = 1, .size = 1024, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(1, entry1);
        dir->apply(std::move(edit));
    }

    auto snap1 = dir->createSnapshot();
    EXPECT_ENTRY_EQ(entry1, dir, 1, snap1);

    PageEntryV3 entry2{.file_id = 2, .size = 1024, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(2, entry2);
        dir->apply(std::move(edit));
    }

    auto snap2 = dir->createSnapshot();
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

    auto snap0 = dir->createSnapshot();
    EXPECT_ENTRY_NOT_EXIST(dir, page_id, snap0);

    PageEntryV3 entry1{.file_id = 1, .size = 1024, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(page_id, entry1);
        dir->apply(std::move(edit));
    }

    auto snap1 = dir->createSnapshot();
    EXPECT_ENTRY_EQ(entry1, dir, page_id, snap1);

    PageEntryV3 entry2{.file_id = 1, .size = 1024, .tag = 0, .offset = 0x1234, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(page_id, entry2);
        dir->apply(std::move(edit));
    }

    auto snap2 = dir->createSnapshot();
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
        dir->apply(std::move(edit));
    }
    auto snap3 = dir->createSnapshot();

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
        dir->apply(std::move(edit));
    }

    auto snap1 = dir->createSnapshot();
    EXPECT_ENTRY_EQ(entry1, dir, 1, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);

    PageEntryV3 entry3{.file_id = 3, .size = 1024, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry4{.file_id = 4, .size = 1024, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.del(2);
        edit.put(3, entry3);
        edit.put(4, entry4);
        dir->apply(std::move(edit));
    }

    auto snap2 = dir->createSnapshot();
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
        dir->apply(std::move(edit));
    }

    { // Ref 3->2
        PageEntriesEdit edit;
        edit.ref(3, 2);
        dir->apply(std::move(edit));
    }
    auto snap1 = dir->createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);

    // Update on ref page is not allowed
    PageEntryV3 entry_updated{.file_id = 999, .size = 16, .tag = 0, .offset = 0x123, .checksum = 0x123};
    {
        PageEntriesEdit edit;
        edit.put(3, entry_updated);
        ASSERT_ANY_THROW(dir->apply(std::move(edit)));
    }

    PageEntryV3 entry_updated2{.file_id = 777, .size = 16, .tag = 0, .offset = 0x123, .checksum = 0x123};
    {
        PageEntriesEdit edit;
        edit.put(2, entry_updated2);
        ASSERT_ANY_THROW(dir->apply(std::move(edit)));
    }
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
        dir->apply(std::move(edit));
    }

    { // Ref 3->2
        PageEntriesEdit edit;
        edit.ref(3, 2);
        dir->apply(std::move(edit));
    }
    auto snap1 = dir->createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);

    // Delete 3, 2 won't get deleted.
    {
        PageEntriesEdit edit;
        edit.del(3);
        dir->apply(std::move(edit));
    }
    auto snap2 = dir->createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap2);
    EXPECT_ENTRY_NOT_EXIST(dir, 3, snap2);

    // Delete 2, 3 won't get deleted.
    {
        PageEntriesEdit edit;
        edit.del(2);
        dir->apply(std::move(edit));
    }
    auto snap3 = dir->createSnapshot();
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
        dir->apply(std::move(edit));
    }

    { // Ref 3->2
        PageEntriesEdit edit;
        edit.ref(3, 2);
        dir->apply(std::move(edit));
    }
    auto snap1 = dir->createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);

    // Ref 4 -> 3
    {
        PageEntriesEdit edit;
        edit.ref(4, 3);
        dir->apply(std::move(edit));
    }
    auto snap2 = dir->createSnapshot();
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
        dir->apply(std::move(edit));
    }

    { // Ref 3->2
        PageEntriesEdit edit;
        edit.ref(3, 2);
        dir->apply(std::move(edit));
    }
    auto snap1 = dir->createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);

    // Ref 3 -> 2
    {
        PageEntriesEdit edit;
        edit.ref(3, 2);
        dir->apply(std::move(edit));
    }
    auto snap2 = dir->createSnapshot();
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
        dir->apply(std::move(edit));
    }

    { // Ref 3->2
        PageEntriesEdit edit;
        edit.ref(3, 2);
        dir->apply(std::move(edit));
    }
    auto snap1 = dir->createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);


    { // Ref 4 -> 3, collapse to 4 -> 2
        PageEntriesEdit edit;
        edit.ref(4, 3);
        dir->apply(std::move(edit));
    }
    auto snap2 = dir->createSnapshot();
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
        dir->apply(std::move(edit));
    }

    // Applying ref to not exist entry is not allowed
    { // Ref 4-> 999
        PageEntriesEdit edit;
        edit.put(3, entry3);
        edit.ref(4, 999);
        ASSERT_ANY_THROW(dir->apply(std::move(edit)));
    }
}
CATCH

TEST_F(PageDirectoryTest, TestRefWontDeadLock)
{
    PageEntriesEdit edit;
    {
        // 1. batch.putExternal(0, 0);
        PageEntryV3 entry1;
        edit.put(0, entry1);

        // 2. batch.putRefPage(1, 0);
        edit.ref(1, 0);
    }

    dir->apply(std::move(edit));

    PageEntriesEdit edit2;
    {
        // 1. batch.putRefPage(2, 1); // ref 2 -> 1 -> 0
        edit2.ref(2, 1);

        // 2. batch.delPage(1); // free ref 1 -> 0
        edit2.del(1);
    }

    dir->apply(std::move(edit2));
}

TEST_F(PageDirectoryTest, NormalPageId)
try
{
    {
        PageEntriesEdit edit;
        edit.put(9, PageEntryV3{});
        edit.putExternal(10);
        dir->apply(std::move(edit));
    }
    auto s0 = dir->createSnapshot();
    // calling getNormalPageId on non-external-page will return itself
    EXPECT_EQ(9, dir->getNormalPageId(9, s0));
    EXPECT_EQ(10, dir->getNormalPageId(10, s0));
    EXPECT_ANY_THROW(dir->getNormalPageId(11, s0)); // not exist at all
    EXPECT_ANY_THROW(dir->getNormalPageId(12, s0)); // not exist at all

    {
        PageEntriesEdit edit;
        edit.ref(11, 10);
        edit.ref(12, 10);
        edit.del(10);
        dir->apply(std::move(edit));
    }
    auto s1 = dir->createSnapshot();
    EXPECT_ANY_THROW(dir->getNormalPageId(10, s1));
    EXPECT_EQ(10, dir->getNormalPageId(11, s1));
    EXPECT_EQ(10, dir->getNormalPageId(12, s1));

    {
        PageEntriesEdit edit;
        edit.del(11);
        dir->apply(std::move(edit));
    }
    auto s2 = dir->createSnapshot();
    EXPECT_ANY_THROW(dir->getNormalPageId(10, s2));
    EXPECT_ANY_THROW(dir->getNormalPageId(11, s2));
    EXPECT_EQ(10, dir->getNormalPageId(12, s2));

    {
        PageEntriesEdit edit;
        edit.del(12);
        dir->apply(std::move(edit));
    }
    auto s3 = dir->createSnapshot();
    EXPECT_ANY_THROW(dir->getNormalPageId(10, s3));
    EXPECT_ANY_THROW(dir->getNormalPageId(11, s3));
    EXPECT_ANY_THROW(dir->getNormalPageId(12, s3));
}
CATCH

#define INSERT_BLOBID_ENTRY(BLOBID, VERSION)                                                                             \
    PageEntryV3 entry_v##VERSION{.file_id = (BLOBID), .size = (VERSION), .tag = 0, .offset = 0x123, .checksum = 0x4567}; \
    entries.createNewEntry(PageVersionType(VERSION), entry_v##VERSION);
#define INSERT_ENTRY(VERSION) INSERT_BLOBID_ENTRY(1, VERSION)
#define INSERT_GC_ENTRY(VERSION, EPOCH)                                                                                                        \
    PageEntryV3 entry_gc_v##VERSION##_##EPOCH{.file_id = 2, .size = 100 * (VERSION) + (EPOCH), .tag = 0, .offset = 0x234, .checksum = 0x5678}; \
    entries.createNewEntry(PageVersionType((VERSION), (EPOCH)), entry_gc_v##VERSION##_##EPOCH);

class VersionedEntriesTest : public ::testing::Test
{
public:
    void SetUp() override
    {
    }

    using DerefCounter = std::map<PageId, std::pair<PageVersionType, Int64>>;
    std::tuple<bool, PageEntriesV3, DerefCounter> runClean(UInt64 seq)
    {
        DerefCounter deref_counter;
        PageEntriesV3 removed_entries;
        bool all_removed = entries.cleanOutdatedEntries(seq, page_id, &deref_counter, removed_entries, entries.acquireLock());
        return {all_removed, removed_entries, deref_counter};
    }

    std::tuple<bool, PageEntriesV3> runDeref(UInt64 seq, PageVersionType ver, Int64 decrease_num)
    {
        PageEntriesV3 removed_entries;
        bool all_removed = entries.derefAndClean(seq, page_id, ver, decrease_num, removed_entries);
        return {all_removed, removed_entries};
    }

protected:
    const PageId page_id = 100;
    VersionedPageEntries entries;
};

TEST_F(VersionedEntriesTest, InsertGet)
{
    INSERT_ENTRY(2);
    INSERT_ENTRY(5);
    INSERT_ENTRY(10);

    // Insert some entries with version
    ASSERT_FALSE(entries.getEntry(1).has_value());
    ASSERT_SAME_ENTRY(*entries.getEntry(2), entry_v2);
    ASSERT_SAME_ENTRY(*entries.getEntry(3), entry_v2);
    ASSERT_SAME_ENTRY(*entries.getEntry(4), entry_v2);
    for (UInt64 seq = 5; seq < 10; ++seq)
    {
        ASSERT_SAME_ENTRY(*entries.getEntry(seq), entry_v5);
    }
    for (UInt64 seq = 10; seq < 20; ++seq)
    {
        ASSERT_SAME_ENTRY(*entries.getEntry(seq), entry_v10);
    }

    // Insert some entries with version && gc epoch
    INSERT_GC_ENTRY(2, 1);
    INSERT_GC_ENTRY(5, 1);
    INSERT_GC_ENTRY(5, 2);
    ASSERT_FALSE(entries.getEntry(1).has_value());
    ASSERT_SAME_ENTRY(*entries.getEntry(2), entry_gc_v2_1);
    ASSERT_SAME_ENTRY(*entries.getEntry(3), entry_gc_v2_1);
    ASSERT_SAME_ENTRY(*entries.getEntry(4), entry_gc_v2_1);
    for (UInt64 seq = 5; seq < 10; ++seq)
    {
        ASSERT_SAME_ENTRY(*entries.getEntry(seq), entry_gc_v5_2);
    }
    for (UInt64 seq = 10; seq < 20; ++seq)
    {
        ASSERT_SAME_ENTRY(*entries.getEntry(seq), entry_v10);
    }

    // Insert delete. Can not get entry with seq >= delete_version.
    // But it won't affect reading with old seq.
    entries.createDelete(PageVersionType(15));
    ASSERT_FALSE(entries.getEntry(1).has_value());
    ASSERT_SAME_ENTRY(*entries.getEntry(2), entry_gc_v2_1);
    ASSERT_SAME_ENTRY(*entries.getEntry(3), entry_gc_v2_1);
    ASSERT_SAME_ENTRY(*entries.getEntry(4), entry_gc_v2_1);
    for (UInt64 seq = 5; seq < 10; ++seq)
    {
        ASSERT_SAME_ENTRY(*entries.getEntry(seq), entry_gc_v5_2);
    }
    for (UInt64 seq = 10; seq < 15; ++seq)
    {
        ASSERT_SAME_ENTRY(*entries.getEntry(seq), entry_v10);
    }
    for (UInt64 seq = 15; seq < 20; ++seq)
    {
        ASSERT_FALSE(entries.getEntry(seq).has_value());
    }
}

TEST_F(VersionedEntriesTest, GC)
try
{
    INSERT_ENTRY(2);
    INSERT_GC_ENTRY(2, 1);
    INSERT_ENTRY(5);
    INSERT_GC_ENTRY(5, 1);
    INSERT_GC_ENTRY(5, 2);
    INSERT_ENTRY(10);
    INSERT_ENTRY(11);
    entries.createDelete(PageVersionType(15));

    // noting to be removed
    auto [all_removed, removed_entries, deref_counter] = runClean(1);
    ASSERT_FALSE(all_removed);
    ASSERT_EQ(removed_entries.size(), 0);
    ASSERT_EQ(deref_counter.size(), 0);

    // <2,0> get removed.
    std::tie(all_removed, removed_entries, deref_counter) = runClean(2);
    ASSERT_FALSE(all_removed);
    ASSERT_EQ(removed_entries.size(), 1);
    auto iter = removed_entries.begin();
    ASSERT_SAME_ENTRY(entry_v2, *iter);
    ASSERT_SAME_ENTRY(entry_gc_v2_1, *entries.getEntry(2));
    ASSERT_EQ(deref_counter.size(), 0);

    // nothing get removed.
    std::tie(all_removed, removed_entries, deref_counter) = runClean(4);
    ASSERT_FALSE(all_removed);
    ASSERT_EQ(removed_entries.size(), 0);
    ASSERT_SAME_ENTRY(entry_gc_v2_1, *entries.getEntry(4));
    ASSERT_EQ(deref_counter.size(), 0);

    // <2,1>, <5,0>, <5,1>, <5,2>, <10,0> get removed.
    std::tie(all_removed, removed_entries, deref_counter) = runClean(11);
    ASSERT_FALSE(all_removed);
    ASSERT_EQ(removed_entries.size(), 5);
    iter = removed_entries.begin();
    ASSERT_SAME_ENTRY(entry_v10, *iter);
    iter++;
    ASSERT_SAME_ENTRY(entry_gc_v5_2, *iter);
    iter++;
    ASSERT_SAME_ENTRY(entry_gc_v5_1, *iter);
    iter++;
    ASSERT_SAME_ENTRY(entry_v5, *iter);
    iter++;
    ASSERT_SAME_ENTRY(entry_gc_v2_1, *iter);
    ASSERT_SAME_ENTRY(entry_v11, *entries.getEntry(11));
    ASSERT_EQ(deref_counter.size(), 0);

    // <11,0> get removed, all cleared.
    std::tie(all_removed, removed_entries, deref_counter) = runClean(20);
    ASSERT_TRUE(all_removed); // should remove this chain
    ASSERT_EQ(removed_entries.size(), 1);
    ASSERT_FALSE(entries.getEntry(20));
    ASSERT_EQ(deref_counter.size(), 0);
}
CATCH

TEST_F(VersionedEntriesTest, DeleteMultiTime)
try
{
    entries.createDelete(PageVersionType(1));
    INSERT_ENTRY(2);
    INSERT_GC_ENTRY(2, 1);
    entries.createDelete(PageVersionType(15));
    entries.createDelete(PageVersionType(17));
    entries.createDelete(PageVersionType(16));

    bool all_removed;
    std::map<PageId, std::pair<PageVersionType, Int64>> deref_counter;
    PageEntriesV3 removed_entries;

    // <2,0> get removed.
    std::tie(all_removed, removed_entries, deref_counter) = runClean(2);
    ASSERT_FALSE(all_removed);
    ASSERT_EQ(removed_entries.size(), 1);
    auto iter = removed_entries.begin();
    ASSERT_SAME_ENTRY(entry_v2, *iter);
    ASSERT_SAME_ENTRY(entry_gc_v2_1, *entries.getEntry(2));
    ASSERT_EQ(deref_counter.size(), 0);

    // clear all
    std::tie(all_removed, removed_entries, deref_counter) = runClean(20);
    ASSERT_EQ(removed_entries.size(), 1);
    ASSERT_TRUE(all_removed); // should remove this chain
    ASSERT_FALSE(entries.getEntry(20));
    ASSERT_EQ(deref_counter.size(), 0);
}
CATCH

TEST_F(VersionedEntriesTest, DontCleanWhenBeingRef)
try
{
    bool all_removed;
    std::map<PageId, std::pair<PageVersionType, Int64>> deref_counter;
    PageEntriesV3 removed_entries;

    INSERT_ENTRY(2);
    entries.incrRefCount(PageVersionType(2));
    entries.incrRefCount(PageVersionType(2));
    entries.createDelete(PageVersionType(5));

    // <2, 0> is not available after seq=5, but not get removed
    ASSERT_SAME_ENTRY(entry_v2, *entries.getEntry(4));
    ASSERT_FALSE(entries.getEntry(5));

    // <2, 0> is not removed since it's being ref
    std::tie(all_removed, removed_entries, deref_counter) = runClean(5);
    ASSERT_FALSE(all_removed);
    ASSERT_EQ(removed_entries.size(), 0);
    ASSERT_FALSE(entries.getEntry(5));
    ASSERT_EQ(deref_counter.size(), 0);

    // decrease 1 ref counting
    std::tie(all_removed, removed_entries) = runDeref(5, PageVersionType(2), 1);
    ASSERT_EQ(removed_entries.size(), 0);
    ASSERT_FALSE(all_removed); // should not remove this chain
    ASSERT_FALSE(entries.getEntry(5));

    // clear all
    std::tie(all_removed, removed_entries) = runDeref(5, PageVersionType(2), 1);
    ASSERT_EQ(removed_entries.size(), 1);
    ASSERT_SAME_ENTRY(removed_entries[0], entry_v2);
    ASSERT_TRUE(all_removed); // should remove this chain
    ASSERT_FALSE(entries.getEntry(5));
}
CATCH

TEST_F(VersionedEntriesTest, DontCleanWhenBeingRef2)
try
{
    bool all_removed;
    std::map<PageId, std::pair<PageVersionType, Int64>> deref_counter;
    PageEntriesV3 removed_entries;

    INSERT_ENTRY(2);
    entries.incrRefCount(PageVersionType(2));
    entries.incrRefCount(PageVersionType(2));
    entries.createDelete(PageVersionType(5));

    // <2, 0> is not available after seq=5, but not get removed
    ASSERT_SAME_ENTRY(entry_v2, *entries.getEntry(4));
    ASSERT_FALSE(entries.getEntry(5));

    // <2, 0> is not removed since it's being ref
    std::tie(all_removed, removed_entries, deref_counter) = runClean(5);
    ASSERT_FALSE(all_removed);
    ASSERT_EQ(removed_entries.size(), 0);
    ASSERT_FALSE(entries.getEntry(5));
    ASSERT_EQ(deref_counter.size(), 0);

    // clear all
    std::tie(all_removed, removed_entries) = runDeref(5, PageVersionType(2), 2);
    ASSERT_EQ(removed_entries.size(), 1);
    ASSERT_SAME_ENTRY(removed_entries[0], entry_v2);
    ASSERT_TRUE(all_removed); // should remove this chain
    ASSERT_FALSE(entries.getEntry(5));
}
CATCH

TEST_F(VersionedEntriesTest, CleanDuplicatedWhenBeingRefAndAppliedUpsert)
try
{
    bool all_removed;
    std::map<PageId, std::pair<PageVersionType, Int64>> deref_counter;
    PageEntriesV3 removed_entries;

    INSERT_ENTRY(2);
    entries.incrRefCount(PageVersionType(2));
    entries.incrRefCount(PageVersionType(2));
    INSERT_GC_ENTRY(2, 1);
    INSERT_GC_ENTRY(2, 2);

    // <2, 2>
    ASSERT_SAME_ENTRY(entry_gc_v2_2, *entries.getEntry(4));

    // <2, 2> is not removed since it's being ref, but <2,0> <2,1> is removed since they are replaced by newer version
    std::tie(all_removed, removed_entries, deref_counter) = runClean(5);
    ASSERT_FALSE(all_removed);
    ASSERT_EQ(removed_entries.size(), 2);
    ASSERT_SAME_ENTRY(removed_entries[0], entry_gc_v2_1);
    ASSERT_SAME_ENTRY(removed_entries[1], entry_v2);
    ASSERT_SAME_ENTRY(entry_gc_v2_2, *entries.getEntry(4));
    ASSERT_EQ(deref_counter.size(), 0);

    // clear all
    std::tie(all_removed, removed_entries) = runDeref(5, PageVersionType(2), 2);
    ASSERT_EQ(removed_entries.size(), 0);
    ASSERT_FALSE(all_removed); // should not remove this chain
    ASSERT_SAME_ENTRY(entry_gc_v2_2, *entries.getEntry(4));
}
CATCH

TEST_F(VersionedEntriesTest, CleanDuplicatedWhenBeingRefAndAppliedUpsert2)
try
{
    bool all_removed;
    std::map<PageId, std::pair<PageVersionType, Int64>> deref_counter;
    PageEntriesV3 removed_entries;

    INSERT_ENTRY(2);
    entries.incrRefCount(PageVersionType(2));
    entries.incrRefCount(PageVersionType(2));
    INSERT_GC_ENTRY(2, 1);
    INSERT_GC_ENTRY(2, 2);
    entries.createDelete(PageVersionType(5));

    // <2, 2> is not available after seq=5, but not get removed
    ASSERT_SAME_ENTRY(entry_gc_v2_2, *entries.getEntry(4));
    ASSERT_FALSE(entries.getEntry(5));

    // <2, 2> is not removed since it's being ref, but <2,0> <2,1> is removed since they are replaced by newer version
    std::tie(all_removed, removed_entries, deref_counter) = runClean(5);
    ASSERT_FALSE(all_removed);
    ASSERT_EQ(removed_entries.size(), 2);
    ASSERT_SAME_ENTRY(removed_entries[0], entry_gc_v2_1);
    ASSERT_SAME_ENTRY(removed_entries[1], entry_v2);
    ASSERT_FALSE(entries.getEntry(5));
    ASSERT_EQ(deref_counter.size(), 0);

    // clear all
    std::tie(all_removed, removed_entries) = runDeref(5, PageVersionType(2), 2);
    ASSERT_EQ(removed_entries.size(), 1);
    ASSERT_SAME_ENTRY(removed_entries[0], entry_gc_v2_2);
    ASSERT_TRUE(all_removed); // should remove this chain
    ASSERT_FALSE(entries.getEntry(5));
}
CATCH

TEST_F(VersionedEntriesTest, ReadAfterGcApplied)
try
{
    bool all_removed;
    std::map<PageId, std::pair<PageVersionType, Int64>> deref_counter;
    PageEntriesV3 removed_entries;

    INSERT_ENTRY(2);
    INSERT_ENTRY(3);
    INSERT_ENTRY(5);

    // Read with snapshot seq=2
    ASSERT_SAME_ENTRY(entry_v2, *entries.getEntry(2));

    // Mock that gc applied and insert <2, 1>
    INSERT_GC_ENTRY(2, 1);

    // Now we should read the entry <2, 1> with seq=2
    ASSERT_SAME_ENTRY(entry_gc_v2_1, *entries.getEntry(2));

    // <2,0> get removed
    std::tie(all_removed, removed_entries, deref_counter) = runClean(2);
    ASSERT_EQ(removed_entries.size(), 1);
}
CATCH

TEST_F(VersionedEntriesTest, getEntriesByBlobId)
{
    INSERT_BLOBID_ENTRY(1, 1);
    INSERT_BLOBID_ENTRY(1, 2);
    INSERT_BLOBID_ENTRY(2, 3);
    INSERT_BLOBID_ENTRY(2, 4);
    INSERT_BLOBID_ENTRY(1, 5);
    INSERT_BLOBID_ENTRY(3, 6);
    INSERT_BLOBID_ENTRY(3, 8);
    INSERT_BLOBID_ENTRY(1, 11);

    PageId page_id = 100;
    auto check_for_blob_id_1 = [&](const PageIdAndVersionedEntries & entries) {
        auto it = entries.begin();

        ASSERT_EQ(std::get<0>(*it), page_id);
        ASSERT_EQ(std::get<1>(*it).sequence, 1);
        ASSERT_SAME_ENTRY(std::get<2>(*it), entry_v1);

        it++;
        ASSERT_EQ(std::get<0>(*it), page_id);
        ASSERT_EQ(std::get<1>(*it).sequence, 2);
        ASSERT_SAME_ENTRY(std::get<2>(*it), entry_v2);

        it++;
        ASSERT_EQ(std::get<0>(*it), page_id);
        ASSERT_EQ(std::get<1>(*it).sequence, 5);
        ASSERT_SAME_ENTRY(std::get<2>(*it), entry_v5);

        it++;
        ASSERT_EQ(std::get<0>(*it), page_id);
        ASSERT_EQ(std::get<1>(*it).sequence, 11);
        ASSERT_SAME_ENTRY(std::get<2>(*it), entry_v11);
    };
    auto check_for_blob_id_2 = [&](const PageIdAndVersionedEntries & entries) {
        auto it = entries.begin();

        ASSERT_EQ(std::get<0>(*it), page_id);
        ASSERT_EQ(std::get<1>(*it).sequence, 3);
        ASSERT_SAME_ENTRY(std::get<2>(*it), entry_v3);

        it++;
        ASSERT_EQ(std::get<0>(*it), page_id);
        ASSERT_EQ(std::get<1>(*it).sequence, 4);
        ASSERT_SAME_ENTRY(std::get<2>(*it), entry_v4);
    };
    auto check_for_blob_id_3 = [&](const PageIdAndVersionedEntries & entries) {
        auto it = entries.begin();

        ASSERT_EQ(std::get<0>(*it), page_id);
        ASSERT_EQ(std::get<1>(*it).sequence, 6);
        ASSERT_SAME_ENTRY(std::get<2>(*it), entry_v6);

        it++;
        ASSERT_EQ(std::get<0>(*it), page_id);
        ASSERT_EQ(std::get<1>(*it).sequence, 8);
        ASSERT_SAME_ENTRY(std::get<2>(*it), entry_v8);
    };

    {
        std::map<BlobFileId, PageIdAndVersionedEntries> blob_entries;
        PageSize total_size = entries.getEntriesByBlobIds({/*empty*/}, page_id, blob_entries);

        ASSERT_EQ(blob_entries.size(), 0);
        ASSERT_EQ(total_size, 0);
    }

    {
        std::map<BlobFileId, PageIdAndVersionedEntries> blob_entries;
        const BlobFileId blob_id = 1;
        PageSize total_size = entries.getEntriesByBlobIds({blob_id}, page_id, blob_entries);

        ASSERT_EQ(blob_entries.size(), 1);
        ASSERT_EQ(blob_entries[blob_id].size(), 4);
        ASSERT_EQ(total_size, 1 + 2 + 5 + 11);
        check_for_blob_id_1(blob_entries[blob_id]);
    }

    {
        std::map<BlobFileId, PageIdAndVersionedEntries> blob_entries;
        const BlobFileId blob_id = 2;
        PageSize total_size = entries.getEntriesByBlobIds({blob_id}, page_id, blob_entries);

        ASSERT_EQ(blob_entries.size(), 1);
        ASSERT_EQ(blob_entries[blob_id].size(), 2);
        ASSERT_EQ(total_size, 3 + 4);
        check_for_blob_id_2(blob_entries[blob_id]);
    }

    {
        std::map<BlobFileId, PageIdAndVersionedEntries> blob_entries;
        const BlobFileId blob_id = 3;
        PageSize total_size = entries.getEntriesByBlobIds({blob_id}, page_id, blob_entries);

        ASSERT_EQ(blob_entries.size(), 1);
        ASSERT_EQ(blob_entries[blob_id].size(), 2);
        ASSERT_EQ(total_size, 6 + 8);
        check_for_blob_id_3(blob_entries[blob_id]);
    }

    // {1, 2}
    {
        std::map<BlobFileId, PageIdAndVersionedEntries> blob_entries;
        PageSize total_size = entries.getEntriesByBlobIds({1, 2}, page_id, blob_entries);

        ASSERT_EQ(blob_entries.size(), 2);
        ASSERT_EQ(blob_entries[1].size(), 4);
        ASSERT_EQ(blob_entries[2].size(), 2);
        ASSERT_EQ(total_size, (1 + 2 + 5 + 11) + (3 + 4));
        check_for_blob_id_1(blob_entries[1]);
        check_for_blob_id_2(blob_entries[2]);
    }

    // {2, 3}
    {
        std::map<BlobFileId, PageIdAndVersionedEntries> blob_entries;
        PageSize total_size = entries.getEntriesByBlobIds({3, 2}, page_id, blob_entries);

        ASSERT_EQ(blob_entries.size(), 2);
        ASSERT_EQ(blob_entries[2].size(), 2);
        ASSERT_EQ(blob_entries[3].size(), 2);
        ASSERT_EQ(total_size, (6 + 8) + (3 + 4));
        check_for_blob_id_2(blob_entries[2]);
        check_for_blob_id_3(blob_entries[3]);
    }

    // {1, 2, 3}
    {
        std::map<BlobFileId, PageIdAndVersionedEntries> blob_entries;
        PageSize total_size = entries.getEntriesByBlobIds({1, 3, 2}, page_id, blob_entries);

        ASSERT_EQ(blob_entries.size(), 3);
        ASSERT_EQ(blob_entries[1].size(), 4);
        ASSERT_EQ(blob_entries[2].size(), 2);
        ASSERT_EQ(blob_entries[3].size(), 2);
        ASSERT_EQ(total_size, (1 + 2 + 5 + 11) + (6 + 8) + (3 + 4));
        check_for_blob_id_1(blob_entries[1]);
        check_for_blob_id_2(blob_entries[2]);
        check_for_blob_id_3(blob_entries[3]);
    }

    // {1, 2, 3, 100}; blob_id 100 is not exist in actual
    {
        std::map<BlobFileId, PageIdAndVersionedEntries> blob_entries;
        PageSize total_size = entries.getEntriesByBlobIds({1, 3, 2, 4}, page_id, blob_entries);

        ASSERT_EQ(blob_entries.size(), 3); // 100 not exist
        ASSERT_EQ(blob_entries.find(100), blob_entries.end());
        ASSERT_EQ(blob_entries[1].size(), 4);
        ASSERT_EQ(blob_entries[2].size(), 2);
        ASSERT_EQ(blob_entries[3].size(), 2);
        ASSERT_EQ(total_size, (1 + 2 + 5 + 11) + (6 + 8) + (3 + 4));
        check_for_blob_id_1(blob_entries[1]);
        check_for_blob_id_2(blob_entries[2]);
        check_for_blob_id_3(blob_entries[3]);
    }
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
        dir->apply(std::move(edit));                                                                                           \
    }
// Insert an entry into mvcc directory
#define INSERT_ENTRY(PAGE_ID, VERSION) INSERT_ENTRY_TO(PAGE_ID, VERSION, 1)
// Insert an entry into mvcc directory, and acquire a snapshot
#define INSERT_ENTRY_ACQ_SNAP(PAGE_ID, VERSION) \
    INSERT_ENTRY(PAGE_ID, VERSION)              \
    auto snapshot##VERSION = dir->createSnapshot();
#define INSERT_DELETE(PAGE_ID)       \
    {                                \
        PageEntriesEdit edit;        \
        edit.del((PAGE_ID));         \
        dir->apply(std::move(edit)); \
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
    auto del_entries = dir->gcInMemEntries();
    // v1, v2 have been removed.
    ASSERT_EQ(del_entries.size(), 2);

    EXPECT_ENTRY_EQ(entry_v3, dir, page_id, snapshot3);
    EXPECT_ENTRY_EQ(entry_v5, dir, page_id, snapshot5);

    // Release all snapshots and run gc again, (min gc version get pushed forward and)
    // all versions get compacted.
    snapshot3.reset();
    snapshot5.reset();
    del_entries = dir->gcInMemEntries();
    ASSERT_EQ(del_entries.size(), 2);

    auto snapshot_after_gc = dir->createSnapshot();
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
        const auto & del_entries = dir->gcInMemEntries();
        // page_id: []; another_page_id: v1 have been removed.
        EXPECT_EQ(del_entries.size(), 1);
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
        const auto & del_entries = dir->gcInMemEntries();
        // page_id: v2; another_page_id: v3 have been removed.
        EXPECT_EQ(del_entries.size(), 2);
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
        const auto & del_entries = dir->gcInMemEntries();
        // page_id: v5; another_page_id: v6,v7,v8 have been removed.
        EXPECT_EQ(del_entries.size(), 5);
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
        auto removed_entries = dir->gcInMemEntries(); // The GC on page_id=50 is blocked by previous `snapshot1`
        EXPECT_EQ(removed_entries.size(), 0);
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
        auto removed_entries = dir->gcInMemEntries();
        EXPECT_EQ(removed_entries.size(), 0);
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
        auto removed_entries = dir->gcInMemEntries(); // this will compact all versions
        // page_id: v3,v5; another_page_id: v1,v2,v4,v6,v7,v8 get removed
        EXPECT_EQ(removed_entries.size(), 8);

        auto snap_after_gc = dir->createSnapshot();
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
        dir->apply(std::move(edit));
    }
    auto snapshot8 = dir->createSnapshot();
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
        auto del_entries = dir->gcInMemEntries();
        // page_id: v3; another_page_id: v1,v2 have been removed.
        EXPECT_EQ(del_entries.size(), 3);
        ASSERT_EQ(dir->numPages(), 2);
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
        auto del_entries = dir->gcInMemEntries();
        // page_id: v5; another_page_id: v4,v6,v7 have been removed.
        EXPECT_EQ(del_entries.size(), 4);
        ASSERT_EQ(dir->numPages(), 1); // page_id should be removed.
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
        auto del_entries = dir->gcInMemEntries();
        // another_page_id: v8 have been removed.
        EXPECT_EQ(del_entries.size(), 1);
    }

    {
        /**
         * after GC => { empty }
         *   snapshot remain: []
         */
        snapshot9.reset();
        auto del_entries = dir->gcInMemEntries();
        // another_page_id: v9 have been removed.
        EXPECT_EQ(del_entries.size(), 1);
        ASSERT_EQ(dir->numPages(), 0); // all should be removed.
    }

    auto snapshot_after_all = dir->createSnapshot();
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
    auto candidate_entries_1 = dir->getEntriesByBlobIds({1});
    EXPECT_EQ(candidate_entries_1.first.size(), 1);
    EXPECT_EQ(candidate_entries_1.first[1].size(), 3); // 3 entries for 2 page id

    auto candidate_entries_2_3 = dir->getEntriesByBlobIds({2, 3});
    EXPECT_EQ(candidate_entries_2_3.first.size(), 2);
    const auto & entries_in_file2 = candidate_entries_2_3.first[2];
    const auto & entries_in_file3 = candidate_entries_2_3.first[3];
    EXPECT_EQ(entries_in_file2.size(), 2); // 2 entries for 1 page id
    EXPECT_EQ(entries_in_file3.size(), 1); // 1 entries for 1 page id

    PageEntriesEdit gc_migrate_entries;
    for (const auto & [file_id, entries] : candidate_entries_1.first)
    {
        (void)file_id;
        for (const auto & [page_id, ver, entry] : entries)
        {
            gc_migrate_entries.upsertPage(page_id, ver, entry);
        }
    }
    for (const auto & [file_id, entries] : candidate_entries_2_3.first)
    {
        (void)file_id;
        for (const auto & [page_id, ver, entry] : entries)
        {
            gc_migrate_entries.upsertPage(page_id, ver, entry);
        }
    }

    // Full GC execute apply
    dir->gcApply(std::move(gc_migrate_entries));
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

    EXPECT_EQ(dir->numPages(), 2);

    // 1.1 Full GC get entries for blob_id in [1]
    auto candidate_entries_1 = dir->getEntriesByBlobIds({1});
    EXPECT_EQ(candidate_entries_1.first.size(), 1);
    EXPECT_EQ(candidate_entries_1.first[1].size(), 3); // 3 entries for 2 page id

    // for blob_id in [2, 3]
    auto candidate_entries_2_3 = dir->getEntriesByBlobIds({2, 3});
    EXPECT_EQ(candidate_entries_2_3.first.size(), 2);
    const auto & entries_in_file2 = candidate_entries_2_3.first[2];
    const auto & entries_in_file3 = candidate_entries_2_3.first[3];
    EXPECT_EQ(entries_in_file2.size(), 2); // 2 entries for 1 page id
    EXPECT_EQ(entries_in_file3.size(), 1); // 1 entry for 1 page_id

    // 2.1 Execute GC
    dir->gcInMemEntries();
    // `page_id` get removed
    EXPECT_EQ(dir->numPages(), 1);

    PageEntriesEdit gc_migrate_entries;
    for (const auto & [file_id, entries] : candidate_entries_1.first)
    {
        (void)file_id;
        for (const auto & [page_id, ver, entry] : entries)
        {
            gc_migrate_entries.upsertPage(page_id, ver, entry);
        }
    }
    for (const auto & [file_id, entries] : candidate_entries_2_3.first)
    {
        (void)file_id;
        for (const auto & [page_id, ver, entry] : entries)
        {
            gc_migrate_entries.upsertPage(page_id, ver, entry);
        }
    }

    // 1.2 Full GC execute apply
    ASSERT_THROW({ dir->gcApply(std::move(gc_migrate_entries)); }, DB::Exception);
}
CATCH

TEST_F(PageDirectoryGCTest, GCOnRefedEntries)
try
{
    // 10->entry1, 11->10=>11->entry1; del 10->entry1
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(10, entry1);
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit;
        edit.ref(11, 10);
        edit.del(10);
        dir->apply(std::move(edit));
    }
    // entry1 should not be removed
    {
        auto outdated_entries = dir->gcInMemEntries();
        EXPECT_TRUE(outdated_entries.empty());
    }

    // del 11->entry1
    {
        PageEntriesEdit edit;
        edit.del(11);
        dir->apply(std::move(edit));
    }
    // entry1 get removed
    {
        auto outdated_entries = dir->gcInMemEntries();
        EXPECT_EQ(1, outdated_entries.size());
        EXPECT_SAME_ENTRY(entry1, *outdated_entries.begin());
    }
}
CATCH

TEST_F(PageDirectoryGCTest, GCOnRefedExternalEntries)
try
{
    // 10->ext, 11->10=>11->ext; del 10->ext
    {
        PageEntriesEdit edit;
        edit.putExternal(10);
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit;
        edit.ref(11, 10);
        edit.del(10);
        dir->apply(std::move(edit));
    }
    // entry1 should not be removed
    {
        auto outdated_entries = dir->gcInMemEntries();
        EXPECT_TRUE(outdated_entries.empty());
        auto alive_ex_id = dir->getAliveExternalIds();
        ASSERT_EQ(alive_ex_id.size(), 1);
        ASSERT_EQ(*alive_ex_id.begin(), 10);
    }

    // del 11->ext
    {
        PageEntriesEdit edit;
        edit.del(11);
        dir->apply(std::move(edit));
    }
    // entry1 get removed
    {
        auto outdated_entries = dir->gcInMemEntries();
        EXPECT_EQ(0, outdated_entries.size());
        auto alive_ex_id = dir->getAliveExternalIds();
        ASSERT_EQ(alive_ex_id.size(), 0);
    }
}
CATCH

#undef INSERT_ENTRY_TO
#undef INSERT_ENTRY
#undef INSERT_ENTRY_ACQ_SNAP
#undef INSERT_DELETE

} // namespace PS::V3::tests
} // namespace DB
