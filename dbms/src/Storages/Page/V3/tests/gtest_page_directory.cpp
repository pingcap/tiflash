#include <Common/Exception.h>
#include <Common/FmtUtils.h>
#include <IO/WriteHelpers.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/tests/entries_helper.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <fmt/format.h>

namespace DB
{
namespace PS::V3::tests
{
class PageDirectoryTest : public ::testing::Test
{
protected:
    PageDirectory dir;
};

TEST_F(PageDirectoryTest, ApplyPutRead)
try
{
    auto snap0 = dir.createSnapshot();
    EXPECT_ENTRY_NOT_EXIST(dir, 1, snap0);

    PageEntryV3 entry1{.file_id = 1, .size = 1024, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(1, entry1);
        dir.apply(std::move(edit));
    }

    auto snap1 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry1, dir, 1, snap1);

    PageEntryV3 entry2{.file_id = 2, .size = 1024, .offset = 0x123, .checksum = 0x4567};
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

    PageEntryV3 entry1{.file_id = 1, .size = 1024, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(page_id, entry1);
        dir.apply(std::move(edit));
    }

    auto snap1 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry1, dir, page_id, snap1);

    PageEntryV3 entry2{.file_id = 1, .size = 1024, .offset = 0x1234, .checksum = 0x4567};
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
    PageEntryV3 entry3{.file_id = 1, .size = 1024, .offset = 0x12345, .checksum = 0x4567};
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
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry2{.file_id = 2, .size = 1024, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(1, entry1);
        edit.put(2, entry2);
        dir.apply(std::move(edit));
    }

    auto snap1 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry1, dir, 1, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);

    PageEntryV3 entry3{.file_id = 3, .size = 1024, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry4{.file_id = 4, .size = 1024, .offset = 0x123, .checksum = 0x4567};
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
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry2{.file_id = 2, .size = 1024, .offset = 0x123, .checksum = 0x4567};
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
    PageEntryV3 entry_updated{.file_id = 999, .size = 16, .offset = 0x123, .checksum = 0x123};
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

    PageEntryV3 entry_updated2{.file_id = 777, .size = 16, .offset = 0x123, .checksum = 0x123};
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
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry2{.file_id = 2, .size = 1024, .offset = 0x123, .checksum = 0x4567};
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
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry2{.file_id = 2, .size = 1024, .offset = 0x123, .checksum = 0x4567};
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
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry2{.file_id = 2, .size = 1024, .offset = 0x123, .checksum = 0x4567};
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
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry2{.file_id = 2, .size = 1024, .offset = 0x123, .checksum = 0x4567};
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
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry2{.file_id = 2, .size = 1024, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(1, entry1);
        edit.put(2, entry2);
        dir.apply(std::move(edit));
    }

    { // Ref 3-> 999
        PageEntriesEdit edit;
        edit.ref(3, 999);
        ASSERT_THROW({ dir.apply(std::move(edit)); }, DB::Exception);
    }
    auto snap1 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_NOT_EXIST(dir, 3, snap1);

    // TODO: restore, invalid ref page is filtered
}
CATCH

#define INSERT_BLOBID_ENTRY(BLOBID, VERSION)                                                                 \
    PageEntryV3 entry_v##VERSION{.file_id = BLOBID, .size = (VERSION), .offset = 0x123, .checksum = 0x4567}; \
    entries.createNewVersion((VERSION), entry_v##VERSION);
#define INSERT_ENTRY(VERSION) INSERT_BLOBID_ENTRY(1, VERSION)
#define INSERT_GC_ENTRY(VERSION, EPOCH)                                                                              \
    PageEntryV3 entry_gc_v##VERSION##_##EPOCH{.file_id = 2, .size = (VERSION), .offset = 0x234, .checksum = 0x5678}; \
    entries.createNewVersion((VERSION), (EPOCH), entry_gc_v##VERSION##_##EPOCH);

TEST(VersionedEntriesTest, InsertGet)
{
    PageDirectory::VersionedPageEntries entries;
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

TEST(VersionedEntriesTest, Gc)
try
{
    PageDirectory::VersionedPageEntries entries;
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

    // noting to be removed
    removed_entries = entries.deleteAndGC(2);
    ASSERT_FALSE(removed_entries.second);
    ASSERT_EQ(removed_entries.first.size(), 0);

    // <2,0> get removed.
    removed_entries = entries.deleteAndGC(4);
    ASSERT_FALSE(removed_entries.second);
    ASSERT_EQ(removed_entries.first.size(), 1);

    // <2,1>, <5,0>, <5,1>, <5,2>, <10,0> get removed.
    removed_entries = entries.deleteAndGC(11);
    ASSERT_FALSE(removed_entries.second);
    ASSERT_EQ(removed_entries.first.size(), 5);

    // <11,0> get removed, all cleared.
    removed_entries = entries.deleteAndGC(20);
    ASSERT_TRUE(removed_entries.second);
    ASSERT_EQ(removed_entries.first.size(), 1);
}
CATCH


TEST(VersionedEntriesTest, getEntriesFromBlobId)
{
    PageDirectory::VersionedPageEntries entries;

    INSERT_BLOBID_ENTRY(1, 1);
    INSERT_BLOBID_ENTRY(1, 2);
    INSERT_BLOBID_ENTRY(2, 3);
    INSERT_BLOBID_ENTRY(2, 4);
    INSERT_BLOBID_ENTRY(1, 5);
    INSERT_BLOBID_ENTRY(3, 6);
    INSERT_BLOBID_ENTRY(3, 8);
    INSERT_BLOBID_ENTRY(1, 11);

    const auto & [versioned_entries1, total_size1] = entries.getEntriesFromBlobId(1);

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

    const auto & [versioned_entries2, total_size2] = entries.getEntriesFromBlobId(2);

    ASSERT_EQ(versioned_entries2.size(), 2);
    ASSERT_EQ(total_size2, 3 + 4);
    it = versioned_entries2.begin();

    ASSERT_EQ(it->first.sequence, 3);
    ASSERT_TRUE(isSameEntry(it->second, entry_v3));

    it++;
    ASSERT_EQ(it->first.sequence, 4);
    ASSERT_TRUE(isSameEntry(it->second, entry_v4));

    const auto & [versioned_entries3, total_size3] = entries.getEntriesFromBlobId(3);

    ASSERT_EQ(versioned_entries3.size(), 2);
    ASSERT_EQ(total_size3, 6 + 8);
    it = versioned_entries3.begin();

    ASSERT_EQ(it->first.sequence, 6);
    ASSERT_TRUE(isSameEntry(it->second, entry_v6));

    it++;
    ASSERT_EQ(it->first.sequence, 8);
    ASSERT_TRUE(isSameEntry(it->second, entry_v8));
}


#undef INSERT_ENTRY
#undef INSERT_GC_ENTRY
// end of testing `VersionedEntriesTest`

class PageDirectoryGCTest : public DB::base::TiFlashStorageTestBasic
{
public:
    void pushMvcc(size_t seq_nums, UInt64 get_snapshot = UINT64_MAX)
    {
        PageId page_id = UINT64_MAX - 100;
        [[maybe_unused]] PageVersionAndEntriesV3 meanless_seq_entries;

        for (size_t idx = 0; idx < seq_nums; idx++)
        {
            putMvcc(page_id, 1, meanless_seq_entries, 0, true);
            if (get_snapshot != UINT64_MAX && idx == get_snapshot)
            {
                snapshots_holder.emplace_back(dir.createSnapshot());
            }
        }
    }

    void putMvcc(PageId page_id,
                 size_t buff_nums,
                 PageVersionAndEntriesV3 & seq_entries,
                 UInt64 seq_start,
                 bool no_need_add = false)
    {
        for (size_t i = 0; i < buff_nums; ++i)
        {
            PageEntriesEdit edit;
            PageEntryV3 entry{.file_id = 1, .size = 1024, .offset = 0x123, .checksum = 0x4567};
            edit.put(page_id, entry);

            if (!no_need_add)
            {
                seq_entries.emplace_back(std::make_tuple(seq_start++, 0, entry));
            }
            dir.apply(std::move(edit));
        }
    }


    void delMvcc(PageId page_id)
    {
        PageEntriesEdit edit;
        edit.del(page_id);
        dir.apply(std::move(edit));
    }

protected:
    PageDirectory dir;
    std::list<PageDirectorySnapshotPtr> snapshots_holder;
};

TEST_F(PageDirectoryGCTest, TestPageDirectoryGCwithAlignSeq)
try
{
    /**
     * before GC => 
     *   pageid : 50
     *   entries: [v1...v5]
     *   hold_seq: [v3,v5]
     * after GC => 
     *   pageid : 50
     *   entries remain: [v3,v4,v5]
     *   snapshot remain: [v3,v5]
     */
    PageId page_id = 50;
    PageVersionAndEntriesV3 exp_seq_entries;

    // put v1 - v2
    putMvcc(page_id, 2, exp_seq_entries, 0, true);

    // put v3
    putMvcc(page_id, 1, exp_seq_entries, 3, false);
    auto snapshot_holder = dir.createSnapshot();

    // put v4
    putMvcc(page_id, 1, exp_seq_entries, 4, false);

    // put v5
    putMvcc(page_id, 1, exp_seq_entries, 5, false);
    auto snapshot_holder2 = dir.createSnapshot();

    const auto & del_entries = dir.gc();

    ASSERT_EQ(del_entries.size(), 1);
    // v1 , v2 have been removed.
    ASSERT_EQ(del_entries[0].size(), 2);

    EXPECT_SEQ_ENTRIES_EQ(exp_seq_entries, dir, page_id);
}
CATCH

TEST_F(PageDirectoryGCTest, TestPageDirectoryGCwithUnalignSeq1)
try
{
    /**
     * case 1
     * before GC => 
     *   pageid : 50
     *   entries: [v3, v5, v10]
     *   lowest_seq: 1
     *   hold_seq: [v1, v3, v5, v10]
     * after GC => 
     *   pageid : 50
     *   entries: [v3, v5, v10]
     *   snapshot remain: [v3, v5, v10]
     */
    PageId page_id = 50;

    PageVersionAndEntriesV3 exp_seq_entries;

    // push v1 - v2
    pushMvcc(2, 0);

    // put v3
    putMvcc(page_id, 1, exp_seq_entries, 3);
    auto snapshot_holder1 = dir.createSnapshot();

    // push v4
    pushMvcc(1);

    // put v5
    putMvcc(page_id, 1, exp_seq_entries, 5);
    auto snapshot_holder2 = dir.createSnapshot();

    // push v6-v9
    pushMvcc(4);

    // put v10
    putMvcc(page_id, 1, exp_seq_entries, 10);
    auto snapshot_holder3 = dir.createSnapshot();

    const auto & del_entries = dir.gc();
    // no entry been removed
    ASSERT_EQ(del_entries.size(), 0);

    EXPECT_SEQ_ENTRIES_EQ(exp_seq_entries, dir, page_id);
}
CATCH

TEST_F(PageDirectoryGCTest, TestPageDirectoryGCwithUnalignSeq2)
try
{
    /**
     * case 2
     * before GC => 
     *   pageid : 50
     *   entries: [v2, v5, v10]
     *   lowest_seq: 3
     *   hold_seq: [v3, v5, v10]
     * after GC => 
     *   pageid : 50
     *   entries: [v5, v10]
     *   snapshot remain: [v2, v5, v10]
     */
    PageId page_id = 50;

    PageVersionAndEntriesV3 exp_seq_entries;

    // push v1 with anonymous entry
    pushMvcc(1);

    // put v2 without hold the snapshot.
    putMvcc(page_id, 1, exp_seq_entries, 2);

    // push v3, v4 with anonymous entry
    // Also hold v3 snapshot
    pushMvcc(2, 0);

    // put v5
    putMvcc(page_id, 1, exp_seq_entries, 5);
    auto snapshot_holder1 = dir.createSnapshot();

    // push v6-v9 with anonymous entry
    pushMvcc(4);

    // put v10
    putMvcc(page_id, 1, exp_seq_entries, 10);
    auto snapshot_holder2 = dir.createSnapshot();

    const auto & del_entries = dir.gc();

    ASSERT_EQ(del_entries.size(), 1);
    // v1  have been removed.
    ASSERT_EQ(del_entries[0].size(), 1);

    EXPECT_SEQ_ENTRIES_EQ(exp_seq_entries, dir, page_id);
}
CATCH

TEST_F(PageDirectoryGCTest, TestPageDirectoryGCwithUnalignSeq3)
try
{
    /**
     * case 3
     * before GC => 
     *   pageid : 50
     *   entries: [v2, v5, v10]
     *   lowest_seq: 7
     *   hold_seq: [v7, v10]
     * after GC => 
     *   pageid : 50
     *   entries remain: [v5, v10]
     *   snapshot remain : [v5, v10]
     */
    PageId page_id = 50;

    PageVersionAndEntriesV3 exp_seq_entries;

    // push v1 with anonymous entry
    pushMvcc(1);

    // put v2 without hold the snapshot.
    // No need add v2 into `exp_seq_entries`
    putMvcc(page_id, 1, exp_seq_entries, 2, true);

    // push v3, v4 with anonymous entry
    pushMvcc(2);

    // put v5 without hold the snapshot.
    putMvcc(page_id, 1, exp_seq_entries, 5);

    // push v6-v9 with anonymous entry
    // Also hold v7 snapshot
    pushMvcc(4, 1);

    // put v10
    putMvcc(page_id, 1, exp_seq_entries, 10);
    auto snapshot_holder = dir.createSnapshot();

    const auto & del_entries = dir.gc();

    ASSERT_EQ(del_entries.size(), 2);

    // v2 have been removed.
    ASSERT_EQ(del_entries[0].size(), 1);

    // v1 v3 v4 v6 have been removed.
    ASSERT_EQ(del_entries[1].size(), 4);

    EXPECT_SEQ_ENTRIES_EQ(exp_seq_entries, dir, page_id);
}
CATCH

TEST_F(PageDirectoryGCTest, TestPageDirectoryGCwithUnalignSeq4)
try
{
    /**
     * case 4
     * before GC => 
     *   pageid : 50
     *   entries: [v2, v5, v10]
     *   lowest_seq: 11
     *   hold_seq: [11]
     * after GC => 
     *   pageid : 50
     *   entries remain: [v10]
     *   snapshot remain : [v10]
     */
    PageId page_id = 50;

    PageVersionAndEntriesV3 exp_seq_entries;

    // push v1 with anonymous entry
    pushMvcc(1);

    // put v2 without hold the snapshot.
    // No need add v2 into `exp_seq_entries`
    putMvcc(page_id, 1, exp_seq_entries, 2, true);

    // push v3, v4 with anonymous entry
    pushMvcc(2);

    // put v5 without hold the snapshot.
    // No need add v2 into `exp_seq_entries`
    putMvcc(page_id, 1, exp_seq_entries, 5, true);

    // push v6-v9 with anonymous entry
    pushMvcc(4);

    // put v10 without hold the snapshot.
    // add v2 into `exp_seq_entries`
    putMvcc(page_id, 1, exp_seq_entries, 10);

    // push v11 with anonymous entry
    // Also hold v11 snapshot
    pushMvcc(1, 0);

    const auto & del_entries = dir.gc();

    ASSERT_EQ(del_entries.size(), 2);

    // v2 v5 have been removed.
    ASSERT_EQ(del_entries[0].size(), 2);

    // v1 v3 v4 v6 v7 v8 v9 have been removed.
    ASSERT_EQ(del_entries[1].size(), 7);

    EXPECT_SEQ_ENTRIES_EQ(exp_seq_entries, dir, page_id);
}
CATCH

TEST_F(PageDirectoryGCTest, TestPageDirectoryGCwithDel)
try
{
    /**
     * before GC => 
     *   pageid : 50
     *   entries: [v2, v5, v10(del)]
     *   lowest_seq: 11
     *   hold_seq: [11]
     * after GC => 
     *   pageid : 50
     *   entries remain: [none]
     *   snapshot remain : [none]
     */
    PageId page_id = 50;

    [[maybe_unused]] PageVersionAndEntriesV3 meanless_seq_entries;

    // push v1 with anonymous entry
    pushMvcc(1);

    // put v2 without hold the snapshot.
    // No need add v2 into `exp_seq_entries`
    putMvcc(page_id, 1, meanless_seq_entries, 2, true);

    // push v3, v4 with anonymous entry
    pushMvcc(2);

    // put v5 without hold the snapshot.
    // No need add v2 into `exp_seq_entries`
    putMvcc(page_id, 1, meanless_seq_entries, 5, true);

    // push v6-v9 with anonymous entry
    pushMvcc(4);

    // put a writebatch with [type=del].
    // Then mvcc will push seq into v10
    delMvcc(page_id);

    // push v11 with anonymous entry
    // Also hold v11 snapshot
    pushMvcc(1, 0);

    const auto del_entries = dir.gc();

    ASSERT_EQ(del_entries.size(), 1);

    // v2 v5 have been removed.
    ASSERT_EQ(del_entries[0].size(), 2);

    auto snapshot = dir.createSnapshot();
    EXPECT_ENTRY_NOT_EXIST(dir, page_id, snapshot);
}
CATCH

TEST_F(PageDirectoryGCTest, gcApply)
{
    PageId page_id = 50;
    PageVersionAndEntriesV3 exp_seq_entries;
    VersionedPageIdAndEntryList versioned_pageid_entry_list;

    PageEntryV3 entry1{.file_id = 0, .size = 1024, .offset = 0x1234, .checksum = 0x5678};
    PageEntryV3 entry2{.file_id = 0, .size = 1024, .offset = 0x12345, .checksum = 0x678910};

    putMvcc(page_id, 4, exp_seq_entries, 1);
    exp_seq_entries.emplace_back(std::make_tuple(4, 1, entry1));
    putMvcc(page_id, 2, exp_seq_entries, 5);
    exp_seq_entries.emplace_back(std::make_tuple(6, 1, entry2));

    versioned_pageid_entry_list.emplace_back(std::make_tuple(page_id, PageVersionType(4, 0), entry1));
    versioned_pageid_entry_list.emplace_back(std::make_tuple(page_id, PageVersionType(6, 0), entry2));

    dir.gcApply(versioned_pageid_entry_list);
    EXPECT_SEQ_ENTRIES_EQ(exp_seq_entries, dir, page_id);
}


} // namespace PS::V3::tests
} // namespace DB
