
// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/Exception.h>
#include <Common/FmtUtils.h>
#include <IO/FileProvider/FileProvider.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/V3/PageDefines.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/tests/entries_helper.h>
#include <TestUtils/MockDiskDelegator.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <common/logger_useful.h>
#include <common/types.h>
#include <fmt/format.h>

#include <iterator>
#include <memory>

#pragma GCC diagnostic push
#ifdef __clang__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
// include to suppress warnings on NO_THREAD_SAFETY_ANALYSIS. clang can't work without this include, don't know why
#include <grpcpp/security/credentials.h>
#pragma GCC diagnostic pop

namespace DB
{
namespace PS::V3::tests
{


#define INSERT_BLOBID_ENTRY(BLOBID, VERSION) \
    PageEntryV3 entry_v##VERSION{            \
        .file_id = (BLOBID),                 \
        .size = (VERSION),                   \
        .padded_size = 0,                    \
        .tag = 0,                            \
        .offset = 0x123,                     \
        .checksum = 0x4567};                 \
    entries.createNewEntry(PageVersion(VERSION), entry_v##VERSION);
#define INSERT_ENTRY(VERSION) INSERT_BLOBID_ENTRY(1, VERSION)
#define INSERT_GC_ENTRY(VERSION, EPOCH)        \
    PageEntryV3 entry_gc_v##VERSION##_##EPOCH{ \
        .file_id = 2,                          \
        .size = 100 * (VERSION) + (EPOCH),     \
        .padded_size = 0,                      \
        .tag = 0,                              \
        .offset = 0x234,                       \
        .checksum = 0x5678};                   \
    (void)entries.createUpsertEntry(           \
        PageVersion((VERSION), (EPOCH)),       \
        entry_gc_v##VERSION##_##EPOCH,         \
        /*strict_check*/ true);


class VersionedEntriesTest : public ::testing::Test
{
public:
    using DerefCounter = std::map<PageIdV3Internal, std::pair<PageVersion, Int64>>;
    std::tuple<bool, PageEntriesV3, DerefCounter> runClean(UInt64 seq) NO_THREAD_SAFETY_ANALYSIS
    {
        DerefCounter deref_counter;
        PageEntriesV3 removed_entries;
        bool all_removed
            = entries.cleanOutdatedEntries(seq, &deref_counter, &removed_entries, nullptr, entries.acquireLock());
        return {all_removed, removed_entries, deref_counter};
    }

    std::tuple<bool, PageEntriesV3> runDeref(UInt64 seq, PageVersion ver, Int64 decrease_num)
    {
        PageEntriesV3 removed_entries;
        bool all_removed
            = entries.derefAndClean(seq, buildV3Id(TEST_NAMESPACE_ID, page_id), ver, decrease_num, &removed_entries);
        return {all_removed, removed_entries};
    }

protected:
    const PageIdU64 page_id = 100;
    u128::VersionedPageEntries entries;
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
    entries.createDelete(PageVersion(15));
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

TEST_F(VersionedEntriesTest, InsertWithLowerVersion)
{
    INSERT_ENTRY(5);
    ASSERT_SAME_ENTRY(*entries.getEntry(5), entry_v5);
    ASSERT_FALSE(entries.getEntry(2).has_value());
    INSERT_ENTRY(2);
    ASSERT_SAME_ENTRY(*entries.getEntry(2), entry_v2);
}

TEST_F(VersionedEntriesTest, EntryIsVisible)
try
{
    // init state
    ASSERT_FALSE(entries.isVisible(0));
    ASSERT_FALSE(entries.isVisible(1));
    ASSERT_FALSE(entries.isVisible(2));
    ASSERT_FALSE(entries.isVisible(10000));

    // insert some entries
    INSERT_ENTRY(2);
    INSERT_ENTRY(3);
    INSERT_ENTRY(5);

    ASSERT_FALSE(entries.isVisible(1));
    ASSERT_TRUE(entries.isVisible(2));
    ASSERT_TRUE(entries.isVisible(3));
    ASSERT_TRUE(entries.isVisible(4));
    ASSERT_TRUE(entries.isVisible(5));
    ASSERT_TRUE(entries.isVisible(6));

    // insert delete
    entries.createDelete(PageVersion(6));

    ASSERT_FALSE(entries.isVisible(1));
    ASSERT_TRUE(entries.isVisible(2));
    ASSERT_TRUE(entries.isVisible(3));
    ASSERT_TRUE(entries.isVisible(4));
    ASSERT_TRUE(entries.isVisible(5));
    ASSERT_FALSE(entries.isVisible(6));
    ASSERT_FALSE(entries.isVisible(10000));

    // insert entry after delete
    INSERT_ENTRY(7);

    ASSERT_FALSE(entries.isVisible(1));
    ASSERT_TRUE(entries.isVisible(2));
    ASSERT_TRUE(entries.isVisible(3));
    ASSERT_TRUE(entries.isVisible(4));
    ASSERT_TRUE(entries.isVisible(5));
    ASSERT_FALSE(entries.isVisible(6));
    ASSERT_TRUE(entries.isVisible(7));
    ASSERT_TRUE(entries.isVisible(10000));
}
CATCH

TEST_F(VersionedEntriesTest, ExternalPageIsVisible)
try
{
    // init state
    ASSERT_FALSE(entries.isVisible(0));
    ASSERT_FALSE(entries.isVisible(1));
    ASSERT_FALSE(entries.isVisible(2));
    ASSERT_FALSE(entries.isVisible(10000));

    // insert some entries
    entries.createNewExternal(PageVersion(2), PageEntryV3{});

    ASSERT_FALSE(entries.isVisible(1));
    ASSERT_TRUE(entries.isVisible(2));
    ASSERT_TRUE(entries.isVisible(10000));

    // insert delete
    entries.createDelete(PageVersion(6));

    ASSERT_FALSE(entries.isVisible(1));
    ASSERT_TRUE(entries.isVisible(2));
    ASSERT_TRUE(entries.isVisible(3));
    ASSERT_TRUE(entries.isVisible(4));
    ASSERT_TRUE(entries.isVisible(5));
    ASSERT_FALSE(entries.isVisible(6));
    ASSERT_FALSE(entries.isVisible(10000));

    // insert entry after delete
    entries.createNewExternal(PageVersion(7), PageEntryV3{});

    // after re-create external page, the visible for 1~5 has changed
    ASSERT_FALSE(entries.isVisible(6));
    ASSERT_TRUE(entries.isVisible(7));
    ASSERT_TRUE(entries.isVisible(10000));
}
CATCH

TEST_F(VersionedEntriesTest, RefPageIsVisible)
try
{
    // init state
    ASSERT_FALSE(entries.isVisible(0));
    ASSERT_FALSE(entries.isVisible(1));
    ASSERT_FALSE(entries.isVisible(2));
    ASSERT_FALSE(entries.isVisible(10000));

    // insert some entries
    entries.createNewRef(PageVersion(2), buildV3Id(TEST_NAMESPACE_ID, 2));

    ASSERT_FALSE(entries.isVisible(1));
    ASSERT_TRUE(entries.isVisible(2));
    ASSERT_TRUE(entries.isVisible(10000));

    // insert delete
    entries.createDelete(PageVersion(6));

    ASSERT_FALSE(entries.isVisible(1));
    ASSERT_TRUE(entries.isVisible(2));
    ASSERT_TRUE(entries.isVisible(3));
    ASSERT_TRUE(entries.isVisible(4));
    ASSERT_TRUE(entries.isVisible(5));
    ASSERT_FALSE(entries.isVisible(6));
    ASSERT_FALSE(entries.isVisible(10000));

    // insert entry after delete
    entries.createNewRef(PageVersion(7), buildV3Id(TEST_NAMESPACE_ID, 2));

    // after re-create ref page, the visible for 1~5 has changed
    ASSERT_FALSE(entries.isVisible(6));
    ASSERT_TRUE(entries.isVisible(7));
    ASSERT_TRUE(entries.isVisible(10000));
}
CATCH

TEST_F(VersionedEntriesTest, CleanOutdateVersions)
try
{
    // Test running gc on a single page, it should clean all
    // outdated versions.
    INSERT_ENTRY(2);
    INSERT_GC_ENTRY(2, 1);
    INSERT_ENTRY(5);
    INSERT_GC_ENTRY(5, 1);
    INSERT_GC_ENTRY(5, 2);
    INSERT_ENTRY(10);
    INSERT_ENTRY(11);
    entries.createDelete(PageVersion(15));

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
    entries.createDelete(PageVersion(1));
    INSERT_ENTRY(2);
    INSERT_GC_ENTRY(2, 1);
    entries.createDelete(PageVersion(15));
    entries.createDelete(PageVersion(17));
    entries.createDelete(PageVersion(16));

    bool all_removed;
    std::map<PageIdV3Internal, std::pair<PageVersion, Int64>> deref_counter;
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
    std::map<PageIdV3Internal, std::pair<PageVersion, Int64>> deref_counter;
    PageEntriesV3 removed_entries;

    INSERT_ENTRY(2);
    entries.incrRefCount(PageVersion(2), PageVersion(3));
    entries.incrRefCount(PageVersion(2), PageVersion(4));
    entries.createDelete(PageVersion(5));

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
    std::tie(all_removed, removed_entries) = runDeref(5, PageVersion(2), 1);
    ASSERT_EQ(removed_entries.size(), 0);
    ASSERT_FALSE(all_removed); // should not remove this chain
    ASSERT_FALSE(entries.getEntry(5));

    // clear all
    std::tie(all_removed, removed_entries) = runDeref(5, PageVersion(2), 1);
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
    std::map<PageIdV3Internal, std::pair<PageVersion, Int64>> deref_counter;
    PageEntriesV3 removed_entries;

    INSERT_ENTRY(2);
    entries.incrRefCount(PageVersion(2), PageVersion(3));
    entries.incrRefCount(PageVersion(2), PageVersion(4));
    entries.createDelete(PageVersion(5));

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
    std::tie(all_removed, removed_entries) = runDeref(5, PageVersion(2), 2);
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
    std::map<PageIdV3Internal, std::pair<PageVersion, Int64>> deref_counter;
    PageEntriesV3 removed_entries;

    INSERT_ENTRY(2);
    entries.incrRefCount(PageVersion(2), PageVersion(3));
    entries.incrRefCount(PageVersion(2), PageVersion(4));
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
    std::tie(all_removed, removed_entries) = runDeref(5, PageVersion(2), 2);
    ASSERT_EQ(removed_entries.size(), 0);
    ASSERT_FALSE(all_removed); // should not remove this chain
    ASSERT_SAME_ENTRY(entry_gc_v2_2, *entries.getEntry(4));
}
CATCH

TEST_F(VersionedEntriesTest, CleanDuplicatedWhenBeingRefAndAppliedUpsert2)
try
{
    bool all_removed;
    std::map<PageIdV3Internal, std::pair<PageVersion, Int64>> deref_counter;
    PageEntriesV3 removed_entries;

    INSERT_ENTRY(2);
    entries.incrRefCount(PageVersion(2), PageVersion(3));
    entries.incrRefCount(PageVersion(2), PageVersion(4));
    INSERT_GC_ENTRY(2, 1);
    INSERT_GC_ENTRY(2, 2);
    entries.createDelete(PageVersion(5));

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
    std::tie(all_removed, removed_entries) = runDeref(5, PageVersion(2), 2);
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
    std::map<PageIdV3Internal, std::pair<PageVersion, Int64>> deref_counter;
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

    PageIdU64 page_id = 100;
    auto check_for_blob_id_1 = [&](const PageDirectory<u128::PageDirectoryTrait>::GcEntries & entries) {
        auto it = entries.begin();
        ASSERT_EQ(std::get<0>(*it).low, page_id);
        ASSERT_EQ(std::get<1>(*it).sequence, 11);
        ASSERT_SAME_ENTRY(std::get<2>(*it), entry_v11);
    };

    {
        PageDirectory<u128::PageDirectoryTrait>::GcEntriesMap blob_entries;
        std::map<PageIdV3Internal, std::tuple<PageIdV3Internal, PageVersion>> rewrite;
        PageSize total_size
            = entries.getEntriesByBlobIds({/*empty*/}, buildV3Id(TEST_NAMESPACE_ID, page_id), blob_entries, rewrite);

        ASSERT_EQ(blob_entries.size(), 0);
        ASSERT_EQ(total_size, 0);
    }

    {
        PageDirectory<u128::PageDirectoryTrait>::GcEntriesMap blob_entries;
        std::map<PageIdV3Internal, std::tuple<PageIdV3Internal, PageVersion>> rewrite;
        const BlobFileId blob_id = 1;
        PageSize total_size
            = entries.getEntriesByBlobIds({blob_id}, buildV3Id(TEST_NAMESPACE_ID, page_id), blob_entries, rewrite);

        ASSERT_EQ(blob_entries.size(), 1);
        ASSERT_EQ(blob_entries[blob_id].size(), 1);
        ASSERT_EQ(total_size, 11);
        check_for_blob_id_1(blob_entries[blob_id]);
    }

    {
        PageDirectory<u128::PageDirectoryTrait>::GcEntriesMap blob_entries;
        std::map<PageIdV3Internal, std::tuple<PageIdV3Internal, PageVersion>> rewrite;
        const BlobFileId blob_id = 2;
        PageSize total_size
            = entries.getEntriesByBlobIds({blob_id}, buildV3Id(TEST_NAMESPACE_ID, page_id), blob_entries, rewrite);

        ASSERT_EQ(blob_entries.empty(), true);
        ASSERT_EQ(total_size, 0);
    }

    {
        PageDirectory<u128::PageDirectoryTrait>::GcEntriesMap blob_entries;
        std::map<PageIdV3Internal, std::tuple<PageIdV3Internal, PageVersion>> rewrite;
        const BlobFileId blob_id = 3;
        PageSize total_size
            = entries.getEntriesByBlobIds({blob_id}, buildV3Id(TEST_NAMESPACE_ID, page_id), blob_entries, rewrite);

        ASSERT_EQ(blob_entries.empty(), true);
        ASSERT_EQ(total_size, 0);
    }

    // {1, 2}
    {
        PageDirectory<u128::PageDirectoryTrait>::GcEntriesMap blob_entries;
        std::map<PageIdV3Internal, std::tuple<PageIdV3Internal, PageVersion>> rewrite;
        PageSize total_size
            = entries.getEntriesByBlobIds({1, 2}, buildV3Id(TEST_NAMESPACE_ID, page_id), blob_entries, rewrite);

        ASSERT_EQ(blob_entries.size(), 1);
        ASSERT_EQ(blob_entries[1].size(), 1);
        ASSERT_EQ(total_size, 11);
        check_for_blob_id_1(blob_entries[1]);
    }

    // {2, 3}
    {
        PageDirectory<u128::PageDirectoryTrait>::GcEntriesMap blob_entries;
        std::map<PageIdV3Internal, std::tuple<PageIdV3Internal, PageVersion>> rewrite;
        PageSize total_size
            = entries.getEntriesByBlobIds({3, 2}, buildV3Id(TEST_NAMESPACE_ID, page_id), blob_entries, rewrite);

        ASSERT_EQ(blob_entries.empty(), true);
        ASSERT_EQ(total_size, 0);
    }

    // {1, 2, 3}
    {
        PageDirectory<u128::PageDirectoryTrait>::GcEntriesMap blob_entries;
        std::map<PageIdV3Internal, std::tuple<PageIdV3Internal, PageVersion>> rewrite;
        PageSize total_size
            = entries.getEntriesByBlobIds({1, 3, 2}, buildV3Id(TEST_NAMESPACE_ID, page_id), blob_entries, rewrite);

        ASSERT_EQ(blob_entries.size(), 1);
        ASSERT_EQ(blob_entries[1].size(), 1);
        ASSERT_EQ(total_size, 11);
        check_for_blob_id_1(blob_entries[1]);
    }

    // {1, 2, 3, 100}; blob_id 100 is not exist in actual
    {
        PageDirectory<u128::PageDirectoryTrait>::GcEntriesMap blob_entries;
        std::map<PageIdV3Internal, std::tuple<PageIdV3Internal, PageVersion>> rewrite;
        PageSize total_size
            = entries.getEntriesByBlobIds({1, 3, 2, 4}, buildV3Id(TEST_NAMESPACE_ID, page_id), blob_entries, rewrite);

        ASSERT_EQ(blob_entries.size(), 1);
        ASSERT_EQ(blob_entries[1].size(), 1);
        ASSERT_EQ(total_size, 11);
        check_for_blob_id_1(blob_entries[1]);
    }
}

#undef INSERT_BLOBID_ENTRY
#undef INSERT_ENTRY
#undef INSERT_GC_ENTRY

} // namespace PS::V3::tests
} // namespace DB
