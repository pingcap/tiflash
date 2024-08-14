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
#include <Common/SyncPoint/Ctl.h>
#include <IO/FileProvider/FileProvider.h>
#include <IO/WriteHelpers.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageConstants.h>
#include <Storages/Page/PageDefinesBase.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/PageDefines.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageDirectory/ExternalIdsByNamespace.h>
#include <Storages/Page/V3/PageDirectoryFactory.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/WAL/serialize.h>
#include <Storages/Page/V3/WALStore.h>
#include <Storages/Page/V3/tests/entries_helper.h>
#include <TestUtils/MockDiskDelegator.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <common/UInt128.h>
#include <common/logger_useful.h>
#include <common/types.h>
#include <fmt/format.h>

#include <future>
#include <iterator>
#include <memory>
#include <random>
#include <unordered_map>
#include <unordered_set>

#include "Storages/Page/V3/PageEntryCheckpointInfo.h"

namespace DB
{
namespace PS::V3::tests
{
using u128::PageEntriesEdit;

TEST(ExternalIdsByNamespaceTest, Simple)
{
    NamespaceID ns_id = 100;
    ExternalIdsByNamespace<typename u128::PageIdTrait> external_ids_by_ns;

    {
        ASSERT_FALSE(external_ids_by_ns.existNamespace(ns_id));
        std::shared_ptr<PageIdV3Internal> holder0 = std::make_shared<PageIdV3Internal>(buildV3Id(ns_id, 10));
        holder0.reset();
        // though holder0 is released, but the ns_id still exist
        // until we call getAliveIds. So not check the ns_id here.
    }

    std::atomic<Int32> who(0);

    std::shared_ptr<PageIdV3Internal> holder = std::make_shared<PageIdV3Internal>(buildV3Id(ns_id, 50));

    auto th_insert = std::async([&]() {
        external_ids_by_ns.addExternalId(holder);

        Int32 expect = 0;
        who.compare_exchange_strong(expect, 1);
    });
    auto th_get_alive = std::async([&]() {
        external_ids_by_ns.getAliveIds(ns_id);
        Int32 expect = 0;
        who.compare_exchange_strong(expect, 2);
    });
    th_get_alive.get();
    th_insert.get();

    {
        // holder keep "50" alive
        auto ids = external_ids_by_ns.getAliveIds(ns_id);
        ASSERT_TRUE(ids.has_value());
        LOG_DEBUG(Logger::get(), "{} end first, size={}", who.load(), ids->size());
        ASSERT_EQ(ids->size(), 1);
        ASSERT_EQ(*ids->begin(), 50);
        ASSERT_TRUE(external_ids_by_ns.existNamespace(ns_id));
    }

    {
        // unregister all ids under the namespace
        // "50" is erased though the holder is not released.
        external_ids_by_ns.unregisterNamespace(ns_id);
        auto ids = external_ids_by_ns.getAliveIds(ns_id);
        ASSERT_FALSE(ids.has_value()); // nullopt
        ASSERT_FALSE(external_ids_by_ns.existNamespace(ns_id));
    }
}

class PageDirectoryTest : public DB::base::TiFlashStorageTestBasic
{
public:
    PageDirectoryTest()
        : log(Logger::get())
    {}

    void SetUp() override
    {
        auto path = getTemporaryPath();
        dropDataOnDisk(path);
        dir = restoreFromDisk();
    }

    static u128::PageDirectoryPtr restoreFromDisk()
    {
        auto path = getTemporaryPath();
        auto provider = DB::tests::TiFlashTestEnv::getDefaultFileProvider();
        PSDiskDelegatorPtr delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        PageDirectoryFactory<u128::FactoryTrait> factory;
        return factory.create("PageDirectoryTest", provider, delegator, WALConfig());
    }

protected:
    static PageIdU64 getNormalPageIdU64(
        const u128::PageDirectoryPtr & d,
        PageIdU64 page_id,
        const PageDirectorySnapshotPtr & snap)
    {
        return d->getNormalPageId(buildV3Id(TEST_NAMESPACE_ID, page_id), snap, true).low;
    }
    static PageEntryV3 getEntry(
        const u128::PageDirectoryPtr & d,
        PageIdU64 page_id,
        const PageDirectorySnapshotPtr & snap)
    {
        return d->getByID(buildV3Id(TEST_NAMESPACE_ID, page_id), snap).second;
    }

protected:
    u128::PageDirectoryPtr dir;

    LoggerPtr log;
};

TEST_F(PageDirectoryTest, ApplyPutRead)
try
{
    auto snap0 = dir->createSnapshot();
    EXPECT_ENTRY_NOT_EXIST(dir, 1, snap0);

    PageEntryV3 entry1{.file_id = 1, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 1), entry1);
        dir->apply(std::move(edit));
    }

    auto snap1 = dir->createSnapshot();
    EXPECT_ENTRY_EQ(entry1, dir, 1, snap1);

    PageEntryV3 entry2{.file_id = 2, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 2), entry2);
        dir->apply(std::move(edit));
    }

    auto snap2 = dir->createSnapshot();
    EXPECT_ENTRY_NOT_EXIST(dir, 2, snap1); // creating snap2 won't affect the result of snap1
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap2);
    EXPECT_ENTRY_EQ(entry1, dir, 1, snap2);
    {
        PageIdV3Internals ids{buildV3Id(TEST_NAMESPACE_ID, 1), buildV3Id(TEST_NAMESPACE_ID, 2)};
        PageIDAndEntriesV3 expected_entries{
            {buildV3Id(TEST_NAMESPACE_ID, 1), entry1},
            {buildV3Id(TEST_NAMESPACE_ID, 2), entry2}};
        EXPECT_ENTRIES_EQ(expected_entries, dir, ids, snap2);
    }

    PageEntryV3
        entry2_v2{.file_id = 2 + 102, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 2));
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 2), entry2_v2);
        dir->apply(std::move(edit));
    }
    auto snap3 = dir->createSnapshot();
    EXPECT_ENTRY_EQ(entry2_v2, dir, 2, snap3);
}
CATCH

TEST_F(PageDirectoryTest, ApplyPutWithIdenticalPages)
try
{
    // Put identical page in different `edit`
    PageIdU64 page_id = 50;

    auto snap0 = dir->createSnapshot();
    EXPECT_ENTRY_NOT_EXIST(dir, page_id, snap0);

    PageEntryV3 entry1{.file_id = 1, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, page_id), entry1);
        dir->apply(std::move(edit));
    }

    auto snap1 = dir->createSnapshot();
    EXPECT_ENTRY_EQ(entry1, dir, page_id, snap1);

    PageEntryV3 entry2{.file_id = 1, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x1234, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, page_id), entry2);
        dir->apply(std::move(edit));
    }

    auto snap2 = dir->createSnapshot();
    EXPECT_ENTRY_EQ(entry1, dir, page_id, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, page_id, snap2);
    {
        PageIdV3Internals ids{buildV3Id(TEST_NAMESPACE_ID, page_id)};
        PageIDAndEntriesV3 expected_entries{{buildV3Id(TEST_NAMESPACE_ID, page_id), entry2}};
        EXPECT_ENTRIES_EQ(expected_entries, dir, ids, snap2);
    }

    // Put identical page within one `edit`
    page_id++;
    PageEntryV3 entry3{.file_id = 1, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x12345, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, page_id), entry1);
        edit.put(buildV3Id(TEST_NAMESPACE_ID, page_id), entry2);
        edit.put(buildV3Id(TEST_NAMESPACE_ID, page_id), entry3);

        // Should not be dead-lock
        dir->apply(std::move(edit));
    }
    auto snap3 = dir->createSnapshot();

    PageIdV3Internals ids{buildV3Id(TEST_NAMESPACE_ID, page_id)};
    PageIDAndEntriesV3 expected_entries{{buildV3Id(TEST_NAMESPACE_ID, page_id), entry3}};
    EXPECT_ENTRIES_EQ(expected_entries, dir, ids, snap3);
}
CATCH

TEST_F(PageDirectoryTest, ApplyPutDelRead)
try
{
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry2{.file_id = 2, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 1), entry1);
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 2), entry2);
        dir->apply(std::move(edit));
    }

    auto snap1 = dir->createSnapshot();
    EXPECT_ENTRY_EQ(entry1, dir, 1, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);

    PageEntryV3 entry3{.file_id = 3, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry4{.file_id = 4, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 2));
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 3), entry3);
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 4), entry4);
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
        PageIdV3Internals ids{
            buildV3Id(TEST_NAMESPACE_ID, 1),
            buildV3Id(TEST_NAMESPACE_ID, 3),
            buildV3Id(TEST_NAMESPACE_ID, 4)};
        PageIDAndEntriesV3 expected_entries{
            {buildV3Id(TEST_NAMESPACE_ID, 1), entry1},
            {buildV3Id(TEST_NAMESPACE_ID, 3), entry3},
            {buildV3Id(TEST_NAMESPACE_ID, 4), entry4}};
        EXPECT_ENTRIES_EQ(expected_entries, dir, ids, snap2);
    }
}
CATCH

TEST_F(PageDirectoryTest, ApplyUpdateOnRefEntries)
try
{
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry2{.file_id = 2, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 1), entry1);
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 2), entry2);
        dir->apply(std::move(edit));
    }

    { // Ref 3->2
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 3), buildV3Id(TEST_NAMESPACE_ID, 2));
        dir->apply(std::move(edit));
    }
    auto snap1 = dir->createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);

    // Update on ref page is not allowed
    PageEntryV3
        entry_updated{.file_id = 999, .size = 16, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x123};
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 3), entry_updated);
        ASSERT_ANY_THROW(dir->apply(std::move(edit)));
    }

    PageEntryV3
        entry_updated2{.file_id = 777, .size = 16, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x123};
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 2), entry_updated2);
        ASSERT_ANY_THROW(dir->apply(std::move(edit)));
    }

    // Write a new entry to make sure apply works normally after exception throw
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 10), entry1);
        dir->apply(std::move(edit));
    }
    auto snap2 = dir->createSnapshot();
    EXPECT_ENTRY_EQ(entry1, dir, 10, snap2);
}
CATCH

TEST_F(PageDirectoryTest, ApplyDeleteOnRefEntries)
try
{
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry2{.file_id = 2, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 1), entry1);
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 2), entry2);
        dir->apply(std::move(edit));
    }

    { // Ref 3->2
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 3), buildV3Id(TEST_NAMESPACE_ID, 2));
        dir->apply(std::move(edit));
    }
    auto snap1 = dir->createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);

    // Delete 3, 2 won't get deleted.
    {
        PageEntriesEdit edit;
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 3));
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
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 2));
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
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry2{.file_id = 2, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 1), entry1);
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 2), entry2);
        dir->apply(std::move(edit));
    }

    { // Ref 3->2
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 3), buildV3Id(TEST_NAMESPACE_ID, 2));
        dir->apply(std::move(edit));
    }
    auto snap1 = dir->createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);

    // Ref 4 -> 3
    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 4), buildV3Id(TEST_NAMESPACE_ID, 3));
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
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry2{.file_id = 2, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 1), entry1);
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 2), entry2);
        dir->apply(std::move(edit));
    }

    { // Ref 3->2
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 3), buildV3Id(TEST_NAMESPACE_ID, 2));
        dir->apply(std::move(edit));
    }
    auto snap1 = dir->createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);


    { // Ref 3 -> 2 again, should be idempotent
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 3), buildV3Id(TEST_NAMESPACE_ID, 2));
        dir->apply(std::move(edit));
    }
    auto snap2 = dir->createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap2);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap2);

    {
        PageEntriesEdit edit;
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 3));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 2));
        dir->apply(std::move(edit));
    }
    auto snap3 = dir->createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap2);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap2);
    EXPECT_ENTRY_NOT_EXIST(dir, 2, snap3);
    EXPECT_ENTRY_NOT_EXIST(dir, 3, snap3);

    {
        // Adding ref after deleted.
        // It will invalid snap1 and snap2
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 3), buildV3Id(TEST_NAMESPACE_ID, 1));
        dir->apply(std::move(edit));
    }
    auto snap4 = dir->createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    // EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap2);
    // EXPECT_ENTRY_EQ(entry2, dir, 3, snap2);
    EXPECT_ENTRY_NOT_EXIST(dir, 2, snap3);
    EXPECT_ENTRY_NOT_EXIST(dir, 3, snap3);
    EXPECT_ENTRY_NOT_EXIST(dir, 2, snap4);
    EXPECT_ENTRY_EQ(entry1, dir, 3, snap4);
}
CATCH

/// Put duplicated RefPages due to ref-path-collapse
TEST_F(PageDirectoryTest, ApplyCollapseDuplicatedRefEntries)
try
{
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry2{.file_id = 2, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 1), entry1);
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 2), entry2);
        dir->apply(std::move(edit));
    }

    { // Ref 3->2
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 3), buildV3Id(TEST_NAMESPACE_ID, 2));
        dir->apply(std::move(edit));
    }
    auto snap1 = dir->createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);


    { // Ref 4 -> 3, collapse to 4 -> 2
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 4), buildV3Id(TEST_NAMESPACE_ID, 3));
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

TEST_F(PageDirectoryTest, RefWontDeadLock)
{
    PageEntriesEdit edit;
    {
        // 1. batch.putExternal(0, 0);
        PageEntryV3 entry1;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 0), entry1);

        // 2. batch.putRefPage(1, 0);
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 1), buildV3Id(TEST_NAMESPACE_ID, 0));
    }

    dir->apply(std::move(edit));

    PageEntriesEdit edit2;
    {
        // 1. batch.putRefPage(2, 1); // ref 2 -> 1 -> 0
        edit2.ref(buildV3Id(TEST_NAMESPACE_ID, 2), buildV3Id(TEST_NAMESPACE_ID, 1));

        // 2. batch.delPage(1); // free ref 1 -> 0
        edit2.del(buildV3Id(TEST_NAMESPACE_ID, 1));
    }

    dir->apply(std::move(edit2));
}

TEST_F(PageDirectoryTest, BatchWriteSuccess)
{
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry2{.file_id = 2, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry3{.file_id = 3, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};

    auto sp_before_leader_apply = SyncPointCtl::enableInScope("before_PageDirectory::leader_apply");
    auto th_write1 = std::async([&]() {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 1), entry1);
        dir->apply(std::move(edit));
    });
    sp_before_leader_apply.waitAndPause();

    // form a write group
    auto sp_after_enter_write_group = SyncPointCtl::enableInScope("after_PageDirectory::enter_write_group");
    auto th_write2 = std::async([&]() {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 2), entry2);
        dir->apply(std::move(edit));
    });
    auto th_write3 = std::async([&]() {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 3), entry3);
        dir->apply(std::move(edit));
    });
    sp_after_enter_write_group.waitAndNext();
    sp_after_enter_write_group.waitAndNext();
    ASSERT_EQ(dir->getWritersQueueSizeForTest(), 3); // 3 writers in write group

    sp_before_leader_apply.next(); // continue first leader_apply
    th_write1.get();

    sp_before_leader_apply.waitAndNext(); // continue second leader_apply
    th_write2.get();
    th_write3.get();
    ASSERT_EQ(dir->getWritersQueueSizeForTest(), 0);

    auto snap = dir->createSnapshot();
    EXPECT_ENTRY_EQ(entry1, dir, 1, snap);
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap);
    EXPECT_ENTRY_EQ(entry3, dir, 3, snap);
}

TEST_F(PageDirectoryTest, BatchWriteException)
{
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};

    auto sp_before_leader_apply = SyncPointCtl::enableInScope("before_PageDirectory::leader_apply");
    auto th_write1 = std::async([&]() {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 1), entry1);
        dir->apply(std::move(edit));
    });
    sp_before_leader_apply.waitAndPause();

    // form a write group
    auto sp_after_enter_write_group = SyncPointCtl::enableInScope("after_PageDirectory::enter_write_group");
    auto th_write2 = std::async([&]() {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 2), buildV3Id(TEST_NAMESPACE_ID, 100));
        ASSERT_ANY_THROW(dir->apply(std::move(edit)));
    });
    auto th_write3 = std::async([&]() {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 3), buildV3Id(TEST_NAMESPACE_ID, 100));
        ASSERT_ANY_THROW(dir->apply(std::move(edit)));
    });
    sp_after_enter_write_group.waitAndNext();
    sp_after_enter_write_group.waitAndNext();
    ASSERT_EQ(dir->getWritersQueueSizeForTest(), 3); // 3 writers in write group

    sp_before_leader_apply.next(); // continue first leader_apply
    th_write1.get();

    sp_before_leader_apply.waitAndNext(); // continue secode leader_apply
    th_write2.get();
    th_write3.get();
    ASSERT_EQ(dir->getWritersQueueSizeForTest(), 0);

    auto snap = dir->createSnapshot();
    EXPECT_ENTRY_EQ(entry1, dir, 1, snap);
}

TEST_F(PageDirectoryTest, IdempotentNewExtPageAfterAllCleaned)
{
    // Make sure creating ext page after itself and all its reference are clean
    // is idempotent
    {
        PageEntriesEdit edit;
        edit.putExternal(buildV3Id(TEST_NAMESPACE_ID, 10));
        dir->apply(std::move(edit));
        auto alive_ids = dir->getAliveExternalIds(TEST_NAMESPACE_ID);
        ASSERT_TRUE(alive_ids.has_value());
        EXPECT_EQ(alive_ids->size(), 1);
        EXPECT_GT(alive_ids->count(10), 0);
    }

    {
        PageEntriesEdit edit;
        edit.putExternal(buildV3Id(TEST_NAMESPACE_ID, 10)); // should be idempotent
        dir->apply(std::move(edit));
        auto alive_ids = dir->getAliveExternalIds(TEST_NAMESPACE_ID);
        ASSERT_TRUE(alive_ids.has_value());
        EXPECT_EQ(alive_ids->size(), 1);
        EXPECT_GT(alive_ids->count(10), 0);
    }

    {
        PageEntriesEdit edit;
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 10));
        dir->apply(std::move(edit));
        dir->gcInMemEntries({}); // clean in memory
        auto alive_ids = dir->getAliveExternalIds(TEST_NAMESPACE_ID);
        ASSERT_TRUE(alive_ids.has_value());
        EXPECT_EQ(alive_ids->size(), 0);
        EXPECT_EQ(alive_ids->count(10), 0); // removed
    }

    {
        // Add again after deleted
        PageEntriesEdit edit;
        edit.putExternal(buildV3Id(TEST_NAMESPACE_ID, 10));
        dir->apply(std::move(edit));
        auto alive_ids = dir->getAliveExternalIds(TEST_NAMESPACE_ID);
        ASSERT_TRUE(alive_ids.has_value());
        EXPECT_EQ(alive_ids->size(), 1);
        EXPECT_GT(alive_ids->count(10), 0);
    }
}

TEST_F(PageDirectoryTest, RefToDeletedPage)
try
{
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry2{.file_id = 2, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry3{.file_id = 3, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 1), entry1);
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 2), entry2);
        dir->apply(std::move(edit));
    }

    // Applying ref to not exist entry is not allowed
    { // Ref 4-> 999
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 3), entry3);
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 4), buildV3Id(TEST_NAMESPACE_ID, 999));
        ASSERT_ANY_THROW(dir->apply(std::move(edit)));
    }
}
CATCH

TEST_F(PageDirectoryTest, RefToDeletedPageTwoHops)
try
{
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 1), entry1);
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 2), buildV3Id(TEST_NAMESPACE_ID, 1));
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit;
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 1));
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 3), buildV3Id(TEST_NAMESPACE_ID, 1));
        ASSERT_ANY_THROW({ dir->apply(std::move(edit)); });
    }
}
CATCH

TEST_F(PageDirectoryTest, RefToDeletedExtPageTwoHops)
try
{
    {
        PageEntriesEdit edit;
        edit.putExternal(buildV3Id(TEST_NAMESPACE_ID, 1), PageEntryV3{});
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 2), buildV3Id(TEST_NAMESPACE_ID, 1));
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit;
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 1));
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 3), buildV3Id(TEST_NAMESPACE_ID, 1));
        ASSERT_ANY_THROW({ dir->apply(std::move(edit)); });
    }
}
CATCH

TEST_F(PageDirectoryTest, NewRefAfterDelThreeHops)
try
{
    // Fix issue: https://github.com/pingcap/tiflash/issues/5570
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 951), entry1);
        dir->apply(std::move(edit));
    }

    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 954), buildV3Id(TEST_NAMESPACE_ID, 951));
        dir->apply(std::move(edit));
    }

    {
        PageEntriesEdit edit;
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 951));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 951));
        dir->apply(std::move(edit));
    }

    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 972), buildV3Id(TEST_NAMESPACE_ID, 954));
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 985), buildV3Id(TEST_NAMESPACE_ID, 954));
        dir->apply(std::move(edit));
    }

    {
        PageEntriesEdit edit;
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 954));
        dir->apply(std::move(edit));
    }

    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 998), buildV3Id(TEST_NAMESPACE_ID, 985));
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 1011), buildV3Id(TEST_NAMESPACE_ID, 985));
        dir->apply(std::move(edit));
    }

    auto snap = dir->createSnapshot();
    ASSERT_ENTRY_EQ(entry1, dir, 998, snap);
}
CATCH

TEST_F(PageDirectoryTest, NewRefAfterDelThreeHopsRemotePage)
try
{
    // Fix issue: https://github.com/pingcap/tiflash/issues/9307
    PageEntryV3 entry1{
        .file_id = 0,
        .size = 1024,
        .padded_size = 0,
        .tag = 0,
        .offset = 0,
        .checksum = 0x0,
        .checkpoint_info = OptionalCheckpointInfo(
            CheckpointLocation{
                .data_file_id = std::make_shared<const std::string>("s3://path/to/file"),
                .offset_in_file = 0xa0,
                .size_in_file = 1024},
            true,
            true),
    };

    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 951), entry1);
        dir->apply(std::move(edit));
    }

    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 954), buildV3Id(TEST_NAMESPACE_ID, 951));
        dir->apply(std::move(edit));
    }

    {
        PageEntriesEdit edit;
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 951));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 951));
        dir->apply(std::move(edit));
    }

    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 972), buildV3Id(TEST_NAMESPACE_ID, 954));
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 985), buildV3Id(TEST_NAMESPACE_ID, 954));
        dir->apply(std::move(edit));
    }

    {
        PageEntriesEdit edit;
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 954));
        dir->apply(std::move(edit));
    }

    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 998), buildV3Id(TEST_NAMESPACE_ID, 985));
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 1011), buildV3Id(TEST_NAMESPACE_ID, 985));
        dir->apply(std::move(edit));
    }

    auto snap = dir->createSnapshot();
    ASSERT_ENTRY_EQ(entry1, dir, 998, snap);

    // Assume we download the data into this file offset
    PageEntryV3 entry2{
        .file_id = 11,
        .size = 1024,
        .padded_size = 0,
        .tag = 0,
        .offset = 0xf0,
        .checksum = 0xabcd,
        .checkpoint_info = OptionalCheckpointInfo(
            CheckpointLocation{
                .data_file_id = std::make_shared<const std::string>("s3://path/to/file"),
                .offset_in_file = 0xa0,
                .size_in_file = 1024},
            true,
            true),
    };

    // Mock that "update remote" after download from remote store by "snap"
    {
        PageEntriesEdit edit;
        edit.updateRemote(buildV3Id(TEST_NAMESPACE_ID, 998), entry2);
        dir->updateLocalCacheForRemotePages(std::move(edit), snap, nullptr);
    }
    snap = dir->createSnapshot();
    ASSERT_ENTRY_EQ(entry2, dir, 998, snap);
}
CATCH

TEST_F(PageDirectoryTest, NewRefAfterDelRandom)
try
{
    PageIdU64 id = 50;
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, id), entry1);
        dir->apply(std::move(edit));
    }

    std::unordered_set<PageIdU64> visible_page_ids{
        id,
    };

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(0, 5);

    constexpr static size_t NUM_TEST = 10000;
    for (size_t test_round = 0; test_round < NUM_TEST; ++test_round)
    {
        SCOPED_TRACE(fmt::format("test idx={}", test_round));
        const bool del_in_same_wb = distrib(gen) % 2 == 0;
        const bool gc_or_not = distrib(gen) < 1;
        LOG_DEBUG(
            log,
            "round={}, del_in_same_wb={}, gc_or_not={}, visible_ids_num={}",
            test_round,
            del_in_same_wb,
            gc_or_not,
            visible_page_ids.size());

        // Generate new ref operations to the visible pages
        const size_t num_ref_page = distrib(gen) + 1;
        std::unordered_map<PageIdU64, PageIdU64> new_ref_page_ids;
        std::uniform_int_distribution<> rand_visible_ids(0, visible_page_ids.size() - 1);
        for (size_t j = 0; j < num_ref_page; ++j)
        {
            // random choose a id from all visible id
            auto r = rand_visible_ids(gen);
            auto rand_it = std::next(std::begin(visible_page_ids), r);
            new_ref_page_ids.emplace(++id, *rand_it);
        }

        // Generate new delete operations among the visible pages and new-generated ref page
        // Delete 1 page at least, delete until 1 page left at most
        std::uniform_int_distribution<> rand_delete_ids(0, visible_page_ids.size() + num_ref_page - 1);
        const size_t num_del_page
            = std::min(std::max(rand_delete_ids(gen), 1), visible_page_ids.size() + num_ref_page - 1);
        std::unordered_set<PageIdU64> delete_ref_page_ids;
        for (size_t j = 0; j < num_del_page; ++j)
        {
            // Random choose a id from all visible id and new-generated ref pages.
            auto r = rand_delete_ids(gen);
            PageIdU64 id_to_del = 0;
            if (static_cast<size_t>(r) < visible_page_ids.size())
            {
                auto rand_it = std::next(std::begin(visible_page_ids), r);
                id_to_del = *rand_it;
            }
            else
            {
                auto rand_it = std::next(std::begin(new_ref_page_ids), r - visible_page_ids.size());
                id_to_del = rand_it->first;
            }
            delete_ref_page_ids.emplace(id_to_del);
        }

        // LOG_DEBUG(log, "round={}, create ids: {}", test_round, new_ref_page_ids);
        // LOG_DEBUG(log, "round={}, delete ids: {}", test_round, delete_ref_page_ids);

        if (del_in_same_wb)
        {
            // create ref and del in the same write batch
            PageEntriesEdit edit;
            for (const auto & x : new_ref_page_ids)
                edit.ref(buildV3Id(TEST_NAMESPACE_ID, x.first), buildV3Id(TEST_NAMESPACE_ID, x.second));
            for (const auto x : delete_ref_page_ids)
                edit.del(buildV3Id(TEST_NAMESPACE_ID, x));
            dir->apply(std::move(edit));
        }
        else
        {
            // first create all ref, then del in another write batch
            {
                PageEntriesEdit edit;
                for (const auto & x : new_ref_page_ids)
                    edit.ref(buildV3Id(TEST_NAMESPACE_ID, x.first), buildV3Id(TEST_NAMESPACE_ID, x.second));
                dir->apply(std::move(edit));
            }
            {
                PageEntriesEdit edit;
                for (const auto x : delete_ref_page_ids)
                    edit.del(buildV3Id(TEST_NAMESPACE_ID, x));
                dir->apply(std::move(edit));
            }
        }

        for (const auto & x : new_ref_page_ids)
            visible_page_ids.insert(x.first);
        for (const auto & x : delete_ref_page_ids)
            visible_page_ids.erase(x);

        if (gc_or_not)
            dir->gcInMemEntries({.need_removed_entries = false});
        auto snap = dir->createSnapshot();
        for (const auto & cur_id : visible_page_ids)
        {
            ASSERT_ENTRY_EQ(entry1, dir, cur_id, snap);
        }
    }
}
CATCH

TEST_F(PageDirectoryTest, NewRefToExtAfterDelRandom)
try
{
    PageIdU64 id = 50;
    {
        PageEntriesEdit edit;
        edit.putExternal(buildV3Id(TEST_NAMESPACE_ID, id));
        dir->apply(std::move(edit));
    }

    std::unordered_set<PageIdU64> visible_page_ids{
        id,
    };

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(0, 5);

    constexpr static size_t NUM_TEST = 10000;
    for (size_t test_round = 0; test_round < NUM_TEST; ++test_round)
    {
        SCOPED_TRACE(fmt::format("test idx={}", test_round));
        const bool del_in_same_wb = distrib(gen) % 2 == 0;
        const bool gc_or_not = distrib(gen) < 1;
        LOG_DEBUG(
            log,
            "round={}, del_in_same_wb={}, gc_or_not={}, visible_ids_num={}",
            test_round,
            del_in_same_wb,
            gc_or_not,
            visible_page_ids.size());

        const size_t num_ref_page = distrib(gen) + 1;
        std::unordered_map<PageIdU64, PageIdU64> new_ref_page_ids;
        std::uniform_int_distribution<> rand_visible_ids(0, visible_page_ids.size() - 1);
        for (size_t j = 0; j < num_ref_page; ++j)
        {
            // random choose a id from all visible id
            auto r = rand_visible_ids(gen);
            auto rand_it = std::next(std::begin(visible_page_ids), r);
            new_ref_page_ids.emplace(++id, *rand_it);
        }

        // Delete 1 page at least, delete until 1 page left at most
        std::uniform_int_distribution<> rand_delete_ids(0, visible_page_ids.size() + num_ref_page - 1);
        const size_t num_del_page
            = std::min(std::max(rand_delete_ids(gen), 1), visible_page_ids.size() + num_ref_page - 1);
        std::unordered_set<PageIdU64> delete_ref_page_ids;
        for (size_t j = 0; j < num_del_page; ++j)
        {
            auto r = rand_delete_ids(gen);
            // random choose a id from all visible id
            if (static_cast<size_t>(r) < visible_page_ids.size())
            {
                auto rand_it = std::next(std::begin(visible_page_ids), r);
                delete_ref_page_ids.emplace(*rand_it);
            }
            else
            {
                auto rand_it = std::next(std::begin(new_ref_page_ids), r - visible_page_ids.size());
                delete_ref_page_ids.emplace(rand_it->first);
            }
        }

        // LOG_DEBUG(log, "round={}, create ids: {}", test_round, new_ref_page_ids);
        // LOG_DEBUG(log, "round={}, delete ids: {}", test_round, delete_ref_page_ids);

        if (del_in_same_wb)
        {
            PageEntriesEdit edit;
            for (const auto & x : new_ref_page_ids)
                edit.ref(buildV3Id(TEST_NAMESPACE_ID, x.first), buildV3Id(TEST_NAMESPACE_ID, x.second));
            for (const auto x : delete_ref_page_ids)
                edit.del(buildV3Id(TEST_NAMESPACE_ID, x));
            dir->apply(std::move(edit));
        }
        else
        {
            {
                PageEntriesEdit edit;
                for (const auto & x : new_ref_page_ids)
                    edit.ref(buildV3Id(TEST_NAMESPACE_ID, x.first), buildV3Id(TEST_NAMESPACE_ID, x.second));
                dir->apply(std::move(edit));
            }
            {
                PageEntriesEdit edit;
                for (const auto x : delete_ref_page_ids)
                    edit.del(buildV3Id(TEST_NAMESPACE_ID, x));
                dir->apply(std::move(edit));
            }
        }

        for (const auto & x : new_ref_page_ids)
            visible_page_ids.insert(x.first);
        for (const auto & x : delete_ref_page_ids)
            visible_page_ids.erase(x);

        if (gc_or_not)
        {
            dir->gcInMemEntries({.need_removed_entries = false});
            const auto all_ids = dir->getAllPageIds();
            for (const auto & cur_id : visible_page_ids)
            {
                EXPECT_GT(all_ids.count(buildV3Id(TEST_NAMESPACE_ID, cur_id)), 0)
                    << fmt::format("cur_id:{}, all_id:{}, visible_ids:{}", cur_id, all_ids, visible_page_ids);
            }
        }
        auto snap = dir->createSnapshot();
        auto alive_ids = dir->getAliveExternalIds(TEST_NAMESPACE_ID);
        ASSERT_TRUE(alive_ids.has_value());
        EXPECT_EQ(alive_ids->size(), 1);
        EXPECT_GT(alive_ids->count(50), 0);
    }
}
CATCH

TEST_F(PageDirectoryTest, NormalPageId)
try
{
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 9), PageEntryV3{});
        edit.putExternal(buildV3Id(TEST_NAMESPACE_ID, 10));
        dir->apply(std::move(edit));
    }
    auto s0 = dir->createSnapshot();
    // calling getNormalPageId on non-external-page will return itself
    EXPECT_EQ(9, getNormalPageIdU64(dir, 9, s0));
    EXPECT_EQ(10, getNormalPageIdU64(dir, 10, s0));
    EXPECT_ANY_THROW(getNormalPageIdU64(dir, 11, s0)); // not exist at all
    EXPECT_ANY_THROW(getNormalPageIdU64(dir, 12, s0)); // not exist at all

    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 11), buildV3Id(TEST_NAMESPACE_ID, 10));
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 12), buildV3Id(TEST_NAMESPACE_ID, 10));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 10));
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 13), buildV3Id(TEST_NAMESPACE_ID, 9));
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 14), buildV3Id(TEST_NAMESPACE_ID, 9));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 9));
        dir->apply(std::move(edit));
    }
    auto s1 = dir->createSnapshot();
    EXPECT_ANY_THROW(getNormalPageIdU64(dir, 10, s1));
    EXPECT_EQ(10, getNormalPageIdU64(dir, 11, s1));
    EXPECT_EQ(10, getNormalPageIdU64(dir, 12, s1));
    EXPECT_ANY_THROW(getNormalPageIdU64(dir, 9, s1));
    EXPECT_EQ(9, getNormalPageIdU64(dir, 13, s1));
    EXPECT_EQ(9, getNormalPageIdU64(dir, 14, s1));

    {
        PageEntriesEdit edit;
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 11));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 14));
        dir->apply(std::move(edit));
    }
    auto s2 = dir->createSnapshot();
    EXPECT_ANY_THROW(getNormalPageIdU64(dir, 10, s2));
    EXPECT_ANY_THROW(getNormalPageIdU64(dir, 11, s2));
    EXPECT_EQ(10, getNormalPageIdU64(dir, 12, s2));
    EXPECT_ANY_THROW(getNormalPageIdU64(dir, 9, s2));
    EXPECT_EQ(9, getNormalPageIdU64(dir, 13, s2));
    EXPECT_ANY_THROW(getNormalPageIdU64(dir, 14, s2));

    {
        PageEntriesEdit edit;
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 12));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 13));
        dir->apply(std::move(edit));
    }
    auto s3 = dir->createSnapshot();
    EXPECT_ANY_THROW(getNormalPageIdU64(dir, 10, s3));
    EXPECT_ANY_THROW(getNormalPageIdU64(dir, 11, s3));
    EXPECT_ANY_THROW(getNormalPageIdU64(dir, 12, s3));
    EXPECT_ANY_THROW(getNormalPageIdU64(dir, 9, s3));
    EXPECT_ANY_THROW(getNormalPageIdU64(dir, 13, s3));
    EXPECT_ANY_THROW(getNormalPageIdU64(dir, 14, s3));
}
CATCH

TEST_F(PageDirectoryTest, EmptyPage)
try
{
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 9), PageEntryV3{.file_id = 551, .size = 0, .offset = 0x15376});
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 10), PageEntryV3{.file_id = 551, .size = 15, .offset = 0x15373});
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 100), PageEntryV3{.file_id = 551, .size = 0, .offset = 0x0});
        edit.put(
            buildV3Id(TEST_NAMESPACE_ID, 101),
            PageEntryV3{.file_id = 552, .size = BLOBFILE_LIMIT_SIZE, .offset = 0x0});
        dir->apply(std::move(edit));
    }

    auto s0 = dir->createSnapshot();
    auto edit = dir->dumpSnapshotToEdit(s0);
    auto restore_from_edit = [](const PageEntriesEdit & edit, BlobStats & stats) {
        auto deseri_edit = u128::Serializer::deserializeFrom(u128::Serializer::serializeTo(edit), nullptr);
        auto provider = DB::tests::TiFlashTestEnv::getDefaultFileProvider();
        auto path = getTemporaryPath();
        PSDiskDelegatorPtr delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        PageDirectoryFactory<u128::FactoryTrait> factory;
        auto d
            = factory.setBlobStats(stats).createFromEditForTest(getCurrentTestName(), provider, delegator, deseri_edit);
        return d;
    };

    {
        auto path = getTemporaryPath();
        PSDiskDelegatorPtr delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        auto config = BlobConfig{};
        BlobStats stats(log, delegator, config);
        {
            std::lock_guard lock(stats.lock_stats);
            stats.createStatNotChecking(551, BLOBFILE_LIMIT_SIZE, lock);
            stats.createStatNotChecking(552, BLOBFILE_LIMIT_SIZE, lock);
        }
        auto restored_dir = restore_from_edit(edit, stats);
        auto snap = restored_dir->createSnapshot();
        ASSERT_EQ(getEntry(restored_dir, 9, snap).offset, 0x15376);
        ASSERT_EQ(getEntry(restored_dir, 10, snap).offset, 0x15373);
        ASSERT_EQ(getEntry(restored_dir, 100, snap).offset, 0x0);
        ASSERT_EQ(getEntry(restored_dir, 101, snap).offset, 0x0);
    }
}
CATCH

class PageDirectoryGCTest : public PageDirectoryTest
{
};

#define INSERT_ENTRY_TO(PAGE_ID, VERSION, BLOB_FILE_ID)                      \
    PageEntryV3 entry_v##VERSION{                                            \
        .file_id = (BLOB_FILE_ID),                                           \
        .size = (VERSION),                                                   \
        .padded_size = 0,                                                    \
        .tag = 0,                                                            \
        .offset = 0x123,                                                     \
        .checksum = 0x4567};                                                 \
    {                                                                        \
        PageEntriesEdit edit;                                                \
        edit.put(buildV3Id(TEST_NAMESPACE_ID, (PAGE_ID)), entry_v##VERSION); \
        dir->apply(std::move(edit));                                         \
    }
// Insert an entry into mvcc directory
#define INSERT_ENTRY(PAGE_ID, VERSION) INSERT_ENTRY_TO(PAGE_ID, VERSION, 1)
// Insert an entry into mvcc directory, and acquire a snapshot
#define INSERT_ENTRY_ACQ_SNAP(PAGE_ID, VERSION) \
    INSERT_ENTRY(PAGE_ID, VERSION)              \
    auto snapshot##VERSION = dir->createSnapshot();
#define INSERT_DELETE(PAGE_ID)                             \
    {                                                      \
        PageEntriesEdit edit;                              \
        edit.del(buildV3Id(TEST_NAMESPACE_ID, (PAGE_ID))); \
        dir->apply(std::move(edit));                       \
    }

TEST_F(PageDirectoryGCTest, ManyEditsAndDumpSnapshot)
{
    PageIdU64 page_id0 = 50;
    PageIdU64 page_id1 = 51;
    PageIdU64 page_id2 = 52;
    PageIdU64 page_id3 = 53;

    PageEntryV3 last_entry_for_0;
    constexpr size_t num_edits_test = 50000;
    for (size_t i = 0; i < num_edits_test; ++i)
    {
        {
            INSERT_ENTRY(page_id0, i);
            last_entry_for_0 = entry_vi;
        }
        {
            INSERT_ENTRY(page_id1, i);
        }
    }
    INSERT_DELETE(page_id1);
    EXPECT_TRUE(dir->tryDumpSnapshot());
    dir.reset();

    dir = restoreFromDisk();
    {
        auto snap = dir->createSnapshot();
        ASSERT_SAME_ENTRY(getEntry(dir, page_id0, snap), last_entry_for_0);
        EXPECT_ENTRY_NOT_EXIST(dir, page_id1, snap);
    }

    PageEntryV3 last_entry_for_2;
    for (size_t i = 0; i < num_edits_test; ++i)
    {
        {
            INSERT_ENTRY(page_id2, i);
            last_entry_for_2 = entry_vi;
        }
        {
            INSERT_ENTRY(page_id3, i);
        }
    }
    INSERT_DELETE(page_id3);
    EXPECT_TRUE(dir->tryDumpSnapshot());

    dir = restoreFromDisk();
    {
        auto snap = dir->createSnapshot();
        ASSERT_SAME_ENTRY(getEntry(dir, page_id0, snap), last_entry_for_0);
        EXPECT_ENTRY_NOT_EXIST(dir, page_id1, snap);
        ASSERT_SAME_ENTRY(getEntry(dir, page_id2, snap), last_entry_for_2);
        EXPECT_ENTRY_NOT_EXIST(dir, page_id3, snap);
    }
}

TEST_F(PageDirectoryGCTest, DumpSnapshotDuringWrite)
{
    // write some data and roll the log file
    for (size_t i = 0; i < 100; ++i)
    {
        INSERT_ENTRY(i + 50, i);
    }
    ASSERT_TRUE(dir->tryDumpSnapshot(nullptr, true));

    // write a few more data
    for (size_t i = 100; i < 110; ++i)
    {
        INSERT_ENTRY(i + 50, i);
    }

    auto sp_before_apply_memory = SyncPointCtl::enableInScope("before_PageDirectory::apply_to_memory");
    auto th_write1 = std::async([&]() {
        PageEntriesEdit edit;
        PageEntryV3
            entry_1_v1{.file_id = 1, .size = 1, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 5352), entry_1_v1);
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 5353), buildV3Id(TEST_NAMESPACE_ID, 5352));
        dir->apply(std::move(edit));
    });
    sp_before_apply_memory.waitAndPause();

    // dump snapshot during write
    ASSERT_TRUE(dir->tryDumpSnapshot(nullptr, true));

    sp_before_apply_memory.next();
    th_write1.get();

    {
        auto snap = dir->createSnapshot();
        auto normal_id = getNormalPageIdU64(dir, 5353, snap);
        EXPECT_EQ(normal_id, 5352);
    }

    dir.reset();

    dir = restoreFromDisk();
    {
        auto snap = dir->createSnapshot();
        auto normal_id = getNormalPageIdU64(dir, 5353, snap);
        EXPECT_EQ(normal_id, 5352);
    }
}

TEST_F(PageDirectoryTest, RestoreWithRefToDeletedPage)
try
{
    {
        PageEntryV3
            entry_1_v1{.file_id = 1, .size = 1, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
        PageEntriesEdit edit; // ingest
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 352), entry_1_v1);
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 353), buildV3Id(TEST_NAMESPACE_ID, 352));
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit; // ingest done
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 352));
        dir->apply(std::move(edit));
    }

    {
        auto snap = dir->createSnapshot();
        auto normal_id = getNormalPageIdU64(dir, 353, snap);
        EXPECT_EQ(normal_id, 352);
    }

    auto s0 = dir->createSnapshot();
    auto edit = dir->dumpSnapshotToEdit(s0);
    auto restore_from_edit = [](const PageEntriesEdit & edit) {
        auto deseri_edit = u128::Serializer::deserializeFrom(u128::Serializer::serializeTo(edit), nullptr);
        auto provider = DB::tests::TiFlashTestEnv::getDefaultFileProvider();
        auto path = getTemporaryPath();
        PSDiskDelegatorPtr delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        PageDirectoryFactory<u128::FactoryTrait> factory;
        auto d = factory.createFromEditForTest(getCurrentTestName(), provider, delegator, deseri_edit);
        return d;
    };

    {
        auto restored_dir = restore_from_edit(edit);
        auto snap = restored_dir->createSnapshot();
        auto normal_id = getNormalPageIdU64(restored_dir, 353, snap);
        EXPECT_EQ(normal_id, 352);
    }
}
CATCH

TEST_F(PageDirectoryTest, RestoreWithIdempotentRef)
try
{
    auto provider = DB::tests::TiFlashTestEnv::getDefaultFileProvider();
    auto path = getTemporaryPath();
    PSDiskDelegatorPtr delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
    PageDirectoryFactory<u128::FactoryTrait> factory;

    // Generate an edit with idempotent REF operation
    const UInt64 filter_seq = 1000;
    u128::PageEntriesEdit edit;
    edit.appendRecord({.type = EditRecordType::UPSERT, .page_id = UInt128(10000), .version = PageVersion(53, 1)});
    edit.appendRecord(
        {.type = EditRecordType::REF,
         .page_id = UInt128(90000),
         .ori_page_id = UInt128(1),
         .version = PageVersion(51)});
    edit.appendRecord({.type = EditRecordType::UPSERT, .page_id = UInt128(1), .version = PageVersion(52, 1)});
    edit.appendRecord({.type = EditRecordType::PUT, .page_id = UInt128(5), .version = PageVersion(1001)});

    auto d = factory.createFromEditForTest(getCurrentTestName(), provider, delegator, edit, filter_seq);
    EXPECT_EQ(d->getMaxIdAfterRestart(), 90000);
}
CATCH

TEST_F(PageDirectoryTest, IncrRefDuringDump)
try
{
    PageEntryV3
        entry_1_v1{.file_id = 50, .size = 7890, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 1), entry_1_v1);
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 2), buildV3Id(TEST_NAMESPACE_ID, 1));
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 3), buildV3Id(TEST_NAMESPACE_ID, 1));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 1));
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit;
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 2));
        dir->apply(std::move(edit));
    }

    {
        dir->gcInMemEntries({});
        ASSERT_EQ(dir->numPages(), 2);
    }

    // create a snap for dump
    auto snap = dir->createSnapshot("");

    // add a ref during dump snapshot
    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 5), buildV3Id(TEST_NAMESPACE_ID, 3));
        dir->apply(std::move(edit));
    }

    // check the being_ref_count in dumped snapshot is correct
    {
        auto edit = dir->dumpSnapshotToEdit(snap);
        ASSERT_EQ(edit.size(), 3);
        const auto & records = edit.getRecords();
        ASSERT_EQ(records[0].type, EditRecordType::VAR_ENTRY);
        ASSERT_EQ(records[0].being_ref_count, 2);
        ASSERT_EQ(records[1].type, EditRecordType::VAR_DELETE);
        ASSERT_EQ(records[2].type, EditRecordType::VAR_REF);
    }

    {
        auto edit = dir->dumpSnapshotToEdit();
        ASSERT_EQ(edit.size(), 4);
        const auto & records = edit.getRecords();
        ASSERT_EQ(records[0].type, EditRecordType::VAR_ENTRY);
        ASSERT_EQ(records[0].being_ref_count, 3);
        ASSERT_EQ(records[1].type, EditRecordType::VAR_DELETE);
        ASSERT_EQ(records[2].type, EditRecordType::VAR_REF);
        ASSERT_EQ(records[3].type, EditRecordType::VAR_REF);
    }
}
CATCH

TEST_F(PageDirectoryTest, Issue7915Case1)
try
{
    PageEntryV3
        entry_1_v1{.file_id = 50, .size = 7890, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 1), entry_1_v1);
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 2), buildV3Id(TEST_NAMESPACE_ID, 1));
        dir->apply(std::move(edit));
    }
    // rotate the current log file
    ASSERT_TRUE(dir->tryDumpSnapshot(nullptr, true));

    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 3), buildV3Id(TEST_NAMESPACE_ID, 2));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 2));
        dir->apply(std::move(edit));
    }
    {
        auto snap = dir->createSnapshot();
        auto normal_id = getNormalPageIdU64(dir, 3, snap);
        EXPECT_EQ(normal_id, 1);
    }

    auto sp_after_create_snap_for_dump = SyncPointCtl::enableInScope("after_PageDirectory::create_snap_for_dump");
    auto th_dump = std::async([&]() {
        // ensure page 2 is deleted in the dumped snapshot
        dir->gcInMemEntries(u128::PageDirectoryType::InMemGCOption{.need_removed_entries = false});
        ASSERT_TRUE(dir->tryDumpSnapshot(nullptr, true));
    });
    sp_after_create_snap_for_dump.waitAndPause();

    // write an arbitrary record to the current log file to prevent it being deleted after dump snapshot
    PageEntryV3
        entry_5_v1{.file_id = 500, .size = 7890, .padded_size = 0, .tag = 0, .offset = 0x321, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 5), entry_5_v1);
        dir->apply(std::move(edit));
    }

    sp_after_create_snap_for_dump.next();
    th_dump.get();

    // restart and check
    dir = restoreFromDisk();
    {
        auto snap = dir->createSnapshot();
        auto normal_id = getNormalPageIdU64(dir, 3, snap);
        EXPECT_EQ(normal_id, 1);
        ASSERT_EQ(dir->numPages(), 3);
        EXPECT_ENTRY_EQ(entry_1_v1, dir, 1, snap);
        EXPECT_ENTRY_EQ(entry_1_v1, dir, 3, snap);
        EXPECT_ENTRY_EQ(entry_5_v1, dir, 5, snap);
    }
}
CATCH

TEST_F(PageDirectoryTest, Issue7915Case2)
try
{
    PageEntryV3
        entry_1_v1{.file_id = 50, .size = 7890, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 1), entry_1_v1);
        dir->apply(std::move(edit));
    }
    // rotate the current log file
    ASSERT_TRUE(dir->tryDumpSnapshot(nullptr, true));

    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 2), buildV3Id(TEST_NAMESPACE_ID, 1));
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit;
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 2));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 1));
        dir->apply(std::move(edit));
    }

    auto sp_after_create_snap_for_dump = SyncPointCtl::enableInScope("after_PageDirectory::create_snap_for_dump");
    auto th_dump = std::async([&]() {
        // ensure page 2, page 1 is deleted in the dumped snapshot
        dir->gcInMemEntries(u128::PageDirectoryType::InMemGCOption{.need_removed_entries = false});
        ASSERT_TRUE(dir->tryDumpSnapshot(nullptr, true));
    });
    sp_after_create_snap_for_dump.waitAndPause();

    // write an arbitrary record to the current log file to prevent it being deleted after dump snapshot
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 5), entry_1_v1);
        dir->apply(std::move(edit));
    }

    sp_after_create_snap_for_dump.next();
    th_dump.get();

    // restart and check
    dir = restoreFromDisk();
    {
        ASSERT_EQ(dir->numPages(), 1);
        auto snap = dir->createSnapshot("");
        EXPECT_ENTRY_EQ(entry_1_v1, dir, 5, snap);
    }
}
CATCH


TEST_F(PageDirectoryTest, Issue7915Case3)
try
{
    PageEntryV3
        entry_1_v1{.file_id = 50, .size = 7890, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3
        entry_full_gc{.file_id = 5050, .size = 7890, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 1), entry_1_v1);
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 10000), entry_1_v1);
        dir->apply(std::move(edit));
    }
    // rotate the current log file
    ASSERT_TRUE(dir->tryDumpSnapshot(nullptr, true));

    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 2), buildV3Id(TEST_NAMESPACE_ID, 1));
        dir->apply(std::move(edit));
    }
    {
        // Mock full gc happens and move page 10000 to another blob file
        const auto gc_entries = dir->getEntriesByBlobIds(std::vector<BlobFileId>{50});
        PageEntriesEdit edit;
        for (const auto & [file_id, versioned_pageid_entry_list] : gc_entries.first)
        {
            for (const auto & [page_id, version, entry] : versioned_pageid_entry_list)
            {
                edit.upsertPage(page_id, version, entry_full_gc);
            }
        }
        dir->gcApply(std::move(edit));
    }
    {
        PageEntriesEdit edit;
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 2));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 1));
        dir->apply(std::move(edit));
    }

    auto sp_after_create_snap_for_dump = SyncPointCtl::enableInScope("after_PageDirectory::create_snap_for_dump");
    auto th_dump = std::async([&]() {
        // ensure page 2, page 1 is deleted in the dumped snapshot
        dir->gcInMemEntries(u128::PageDirectoryType::InMemGCOption{.need_removed_entries = false});
        ASSERT_TRUE(dir->tryDumpSnapshot(nullptr, true));
    });
    sp_after_create_snap_for_dump.waitAndPause();

    // write an arbitrary record to the current log file to prevent it being deleted after dump snapshot
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 5), entry_1_v1);
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 10005), buildV3Id(TEST_NAMESPACE_ID, 10000));
        dir->apply(std::move(edit));
    }

    sp_after_create_snap_for_dump.next();
    th_dump.get();

    // restart and check
    dir = restoreFromDisk();
    {
        ASSERT_EQ(dir->numPages(), 3);
        // page 1, 2 should be deleted, and page 10000, 5 is restored
        auto snap = dir->createSnapshot();
        EXPECT_ENTRY_EQ(entry_full_gc, dir, 10000, snap);
        EXPECT_ENTRY_EQ(entry_1_v1, dir, 5, snap);
        // the ref 10005 -> 10000 is not ignored
        EXPECT_ENTRY_EQ(entry_full_gc, dir, 10005, snap);
    }
}
CATCH

TEST_F(PageDirectoryTest, Issue7915Case4)
try
{
    PageEntryV3
        entry_1_v1{.file_id = 50, .size = 7890, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3
        entry_2_v1{.file_id = 50, .size = 7890, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 1), entry_1_v1);
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 2), entry_2_v1);
        dir->apply(std::move(edit));
    }

    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 3), buildV3Id(TEST_NAMESPACE_ID, 2));
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit;
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 2));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 3));
        dir->apply(std::move(edit));
    }

    {
        dir->gcInMemEntries(u128::PageDirectoryType::InMemGCOption{.need_removed_entries = false});
        ASSERT_TRUE(dir->tryDumpSnapshot(nullptr, true));
    }

    // restart and check
    dir = restoreFromDisk();
    {
        EXPECT_EQ(dir->numPages(), 1);
        auto snap = dir->createSnapshot("");
        EXPECT_ENTRY_EQ(entry_1_v1, dir, 1, snap);
    }

    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 10), entry_2_v1);
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 11), buildV3Id(TEST_NAMESPACE_ID, 10));
        dir->apply(std::move(edit));
    }

    // restart again
    dir = restoreFromDisk();
    {
        EXPECT_EQ(dir->numPages(), 3);
        auto snap = dir->createSnapshot("");
        EXPECT_ENTRY_EQ(entry_1_v1, dir, 1, snap);
        auto normal_id = getNormalPageIdU64(dir, 11, snap);
        EXPECT_EQ(normal_id, 10);
    }
}
CATCH

TEST(MultiVersionRefCount, RefAndCollapse)
try
{
    MultiVersionRefCount ref_counts;
    {
        ref_counts.incrRefCount(PageVersion(2), 1);
        ref_counts.incrRefCount(PageVersion(4), 2);
        ref_counts.incrRefCount(PageVersion(8), 1);
        ASSERT_EQ(ref_counts.getRefCountInSnap(1), 1);
        ASSERT_EQ(ref_counts.getRefCountInSnap(2), 2);
        ASSERT_EQ(ref_counts.getRefCountInSnap(8), 5);
        ASSERT_EQ(ref_counts.getLatestRefCount(), 5);
    }

    // decr ref and collapse
    {
        ASSERT_EQ(ref_counts.versioned_ref_counts->size(), 3);
        ref_counts.decrRefCountInSnap(4, 2);
        ASSERT_EQ(ref_counts.versioned_ref_counts->size(), 2);
        ASSERT_EQ(ref_counts.getRefCountInSnap(4), 2);
        ASSERT_EQ(ref_counts.getRefCountInSnap(8), 3);
        ref_counts.decrRefCountInSnap(10, 2);
        ASSERT_EQ(ref_counts.getLatestRefCount(), 1);
        ASSERT_EQ(ref_counts.versioned_ref_counts, nullptr);
    }
}
CATCH

TEST(MultiVersionRefCount, DecrRefWithSeq0)
try
{
    MultiVersionRefCount ref_counts;
    {
        ref_counts.incrRefCount(PageVersion(2), 1);
        ref_counts.incrRefCount(PageVersion(3), 1);
        ref_counts.incrRefCount(PageVersion(4), 1);
        ref_counts.incrRefCount(PageVersion(8), 1);
        ref_counts.incrRefCount(PageVersion(9), 1);
        ASSERT_EQ(ref_counts.versioned_ref_counts->size(), 5);
    }

    {
        ref_counts.decrRefCountInSnap(0, 2);
        ASSERT_EQ(ref_counts.versioned_ref_counts->size(), 6);
        ASSERT_EQ(ref_counts.getLatestRefCount(), 4);
    }
} // namespace PS::V3::tests
CATCH

TEST_F(PageDirectoryGCTest, GCPushForward)
try
{
    PageIdU64 page_id = 50;

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
    auto del_entries = dir->gcInMemEntries({});
    // v1, v2 have been removed.
    ASSERT_EQ(del_entries.size(), 2);

    EXPECT_ENTRY_EQ(entry_v3, dir, page_id, snapshot3);
    EXPECT_ENTRY_EQ(entry_v5, dir, page_id, snapshot5);

    // Release all snapshots and run gc again, (min gc version get pushed forward and)
    // all versions get compacted.
    snapshot3.reset();
    snapshot5.reset();
    del_entries = dir->gcInMemEntries({});
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
    PageIdU64 page_id = 50;
    PageIdU64 another_page_id = 512;

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
        const auto & del_entries = dir->gcInMemEntries({});
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
        const auto & del_entries = dir->gcInMemEntries({});
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
        const auto & del_entries = dir->gcInMemEntries({});
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
    PageIdU64 page_id = 50;
    PageIdU64 another_page_id = 512;

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
        auto removed_entries = dir->gcInMemEntries({}); // The GC on page_id=50 is blocked by previous `snapshot1`
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
        auto removed_entries = dir->gcInMemEntries({});
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
        auto removed_entries = dir->gcInMemEntries({}); // this will compact all versions
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
    PageIdU64 page_id = 50;
    PageIdU64 another_page_id = 512;

    // Push entries
    INSERT_ENTRY(another_page_id, 1);
    INSERT_ENTRY(another_page_id, 2);
    INSERT_ENTRY(page_id, 3);
    INSERT_ENTRY(another_page_id, 4);
    INSERT_ENTRY_ACQ_SNAP(page_id, 5);
    INSERT_ENTRY(another_page_id, 6);
    INSERT_ENTRY(another_page_id, 7);
    PageEntryV3 entry_v8{.file_id = 1, .size = 8, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.del(buildV3Id(TEST_NAMESPACE_ID, page_id));
        edit.put(buildV3Id(TEST_NAMESPACE_ID, another_page_id), entry_v8);
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
        auto del_entries = dir->gcInMemEntries({});
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
        auto del_entries = dir->gcInMemEntries({});
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
        auto del_entries = dir->gcInMemEntries({});
        // another_page_id: v8 have been removed.
        EXPECT_EQ(del_entries.size(), 1);
    }

    {
        /**
         * after GC => { empty }
         *   snapshot remain: []
         */
        snapshot9.reset();
        auto del_entries = dir->gcInMemEntries({});
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
    PageIdU64 page_id = 50;
    PageIdU64 another_page_id = 512;
    INSERT_ENTRY_TO(page_id, 1, 1);
    INSERT_ENTRY_TO(page_id, 2, 2);
    INSERT_ENTRY_TO(another_page_id, 3, 2);
    INSERT_ENTRY_TO(page_id, 4, 1);
    INSERT_ENTRY_TO(page_id, 5, 3);
    INSERT_ENTRY_TO(another_page_id, 6, 1);

    // Full GC get entries
    auto candidate_entries_1 = dir->getEntriesByBlobIds({1});
    EXPECT_EQ(candidate_entries_1.first.size(), 1);
    EXPECT_EQ(candidate_entries_1.first[1].size(), 1); // 1 entries for 1 page id

    auto candidate_entries_2_3 = dir->getEntriesByBlobIds({2, 3});
    EXPECT_EQ(candidate_entries_2_3.first.size(), 1);
    const auto & entries_in_file2 = candidate_entries_2_3.first[2];
    const auto & entries_in_file3 = candidate_entries_2_3.first[3];
    EXPECT_EQ(entries_in_file2.empty(), true);
    EXPECT_EQ(entries_in_file3.size(), 1); // 1 entries for 1 page id

    PageEntriesEdit gc_migrate_entries;
    for (const auto & [file_id, entries] : candidate_entries_1.first)
    {
        (void)file_id;
        for (const auto & [gc_page_id, ver, entry] : entries)
        {
            gc_migrate_entries.upsertPage(gc_page_id, ver, entry);
        }
    }
    for (const auto & [file_id, entries] : candidate_entries_2_3.first)
    {
        (void)file_id;
        for (const auto & [gc_page_id, ver, entry] : entries)
        {
            gc_migrate_entries.upsertPage(gc_page_id, ver, entry);
        }
    }

    // Full GC execute apply
    dir->gcApply(std::move(gc_migrate_entries));

    auto snap = dir->createSnapshot();
    ASSERT_ENTRY_EQ(entry_v5, dir, page_id, snap);
    ASSERT_ENTRY_EQ(entry_v6, dir, another_page_id, snap);
}
CATCH

TEST_F(PageDirectoryGCTest, MVCCAndFullGCInConcurrent)
try
{
    PageIdU64 page_id = 50;
    PageIdU64 another_page_id = 512;
    INSERT_ENTRY_TO(page_id, 1, 1);
    INSERT_ENTRY_TO(page_id, 2, 2);
    INSERT_ENTRY_TO(page_id, 3, 2);
    INSERT_ENTRY_TO(page_id, 4, 1);
    INSERT_ENTRY_TO(page_id, 5, 3);
    INSERT_ENTRY_TO(another_page_id, 6, 1);
    INSERT_DELETE(page_id);

    EXPECT_EQ(dir->numPages(), 2);

    // A.1 Full GC get entries for blob_id in [1]
    auto candidate_entries_1 = dir->getEntriesByBlobIds({1});
    EXPECT_EQ(candidate_entries_1.first.size(), 1);
    EXPECT_EQ(candidate_entries_1.first[1].size(), 1); // 1 entries for `another_page_id`

    // for blob_id in [2, 3]
    auto candidate_entries_2_3 = dir->getEntriesByBlobIds({2, 3});
    EXPECT_EQ(candidate_entries_2_3.first.empty(), true);

    // B.1 Execute GC
    dir->gcInMemEntries({});
    // `page_id` get removed
    EXPECT_EQ(dir->numPages(), 1);

    PageEntriesEdit gc_migrate_entries;
    for (const auto & [file_id, entries] : candidate_entries_1.first)
    {
        (void)file_id;
        for (const auto & [gc_page_id, ver, entry] : entries)
        {
            gc_migrate_entries.upsertPage(gc_page_id, ver, entry);
        }
    }
    for (const auto & [file_id, entries] : candidate_entries_2_3.first)
    {
        (void)file_id;
        for (const auto & [gc_page_id, ver, entry] : entries)
        {
            gc_migrate_entries.upsertPage(gc_page_id, ver, entry);
        }
    }

    // A.2 Full GC execute apply, upsert `another_page_id`, but we still don't
    // support Full GC and gcInMem run conncurrently
    dir->gcApply(std::move(gc_migrate_entries));

    auto snap = dir->createSnapshot();
    ASSERT_ENTRY_EQ(entry_v6, dir, another_page_id, snap);
}
CATCH

TEST_F(PageDirectoryGCTest, GCOnRefedEntries)
try
{
    // 10->entry1, 11->10=>11->entry1; del 10->entry1
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 10), entry1);
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 11), buildV3Id(TEST_NAMESPACE_ID, 10));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 10));
        dir->apply(std::move(edit));
    }
    // entry1 should not be removed
    {
        auto outdated_entries = dir->gcInMemEntries({});
        EXPECT_TRUE(outdated_entries.empty());
    }

    // del 11->entry1
    {
        PageEntriesEdit edit;
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 11));
        dir->apply(std::move(edit));
    }
    // entry1 get removed
    {
        auto outdated_entries = dir->gcInMemEntries({});
        EXPECT_EQ(1, outdated_entries.size());
        EXPECT_SAME_ENTRY(entry1, *outdated_entries.begin());
    }
}
CATCH

TEST_F(PageDirectoryGCTest, GCOnRefedEntries2)
try
{
    // 10->entry1, 11->10=>11->entry1; del 10->entry1
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 10), entry1);
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 11), buildV3Id(TEST_NAMESPACE_ID, 10));
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 12), buildV3Id(TEST_NAMESPACE_ID, 10));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 10));
        dir->apply(std::move(edit));
    }
    // entry1 should not be removed
    {
        auto outdated_entries = dir->gcInMemEntries({});
        EXPECT_TRUE(outdated_entries.empty());
    }

    // del 11->entry1
    {
        PageEntriesEdit edit;
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 11));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 12));
        dir->apply(std::move(edit));
    }
    // entry1 get removed
    {
        auto outdated_entries = dir->gcInMemEntries({});
        EXPECT_EQ(1, outdated_entries.size());
        EXPECT_SAME_ENTRY(entry1, *outdated_entries.begin());
    }
}
CATCH

TEST_F(PageDirectoryGCTest, RewriteRefedId)
try
{
    // 10->entry1, 11->10, 12->10
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 10), entry1);
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 11), buildV3Id(TEST_NAMESPACE_ID, 10));
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 12), buildV3Id(TEST_NAMESPACE_ID, 10));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 10));
        dir->apply(std::move(edit));
    }
    // entry1 should not be removed
    {
        auto outdated_entries = dir->gcInMemEntries({});
        EXPECT_TRUE(outdated_entries.empty());
    }

    PageEntryV3 entry2{.file_id = 2, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3
        entry3{.file_id = 2, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123 + 1024, .checksum = 0x4567};
    {
        // this will return ref page 11 and 12 that need to be rewritten
        // to new blob file.
        auto full_gc_entries = dir->getEntriesByBlobIds({1});
        ASSERT_EQ(full_gc_entries.first.size(), 1);
        auto ids = full_gc_entries.first.at(1);
        ASSERT_EQ(ids.size(), 2);
        ASSERT_EQ(std::get<0>(ids[0]), buildV3Id(TEST_NAMESPACE_ID, 11));
        ASSERT_EQ(std::get<0>(ids[1]), buildV3Id(TEST_NAMESPACE_ID, 12));

        // Mock full gc happen in BlobStore. upsert 11->entry2, upsert 12->entry3
        PageEntriesEdit edit;
        edit.upsertPage(std::get<0>(ids[0]), std::get<1>(ids[0]), entry2);
        edit.upsertPage(std::get<0>(ids[1]), std::get<1>(ids[1]), entry3);
        // this will rewrite ref page 11, 12 to normal page
        dir->gcApply(std::move(edit));
    }

    // page 10 get removed
    auto removed_entries = dir->gcInMemEntries({});
    ASSERT_EQ(removed_entries.size(), 1);
    EXPECT_SAME_ENTRY(removed_entries[0], entry1);

    {
        auto snap = dir->createSnapshot();
        EXPECT_ENTRY_EQ(entry2, dir, 11, snap);
        EXPECT_ENTRY_EQ(entry3, dir, 12, snap);
        EXPECT_ENTRY_NOT_EXIST(dir, 10, snap);
    }

    // del 11->entry2
    {
        PageEntriesEdit edit;
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 11));
        dir->apply(std::move(edit));
        // entry2 get removed
        auto outdated_entries = dir->gcInMemEntries({});
        ASSERT_EQ(1, outdated_entries.size());
        EXPECT_SAME_ENTRY(entry2, outdated_entries[0]);
    }
    // del 12->entry3
    {
        PageEntriesEdit edit;
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 12));
        dir->apply(std::move(edit));
        // entry3 get removed
        auto outdated_entries = dir->gcInMemEntries({});
        ASSERT_EQ(1, outdated_entries.size());
        EXPECT_SAME_ENTRY(entry3, outdated_entries[0]);
    }

    ASSERT_EQ(dir->getAllPageIds().empty(), true);
}
CATCH

TEST_F(PageDirectoryGCTest, RewriteRefedIdToExternalPage)
try
{
    // 10->entry1, 11->10, 12->10
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        u128::PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 10), entry1);
        dir->apply(std::move(edit));
    }
    {
        u128::PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 11), buildV3Id(TEST_NAMESPACE_ID, 10));
        dir->apply(std::move(edit));
    }
    {
        u128::PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 12), buildV3Id(TEST_NAMESPACE_ID, 10));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 10));
        dir->apply(std::move(edit));
    }
    // 50->ext_id, 51->50
    {
        u128::PageEntriesEdit edit;
        edit.putExternal(buildV3Id(TEST_NAMESPACE_ID, 50));
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 51), buildV3Id(TEST_NAMESPACE_ID, 50));
        dir->apply(std::move(edit));
    }
    // entry1 should not be removed
    {
        auto outdated_entries = dir->gcInMemEntries({});
        EXPECT_TRUE(outdated_entries.empty());
    }

    PageEntryV3 entry2{.file_id = 2, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3
        entry3{.file_id = 2, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123 + 1024, .checksum = 0x4567};
    {
        // this will return ref page 11 and 12 that need to be rewritten
        // to new blob file.
        auto full_gc_entries = dir->getEntriesByBlobIds({1});
        ASSERT_EQ(full_gc_entries.first.size(), 1);
        auto ids = full_gc_entries.first.at(1);
        ASSERT_EQ(ids.size(), 2);
        ASSERT_EQ(std::get<0>(ids[0]), buildV3Id(TEST_NAMESPACE_ID, 11));
        ASSERT_EQ(std::get<0>(ids[1]), buildV3Id(TEST_NAMESPACE_ID, 12));

        // Mock full gc happen in BlobStore. upsert 11->entry2, upsert 12->entry3
        u128::PageEntriesEdit edit;
        edit.upsertPage(std::get<0>(ids[0]), std::get<1>(ids[0]), entry2);
        edit.upsertPage(std::get<0>(ids[1]), std::get<1>(ids[1]), entry3);
        // this will rewrite ref page 11, 12 to normal page
        dir->gcApply(std::move(edit));
    }

    // page 10 get removed
    auto removed_entries = dir->gcInMemEntries({});
    ASSERT_EQ(removed_entries.size(), 1);
    EXPECT_SAME_ENTRY(removed_entries[0], entry1);

    {
        auto snap = dir->createSnapshot();
        EXPECT_ENTRY_EQ(entry2, dir, 11, snap);
        EXPECT_ENTRY_EQ(entry3, dir, 12, snap);
        EXPECT_ENTRY_NOT_EXIST(dir, 10, snap);
        auto external_ids = dir->getAliveExternalIds(TEST_NAMESPACE_ID);
        ASSERT_GT(external_ids->count(50), 0);
    }

    // del 11->entry2
    {
        u128::PageEntriesEdit edit;
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 11));
        dir->apply(std::move(edit));
        // entry2 get removed
        auto outdated_entries = dir->gcInMemEntries({});
        ASSERT_EQ(1, outdated_entries.size());
        EXPECT_SAME_ENTRY(entry2, outdated_entries[0]);
    }
    // del 12->entry3
    {
        u128::PageEntriesEdit edit;
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 12));
        dir->apply(std::move(edit));
        // entry3 get removed
        auto outdated_entries = dir->gcInMemEntries({});
        ASSERT_EQ(1, outdated_entries.size());
        EXPECT_SAME_ENTRY(entry3, outdated_entries[0]);
    }

    auto external_ids = dir->getAliveExternalIds(TEST_NAMESPACE_ID);
    ASSERT_GT(external_ids->count(50), 0);
    auto all_page_ids = dir->getAllPageIds();
    ASSERT_EQ(all_page_ids.size(), 2);
    ASSERT_GT(all_page_ids.count(buildV3Id(TEST_NAMESPACE_ID, 50)), 0);
    ASSERT_GT(all_page_ids.count(buildV3Id(TEST_NAMESPACE_ID, 51)), 0);
}
CATCH

TEST_F(PageDirectoryGCTest, RewriteRefedIdWithConcurrentDelete)
try
{
    // 10->entry1, 11->10, 12->10
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 10), entry1);
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 11), buildV3Id(TEST_NAMESPACE_ID, 10));
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 12), buildV3Id(TEST_NAMESPACE_ID, 10));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 10));
        dir->apply(std::move(edit));
    }
    // entry1 should not be removed
    {
        auto outdated_entries = dir->gcInMemEntries({});
        EXPECT_TRUE(outdated_entries.empty());
    }

    PageEntryV3 entry2{.file_id = 2, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3
        entry3{.file_id = 2, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123 + 1024, .checksum = 0x4567};
    {
        // this will return ref page 11 and 12 that need to be rewritten
        // to new blob file.
        auto full_gc_entries = dir->getEntriesByBlobIds({1});
        ASSERT_EQ(full_gc_entries.first.size(), 1);
        auto ids = full_gc_entries.first.at(1);
        ASSERT_EQ(ids.size(), 2);
        ASSERT_EQ(std::get<0>(ids[0]), buildV3Id(TEST_NAMESPACE_ID, 11));
        ASSERT_EQ(std::get<0>(ids[1]), buildV3Id(TEST_NAMESPACE_ID, 12));

        // unlike `RewriteRefedId`, foreground delete 11, 12 before
        // full gc apply upserts
        PageEntriesEdit fore_edit;
        fore_edit.del(buildV3Id(TEST_NAMESPACE_ID, 11));
        fore_edit.del(buildV3Id(TEST_NAMESPACE_ID, 12));
        dir->apply(std::move(fore_edit));

        // full gc ends, apply upserts
        // upsert 11->entry2
        // upsert 12->entry3
        PageEntriesEdit edit;
        edit.upsertPage(std::get<0>(ids[0]), std::get<1>(ids[0]), entry2);
        edit.upsertPage(std::get<0>(ids[1]), std::get<1>(ids[1]), entry3);
        // this will rewrite ref page 11, 12 to normal page
        dir->gcApply(std::move(edit));
    }

    // page 10,11,12 get removed
    auto removed_entries = dir->gcInMemEntries({});
    ASSERT_EQ(removed_entries.size(), 3);
    EXPECT_SAME_ENTRY(removed_entries[0], entry1);
    EXPECT_SAME_ENTRY(removed_entries[1], entry2);
    EXPECT_SAME_ENTRY(removed_entries[2], entry3);

    {
        auto snap = dir->createSnapshot();
        EXPECT_ENTRY_NOT_EXIST(dir, 11, snap);
        EXPECT_ENTRY_NOT_EXIST(dir, 12, snap);
        EXPECT_ENTRY_NOT_EXIST(dir, 10, snap);
    }

    ASSERT_EQ(dir->getAllPageIds().empty(), true);
}
CATCH

TEST_F(PageDirectoryGCTest, GCOnRefedExternalEntries)
try
{
    // 10->ext, 11->10=>11->ext; del 10->ext
    {
        PageEntriesEdit edit;
        edit.putExternal(buildV3Id(TEST_NAMESPACE_ID, 10));
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 11), buildV3Id(TEST_NAMESPACE_ID, 10));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 10));
        dir->apply(std::move(edit));
    }
    // entry1 should not be removed
    {
        auto outdated_entries = dir->gcInMemEntries({});
        EXPECT_TRUE(outdated_entries.empty());
        auto alive_ex_id = dir->getAliveExternalIds(TEST_NAMESPACE_ID);
        ASSERT_TRUE(alive_ex_id.has_value());
        ASSERT_EQ(alive_ex_id->size(), 1);
        ASSERT_EQ(*alive_ex_id->begin(), 10);
    }

    // del 11->ext
    {
        PageEntriesEdit edit;
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 11));
        dir->apply(std::move(edit));
    }
    // entry1 get removed
    {
        auto outdated_entries = dir->gcInMemEntries({});
        EXPECT_EQ(0, outdated_entries.size());
        auto alive_ex_id = dir->getAliveExternalIds(TEST_NAMESPACE_ID);
        ASSERT_TRUE(alive_ex_id.has_value());
        ASSERT_EQ(alive_ex_id->size(), 0);
    }
}
CATCH


TEST_F(PageDirectoryGCTest, GCOnRefedExternalEntries2)
try
{
    {
        PageEntriesEdit edit; // ingest
        edit.putExternal(buildV3Id(TEST_NAMESPACE_ID, 352));
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 353), buildV3Id(TEST_NAMESPACE_ID, 352));
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit; // ingest done
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 352));
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit; // split
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 357), buildV3Id(TEST_NAMESPACE_ID, 353));
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 359), buildV3Id(TEST_NAMESPACE_ID, 353));
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit; // split done
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 353));
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit; // one of segment delta-merge
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 359));
        dir->apply(std::move(edit));
    }

    {
        auto snap = dir->createSnapshot();
        auto normal_id = getNormalPageIdU64(dir, 357, snap);
        EXPECT_EQ(normal_id, 352);
    }
    dir->gcInMemEntries({});
    {
        auto snap = dir->createSnapshot();
        auto normal_id = getNormalPageIdU64(dir, 357, snap);
        EXPECT_EQ(normal_id, 352);
    }

    auto s0 = dir->createSnapshot();
    auto edit = dir->dumpSnapshotToEdit(s0);
    auto restore_from_edit = [](const PageEntriesEdit & edit) {
        auto deseri_edit = u128::Serializer::deserializeFrom(u128::Serializer::serializeTo(edit), nullptr);
        auto provider = DB::tests::TiFlashTestEnv::getDefaultFileProvider();
        auto path = getTemporaryPath();
        PSDiskDelegatorPtr delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        PageDirectoryFactory<u128::FactoryTrait> factory;
        auto d = factory.createFromEditForTest(getCurrentTestName(), provider, delegator, deseri_edit);
        return d;
    };
    {
        auto restored_dir = restore_from_edit(edit);
        auto snap = restored_dir->createSnapshot();
        auto normal_id = getNormalPageIdU64(restored_dir, 357, snap);
        EXPECT_EQ(normal_id, 352);
    }
}
CATCH


TEST_F(PageDirectoryGCTest, DumpAndRestore)
try
{
    auto restore_from_edit = [](const PageEntriesEdit & edit) {
        auto deseri_edit = u128::Serializer::deserializeFrom(u128::Serializer::serializeTo(edit), nullptr);
        auto provider = DB::tests::TiFlashTestEnv::getDefaultFileProvider();
        auto path = getTemporaryPath();
        PSDiskDelegatorPtr delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        PageDirectoryFactory<u128::FactoryTrait> factory;
        auto d = factory.createFromEditForTest(getCurrentTestName(), provider, delegator, deseri_edit);
        return d;
    };

    PageEntryV3 entry_1_v1{.file_id = 1, .size = 1, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry_1_v2{.file_id = 1, .size = 2, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry_2_v1{.file_id = 2, .size = 1, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry_2_v2{.file_id = 2, .size = 2, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 1), entry_1_v1);
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 1), entry_1_v2);
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 2), entry_2_v1);
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 2), entry_2_v2);
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 3));
        dir->apply(std::move(edit));
    }
    // dump 0
    auto s0 = dir->createSnapshot();
    auto check_s0 = [&, this]() {
        auto edit = dir->dumpSnapshotToEdit(s0);
        auto restored_dir = restore_from_edit(edit);
        auto temp_snap = restored_dir->createSnapshot();
        EXPECT_SAME_ENTRY(getEntry(restored_dir, 1, temp_snap), entry_1_v2);
        EXPECT_SAME_ENTRY(getEntry(restored_dir, 2, temp_snap), entry_2_v2);
        EXPECT_ANY_THROW(getEntry(restored_dir, 3, temp_snap));
    };
    check_s0();

    // 10->ext, 11->10, del 10->ext
    // 50->entry, 51->50, 52->51=>50, del 50
    PageEntryV3 entry_50{.file_id = 1, .size = 50, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry_60{.file_id = 1, .size = 90, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 2));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 1));
        edit.putExternal(buildV3Id(TEST_NAMESPACE_ID, 10));
        edit.putExternal(buildV3Id(TEST_NAMESPACE_ID, 20));
        edit.putExternal(buildV3Id(TEST_NAMESPACE_ID, 30));
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 50), entry_50);
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 60), entry_60);
        dir->apply(std::move(edit));
    }
    auto s1 = dir->createSnapshot();
    auto check_s1 = [&, this]() {
        auto edit = dir->dumpSnapshotToEdit(s1);
        auto restored_dir = restore_from_edit(edit);
        auto temp_snap = restored_dir->createSnapshot();
        EXPECT_ANY_THROW(getEntry(restored_dir, 1, temp_snap));
        EXPECT_ANY_THROW(getEntry(restored_dir, 2, temp_snap));
        EXPECT_ANY_THROW(getEntry(restored_dir, 3, temp_snap));
        auto alive_ex = restored_dir->getAliveExternalIds(TEST_NAMESPACE_ID);
        ASSERT_TRUE(alive_ex.has_value());
        EXPECT_EQ(alive_ex->size(), 3);
        EXPECT_GT(alive_ex->count(10), 0);
        EXPECT_GT(alive_ex->count(20), 0);
        EXPECT_GT(alive_ex->count(30), 0);
        EXPECT_SAME_ENTRY(getEntry(restored_dir, 50, temp_snap), entry_50);
        EXPECT_SAME_ENTRY(getEntry(restored_dir, 60, temp_snap), entry_60);
    };
    check_s0();
    check_s1();

    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 11), buildV3Id(TEST_NAMESPACE_ID, 10));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 10));

        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 21), buildV3Id(TEST_NAMESPACE_ID, 20));
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 22), buildV3Id(TEST_NAMESPACE_ID, 20));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 20));

        edit.del(buildV3Id(TEST_NAMESPACE_ID, 30));

        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 51), buildV3Id(TEST_NAMESPACE_ID, 50));
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 52), buildV3Id(TEST_NAMESPACE_ID, 51));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 50));

        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 61), buildV3Id(TEST_NAMESPACE_ID, 60));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 61));
        dir->apply(std::move(edit));
    }
    auto s2 = dir->createSnapshot();
    auto check_s2 = [&, this]() {
        auto edit = dir->dumpSnapshotToEdit(s2);
        auto restored_dir = restore_from_edit(edit);
        auto temp_snap = restored_dir->createSnapshot();
        EXPECT_ANY_THROW(getEntry(restored_dir, 1, temp_snap));
        EXPECT_ANY_THROW(getEntry(restored_dir, 2, temp_snap));
        EXPECT_ANY_THROW(getEntry(restored_dir, 3, temp_snap));
        auto alive_ex = restored_dir->getAliveExternalIds(TEST_NAMESPACE_ID);
        ASSERT_TRUE(alive_ex.has_value());
        EXPECT_EQ(alive_ex->size(), 2);
        EXPECT_GT(alive_ex->count(10), 0);
        EXPECT_EQ(getNormalPageIdU64(restored_dir, 11, temp_snap), 10);

        EXPECT_GT(alive_ex->count(20), 0);
        EXPECT_EQ(getNormalPageIdU64(restored_dir, 21, temp_snap), 20);
        EXPECT_EQ(getNormalPageIdU64(restored_dir, 22, temp_snap), 20);

        EXPECT_EQ(alive_ex->count(30), 0); // removed

        EXPECT_ANY_THROW(getEntry(restored_dir, 50, temp_snap));
        EXPECT_SAME_ENTRY(getEntry(restored_dir, 51, temp_snap), entry_50);
        EXPECT_SAME_ENTRY(getEntry(restored_dir, 52, temp_snap), entry_50);

        EXPECT_SAME_ENTRY(getEntry(restored_dir, 60, temp_snap), entry_60);
        EXPECT_ANY_THROW(getEntry(restored_dir, 61, temp_snap));
    };
    check_s0();
    check_s1();
    check_s2();

    {
        // only 51->50 left
        PageEntriesEdit edit;
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 11));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 21));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 22));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 52));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 60));
        dir->apply(std::move(edit));
    }
    auto s3 = dir->createSnapshot();
    auto check_s3 = [&, this]() {
        auto edit = dir->dumpSnapshotToEdit(s3);
        auto restored_dir = restore_from_edit(edit);
        auto temp_snap = restored_dir->createSnapshot();
        EXPECT_ANY_THROW(getEntry(restored_dir, 1, temp_snap));
        EXPECT_ANY_THROW(getEntry(restored_dir, 2, temp_snap));
        EXPECT_ANY_THROW(getEntry(restored_dir, 3, temp_snap));
        auto alive_ex = restored_dir->getAliveExternalIds(TEST_NAMESPACE_ID);
        ASSERT_TRUE(alive_ex.has_value());
        EXPECT_EQ(alive_ex->size(), 0);
        EXPECT_EQ(alive_ex->count(10), 0); // removed
        EXPECT_EQ(alive_ex->count(20), 0); // removed
        EXPECT_EQ(alive_ex->count(30), 0); // removed

        EXPECT_ANY_THROW(getEntry(restored_dir, 50, temp_snap));
        EXPECT_SAME_ENTRY(getEntry(restored_dir, 51, temp_snap), entry_50);
        EXPECT_ANY_THROW(getEntry(restored_dir, 52, temp_snap));

        EXPECT_ANY_THROW(getEntry(restored_dir, 60, temp_snap));
        EXPECT_ANY_THROW(getEntry(restored_dir, 61, temp_snap));
    };
    check_s0();
    check_s1();
    check_s2();
    check_s3();

    {
        // only 51->50 left
        PageEntriesEdit edit;
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 51));
        dir->apply(std::move(edit));
    }
    auto s4 = dir->createSnapshot();
    auto check_s4 = [&, this]() {
        auto edit = dir->dumpSnapshotToEdit(s4);
        auto restored_dir = restore_from_edit(edit);
        auto temp_snap = restored_dir->createSnapshot();
        EXPECT_ANY_THROW(getEntry(restored_dir, 1, temp_snap));
        EXPECT_ANY_THROW(getEntry(restored_dir, 2, temp_snap));
        EXPECT_ANY_THROW(getEntry(restored_dir, 3, temp_snap));
        auto alive_ex = restored_dir->getAliveExternalIds(TEST_NAMESPACE_ID);
        ASSERT_TRUE(alive_ex.has_value());
        EXPECT_EQ(alive_ex->size(), 0);
        EXPECT_EQ(alive_ex->count(10), 0); // removed
        EXPECT_EQ(alive_ex->count(20), 0); // removed
        EXPECT_EQ(alive_ex->count(30), 0); // removed

        EXPECT_ANY_THROW(getEntry(restored_dir, 50, temp_snap));
        EXPECT_ANY_THROW(getEntry(restored_dir, 51, temp_snap));
        EXPECT_ANY_THROW(getEntry(restored_dir, 52, temp_snap));

        EXPECT_ANY_THROW(getEntry(restored_dir, 60, temp_snap));
        EXPECT_ANY_THROW(getEntry(restored_dir, 61, temp_snap));
    };
    check_s0();
    check_s1();
    check_s2();
    check_s3();
    check_s4();
}
CATCH

TEST_F(PageDirectoryGCTest, RestoreWithRef)
try
{
    BlobFileId file_id1 = 1;
    BlobFileId file_id2 = 5;

    const auto & path = getTemporaryPath();
    createIfNotExist(path);
    Poco::File(fmt::format("{}/{}{}", path, BlobFile::BLOB_PREFIX_NAME, file_id1)).createFile();
    Poco::File(fmt::format("{}/{}{}", path, BlobFile::BLOB_PREFIX_NAME, file_id2)).createFile();

    PageEntryV3
        entry_1_v1{.file_id = file_id1, .size = 7890, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3
        entry_5_v1{.file_id = file_id2, .size = 255, .padded_size = 0, .tag = 0, .offset = 0x100, .checksum = 0x4567};
    PageEntryV3
        entry_5_v2{.file_id = file_id2, .size = 255, .padded_size = 0, .tag = 0, .offset = 0x400, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 1), entry_1_v1);
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 5), entry_5_v1);
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 2), buildV3Id(TEST_NAMESPACE_ID, 1));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 1));
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 5), entry_5_v2); // replaced for page 5 entry
        dir->apply(std::move(edit));
    }

    auto restore_from_edit = [](PageEntriesEdit & edit, BlobStats & stats) {
        auto provider = DB::tests::TiFlashTestEnv::getDefaultFileProvider();
        auto path = getTemporaryPath();
        PSDiskDelegatorPtr delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        PageDirectoryFactory<u128::FactoryTrait> factory;
        auto d = factory.setBlobStats(stats).createFromEditForTest(getCurrentTestName(), provider, delegator, edit);
        return d;
    };
    {
        auto snap = dir->createSnapshot();
        auto edit = dir->dumpSnapshotToEdit(snap);
        auto path = getTemporaryPath();
        PSDiskDelegatorPtr delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        auto config = BlobConfig{};
        BlobStats stats(log, delegator, config);
        {
            std::lock_guard lock(stats.lock_stats);
            stats.createStatNotChecking(file_id1, BLOBFILE_LIMIT_SIZE, lock);
            stats.createStatNotChecking(file_id2, BLOBFILE_LIMIT_SIZE, lock);
        }
        auto restored_dir = restore_from_edit(edit, stats);
        auto temp_snap = restored_dir->createSnapshot();
        EXPECT_SAME_ENTRY(entry_1_v1, getEntry(restored_dir, 2, temp_snap));
        EXPECT_ANY_THROW(getEntry(restored_dir, 1, temp_snap));
        EXPECT_SAME_ENTRY(entry_5_v2, getEntry(restored_dir, 5, temp_snap));

        // The entry_1_v1 should be restored to stats
        auto stat_for_file_1 = stats.blobIdToStat(file_id1, /*ignore_not_exist*/ false);
        EXPECT_TRUE(stat_for_file_1->smap->isMarkUsed(entry_1_v1.offset, entry_1_v1.size));
        auto stat_for_file_5 = stats.blobIdToStat(file_id2, /*ignore_not_exist*/ false);
        // entry_5_v1 should not be restored to stats
        EXPECT_FALSE(stat_for_file_5->smap->isMarkUsed(entry_5_v1.offset, entry_5_v1.size));
        EXPECT_TRUE(stat_for_file_5->smap->isMarkUsed(entry_5_v2.offset, entry_5_v2.size));
    }
}
CATCH

TEST_F(PageDirectoryGCTest, CleanAfterDecreaseRef)
try
{
    PageEntryV3 entry_50_1{.file_id = 1, .size = 7890, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry_50_2{.file_id = 2, .size = 7890, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};

    auto restore_from_edit = [](PageEntriesEdit & edit) {
        auto provider = DB::tests::TiFlashTestEnv::getDefaultFileProvider();
        auto path = getTemporaryPath();
        PSDiskDelegatorPtr delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        PageDirectoryFactory<u128::FactoryTrait> factory;
        auto d = factory.createFromEditForTest(getCurrentTestName(), provider, delegator, edit);
        return d;
    };

    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 50), entry_50_1);
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 50), entry_50_2);
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 51), buildV3Id(TEST_NAMESPACE_ID, 50));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 50));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 51));
        auto restored_dir = restore_from_edit(edit);
        auto page_ids = restored_dir->getAllPageIds();
        ASSERT_EQ(page_ids.size(), 0);
    }
}
CATCH

TEST_F(PageDirectoryGCTest, IncrRefDuringGC)
try
{
    PageEntryV3
        entry_1_v1{.file_id = 50, .size = 7890, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 1), entry_1_v1);
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 2), buildV3Id(TEST_NAMESPACE_ID, 1));
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 3), buildV3Id(TEST_NAMESPACE_ID, 1));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 1));
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit;
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 2));
        dir->apply(std::move(edit));
    }

    {
        // begin gc and stop after get lowest seq
        auto sp_gc = SyncPointCtl::enableInScope("after_PageDirectory::doGC_getLowestSeq");
        auto th_gc = std::async([&]() { dir->gcInMemEntries({}); });
        sp_gc.waitAndPause();

        // add a ref during gc
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 5), buildV3Id(TEST_NAMESPACE_ID, 3));
        dir->apply(std::move(edit));

        // continue gc and finish
        sp_gc.next();
        th_gc.get();

        ASSERT_EQ(dir->numPages(), 3);
    }

    {
        auto snap = dir->createSnapshot();
        EXPECT_SAME_ENTRY(entry_1_v1, getEntry(dir, 5, snap));
    }
    {
        PageEntriesEdit edit;
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 3));
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 5));
        dir->apply(std::move(edit));
    }

    dir->gcInMemEntries({});
    ASSERT_EQ(dir->numPages(), 0);
}
CATCH


TEST_F(PageDirectoryGCTest, IncrRefDuringGC2)
try
{
    PageEntryV3
        entry_1_v1{.file_id = 50, .size = 7890, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(buildV3Id(TEST_NAMESPACE_ID, 1), entry_1_v1);
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 2), buildV3Id(TEST_NAMESPACE_ID, 1));
        dir->apply(std::move(edit));
    }
    {
        PageEntriesEdit edit;
        edit.del(buildV3Id(TEST_NAMESPACE_ID, 2));
        dir->apply(std::move(edit));
    }

    auto after_get_gc_seq = SyncPointCtl::enableInScope("after_PageDirectory::doGC_getLowestSeq");
    auto th_gc = std::async([&]() { dir->gcInMemEntries({}); });
    after_get_gc_seq.waitAndPause();

    // add a ref during gcInMemEntries
    {
        PageEntriesEdit edit;
        edit.ref(buildV3Id(TEST_NAMESPACE_ID, 5), buildV3Id(TEST_NAMESPACE_ID, 1));
        dir->apply(std::move(edit));
    }

    after_get_gc_seq.next();
    th_gc.get();

    {
        auto snap = dir->createSnapshot();
        auto normal_id = getNormalPageIdU64(dir, 5, snap);
        EXPECT_EQ(normal_id, 1);
        ASSERT_EQ(dir->numPages(), 2);
    }
}
CATCH

#undef INSERT_ENTRY_TO
#undef INSERT_ENTRY
#undef INSERT_ENTRY_ACQ_SNAP
#undef INSERT_DELETE

} // namespace PS::V3::tests
} // namespace DB
