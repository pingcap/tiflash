// Copyright 2025 PingCAP, Inc.
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

#include <Storages/KVStore/Types.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageDirectoryFactory.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/WAL/WALConfig.h>
#include <TestUtils/MockDiskDelegator.h>
#include <TestUtils/TiFlashStorageTestBasic.h>

namespace DB::PS::V3::tests
{
// using DB::PS::V3::universal::PageEntriesEdit;
class UniPageDirectoryTest : public DB::base::TiFlashStorageTestBasic
{
public:
    UniPageDirectoryTest()
        : log(Logger::get())
    {}

    void SetUp() override
    {
        auto path = getTemporaryPath();
        dropDataOnDisk(path);
        auto provider = DB::tests::TiFlashTestEnv::getDefaultFileProvider();
        PSDiskDelegatorPtr delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        PageDirectoryFactory<universal::FactoryTrait> factory;
        dir = factory.create("UniPageDirectoryTest", provider, delegator, WALConfig());
    }

protected:
    universal::PageDirectoryPtr dir;
    LoggerPtr log;
};

TEST_F(UniPageDirectoryTest, GcInMemBlockByGeneralSnap)
{
    KeyspaceID keyspace = 100;
    TableID table_id = 200;
    RegionID region_id = 300;
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};

    // write 1 page for "Log" and 100 pages for "RaftLog"
    {
        universal::PageEntriesEdit edit;
        edit.put(
            UniversalPageIdFormat::toFullPageId(
                UniversalPageIdFormat::toFullPrefix(keyspace, StorageType::Log, table_id),
                1000),
            entry1);
        for (UInt64 log_id = 10; log_id < 110; ++log_id)
        {
            edit.put(
                UniversalPageIdFormat::toFullPageId(UniversalPageIdFormat::toFullRaftLogPrefix(region_id), log_id),
                entry1);
        }
        dir->apply(std::move(edit));
    }
    ASSERT_EQ(dir->numPages(), 101);

    // Acquire a general snapshot
    auto snap_general = dir->createSnapshot(SnapshotType::General, "");

    // delete all pages
    {
        universal::PageEntriesEdit edit;
        edit.del(UniversalPageIdFormat::toFullPageId(
            UniversalPageIdFormat::toFullPrefix(keyspace, StorageType::Log, table_id),
            1000));
        for (UInt64 log_id = 10; log_id < 110; ++log_id)
        {
            edit.del(
                UniversalPageIdFormat::toFullPageId(UniversalPageIdFormat::toFullRaftLogPrefix(region_id), log_id));
        }
        dir->apply(std::move(edit));
    }

    // But no pages will be removed because the general snapshot is still alive
    auto removed_entries = dir->gcInMemEntries({});
    ASSERT_EQ(removed_entries.size(), 0);
    ASSERT_EQ(dir->numPages(), 101);

    // write more "RaftLog" pages
    {
        universal::PageEntriesEdit edit;
        for (UInt64 log_id = 110; log_id < 150; ++log_id)
        {
            edit.put(
                UniversalPageIdFormat::toFullPageId(UniversalPageIdFormat::toFullRaftLogPrefix(region_id), log_id),
                entry1);
        }
        dir->apply(std::move(edit));
    }
    ASSERT_EQ(dir->numPages(), 141);
    // and then delete them
    {
        universal::PageEntriesEdit edit;
        for (UInt64 log_id = 110; log_id < 150; ++log_id)
        {
            edit.del(
                UniversalPageIdFormat::toFullPageId(UniversalPageIdFormat::toFullRaftLogPrefix(region_id), log_id));
        }
        dir->apply(std::move(edit));
    }
    // still no pages will be removed because the general snapshot is still alive
    removed_entries = dir->gcInMemEntries({});
    ASSERT_EQ(removed_entries.size(), 0);
    ASSERT_EQ(dir->numPages(), 141);

    // release the general snapshot
    snap_general.reset();

    // now all pages can be removed
    removed_entries = dir->gcInMemEntries({});
    ASSERT_EQ(removed_entries.size(), 141);
    // the dir is empty now
    ASSERT_EQ(dir->numPages(), 0);
}

TEST_F(UniPageDirectoryTest, GcInMemBlockByReadingSnap)
{
    KeyspaceID keyspace = 100;
    TableID table_id = 200;
    RegionID region_id = 300;
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};

    // write 1 page for "Log" and 100 pages for "RaftLog"
    {
        universal::PageEntriesEdit edit;
        edit.put(
            UniversalPageIdFormat::toFullPageId(
                UniversalPageIdFormat::toFullPrefix(keyspace, StorageType::Log, table_id),
                1000),
            entry1);
        for (UInt64 log_id = 10; log_id < 110; ++log_id)
        {
            edit.put(
                UniversalPageIdFormat::toFullPageId(UniversalPageIdFormat::toFullRaftLogPrefix(region_id), log_id),
                entry1);
        }
        dir->apply(std::move(edit));
    }
    ASSERT_EQ(dir->numPages(), 101);

    // Acquire a snapshot for DeltaTree engine only
    auto snap_reading = dir->createSnapshot(SnapshotType::DeltaTreeOnly, "");

    // delete all pages
    {
        universal::PageEntriesEdit edit;
        edit.del(UniversalPageIdFormat::toFullPageId(
            UniversalPageIdFormat::toFullPrefix(keyspace, StorageType::Log, table_id),
            1000));
        for (UInt64 log_id = 10; log_id < 110; ++log_id)
        {
            edit.del(
                UniversalPageIdFormat::toFullPageId(UniversalPageIdFormat::toFullRaftLogPrefix(region_id), log_id));
        }
        dir->apply(std::move(edit));
    }

    // The page for "RaftLog" are removed but the page for "Log" is not
    auto removed_entries = dir->gcInMemEntries({});
    ASSERT_EQ(removed_entries.size(), 100);
    ASSERT_EQ(dir->numPages(), 1);

    // write more "RaftLog" pages
    {
        universal::PageEntriesEdit edit;
        for (UInt64 log_id = 110; log_id < 150; ++log_id)
        {
            edit.put(
                UniversalPageIdFormat::toFullPageId(UniversalPageIdFormat::toFullRaftLogPrefix(region_id), log_id),
                entry1);
        }
        dir->apply(std::move(edit));
    }
    ASSERT_EQ(dir->numPages(), 41);
    // and then delete them
    {
        universal::PageEntriesEdit edit;
        for (UInt64 log_id = 110; log_id < 150; ++log_id)
        {
            edit.del(
                UniversalPageIdFormat::toFullPageId(UniversalPageIdFormat::toFullRaftLogPrefix(region_id), log_id));
        }
        dir->apply(std::move(edit));
    }
    // the newly added pages for "RaftLog" are also removed
    removed_entries = dir->gcInMemEntries({});
    ASSERT_EQ(removed_entries.size(), 40);
    ASSERT_EQ(dir->numPages(), 1);

    // release the snapshot
    snap_reading.reset();

    // now the last page can be removed
    removed_entries = dir->gcInMemEntries({});
    ASSERT_EQ(removed_entries.size(), 1);
    // the dir is empty now
    ASSERT_EQ(dir->numPages(), 0);
}

TEST_F(UniPageDirectoryTest, GcInMemBlockByMixedSnap)
{
    KeyspaceID keyspace = 100;
    TableID table_id = 200;
    RegionID region_id = 300;
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .padded_size = 0, .tag = 0, .offset = 0x123, .checksum = 0x4567};

    // write 1 page for "Log" and 100 pages for "RaftLog"
    {
        universal::PageEntriesEdit edit;
        edit.put(
            UniversalPageIdFormat::toFullPageId(
                UniversalPageIdFormat::toFullPrefix(keyspace, StorageType::Log, table_id),
                1000),
            entry1);
        for (UInt64 log_id = 10; log_id < 110; ++log_id)
        {
            edit.put(
                UniversalPageIdFormat::toFullPageId(UniversalPageIdFormat::toFullRaftLogPrefix(region_id), log_id),
                entry1);
        }
        dir->apply(std::move(edit));
    }
    ASSERT_EQ(dir->numPages(), 101);

    // Acquire a snapshot for DeltaTree engine only
    auto snap_reading = dir->createSnapshot(SnapshotType::DeltaTreeOnly, "");

    // delete the "Log" page
    {
        universal::PageEntriesEdit edit;
        edit.del(UniversalPageIdFormat::toFullPageId(
            UniversalPageIdFormat::toFullPrefix(keyspace, StorageType::Log, table_id),
            1000));
        dir->apply(std::move(edit));
    }

    // The page for "Log" is not removed because the reading snapshot is still alive
    auto removed_entries = dir->gcInMemEntries({});
    ASSERT_EQ(removed_entries.size(), 0);
    ASSERT_EQ(dir->numPages(), 101);

    // Acquire a general snapshot
    auto snap_general = dir->createSnapshot(SnapshotType::General, "");

    // The page should also not be removed because the reading snapshot is still alive
    removed_entries = dir->gcInMemEntries({});
    ASSERT_EQ(removed_entries.size(), 0);
    ASSERT_EQ(dir->numPages(), 101);

    // Release the general snapshot
    snap_general.reset();

    // Now the page for "Log" is not removed because the reading snapshot is still alive
    removed_entries = dir->gcInMemEntries({});
    ASSERT_EQ(removed_entries.size(), 0);
    ASSERT_EQ(dir->numPages(), 101);

    // delete the "RaftLog" pages
    {
        universal::PageEntriesEdit edit;
        for (UInt64 log_id = 10; log_id < 110; ++log_id)
        {
            edit.del(
                UniversalPageIdFormat::toFullPageId(UniversalPageIdFormat::toFullRaftLogPrefix(region_id), log_id));
        }
        dir->apply(std::move(edit));
    }
    // The pages for "RaftLog" are removed but the page for "Log" is not
    removed_entries = dir->gcInMemEntries({});
    ASSERT_EQ(removed_entries.size(), 100);
    ASSERT_EQ(dir->numPages(), 1);

    // Release the reading snapshot
    snap_reading.reset();
    // Now the page for "Log" can be removed
    removed_entries = dir->gcInMemEntries({});
    ASSERT_EQ(removed_entries.size(), 1);
    ASSERT_EQ(dir->numPages(), 0);
}
} // namespace DB::PS::V3::tests
