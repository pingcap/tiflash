#include <Storages/Page/VersionSet/PageEntriesVersionSet.h>

#include "gtest/gtest.h"

namespace DB
{
namespace tests
{

class PageEntryMap_test : public ::testing::Test
{
public:
    PageEntryMap_test() : map(nullptr), log(&Poco::Logger::get("PageEntryMap_test")), versions("entries_map_test", config_, log) {}

protected:
    void SetUp() override
    {
        // Generate an empty PageEntries for each test
        auto               snapshot = versions.getSnapshot();
        PageEntriesBuilder builder(snapshot->version());
        map = builder.build();
    }

    void TearDown() override { delete map; }

    PageEntries * map;

private:
    ::DB::MVCC::VersionSetConfig config_;
    Poco::Logger *               log;
    PageEntriesVersionSet        versions;
};

TEST_F(PageEntryMap_test, Empty)
{
    size_t item_count = 0;
    for (auto iter = map->cbegin(); iter != map->cend(); ++iter)
    {
        item_count += 1;
    }
    ASSERT_EQ(item_count, 0UL);
    ASSERT_EQ(map->maxId(), 0UL);


    // add some Pages, RefPages
    PageEntry p0entry;
    p0entry.file_id  = 1;
    p0entry.level    = 0;
    p0entry.checksum = 0x123;
    map->put(0, p0entry);
    map->ref(1, 0);
    item_count = 0;
    for (auto iter = map->cbegin(); iter != map->cend(); ++iter)
    {
        item_count += 1;
    }
    ASSERT_EQ(item_count, 2UL);
    ASSERT_EQ(map->maxId(), 1UL);

    map->clear();
    item_count = 0;
    for (auto iter = map->cbegin(); iter != map->cend(); ++iter)
    {
        item_count += 1;
    }
    ASSERT_EQ(item_count, 0UL);
    ASSERT_EQ(map->maxId(), 0UL);
}

TEST_F(PageEntryMap_test, UpdatePageEntry)
{
    const PageId page_id = 0;
    PageEntry    entry0;
    entry0.checksum = 0x123;
    map->put(page_id, entry0);
    ASSERT_EQ(map->at(page_id).checksum, entry0.checksum);

    PageEntry entry1;
    entry1.checksum = 0x456;
    map->put(page_id, entry1);
    ASSERT_EQ(map->at(page_id).checksum, entry1.checksum);

    map->del(page_id);
    ASSERT_EQ(map->find(page_id), std::nullopt);
}

TEST_F(PageEntryMap_test, PutDel)
{
    PageEntry p0entry;
    p0entry.file_id  = 1;
    p0entry.level    = 0;
    p0entry.checksum = 0x123;
    map->put(0, p0entry);
    {
        ASSERT_NE(map->find(0), std::nullopt);
        const PageEntry & entry = map->at(0);
        EXPECT_EQ(entry.file_id, p0entry.file_id);
        EXPECT_EQ(entry.level, p0entry.level);
        EXPECT_EQ(entry.checksum, p0entry.checksum);
    }
    // add RefPage2 -> Page0
    map->ref(2, 0);
    {
        ASSERT_NE(map->find(2), std::nullopt);
        const PageEntry & entry = map->at(2);
        EXPECT_EQ(entry.file_id, p0entry.file_id);
        EXPECT_EQ(entry.level, p0entry.level);
        EXPECT_EQ(entry.checksum, p0entry.checksum);
    }

    // remove RefPage0
    map->del(0);
    // now RefPage0 removed
    ASSERT_EQ(map->find(0), std::nullopt);
    {
        // RefPage2 exist
        ASSERT_NE(map->find(2), std::nullopt);
        const PageEntry & entry = map->at(2);
        EXPECT_EQ(entry.file_id, p0entry.file_id);
        EXPECT_EQ(entry.level, p0entry.level);
        EXPECT_EQ(entry.checksum, p0entry.checksum);
    }

    // remove RefPage2
    map->del(2);
    ASSERT_EQ(map->find(0), std::nullopt);
    ASSERT_EQ(map->find(2), std::nullopt);
}

TEST_F(PageEntryMap_test, IdempotentDel)
{
    PageEntry p0entry;
    p0entry.file_id  = 1;
    p0entry.checksum = 0x123;
    map->put(0, p0entry);
    {
        ASSERT_NE(map->find(0), std::nullopt);
        const PageEntry & entry = map->at(0);
        EXPECT_EQ(entry.file_id, p0entry.file_id);
        EXPECT_EQ(entry.checksum, p0entry.checksum);
    }
    map->ref(2, 0);
    {
        ASSERT_NE(map->find(2), std::nullopt);
        const PageEntry & entry = map->at(2);
        EXPECT_EQ(entry.file_id, p0entry.file_id);
        EXPECT_EQ(entry.checksum, p0entry.checksum);
    }

    map->del(0);
    {
        // Should not found Page0, but Page2 is still available
        ASSERT_EQ(map->find(0), std::nullopt);
        auto entry = map->find(2);
        ASSERT_TRUE(entry);
        EXPECT_EQ(entry->file_id, p0entry.file_id);
        EXPECT_EQ(entry->checksum, p0entry.checksum);
        entry = map->findNormalPageEntry(0);
        ASSERT_TRUE(entry);
        EXPECT_EQ(entry->file_id, p0entry.file_id);
        EXPECT_EQ(entry->checksum, p0entry.checksum);
    }

    // Del should be idempotent
    map->del(0);
    {
        ASSERT_EQ(map->find(0), std::nullopt);
        auto entry = map->find(2);
        ASSERT_TRUE(entry);
        EXPECT_EQ(entry->file_id, p0entry.file_id);
        EXPECT_EQ(entry->checksum, p0entry.checksum);
        entry = map->findNormalPageEntry(0);
        ASSERT_TRUE(entry);
        EXPECT_EQ(entry->file_id, p0entry.file_id);
        EXPECT_EQ(entry->checksum, p0entry.checksum);
    }
}

TEST_F(PageEntryMap_test, UpdateRefPageEntry)
{
    const PageId page_id = 0;
    const PageId ref_id  = 1; // RefPage1 -> Page0
    PageEntry    entry0;
    entry0.checksum = 0x123;
    map->put(page_id, entry0);
    ASSERT_NE(map->find(page_id), std::nullopt);
    ASSERT_EQ(map->at(page_id).checksum, entry0.checksum);

    map->ref(ref_id, page_id);
    ASSERT_NE(map->find(ref_id), std::nullopt);
    ASSERT_EQ(map->at(ref_id).checksum, entry0.checksum);

    // update on Page0, both Page0 and RefPage1 entry get update
    PageEntry entry1;
    entry1.checksum = 0x456;
    map->put(page_id, entry1);
    ASSERT_EQ(map->at(page_id).checksum, entry1.checksum);
    ASSERT_EQ(map->at(ref_id).checksum, entry1.checksum);

    // update on RefPage1, both Page0 and RefPage1 entry get update
    PageEntry entry2;
    entry2.checksum = 0x789;
    map->put(page_id, entry2);
    ASSERT_EQ(map->at(page_id).checksum, entry2.checksum);
    ASSERT_EQ(map->at(ref_id).checksum, entry2.checksum);

    // delete pages
    map->del(page_id);
    ASSERT_EQ(map->find(page_id), std::nullopt);
    ASSERT_NE(map->find(ref_id), std::nullopt);

    map->del(ref_id);
    ASSERT_EQ(map->find(ref_id), std::nullopt);
}

TEST_F(PageEntryMap_test, UpdateRefPageEntry2)
{
    PageEntry entry0;
    entry0.checksum = 0xf;
    map->put(0, entry0);
    map->ref(1, 0);
    map->del(0);
    ASSERT_EQ(map->find(0), std::nullopt);
    ASSERT_EQ(map->at(1).checksum, 0xfUL);

    // update Page0, both Page0 and RefPage1 got update
    PageEntry entry1;
    entry1.checksum = 0x1;
    map->put(0, entry1);
    ASSERT_EQ(map->at(0).checksum, 0x1UL);
    ASSERT_EQ(map->at(1).checksum, 0x1UL);
}

TEST_F(PageEntryMap_test, AddRefToNonExistPage)
{
    PageEntry p0entry;
    p0entry.file_id = 1;
    p0entry.level = 0, p0entry.checksum = 0x123;
    map->put(0, p0entry);
    // if try to add ref to non-exist page
    ASSERT_THROW({ map->ref(3, 2); }, DB::Exception);
    // if try to access to non exist page, we get an exception
    ASSERT_THROW({ map->at(3); }, DB::Exception);
}

TEST_F(PageEntryMap_test, PutDuplicateRef)
{
    PageEntry p0entry;
    p0entry.checksum = 0xFF;
    map->put(0, p0entry);
    ASSERT_EQ(map->at(0).checksum, p0entry.checksum);

    // if put RefPage1 -> Page0 twice, the second ref call is collapse
    map->ref(1, 0);
    ASSERT_EQ(map->at(1).checksum, p0entry.checksum);
    map->ref(1, 0);
    ASSERT_EQ(map->at(1).checksum, p0entry.checksum);

    map->del(0);
    ASSERT_EQ(map->find(0), std::nullopt);
    ASSERT_EQ(map->at(1).checksum, p0entry.checksum);
}

TEST_F(PageEntryMap_test, PutRefOnRef)
{
    PageEntry p0entry;
    p0entry.file_id  = 1;
    p0entry.level    = 0;
    p0entry.checksum = 0x123;
    // put Page0
    map->put(0, p0entry);
    // add RefPage2 -> Page0
    map->ref(2, 0);
    // add RefPage3 -> RefPage2 -> Page0
    map->ref(3, 2);
    {
        ASSERT_NE(map->find(3), std::nullopt);
        const PageEntry & entry = map->at(3);
        EXPECT_EQ(entry.file_id, p0entry.file_id);
        EXPECT_EQ(entry.level, p0entry.level);
        EXPECT_EQ(entry.checksum, p0entry.checksum);
    }

    // remove RefPage2
    map->del(2);
    // now RefPage2 removed
    ASSERT_EQ(map->find(2), std::nullopt);
    {
        // RefPage0 exist
        ASSERT_NE(map->find(0), std::nullopt);
        const PageEntry & entry = map->at(0);
        EXPECT_EQ(entry.file_id, p0entry.file_id);
        EXPECT_EQ(entry.level, p0entry.level);
        EXPECT_EQ(entry.checksum, p0entry.checksum);
    }
    {
        // RefPage3 exist
        ASSERT_NE(map->find(3), std::nullopt);
        const PageEntry & entry = map->at(3);
        EXPECT_EQ(entry.file_id, p0entry.file_id);
        EXPECT_EQ(entry.level, p0entry.level);
        EXPECT_EQ(entry.checksum, p0entry.checksum);
    }

    // remove RefPage0
    map->del(0);
    // now RefPage0 is removed
    ASSERT_EQ(map->find(0), std::nullopt);
    ASSERT_EQ(map->find(2), std::nullopt);
    {
        // RefPage3 exist
        ASSERT_NE(map->find(3), std::nullopt);
        const PageEntry & entry = map->at(3);
        EXPECT_EQ(entry.file_id, p0entry.file_id);
        EXPECT_EQ(entry.level, p0entry.level);
        EXPECT_EQ(entry.checksum, p0entry.checksum);
    }

    // remove RefPage3
    map->del(3);
    // now RefPage3 is removed
    ASSERT_EQ(map->find(3), std::nullopt);
    ASSERT_EQ(map->find(0), std::nullopt);
    ASSERT_EQ(map->find(2), std::nullopt);
}

TEST_F(PageEntryMap_test, ReBindRef)
{
    PageEntry entry0;
    entry0.file_id  = 1;
    entry0.level    = 0;
    entry0.checksum = 0x123;
    PageEntry entry1;
    entry1.file_id  = 1;
    entry1.level    = 0;
    entry1.checksum = 0x123;
    // put Page0, Page1
    map->put(0, entry0);
    ASSERT_EQ(map->at(0).checksum, entry0.checksum);
    map->put(1, entry1);
    ASSERT_EQ(map->at(1).checksum, entry1.checksum);

    // rebind RefPage0 -> Page1
    map->ref(0, 1);
    ASSERT_EQ(map->at(0).checksum, entry1.checksum);

    map->del(1);
    ASSERT_EQ(map->at(0).checksum, entry1.checksum);
    map->del(0);
}

TEST_F(PageEntryMap_test, Scan)
{
    PageEntry p0entry;
    p0entry.file_id  = 1;
    p0entry.level    = 0;
    p0entry.checksum = 0x123;
    PageEntry p1entry;
    p1entry.file_id  = 1;
    p1entry.level    = 0;
    p1entry.checksum = 0x456;
    map->put(0, p0entry);
    map->put(1, p1entry);
    map->ref(10, 0);
    map->ref(11, 1);

    // scan through all RefPages {0, 1, 10, 11}
    std::set<PageId> page_ids;
    for (auto iter = map->cbegin(); iter != map->cend(); ++iter)
    {
        page_ids.insert(iter.pageId());
        if (iter.pageId() % 10 == 0)
        {
            const PageEntry & entry = iter.pageEntry();
            EXPECT_EQ(entry.file_id, p0entry.file_id);
            EXPECT_EQ(entry.level, p0entry.level);
            EXPECT_EQ(entry.checksum, p0entry.checksum);
        }
        else if (iter.pageId() % 10 == 1)
        {
            const PageEntry & entry = iter.pageEntry();
            EXPECT_EQ(entry.file_id, p1entry.file_id);
            EXPECT_EQ(entry.level, p1entry.level);
            EXPECT_EQ(entry.checksum, p1entry.checksum);
        }
    }
    ASSERT_EQ(page_ids.size(), 4UL);

    // clear all mapping
    map->clear();
    page_ids.clear();
    for (auto iter = map->cbegin(); iter != map->cend(); ++iter)
    {
        page_ids.insert(iter.pageId());
    }
    ASSERT_TRUE(page_ids.empty());
}

} // namespace tests
} // namespace DB
