#include "gtest/gtest.h"

#include <Storages/Page/VersionSet/PageEntriesVersionSet.h>

namespace DB
{
namespace tests
{

class PageEntryMap_test : public ::testing::Test
{
public:
    PageEntryMap_test() : map(nullptr), versions() {}

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
    PageEntriesVersionSet versions;
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
    PageEntry p0entry{.file_id = 1, .level = 0, .checksum = 0x123};
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
    const PageId    page_id = 0;
    const PageEntry entry0{.checksum = 0x123};
    map->put(page_id, entry0);
    ASSERT_EQ(map->at(page_id).checksum, entry0.checksum);

    const PageEntry entry1{.checksum = 0x456};
    map->put(page_id, entry1);
    ASSERT_EQ(map->at(page_id).checksum, entry1.checksum);

    map->del(page_id);
    ASSERT_EQ(map->find(page_id), nullptr);
}

TEST_F(PageEntryMap_test, PutDel)
{
    PageEntry p0entry{.file_id = 1, .level = 0, .checksum = 0x123};
    map->put(0, p0entry);
    {
        ASSERT_NE(map->find(0), nullptr);
        const PageEntry & entry = map->at(0);
        EXPECT_EQ(entry.file_id, p0entry.file_id);
        EXPECT_EQ(entry.level, p0entry.level);
        EXPECT_EQ(entry.checksum, p0entry.checksum);
    }
    // add RefPage2 -> Page0
    map->ref(2, 0);
    {
        ASSERT_NE(map->find(2), nullptr);
        const PageEntry & entry = map->at(2);
        EXPECT_EQ(entry.file_id, p0entry.file_id);
        EXPECT_EQ(entry.level, p0entry.level);
        EXPECT_EQ(entry.checksum, p0entry.checksum);
    }

    // remove RefPage0
    map->del(0);
    // now RefPage0 removed
    ASSERT_EQ(map->find(0), nullptr);
    {
        // RefPage2 exist
        ASSERT_NE(map->find(2), nullptr);
        const PageEntry & entry = map->at(2);
        EXPECT_EQ(entry.file_id, p0entry.file_id);
        EXPECT_EQ(entry.level, p0entry.level);
        EXPECT_EQ(entry.checksum, p0entry.checksum);
    }

    // remove RefPage2
    map->del(2);
    ASSERT_EQ(map->find(0), nullptr);
    ASSERT_EQ(map->find(2), nullptr);
}

TEST_F(PageEntryMap_test, UpdateRefPageEntry)
{
    const PageId    page_id = 0;
    const PageId    ref_id  = 1; // RefPage1 -> Page0
    const PageEntry entry0{.checksum = 0x123};
    map->put(page_id, entry0);
    ASSERT_NE(map->find(page_id), nullptr);
    ASSERT_EQ(map->at(page_id).checksum, entry0.checksum);

    map->ref(ref_id, page_id);
    ASSERT_NE(map->find(ref_id), nullptr);
    ASSERT_EQ(map->at(ref_id).checksum, entry0.checksum);

    // update on Page0, both Page0 and RefPage1 entry get update
    const PageEntry entry1{.checksum = 0x456};
    map->put(page_id, entry1);
    ASSERT_EQ(map->at(page_id).checksum, entry1.checksum);
    ASSERT_EQ(map->at(ref_id).checksum, entry1.checksum);

    // update on RefPage1, both Page0 and RefPage1 entry get update
    const PageEntry entry2{.checksum = 0x789};
    map->put(page_id, entry2);
    ASSERT_EQ(map->at(page_id).checksum, entry2.checksum);
    ASSERT_EQ(map->at(ref_id).checksum, entry2.checksum);

    // delete pages
    map->del(page_id);
    ASSERT_EQ(map->find(page_id), nullptr);
    ASSERT_NE(map->find(ref_id), nullptr);

    map->del(ref_id);
    ASSERT_EQ(map->find(ref_id), nullptr);
}

TEST_F(PageEntryMap_test, UpdateRefPageEntry2)
{
    const PageEntry entry0{.checksum = 0xf};
    map->put(0, entry0);
    map->ref(1, 0);
    map->del(0);
    ASSERT_EQ(map->find(0), nullptr);
    ASSERT_EQ(map->at(1).checksum, 0xfUL);

    // update Page0, both Page0 and RefPage1 got update
    const PageEntry entry1{.checksum = 0x1};
    map->put(0, entry1);
    ASSERT_EQ(map->at(0).checksum, 0x1UL);
    ASSERT_EQ(map->at(1).checksum, 0x1UL);
}

TEST_F(PageEntryMap_test, AddRefToNonExistPage)
{
    PageEntry p0entry{.file_id = 1, .level = 0, .checksum = 0x123};
    map->put(0, p0entry);
    // if try to add ref to non-exist page
    ASSERT_THROW({ map->ref<true>(3, 2); }, DB::Exception);
    // if try to access to non exist page, we get an exception
    ASSERT_THROW({ map->at(3); }, DB::Exception);

    // accept add RefPage{3} to non-exist Page{2}
    ASSERT_NO_THROW(map->ref<false>(3, 2));
    // FIXME we can find iterator by RefPage's id
    //auto iter_to_non_exist_ref_page = map->find(3);
    //ASSERT_NE(iter_to_non_exist_ref_page, nullptr);
    // FIXME but if we want to access that non-exist Page, we get an exception
    //ASSERT_THROW({ iter_to_non_exist_ref_page.pageEntry(); }, DB::Exception);
    // if try to access to non exist page, we get an exception
    ASSERT_THROW({ map->at(3); }, DB::Exception);
}

TEST_F(PageEntryMap_test, PutDuplicateRef)
{
    PageEntry p0entry{.checksum = 0xFF};
    map->put(0, p0entry);
    ASSERT_EQ(map->at(0).checksum, p0entry.checksum);

    // if put RefPage1 -> Page0 twice, the second ref call is collapse
    map->ref(1, 0);
    ASSERT_EQ(map->at(1).checksum, p0entry.checksum);
    map->ref(1, 0);
    ASSERT_EQ(map->at(1).checksum, p0entry.checksum);

    map->del(0);
    ASSERT_EQ(map->find(0), nullptr);
    ASSERT_EQ(map->at(1).checksum, p0entry.checksum);
}

TEST_F(PageEntryMap_test, PutRefOnRef)
{
    PageEntry p0entry{.file_id = 1, .level = 0, .checksum = 0x123};
    // put Page0
    map->put(0, p0entry);
    // add RefPage2 -> Page0
    map->ref(2, 0);
    // add RefPage3 -> RefPage2 -> Page0
    map->ref(3, 2);
    {
        ASSERT_NE(map->find(3), nullptr);
        const PageEntry & entry = map->at(3);
        EXPECT_EQ(entry.file_id, p0entry.file_id);
        EXPECT_EQ(entry.level, p0entry.level);
        EXPECT_EQ(entry.checksum, p0entry.checksum);
    }

    // remove RefPage2
    map->del(2);
    // now RefPage2 removed
    ASSERT_EQ(map->find(2), nullptr);
    {
        // RefPage0 exist
        ASSERT_NE(map->find(0), nullptr);
        const PageEntry & entry = map->at(0);
        EXPECT_EQ(entry.file_id, p0entry.file_id);
        EXPECT_EQ(entry.level, p0entry.level);
        EXPECT_EQ(entry.checksum, p0entry.checksum);
    }
    {
        // RefPage3 exist
        ASSERT_NE(map->find(3), nullptr);
        const PageEntry & entry = map->at(3);
        EXPECT_EQ(entry.file_id, p0entry.file_id);
        EXPECT_EQ(entry.level, p0entry.level);
        EXPECT_EQ(entry.checksum, p0entry.checksum);
    }

    // remove RefPage0
    map->del(0);
    // now RefPage0 is removed
    ASSERT_EQ(map->find(0), nullptr);
    ASSERT_EQ(map->find(2), nullptr);
    {
        // RefPage3 exist
        ASSERT_NE(map->find(3), nullptr);
        const PageEntry & entry = map->at(3);
        EXPECT_EQ(entry.file_id, p0entry.file_id);
        EXPECT_EQ(entry.level, p0entry.level);
        EXPECT_EQ(entry.checksum, p0entry.checksum);
    }

    // remove RefPage3
    map->del(3);
    // now RefPage3 is removed
    ASSERT_EQ(map->find(3), nullptr);
    ASSERT_EQ(map->find(0), nullptr);
    ASSERT_EQ(map->find(2), nullptr);
}

TEST_F(PageEntryMap_test, ReBindRef)
{
    PageEntry entry0{.file_id = 1, .level = 0, .checksum = 0x123};
    PageEntry entry1{.file_id = 1, .level = 0, .checksum = 0x123};
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
    PageEntry p0entry{.file_id = 1, .level = 0, .checksum = 0x123};
    PageEntry p1entry{.file_id = 2, .level = 1, .checksum = 0x456};
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
