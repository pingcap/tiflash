#include "gtest/gtest.h"

#include <Storages/Page/PageEntryMap.h>
namespace DB
{
namespace tests
{
TEST(PageEntryMap_test, Empty)
{
    PageEntryMap map;
    ASSERT_TRUE(map.empty());
    size_t item_count = 0;
    for (auto iter = map.cbegin(); iter != map.cend(); ++iter)
    {
        item_count += 1;
    }
    ASSERT_EQ(item_count, 0UL);
    ASSERT_EQ(map.size(), 0UL);


    // add some Pages, RefPages
    PageEntry p0entry{.file_id = 1, .level = 0, .checksum = 0x123};
    map.put(0, p0entry);
    map.ref(1, 0);
    ASSERT_FALSE(map.empty());
    item_count = 0;
    for (auto iter = map.cbegin(); iter != map.cend(); ++iter)
    {
        item_count += 1;
    }
    ASSERT_EQ(item_count, 2UL);
    ASSERT_EQ(map.size(), 2UL);

    map.clear();
    ASSERT_TRUE(map.empty());
    item_count = 0;
    for (auto iter = map.cbegin(); iter != map.cend(); ++iter)
    {
        item_count += 1;
    }
    ASSERT_EQ(item_count, 0UL);
    ASSERT_EQ(map.size(), 0UL);
}

TEST(PageEntryMap_test, UpdatePageEntry)
{
    const PageId    page_id = 0;
    PageEntryMap    map;
    const PageEntry entry0{.checksum = 0x123};
    map.put(page_id, entry0);
    ASSERT_EQ(map.at(page_id).checksum, entry0.checksum);

    const PageEntry entry1{.checksum = 0x456};
    map.put(page_id, entry1);
    ASSERT_EQ(map.at(page_id).checksum, entry1.checksum);

    map.del(page_id);
    ASSERT_EQ(map.find(page_id), map.end());
    ASSERT_TRUE(map.empty());
}

TEST(PageEntryMap_test, PutDel)
{
    PageEntryMap map;
    PageEntry    p0entry{.file_id = 1, .level = 0, .checksum = 0x123};
    map.put(0, p0entry);
    ASSERT_FALSE(map.empty());
    {
        ASSERT_NE(map.find(0), map.end());
        const PageEntry & entry = map.at(0);
        EXPECT_EQ(entry.file_id, p0entry.file_id);
        EXPECT_EQ(entry.level, p0entry.level);
        EXPECT_EQ(entry.checksum, p0entry.checksum);
    }
    // add RefPage2 -> Page0
    map.ref(2, 0);
    ASSERT_FALSE(map.empty());
    {
        ASSERT_NE(map.find(2), map.end());
        const PageEntry & entry = map.at(2);
        EXPECT_EQ(entry.file_id, p0entry.file_id);
        EXPECT_EQ(entry.level, p0entry.level);
        EXPECT_EQ(entry.checksum, p0entry.checksum);
    }

    // remove RefPage0
    map.del(0);
    // now RefPage0 removed
    ASSERT_EQ(map.find(0), map.end());
    {
        // RefPage2 exist
        ASSERT_NE(map.find(2), map.end());
        const PageEntry & entry = map.find(2).pageEntry();
        EXPECT_EQ(entry.file_id, p0entry.file_id);
        EXPECT_EQ(entry.level, p0entry.level);
        EXPECT_EQ(entry.checksum, p0entry.checksum);
    }

    // remove RefPage2
    map.del(2);
    ASSERT_EQ(map.find(0), map.end());
    ASSERT_EQ(map.find(2), map.end());

    ASSERT_TRUE(map.empty());
}

TEST(PageEntryMap_test, UpdateRefPageEntry)
{
    const PageId    page_id = 0;
    const PageId    ref_id  = 1; // RefPage1 -> Page0
    PageEntryMap    map;
    const PageEntry entry0{.checksum = 0x123};
    map.put(page_id, entry0);
    ASSERT_NE(map.find(page_id), map.end());
    ASSERT_EQ(map.at(page_id).checksum, entry0.checksum);

    map.ref(ref_id, page_id);
    ASSERT_NE(map.find(ref_id), map.end());
    ASSERT_EQ(map.at(ref_id).checksum, entry0.checksum);

    // update on Page0, both Page0 and RefPage1 entry get update
    const PageEntry entry1{.checksum = 0x456};
    map.put(page_id, entry1);
    ASSERT_EQ(map.at(page_id).checksum, entry1.checksum);
    ASSERT_EQ(map.at(ref_id).checksum, entry1.checksum);

    // update on RefPage1, both Page0 and RefPage1 entry get update
    const PageEntry entry2{.checksum = 0x789};
    map.put(page_id, entry2);
    ASSERT_EQ(map.at(page_id).checksum, entry2.checksum);
    ASSERT_EQ(map.at(ref_id).checksum, entry2.checksum);

    // delete pages
    map.del(page_id);
    ASSERT_EQ(map.find(page_id), map.end());
    ASSERT_NE(map.find(ref_id), map.end());
    ASSERT_FALSE(map.empty());

    map.del(ref_id);
    ASSERT_EQ(map.find(ref_id), map.end());
    ASSERT_TRUE(map.empty());
}

TEST(PageEntryMap_test, AddIllegalRef)
{
    PageEntryMap map;
    ASSERT_TRUE(map.empty());
    PageEntry p0entry{.file_id = 1, .level = 0, .checksum = 0x123};
    map.put(0, p0entry);
    ASSERT_FALSE(map.empty());
    // if try to add ref
    ASSERT_THROW({ map.ref(3, 2); }, DB::Exception);
    ASSERT_FALSE(map.empty());
}

TEST(PageEntryMap_test, PutRefOnRef)
{
    PageEntryMap map;
    PageEntry    p0entry{.file_id = 1, .level = 0, .checksum = 0x123};
    // put Page0
    map.put(0, p0entry);
    // add RefPage2 -> Page0
    map.ref(2, 0);
    // add RefPage3 -> RefPage2 -> Page0
    map.ref(3, 2);
    {
        ASSERT_NE(map.find(3), map.end());
        const PageEntry & entry = map.at(3);
        EXPECT_EQ(entry.file_id, p0entry.file_id);
        EXPECT_EQ(entry.level, p0entry.level);
        EXPECT_EQ(entry.checksum, p0entry.checksum);
    }

    // remove RefPage2
    map.del(2);
    // now RefPage2 removed
    ASSERT_EQ(map.find(2), map.end());
    {
        // RefPage0 exist
        ASSERT_NE(map.find(0), map.end());
        const PageEntry & entry = map.find(0).pageEntry();
        EXPECT_EQ(entry.file_id, p0entry.file_id);
        EXPECT_EQ(entry.level, p0entry.level);
        EXPECT_EQ(entry.checksum, p0entry.checksum);
    }
    {
        // RefPage3 exist
        ASSERT_NE(map.find(3), map.end());
        const PageEntry & entry = map.find(3).pageEntry();
        EXPECT_EQ(entry.file_id, p0entry.file_id);
        EXPECT_EQ(entry.level, p0entry.level);
        EXPECT_EQ(entry.checksum, p0entry.checksum);
    }

    // remove RefPage0
    map.del(0);
    // now RefPage0 is removed
    ASSERT_EQ(map.find(0), map.end());
    ASSERT_EQ(map.find(2), map.end());
    {
        // RefPage3 exist
        ASSERT_NE(map.find(3), map.end());
        const PageEntry & entry = map.find(3).pageEntry();
        EXPECT_EQ(entry.file_id, p0entry.file_id);
        EXPECT_EQ(entry.level, p0entry.level);
        EXPECT_EQ(entry.checksum, p0entry.checksum);
    }

    // remove RefPage3
    map.del(3);
    // now RefPage3 is removed
    ASSERT_EQ(map.find(3), map.end());
    ASSERT_EQ(map.find(0), map.end());
    ASSERT_EQ(map.find(2), map.end());

    ASSERT_TRUE(map.empty());
}

TEST(PageEntryMap_test, ReBindRef)
{
    PageEntryMap map;
    PageEntry    entry0{.file_id = 1, .level = 0, .checksum = 0x123};
    PageEntry    entry1{.file_id = 1, .level = 0, .checksum = 0x123};
    // put Page0, Page1
    map.put(0, entry0);
    ASSERT_EQ(map.at(0).checksum, entry0.checksum);
    map.put(1, entry1);
    ASSERT_EQ(map.at(1).checksum, entry1.checksum);

    // rebind RefPage0 -> Page1
    map.ref(0, 1);
    ASSERT_EQ(map.at(0).checksum, entry1.checksum);

    map.del(1);
    ASSERT_EQ(map.at(0).checksum, entry1.checksum);
    map.del(0);
    ASSERT_TRUE(map.empty());
}

TEST(PageEntryMap_test, Scan)
{
    PageEntryMap map;
    PageEntry    p0entry{.file_id = 1, .level = 0, .checksum = 0x123};
    PageEntry    p1entry{.file_id = 2, .level = 1, .checksum = 0x456};
    map.put(0, p0entry);
    map.put(1, p1entry);
    map.ref(10, 0);
    map.ref(11, 1);

    // scan through all RefPages {0, 1, 10, 11}
    std::set<PageId> page_ids;
    for (auto iter = map.cbegin(); iter != map.cend(); ++iter)
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
    ASSERT_FALSE(map.empty());
    map.clear();
    ASSERT_TRUE(map.empty());
    page_ids.clear();
    for (auto iter = map.cbegin(); iter != map.cend(); ++iter)
    {
        page_ids.insert(iter.pageId());
    }
    ASSERT_TRUE(page_ids.empty());
}

} // namespace tests
} // namespace DB
