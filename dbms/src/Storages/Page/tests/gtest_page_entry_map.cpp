#include "gtest/gtest.h"

#include <Storages/Page/Page.h>
namespace DB
{
namespace tests
{

TEST(PageEntryMap_test, PutDel)
{
    PageCacheMap map;
    ASSERT_TRUE(map.empty());
    PageCache p0entry{.file_id = 1, .level = 0, .checksum = 0x123};
    map.put(0, p0entry);
    ASSERT_FALSE(map.empty());
    {
        ASSERT_NE(map.find(0), map.end());
        const PageCache & entry = map.at(0);
        EXPECT_EQ(entry.file_id, p0entry.file_id);
        EXPECT_EQ(entry.level, p0entry.level);
        EXPECT_EQ(entry.checksum, p0entry.checksum);
    }
    // add RefPage2 -> Page0
    map.ref(2, 0);
    ASSERT_FALSE(map.empty());
    {
        ASSERT_NE(map.find(2), map.end());
        const PageCache & entry = map.at(2);
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
        const PageCache & entry = map.find(2).pageCache();
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

TEST(PageEntryMap_test, IllegalRef)
{
    PageCacheMap map;
    ASSERT_TRUE(map.empty());
    PageCache p0entry{.file_id = 1, .level = 0, .checksum = 0x123};
    map.put(0, p0entry);
    ASSERT_FALSE(map.empty());
    ASSERT_THROW({ map.ref(3, 2); }, DB::Exception);
}

TEST(PageEntryMap_test, PutRefOnRef)
{
    PageCacheMap map;
    PageCache    p0entry{.file_id = 1, .level = 0, .checksum = 0x123};
    // put Page0
    map.put(0, p0entry);
    // add RefPage2 -> Page0
    map.ref(2, 0);
    // add RefPage3 -> RefPage2 -> Page0
    map.ref(3, 2);
    {
        ASSERT_NE(map.find(3), map.end());
        const PageCache & entry = map.at(3);
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
        const PageCache & entry = map.find(0).pageCache();
        EXPECT_EQ(entry.file_id, p0entry.file_id);
        EXPECT_EQ(entry.level, p0entry.level);
        EXPECT_EQ(entry.checksum, p0entry.checksum);
    }
    {
        // RefPage3 exist
        ASSERT_NE(map.find(3), map.end());
        const PageCache & entry = map.find(3).pageCache();
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
        const PageCache & entry = map.find(3).pageCache();
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

TEST(PageEntryMap_test, Scan)
{
    PageCacheMap map;
    PageCache    p0entry{.file_id = 1, .level = 0, .checksum = 0x123};
    map.put(0, p0entry);
    PageCache p1entry{.file_id = 2, .level = 1, .checksum = 0x456};
    map.put(1, p1entry);
    map.ref(10, 0);
    map.ref(11, 1);

    std::set<PageId> page_ids;
    for (auto iter = map.begin(); iter != map.end(); ++iter)
    {
        page_ids.insert(iter.pageId());
        if (iter.pageId() % 10 == 0)
        {
            const PageCache & entry = iter.pageCache();
            EXPECT_EQ(entry.file_id, p0entry.file_id);
            EXPECT_EQ(entry.level, p0entry.level);
            EXPECT_EQ(entry.checksum, p0entry.checksum);
        }
        else if (iter.pageId() % 10 == 1)
        {
            const PageCache & entry = iter.pageCache();
            EXPECT_EQ(entry.file_id, p1entry.file_id);
            EXPECT_EQ(entry.level, p1entry.level);
            EXPECT_EQ(entry.checksum, p1entry.checksum);
        }
    }
    ASSERT_EQ(page_ids.size(), 4);

    // clear all mapping
    ASSERT_FALSE(map.empty());
    map.clear();
    ASSERT_TRUE(map.empty());
    page_ids.clear();
    for (auto iter = map.begin(); iter != map.end(); ++iter)
    {
        page_ids.insert(iter.pageId());
    }
    ASSERT_TRUE(page_ids.empty());
}

} // namespace tests
} // namespace DB
