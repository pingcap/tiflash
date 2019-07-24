#include "gtest/gtest.h"

#include <Storages/Page/PageEntryMapDeltaVersionSet.h>
#include <Storages/Page/PageEntryMapVersionSet.h>

namespace DB
{
namespace tests
{

class PageEntryMapDelta_test : public ::testing::Test
{
public:
    PageEntryMapDelta_test() : map(nullptr), versions() {}

protected:
    void SetUp() override
    {
        // Generate an empty PageEntryMap for each test
        auto                     snapshot = versions.getSnapshot();
        PageEntryMapDeltaBuilder builder(snapshot->version());
        map = snapshot->version()->tail;
    }

    void TearDown() override {}

    PageEntryMapDeltaVersionSet::VersionPtr map;

private:
    PageEntryMapDeltaVersionSet versions;
};

TEST_F(PageEntryMapDelta_test, Empty)
{
    ASSERT_TRUE(map->empty());
    ASSERT_EQ(map->maxId(), 0UL);

    // add some Pages, RefPages
    PageEntry p0entry{.file_id = 1, .level = 0, .checksum = 0x123};
    map->put(0, p0entry);
    map->ref(1, 0);
    ASSERT_FALSE(map->empty());
    ASSERT_EQ(map->maxId(), 1UL);

    map->clear();
    ASSERT_TRUE(map->empty());
    ASSERT_EQ(map->maxId(), 0UL);
}

TEST_F(PageEntryMapDelta_test, UpdatePageEntry)
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

TEST_F(PageEntryMapDelta_test, PutDel)
{
    PageEntry p0entry{.file_id = 1, .level = 0, .checksum = 0x123};
    map->put(0, p0entry);
    ASSERT_FALSE(map->empty());
    {
        ASSERT_NE(map->find(0), nullptr);
        const PageEntry & entry = map->at(0);
        EXPECT_EQ(entry.file_id, p0entry.file_id);
        EXPECT_EQ(entry.level, p0entry.level);
        EXPECT_EQ(entry.checksum, p0entry.checksum);
    }
    // add RefPage2 -> Page0
    map->ref(2, 0);
    ASSERT_FALSE(map->empty());
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
        auto entry = map->find(2);
        ASSERT_NE(entry, nullptr);
        EXPECT_EQ(entry->file_id, p0entry.file_id);
        EXPECT_EQ(entry->level, p0entry.level);
        EXPECT_EQ(entry->checksum, p0entry.checksum);
    }

    // remove RefPage2
    map->del(2);
    ASSERT_EQ(map->find(0), nullptr);
    ASSERT_EQ(map->find(2), nullptr);
}

TEST_F(PageEntryMapDelta_test, UpdateRefPageEntry)
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
    ASSERT_FALSE(map->empty());

    map->del(ref_id);
    ASSERT_EQ(map->find(ref_id), nullptr);
}

TEST_F(PageEntryMapDelta_test, AddRefToNonExistPage)
{
    ASSERT_TRUE(map->empty());
    PageEntry p0entry{.file_id = 1, .level = 0, .checksum = 0x123};
    map->put(0, p0entry);
    ASSERT_FALSE(map->empty());
    // if try to add ref
    map->ref(3, 2);
    auto [is_ref, ori_page_id] = map->isRefId(3);
    ASSERT_TRUE(is_ref);
    ASSERT_EQ(ori_page_id, 2UL);
}

TEST_F(PageEntryMapDelta_test, PutDuplicateRef)
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

TEST_F(PageEntryMapDelta_test, PutRefOnRef)
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
        auto entry = map->find(0);
        ASSERT_NE(entry, nullptr);
        EXPECT_EQ(entry->file_id, p0entry.file_id);
        EXPECT_EQ(entry->level, p0entry.level);
        EXPECT_EQ(entry->checksum, p0entry.checksum);
    }
    {
        // RefPage3 exist
        auto entry = map->find(3);
        ASSERT_NE(entry, nullptr);
        EXPECT_EQ(entry->file_id, p0entry.file_id);
        EXPECT_EQ(entry->level, p0entry.level);
        EXPECT_EQ(entry->checksum, p0entry.checksum);
    }

    // remove RefPage0
    map->del(0);
    // now RefPage0 is removed
    ASSERT_EQ(map->find(0), nullptr);
    ASSERT_EQ(map->find(2), nullptr);
    {
        // RefPage3 exist
        auto entry = map->find(3);
        ASSERT_NE(entry, nullptr);
        EXPECT_EQ(entry->file_id, p0entry.file_id);
        EXPECT_EQ(entry->level, p0entry.level);
        EXPECT_EQ(entry->checksum, p0entry.checksum);
    }

    // remove RefPage3
    map->del(3);
    // now RefPage3 is removed
    ASSERT_EQ(map->find(3), nullptr);
    ASSERT_EQ(map->find(0), nullptr);
    ASSERT_EQ(map->find(2), nullptr);
}

TEST_F(PageEntryMapDelta_test, ReBindRef)
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

class PageEntryMapDeltaBuilder_test : public ::testing::Test
{
public:
    void SetUp() override
    {
        base  = PageEntryMapBase::createBase();
        delta = PageEntryMapBase::createDelta();
    }

    void TearDown() override {}

protected:
    std::shared_ptr<PageEntryMapBase> base;
    std::shared_ptr<PageEntryMapBase> delta;
};

TEST_F(PageEntryMapDeltaBuilder_test, DeltaAddRef)
{
    base->put(0, PageEntry{.checksum = 0x123});
    base->ref(2, 0);

    delta->ref(3, 2);

    base = PageEntryMapDeltaBuilder::compactDeltaAndBase(base, delta);

    auto entry = base->find(3);
    ASSERT_NE(entry, nullptr);
    ASSERT_EQ(entry->checksum, 0x123UL);
}

TEST_F(PageEntryMapDeltaBuilder_test, DeltaPutThenDel)
{
    delta->put(2, PageEntry{.checksum = 0x123});
    delta->ref(3, 2);
    delta->del(2);

    base = PageEntryMapDeltaBuilder::compactDeltaAndBase(base, delta);

    auto entry2 = base->find(2);
    ASSERT_EQ(entry2, nullptr);

    auto entry3 = base->find(3);
    ASSERT_NE(entry3, nullptr);
    ASSERT_EQ(entry3->checksum, 0x123UL);
}

TEST_F(PageEntryMapDeltaBuilder_test, DeltaDelThenPut)
{
    base->put(2, PageEntry{.checksum = 0x1});

    delta->del(2);
    delta->put(2, PageEntry{.checksum = 0x123});

    base = PageEntryMapDeltaBuilder::compactDeltaAndBase(base, delta);

    auto entry = base->find(2);
    ASSERT_NE(entry, nullptr);
    ASSERT_EQ(entry->checksum, 0x123UL);
}

} // namespace tests
} // namespace DB
