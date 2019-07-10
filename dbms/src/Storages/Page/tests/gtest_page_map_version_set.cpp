#include <gtest/gtest-typed-test.h>
#include <gtest/gtest.h>

#include <Storages/Page/PageEntryMapVersionSet.h>

namespace DB
{
namespace tests
{

template <typename T>
class PageMapVersionSet_test : public ::testing::Test
{
};

TYPED_TEST_CASE_P(PageMapVersionSet_test);

TYPED_TEST_P(PageMapVersionSet_test, GcConcurrencyDelPage)
{
    PageId    pid = 0;
    TypeParam versions;
    // Page0 is in PageFile{2, 0} at first
    {
        PageEntriesEdit init_edit;
        init_edit.put(pid, PageEntry{.file_id = 2, .level = 1});
        versions.apply(init_edit);
    }

    // gc try to move Page0 -> PageFile{5, 1}, but is interrupt by write thread before gcApply
    PageEntriesEdit gc_edit;
    gc_edit.put(pid, PageEntry{.file_id = 5, .level = 1});

    {
        // write thread del Page0 before gc thread get unique_lock of `read_mutex`
        PageEntriesEdit write_edit;
        write_edit.del(0);
        versions.apply(write_edit);
    }

    // gc continue
    versions.gcApply(gc_edit);

    // Page0 don't update to page_map
    auto       snapshot = versions.getSnapshot();
    const auto iter     = snapshot->version()->find(pid);
    ASSERT_EQ(iter, snapshot->version()->end());
}

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunneeded-internal-declaration"
static void EXPECT_PagePos_LT(PageFileIdAndLevel p0, PageFileIdAndLevel p1)
{
    EXPECT_LT(p0, p1);
}
#pragma clang diagnostic pop

TYPED_TEST_P(PageMapVersionSet_test, GcPageMove)
{
    EXPECT_PagePos_LT({4, 0}, {5, 1});
    EXPECT_PagePos_LT({5, 0}, {5, 1});
    EXPECT_PagePos_LT({5, 1}, {6, 1});
    EXPECT_PagePos_LT({5, 2}, {6, 1});

    TypeParam versions;

    const PageId pid = 0;
    const PageId ref_pid = 1;
    // old Page0 is in PageFile{5, 0}
    PageEntriesEdit init_edit;
    init_edit.put(pid, PageEntry{.file_id = 5, .level = 0});
    init_edit.ref(ref_pid, pid);
    versions.apply(init_edit);

    // gc move Page0 -> PageFile{5,1}
    PageEntriesEdit gc_edit;
    gc_edit.put(pid,
                PageEntry{
                    .file_id = 5,
                    .level   = 1,
                });
    versions.gcApply(gc_edit);

    // Page get updated
    auto            snapshot = versions.getSnapshot();
    PageEntry entry    = snapshot->version()->at(pid);
    ASSERT_TRUE(entry.isValid());
    ASSERT_EQ(entry.file_id, 5ULL);
    ASSERT_EQ(entry.level, 1U);
    ASSERT_EQ(entry.ref, 2u);

    // RefPage got update at the same time
    entry = snapshot->version()->at(ref_pid);
    ASSERT_TRUE(entry.isValid());
    ASSERT_EQ(entry.file_id, 5u);
    ASSERT_EQ(entry.level, 1u);
    ASSERT_EQ(entry.ref, 2u);
}

TYPED_TEST_P(PageMapVersionSet_test, GcConcurrencySetPage)
{
    const PageId pid = 0;
    TypeParam    versions;


    // gc move Page0 -> PageFile{5,1}
    PageEntriesEdit gc_edit;
    gc_edit.put(pid,
                PageEntry{
                    .file_id = 5,
                    .level   = 1,
                });

    {
        // write thread insert newer Page0 before gc thread get unique_lock on `read_mutex`
        PageEntriesEdit write_edit;
        write_edit.put(pid, PageEntry{.file_id = 6, .level = 0});
        versions.apply(write_edit);
    }

    // gc continue
    versions.gcApply(gc_edit);

    // read
    auto            snapshot = versions.getSnapshot();
    const PageEntry entry    = snapshot->version()->at(pid);
    ASSERT_TRUE(entry.isValid());
    ASSERT_EQ(entry.file_id, 6ULL);
    ASSERT_EQ(entry.level, 0U);
}

TYPED_TEST_P(PageMapVersionSet_test, Snapshot)
{
    TypeParam versions;
    ASSERT_EQ(versions.size(), 1UL);
    {
        PageEntriesEdit init_edit;
        init_edit.put(0, PageEntry{.checksum = 0x123});
        init_edit.put(1, PageEntry{.checksum = 0x1234});
        versions.apply(init_edit);
        ASSERT_EQ(versions.size(), 1UL);
    }

    auto s1 = versions.getSnapshot();
    {
        PageEntriesEdit edit;
        edit.put(0, PageEntry{.checksum = 0x456});
        edit.del(1);
        versions.apply(edit);
    }
    ASSERT_EQ(versions.size(), 2UL); // previous version is hold by `s1`, list size grow to 2

    auto s2 = versions.getSnapshot();
    ASSERT_NE(s2->version()->find(0), s2->version()->end());
    PageEntry p0 = s2->version()->at(0);
    ASSERT_EQ(p0.checksum, 0x456UL); // entry is updated in snapshot 2
    ASSERT_EQ(s2->version()->find(1), s2->version()->end());
}

REGISTER_TYPED_TEST_CASE_P(PageMapVersionSet_test, GcConcurrencyDelPage, GcPageMove, GcConcurrencySetPage, Snapshot);

INSTANTIATE_TYPED_TEST_CASE_P(VersionSet, PageMapVersionSet_test, PageEntryMapVersionSet);
//INSTANTIATE_TYPED_TEST_CASE_P(VersionSetDelta, PageMapVersionSet_test, PageEntryMapDeltaVersionSet);


} // namespace tests
} // namespace DB
