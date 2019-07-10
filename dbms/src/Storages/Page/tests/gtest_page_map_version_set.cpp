#include <gtest/gtest.h>

#include <Storages/Page/PageEntryMapVersionSet.h>

namespace DB
{
namespace tests
{

TEST(PageStorageVersionSet_test, GcConcurrencyDelPage)
{
    PageId                 pid = 0;
    PageEntryMapVersionSet versions;
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

static void EXPECT_PagePos_LT(PageFileIdAndLevel p0, PageFileIdAndLevel p1)
{
    EXPECT_LT(p0, p1);
}

TEST(PageStorageVersionSet_test, GcPageMove)
{
    EXPECT_PagePos_LT({4, 0}, {5, 1});
    EXPECT_PagePos_LT({5, 0}, {5, 1});
    EXPECT_PagePos_LT({5, 1}, {6, 1});
    EXPECT_PagePos_LT({5, 2}, {6, 1});

    PageEntryMapVersionSet versions;

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

TEST(PageStorageVersionSet_test, GcConcurrencySetPage)
{
    const PageId           pid = 0;
    PageEntryMapVersionSet versions;


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

} // namespace tests
} // namespace DB
