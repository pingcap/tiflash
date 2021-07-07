#include <type_traits>

#define protected public
#include <Storages/Page/mvcc/VersionSetWithDelta.h>
#undef protected

#include <Poco/AutoPtr.h>
#include <Storages/Page/VersionSet/PageEntriesVersionSet.h>
#include <Storages/Page/VersionSet/PageEntriesVersionSetWithDelta.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace tests
{

template <typename T>
class PageMapVersionSet_test : public ::testing::Test
{
public:
    PageMapVersionSet_test() : log(&Poco::Logger::get("PageMapVersionSet_test")) {}

    static void SetUpTestCase() {}

    void SetUp() override
    {
        config_.compact_hint_delta_entries   = 1;
        config_.compact_hint_delta_deletions = 1;
    }

protected:
    ::DB::MVCC::VersionSetConfig config_;
    Poco::Logger *               log;
};

TYPED_TEST_CASE_P(PageMapVersionSet_test);

TYPED_TEST_P(PageMapVersionSet_test, ApplyEdit)
{
    TypeParam versions("vset_test", this->config_, this->log);
    LOG_TRACE(&Logger::root(), "init      :" + versions.toDebugString());
    {
        PageEntriesEdit edit;
        PageEntry       e;
        e.checksum = 0x123;
        edit.put(0, e);
        versions.apply(edit);
    }
    // VersionSet, new version generate && old version removed at the same time
    // VersionSetWithDelta, delta version merged
    LOG_TRACE(&Logger::root(), "apply    A:" + versions.toDebugString());
    EXPECT_EQ(versions.size(), 1UL);
    {
        PageEntriesEdit edit;
        PageEntry       e;
        e.checksum = 0x456;
        edit.put(1, e);
        edit.ref(2, 0);
        versions.apply(edit);
    }
    LOG_TRACE(&Logger::root(), "apply    B:" + versions.toDebugString());
    auto s2 = versions.getSnapshot();
    EXPECT_EQ(versions.size(), 1UL);
    auto entry = s2->version()->at(0);
    ASSERT_EQ(entry.checksum, 0x123UL);
    ASSERT_EQ(entry.ref, 2UL);
    auto entry2 = s2->version()->at(1);
    ASSERT_EQ(entry2.checksum, 0x456UL);
    ASSERT_EQ(entry2.ref, 1UL);
    s2.reset(); // release snapshot
    EXPECT_EQ(versions.size(), 1UL);
}

/// Generate two different snapshot(s1, s2) with apply new edits.
/// s2 released first, then release s1
TYPED_TEST_P(PageMapVersionSet_test, ApplyEditWithReadLock)
{
    TypeParam versions("vset_test", this->config_, this->log);
    auto      s1 = versions.getSnapshot();
    EXPECT_EQ(versions.size(), 1UL);
    LOG_TRACE(&Logger::root(), "snapshot 1:" + versions.toDebugString());
    {
        PageEntriesEdit edit;
        PageEntry       e;
        e.checksum = 0x123;
        edit.put(0, e);
        versions.apply(edit);
    }
    EXPECT_EQ(versions.size(), 2UL); // former node is hold by s1, append new version
    LOG_TRACE(&Logger::root(), "apply    B:" + versions.toDebugString());

    // Get snapshot for checking edit is success
    auto s2 = versions.getSnapshot();
    LOG_TRACE(&Logger::root(), "snapshot 2:" + versions.toDebugString());
    auto entry = s2->version()->at(0);
    ASSERT_EQ(entry.checksum, 0x123UL);

    // Release snapshot2
    s2.reset();
    LOG_TRACE(&Logger::root(), "rel snap 2:" + versions.toDebugString());
    /// For VersionSet, size is 2 since A is still hold by s1
    /// For VersionDeltaSet, size is 1 since we do a compaction on delta
    if constexpr (std::is_same_v<TypeParam, PageEntriesVersionSet>)
        EXPECT_EQ(versions.size(), 2UL);
    else
        EXPECT_EQ(versions.size(), 1UL);

    s1.reset();
    LOG_TRACE(&Logger::root(), "rel snap 1:" + versions.toDebugString());
    // VersionSet, old version removed from version set
    // VersionSetWithDelta, delta version merged
    EXPECT_EQ(versions.size(), 1UL);

    // Ensure that after old snapshot released, new snapshot get the same content
    auto s3 = versions.getSnapshot();
    entry   = s3->version()->at(0);
    ASSERT_EQ(entry.checksum, 0x123UL);
    s3.reset();

    {
        PageEntriesEdit edit;
        PageEntry       e;
        e.checksum = 0x456;
        edit.put(0, e);
        versions.apply(edit);
    }
    LOG_TRACE(&Logger::root(), "apply    C:" + versions.toDebugString());
    // VersionSet, new version gen and old version remove at the same time
    // VersionSetWithDelta, C merge to delta
    EXPECT_EQ(versions.size(), 1UL);
    auto s4 = versions.getSnapshot();
    entry   = s4->version()->at(0);
    ASSERT_EQ(entry.checksum, 0x456UL);
}

/// Generate two different snapshot(s1, s2) with apply new edits.
/// s1 released first, then release s2
TYPED_TEST_P(PageMapVersionSet_test, ApplyEditWithReadLock2)
{
    TypeParam versions("vset_test", this->config_, this->log);
    auto      s1 = versions.getSnapshot();
    LOG_TRACE(&Logger::root(), "snapshot 1:" + versions.toDebugString());
    PageEntriesEdit edit;
    PageEntry       e;
    e.checksum = 0x123;
    edit.put(0, e);
    versions.apply(edit);
    LOG_TRACE(&Logger::root(), "apply    B:" + versions.toDebugString());
    auto s2    = versions.getSnapshot();
    auto entry = s2->version()->at(0);
    ASSERT_EQ(entry.checksum, 0x123UL);

    s1.reset();
    LOG_TRACE(&Logger::root(), "rel snap 1:" + versions.toDebugString());
    // VersionSet, size decrease to 1 when s1 release
    // VersionSetWithDelta, size is 2 since we can not do a compaction on delta
    if constexpr (std::is_same_v<TypeParam, PageEntriesVersionSet>)
        EXPECT_EQ(versions.size(), 1UL);
    else
        EXPECT_EQ(versions.size(), 2UL);

    s2.reset();
    LOG_TRACE(&Logger::root(), "rel snap 2:" + versions.toDebugString());
    EXPECT_EQ(versions.size(), 1UL);
}

/// Generate two different snapshot(s1, s2) with apply new edits.
/// s1 released first, then release s2
TYPED_TEST_P(PageMapVersionSet_test, ApplyEditWithReadLock3)
{
    TypeParam versions("vset_test", this->config_, this->log);
    auto      s1 = versions.getSnapshot();
    LOG_TRACE(&Logger::root(), "snapshot 1:" + versions.toDebugString());
    {
        PageEntriesEdit edit;
        PageEntry       e;
        e.checksum = 0x123;
        edit.put(0, e);
        versions.apply(edit);
    }
    LOG_TRACE(&Logger::root(), "apply    B:" + versions.toDebugString());
    auto s2    = versions.getSnapshot();
    auto entry = s2->version()->at(0);
    ASSERT_EQ(entry.checksum, 0x123UL);

    {
        PageEntriesEdit edit;
        PageEntry       e;
        e.checksum = 0xff;
        edit.put(1, e);
        versions.apply(edit);
    }
    LOG_TRACE(&Logger::root(), "apply    C:" + versions.toDebugString());
    auto s3 = versions.getSnapshot();
    entry   = s3->version()->at(1);
    ASSERT_EQ(entry.checksum, 0xFFUL);

    s1.reset();
    LOG_TRACE(&Logger::root(), "rel snap 1:" + versions.toDebugString());
    // VersionSet, size decrease to 2 when s1 release
    // VersionSetWithDelta, size is 3 since we can not do a compaction on delta
    if constexpr (std::is_same_v<TypeParam, PageEntriesVersionSet>)
        EXPECT_EQ(versions.size(), 2UL);
    else
        EXPECT_EQ(versions.size(), 3UL);

    s2.reset();
    LOG_TRACE(&Logger::root(), "rel snap 2:" + versions.toDebugString());
    if constexpr (std::is_same_v<TypeParam, PageEntriesVersionSet>)
        EXPECT_EQ(versions.size(), 1UL);
    else
        EXPECT_EQ(versions.size(), 2UL);

    s3.reset();
    LOG_TRACE(&Logger::root(), "rel snap 3:" + versions.toDebugString());
    EXPECT_EQ(versions.size(), 1UL);
}

namespace
{

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-function"

std::set<PageId> getNormalPageIDs(const PageEntriesVersionSet::SnapshotPtr & s)
{
    std::set<PageId> ids;
    for (auto iter = s->version()->pages_cbegin(); iter != s->version()->pages_cend(); iter++)
        ids.insert(iter->first);
    return ids;
}

std::set<PageId> getNormalPageIDs(const PageEntriesVersionSetWithDelta::SnapshotPtr & s)
{
    return s->version()->validNormalPageIds();
}

#pragma clang diagnostic pop

} // namespace

TYPED_TEST_P(PageMapVersionSet_test, Restore)
{
    TypeParam versions("vset_test", this->config_, this->log);
    if constexpr (std::is_same_v<TypeParam, PageEntriesVersionSet>)
    {
        // For PageEntriesVersionSet, we need a builder
        auto s1 = versions.getSnapshot();

        typename TypeParam::BuilderType builder(s1->version(), true, &Poco::Logger::root());
        {
            PageEntriesEdit edit;
            PageEntry       e;
            e.checksum = 1;
            edit.put(1, e);
            edit.del(1);
            e.checksum = 2;
            edit.put(2, e);
            e.checksum = 3;
            edit.put(3, e);
            builder.apply(edit);
        }
        {
            PageEntriesEdit edit;
            edit.del(2);
            builder.apply(edit);
        }
        versions.restore(builder.build());
    }
    else
    {
        // For PageEntriesVersionSetWithDelta, we directly apply edit to versions
        {
            PageEntriesEdit edit;
            PageEntry       e;
            e.checksum = 1;
            edit.put(1, e);
            edit.del(1);
            e.checksum = 2;
            edit.put(2, e);
            e.checksum = 3;
            edit.put(3, e);
            versions.apply(edit);
        }
        {
            PageEntriesEdit edit;
            edit.del(2);
            versions.apply(edit);
        }
    }

    auto s     = versions.getSnapshot();
    auto entry = s->version()->find(1);
    ASSERT_EQ(entry, std::nullopt);
    auto entry2 = s->version()->find(2);
    ASSERT_EQ(entry2, std::nullopt);
    auto entry3 = s->version()->find(3);
    ASSERT_NE(entry3, std::nullopt);
    ASSERT_EQ(entry3->checksum, 3UL);

    std::set<PageId> valid_normal_page_ids = getNormalPageIDs(s);
    ASSERT_FALSE(valid_normal_page_ids.count(1) > 0);
    ASSERT_FALSE(valid_normal_page_ids.count(2) > 0);
    ASSERT_TRUE(valid_normal_page_ids.count(3) > 0);
}

TYPED_TEST_P(PageMapVersionSet_test, PutOrDelRefPage)
{
    TypeParam versions("vset_test", this->config_, this->log);
    {
        PageEntriesEdit edit;
        PageEntry       e;
        e.checksum = 0xf;
        edit.put(2, e);
        versions.apply(edit);
    }
    auto s1 = versions.getSnapshot();
    ASSERT_EQ(s1->version()->at(2).checksum, 0xfUL);

    //  Put RefPage3 -> Page2
    {
        PageEntriesEdit edit;
        edit.ref(3, 2);
        versions.apply(edit);
    }
    auto s2                      = versions.getSnapshot();
    auto ensure_snapshot2_status = [&s2]() {
        // Check the ref-count
        auto entry3 = s2->version()->at(3);
        ASSERT_EQ(entry3.checksum, 0xfUL);
        ASSERT_EQ(entry3.ref, 2UL);

        auto entry2 = s2->version()->at(2);
        ASSERT_EQ(entry2.checksum, 0xfUL);
        ASSERT_EQ(entry2.ref, 2UL);

        auto normal_entry2 = s2->version()->findNormalPageEntry(2);
        ASSERT_TRUE(normal_entry2);
        ASSERT_EQ(normal_entry2->checksum, 0xfUL);
        ASSERT_EQ(normal_entry2->ref, 2UL);

        std::set<PageId> valid_normal_page_ids = getNormalPageIDs(s2);
        ASSERT_TRUE(valid_normal_page_ids.count(2) > 0);
        ASSERT_FALSE(valid_normal_page_ids.count(3) > 0);
    };
    ensure_snapshot2_status();

    // Del Page2
    {
        PageEntriesEdit edit;
        edit.del(2);
        versions.apply(edit);
    }
    auto s3                      = versions.getSnapshot();
    auto ensure_snapshot3_status = [&s3]() {
        // Check that NormalPage2's ref-count is decreased.
        auto entry3 = s3->version()->at(3);
        ASSERT_EQ(entry3.checksum, 0xfUL);
        ASSERT_EQ(entry3.ref, 1UL);

        auto entry2 = s3->version()->find(2);
        ASSERT_FALSE(entry2);

        auto normal_entry2 = s3->version()->findNormalPageEntry(2);
        ASSERT_TRUE(normal_entry2);
        ASSERT_EQ(normal_entry2->checksum, 0xfUL);
        ASSERT_EQ(normal_entry2->ref, 1UL);

        std::set<PageId> valid_normal_page_ids = getNormalPageIDs(s3);
        ASSERT_TRUE(valid_normal_page_ids.count(2) > 0);
        ASSERT_FALSE(valid_normal_page_ids.count(3) > 0);
    };
    ensure_snapshot3_status();

    // Del RefPage3
    {
        PageEntriesEdit edit;
        edit.del(3);
        versions.apply(edit);
    }
    auto s4                      = versions.getSnapshot();
    auto ensure_snapshot4_status = [&s4]() {
        auto entry3 = s4->version()->find(3);
        ASSERT_FALSE(entry3);

        auto entry2 = s4->version()->find(2);
        ASSERT_FALSE(entry2);

        auto normal_entry2 = s4->version()->findNormalPageEntry(2);
        if constexpr (std::is_same_v<TypeParam, PageEntriesVersionSet>)
        {
            // For PageEntriesVersionSet, we delete the normal page
            ASSERT_FALSE(normal_entry2);
        }
        else
        {
            // For PageEntriesVersionSetWithDelta, a tombstone is left.
            ASSERT_TRUE(normal_entry2);
            ASSERT_EQ(normal_entry2->checksum, 0xfUL);
            ASSERT_TRUE(normal_entry2->isTombstone());
        }

        // We can not get 2 or 3 as normal page
        std::set<PageId> valid_normal_page_ids = getNormalPageIDs(s4);
        ASSERT_FALSE(valid_normal_page_ids.count(2) > 0);
        ASSERT_FALSE(valid_normal_page_ids.count(3) > 0);
    };
    ensure_snapshot4_status();

    // Test if one snapshot removed, other snapshot is not affected.
    s3.reset();
    ensure_snapshot4_status();
    ensure_snapshot2_status();

    s2.reset();
    ensure_snapshot4_status();
}

TYPED_TEST_P(PageMapVersionSet_test, IdempotentDel)
{
    TypeParam versions("vset_test", this->config_, this->log);
    {
        PageEntriesEdit edit;
        PageEntry       e;
        e.checksum = 0xf;
        edit.put(2, e);
        edit.ref(3, 2);
        versions.apply(edit);
    }
    auto s1 = versions.getSnapshot();
    ASSERT_EQ(s1->version()->at(2).checksum, 0xfUL);

    // Del Page2
    {
        PageEntriesEdit edit;
        edit.del(2);
        versions.apply(edit);
    }
    auto s2 = versions.getSnapshot();
    {
        auto ref_entry = s2->version()->at(3);
        ASSERT_EQ(ref_entry.checksum, 0xfUL);
        auto normal_entry = s2->version()->findNormalPageEntry(2);
        ASSERT_TRUE(normal_entry);
        ASSERT_EQ(normal_entry->ref, 1UL);
        ASSERT_EQ(ref_entry.checksum, normal_entry->checksum);
    }

    // Del Page2 again, should be idempotent.
    {
        PageEntriesEdit edit;
        edit.del(2);
        versions.apply(edit);
    }
    auto s3 = versions.getSnapshot();
    {
        auto ref_entry = s3->version()->at(3);
        ASSERT_EQ(ref_entry.checksum, 0xfUL);
        auto normal_entry = s3->version()->findNormalPageEntry(2);
        ASSERT_TRUE(normal_entry);
        ASSERT_EQ(normal_entry->ref, 1UL);
        ASSERT_EQ(ref_entry.checksum, normal_entry->checksum);
    }
}

TYPED_TEST_P(PageMapVersionSet_test, GcConcurrencyDelPage)
{
    PageId    pid = 0;
    TypeParam versions("vset_test", this->config_, this->log);
    // Page0 is in PageFile{2, 0} at first
    {
        PageEntriesEdit init_edit;
        PageEntry       e;
        e.file_id = 2;
        e.level   = 1;
        init_edit.put(pid, e);
        versions.apply(init_edit);
    }

    // gc try to move Page0 -> PageFile{5, 1}, but is interrupt by write thread before gcApply
    PageEntriesEdit gc_edit;
    PageEntry       e;
    e.file_id = 5;
    e.level   = 1;
    gc_edit.upsertPage(pid, e);

    {
        // write thread del Page0 before gc thread get unique_lock of `read_mutex`
        PageEntriesEdit write_edit;
        write_edit.del(0);
        versions.apply(write_edit);
    }

    // gc continue
    versions.gcApply(gc_edit);

    // Page0 don't update to page_map
    auto snapshot = versions.getSnapshot();
    auto entry    = snapshot->version()->find(pid);
    ASSERT_EQ(entry, std::nullopt);
}

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunneeded-internal-declaration"
static void              EXPECT_PagePos_LT(PageFileIdAndLevel p0, PageFileIdAndLevel p1)
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

    TypeParam versions("vset_test", this->config_, this->log);

    const PageId pid     = 0;
    const PageId ref_pid = 1;
    // old Page0 is in PageFile{5, 0}
    {
        PageEntriesEdit init_edit;
        PageEntry       e;
        e.file_id = 5;
        e.level   = 0;
        init_edit.put(pid, e);
        init_edit.ref(ref_pid, pid);
        versions.apply(init_edit);
    }

    // gc move Page0 -> PageFile{5,1}
    PageEntriesEdit gc_edit;
    {
        PageEntry e;
        e.file_id = 5;
        e.level   = 1;
        gc_edit.upsertPage(pid, e);
        versions.gcApply(gc_edit);
    }

    // Page get updated
    auto      snapshot = versions.getSnapshot();
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
    TypeParam    versions("vset_test", this->config_, this->log);


    // gc move Page0 -> PageFile{5,1}
    PageEntriesEdit gc_edit;
    {
        PageEntry e;
        e.file_id = 5;
        e.level   = 1;
        gc_edit.upsertPage(pid, e);
    }

    {
        // write thread insert newer Page0 before gc thread get unique_lock on `read_mutex`
        PageEntriesEdit write_edit;
        PageEntry       e;
        e.file_id = 6;
        e.level   = 0;
        write_edit.put(pid, e);
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

TYPED_TEST_P(PageMapVersionSet_test, UpdateOnRefPage)
{
    TypeParam versions("vset_test", this->config_, this->log);
    {
        PageEntriesEdit edit;
        PageEntry       e;
        e.checksum = 0xf;
        edit.put(2, e);
        edit.ref(3, 2);
        versions.apply(edit);
    }
    auto s1 = versions.getSnapshot();
    ASSERT_EQ(s1->version()->at(2).checksum, 0xfUL);
    ASSERT_EQ(s1->version()->at(3).checksum, 0xfUL);

    // Update RefPage3, both Page2 and RefPage3 got updated.
    {
        PageEntriesEdit edit;
        PageEntry       e;
        e.checksum = 0xff;
        edit.put(3, e);
        versions.apply(edit);
    }
    auto s2 = versions.getSnapshot();
    ASSERT_EQ(s2->version()->at(3).checksum, 0xffUL);
    ASSERT_EQ(s2->version()->at(2).checksum, 0xffUL);
    s2.reset();
    s1.reset();
    auto s3 = versions.getSnapshot();
    ASSERT_EQ(s3->version()->at(3).checksum, 0xffUL);
    ASSERT_EQ(s3->version()->at(2).checksum, 0xffUL);
    //s3.reset();

    // Del Page2, RefPage3 still there
    {
        PageEntriesEdit edit;
        edit.del(2);
        versions.apply(edit);
    }
    auto s4 = versions.getSnapshot();
    ASSERT_EQ(s4->version()->find(2), std::nullopt);
    ASSERT_EQ(s4->version()->at(3).checksum, 0xffUL);
    s4.reset();
    ASSERT_EQ(s3->version()->at(2).checksum, 0xffUL);
    ASSERT_EQ(s3->version()->at(3).checksum, 0xffUL);
    s3.reset();

    auto s5 = versions.getSnapshot();
    ASSERT_EQ(s5->version()->find(2), std::nullopt);
    ASSERT_EQ(s5->version()->at(3).checksum, 0xffUL);
}

TYPED_TEST_P(PageMapVersionSet_test, UpdateOnRefPage2)
{
    TypeParam versions("vset_test", this->config_, this->log);
    {
        PageEntriesEdit edit;
        PageEntry       e;
        e.checksum = 0xf;
        edit.put(2, e);
        edit.ref(3, 2);
        edit.del(2);
        versions.apply(edit);
    }
    auto s1 = versions.getSnapshot();
    ASSERT_EQ(s1->version()->find(2), std::nullopt);
    ASSERT_EQ(s1->version()->at(3).checksum, 0xfUL);

    {
        PageEntriesEdit edit;
        PageEntry       e;
        e.checksum = 0x9;
        edit.put(2, e);
        edit.del(2);
        versions.apply(edit);
    }
    auto s2 = versions.getSnapshot();
    ASSERT_EQ(s2->version()->find(2), std::nullopt);
    ASSERT_EQ(s2->version()->at(3).checksum, 0x9UL);
}

TYPED_TEST_P(PageMapVersionSet_test, IsRefId)
{
    TypeParam versions("vset_test", this->config_, this->log);
    {
        PageEntriesEdit edit;
        PageEntry       e;
        e.checksum = 0xf;
        edit.put(1, e);
        edit.ref(2, 1);
        versions.apply(edit);
    }
    auto   s1 = versions.getSnapshot();
    bool   is_ref;
    PageId normal_page_id;
    std::tie(is_ref, normal_page_id) = s1->version()->isRefId(2);
    ASSERT_TRUE(is_ref);
    ASSERT_EQ(normal_page_id, 1UL);

    {
        PageEntriesEdit edit;
        edit.del(2);
        versions.apply(edit);
    }
    auto s2                          = versions.getSnapshot();
    std::tie(is_ref, normal_page_id) = s2->version()->isRefId(2);
    ASSERT_FALSE(is_ref);
}

TYPED_TEST_P(PageMapVersionSet_test, Snapshot)
{
    TypeParam versions("vset_test", this->config_, this->log);
    ASSERT_EQ(versions.size(), 1UL);
    {
        PageEntriesEdit init_edit;
        PageEntry       e;
        e.checksum = 0x123;
        init_edit.put(0, e);
        e.checksum = 0x1234;
        init_edit.put(1, e);
        versions.apply(init_edit);
        ASSERT_EQ(versions.size(), 1UL);
    }

    auto s1 = versions.getSnapshot();

    // Apply edit that
    // * update Page 0 with checksum = 0x456
    // * delete Page 1
    {
        PageEntriesEdit edit;
        PageEntry       e;
        e.checksum = 0x456;
        edit.put(0, e);
        edit.del(1);
        versions.apply(edit);
    }
    ASSERT_EQ(versions.size(), 2UL); // previous version is hold by `s1`, list size grow to 2

    // check that snapshot s1 is not effected by later edits.
    ASSERT_EQ(s1->version()->at(0).checksum, 0x123UL);
    ASSERT_EQ(s1->version()->at(1).checksum, 0x1234UL);

    auto s2 = versions.getSnapshot();
    auto p0 = s2->version()->find(0);
    ASSERT_NE(p0, std::nullopt);
    ASSERT_EQ(p0->checksum, 0x456UL); // entry is updated in snapshot 2
    auto p1 = s2->version()->find(1);
    ASSERT_EQ(p1, std::nullopt);
}

namespace
{

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-function"
String                   liveFilesToString(const std::set<PageFileIdAndLevel> & files)
{
    std::stringstream ss;
    bool              is_first = true;
    for (const auto & file : files)
    {
        if (!is_first)
        {
            ss << ",";
        }
        ss << "{" << file.first << "," << file.second << "}";
        is_first = false;
    }
    return ss.str();
}
String livePagesToString(const std::set<PageId> & ids)
{
    std::stringstream ss;
    bool              is_first = true;
    for (const auto & page_id : ids)
    {
        if (!is_first)
            ss << ",";
        ss << page_id;
        is_first = false;
    }
    return ss.str();
}
#pragma clang diagnostic pop

} // namespace

TYPED_TEST_P(PageMapVersionSet_test, LiveFiles)
{
    TypeParam versions("vset_test", this->config_, this->log);

    {
        PageEntriesEdit edit;
        PageEntry       e;
        e.file_id = 1;
        e.level   = 0;
        edit.put(0, e);
        e.file_id = 2;
        edit.put(1, e);
        e.file_id = 3;
        edit.put(2, e);
        versions.apply(edit);
    }
    auto s1 = versions.getSnapshot();
    {
        PageEntriesEdit edit;
        edit.del(0);
        PageEntry e;
        e.file_id = 3;
        e.level   = 1;
        edit.put(3, e);
        versions.apply(edit);
    }
    auto s2 = versions.getSnapshot();
    {
        PageEntriesEdit edit;
        edit.del(3);
        versions.apply(edit);
    }
    auto s3 = versions.getSnapshot();
    s3.reset(); // do compact on version-list, and
    //std::cerr << "s3 reseted." << std::endl;
    auto [livefiles, live_normal_pages] = versions.listAllLiveFiles(versions.acquireForLock());
    ASSERT_EQ(livefiles.size(), 4UL) << liveFilesToString(livefiles);
    ASSERT_EQ(livefiles.count(std::make_pair(1, 0)), 1UL); // hold by s1
    ASSERT_EQ(livefiles.count(std::make_pair(2, 0)), 1UL); // hold by current, s1, s2
    ASSERT_EQ(livefiles.count(std::make_pair(3, 0)), 1UL); // hold by current, s1, s2
    ASSERT_EQ(livefiles.count(std::make_pair(3, 1)), 1UL); // hold by s2
    ASSERT_EQ(live_normal_pages.size(), 4UL) << livePagesToString(live_normal_pages);
    EXPECT_GT(live_normal_pages.count(0), 0UL);
    EXPECT_GT(live_normal_pages.count(1), 0UL);
    EXPECT_GT(live_normal_pages.count(2), 0UL);
    EXPECT_GT(live_normal_pages.count(3), 0UL);

    s2.reset();
    //std::cerr << "s2 reseted." << std::endl;
    std::tie(livefiles, live_normal_pages) = versions.listAllLiveFiles(versions.acquireForLock());
    ASSERT_EQ(livefiles.size(), 3UL) << liveFilesToString(livefiles);
    ASSERT_EQ(livefiles.count(std::make_pair(1, 0)), 1UL); // hold by s1
    ASSERT_EQ(livefiles.count(std::make_pair(2, 0)), 1UL); // hold by current, s1
    ASSERT_EQ(livefiles.count(std::make_pair(3, 0)), 1UL); // hold by current, s1
    ASSERT_EQ(live_normal_pages.size(), 3UL) << livePagesToString(live_normal_pages);
    EXPECT_GT(live_normal_pages.count(0), 0UL);
    EXPECT_GT(live_normal_pages.count(1), 0UL);
    EXPECT_GT(live_normal_pages.count(2), 0UL);

    s1.reset();
    //std::cerr << "s1 reseted." << std::endl;
    std::tie(livefiles, live_normal_pages) = versions.listAllLiveFiles(versions.acquireForLock());
    ASSERT_EQ(livefiles.size(), 2UL) << liveFilesToString(livefiles);
    ASSERT_EQ(livefiles.count(std::make_pair(2, 0)), 1UL); // hold by current
    ASSERT_EQ(livefiles.count(std::make_pair(3, 0)), 1UL); // hold by current
    ASSERT_EQ(live_normal_pages.size(), 2UL) << livePagesToString(live_normal_pages);
    EXPECT_GT(live_normal_pages.count(1), 0UL);
    EXPECT_GT(live_normal_pages.count(2), 0UL);
}

TYPED_TEST_P(PageMapVersionSet_test, PutOnTombstonePageEntry)
{
    if constexpr (std::is_same_v<TypeParam, PageEntriesVersionSetWithDelta>)
    {
        const PageId page_id = 2;

        ::DB::MVCC::VersionSetConfig config;
        TypeParam                    versions("vset_test", config, this->log);
        {
            // First we put a page and add read lock by acquiring a snapshot(s1)
            PageEntriesEdit edit;
            PageEntry       e;
            e.checksum = 0xf;
            edit.put(page_id, e);
            versions.apply(edit);
        }
        auto s1 = versions.getSnapshot();

        {
            // Then delete that page, because there is read lock on previouse version,
            // we need to put tombstone on new version
            PageEntriesEdit edit;
            edit.del(page_id);
            versions.apply(edit);
            // Now there is a tombstone on current version.
            auto s2    = versions.getSnapshot();
            auto entry = s2->version()->find(page_id);
            ASSERT_FALSE(entry); // Get tombstone by find return nullopt
            auto normal_entry = s2->version()->findNormalPageEntry(page_id);
            ASSERT_TRUE(normal_entry);
            ASSERT_TRUE(normal_entry->isTombstone());
            ASSERT_EQ(normal_entry->checksum, 0xfUL);
        }

        {
            // Then we put a new version of that page, its entry ref-count should be 1
            PageEntriesEdit edit;
            PageEntry       e;
            e.checksum = 0x6;
            edit.put(page_id, e);
            versions.apply(edit);
            auto s3    = versions.getSnapshot();
            auto entry = s3->version()->find(page_id);
            ASSERT_TRUE(entry);
            ASSERT_EQ(entry->ref, 1UL);
            ASSERT_FALSE(entry->isTombstone());
            ASSERT_EQ(entry->checksum, 0x6UL);
        }
    }
}

REGISTER_TYPED_TEST_CASE_P(PageMapVersionSet_test,
                           ApplyEdit,
                           ApplyEditWithReadLock,
                           ApplyEditWithReadLock2,
                           ApplyEditWithReadLock3,
                           Restore,
                           GcConcurrencyDelPage,
                           GcPageMove,
                           GcConcurrencySetPage,
                           PutOrDelRefPage,
                           IdempotentDel,
                           UpdateOnRefPage,
                           UpdateOnRefPage2,
                           IsRefId,
                           Snapshot,
                           LiveFiles,
                           PutOnTombstonePageEntry);

using VersionSetTypes = ::testing::Types<PageEntriesVersionSet, PageEntriesVersionSetWithDelta>;
INSTANTIATE_TYPED_TEST_CASE_P(VersionSetTypedTest, PageMapVersionSet_test, VersionSetTypes);


} // namespace tests
} // namespace DB
