#include <gtest/gtest-typed-test.h>
#include <gtest/gtest.h>

#include <type_traits>

#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/PatternFormatter.h>
#include <Storages/Page/PageEntryMapDeltaVersionSet.h>
#include <Storages/Page/PageEntryMapVersionSet.h>

namespace DB
{
namespace tests
{

template <typename T>
class PageMapVersionSet_test : public ::testing::Test
{
public:
    static void SetUpTestCase()
    {
        Poco::AutoPtr<Poco::ConsoleChannel>   channel = new Poco::ConsoleChannel(std::cerr);
        Poco::AutoPtr<Poco::PatternFormatter> formatter(new Poco::PatternFormatter);
        formatter->setProperty("pattern", "%L%Y-%m-%d %H:%M:%S.%i <%p> %s: %t");
        Poco::AutoPtr<Poco::FormattingChannel> formatting_channel(new Poco::FormattingChannel(formatter, channel));
        Logger::root().setChannel(formatting_channel);
        Logger::root().setLevel("trace");
    }
};

TYPED_TEST_CASE_P(PageMapVersionSet_test);

TYPED_TEST_P(PageMapVersionSet_test, ApplyEdit)
{
    TypeParam versions;
    LOG_TRACE(&Logger::root(), "init      :" + versions.toDebugStringUnlocked());
    {
        PageEntriesEdit edit;
        edit.put(0, PageEntry{.checksum = 0x123});
        versions.apply(edit);
    }
    // VersionSet, new version generate && old version removed at the same time
    // VersionDeltaSet, delta version merged
    LOG_TRACE(&Logger::root(), "apply    A:" + versions.toDebugStringUnlocked());
    EXPECT_EQ(versions.size(), 1UL);
    {
        PageEntriesEdit edit;
        edit.put(1, PageEntry{.checksum = 0x456});
        versions.apply(edit);
    }
    LOG_TRACE(&Logger::root(), "apply    B:" + versions.toDebugStringUnlocked());
    auto s2 = versions.getSnapshot();
    EXPECT_EQ(versions.size(), 1UL);
    auto entry = s2->version()->at(0);
    ASSERT_EQ(entry.checksum, 0x123UL);
    auto entry2 = s2->version()->at(1);
    ASSERT_EQ(entry2.checksum, 0x456UL);
    s2.reset(); // release snapshot
    EXPECT_EQ(versions.size(), 1UL);
}

TYPED_TEST_P(PageMapVersionSet_test, ApplyEditWithReadLock)
{
    TypeParam versions;
    auto      s1 = versions.getSnapshot();
    EXPECT_EQ(versions.size(), 1UL);
    LOG_TRACE(&Logger::root(), "snapshot 1:" + versions.toDebugStringUnlocked());
    {
        PageEntriesEdit edit;
        edit.put(0, PageEntry{.checksum = 0x123});
        versions.apply(edit);
    }
    EXPECT_EQ(versions.size(), 2UL); // former node is hold by s1, append new version
    LOG_TRACE(&Logger::root(), "apply    B:" + versions.toDebugStringUnlocked());
    auto s2 = versions.getSnapshot();
    LOG_TRACE(&Logger::root(), "snapshot 2:" + versions.toDebugStringUnlocked());
    auto entry = s2->version()->at(0);
    ASSERT_EQ(entry.checksum, 0x123UL);

    s2.reset();
    LOG_TRACE(&Logger::root(), "rel snap 2:" + versions.toDebugStringUnlocked());
    // VersionSet, size is 2 since A is still hold by s1
    // VersionDeltaSet, size is 1 since we do a compaction on delta
    if constexpr (std::is_same_v<TypeParam, PageEntryMapVersionSet>)
        EXPECT_EQ(versions.size(), 2UL);
    else
        EXPECT_EQ(versions.size(), 1UL);

    s1.reset();
    LOG_TRACE(&Logger::root(), "rel snap 1:" + versions.toDebugStringUnlocked());
    // VersionSet, old version removed from version set
    // VersionDeltaSet, delta version merged
    EXPECT_EQ(versions.size(), 1UL);

    {
        PageEntriesEdit edit;
        edit.put(0, PageEntry{.checksum = 0x12});
        versions.apply(edit);
    }
    LOG_TRACE(&Logger::root(), "apply    C:" + versions.toDebugStringUnlocked());
    // VersionSet, new version gen and old version remove at the same time
    // VersionDeltaSet, C merge to delta
    EXPECT_EQ(versions.size(), 1UL);
    auto s3 = versions.getSnapshot();
    entry   = s3->version()->at(0);
    ASSERT_EQ(entry.checksum, 0x12UL);
}

TYPED_TEST_P(PageMapVersionSet_test, ApplyEditWithReadLock2)
{
    TypeParam versions;
    auto      s1 = versions.getSnapshot();
    LOG_TRACE(&Logger::root(), "snapshot 1:" + versions.toDebugStringUnlocked());
    PageEntriesEdit edit;
    edit.put(0, PageEntry{.checksum = 0x123});
    versions.apply(edit);
    LOG_TRACE(&Logger::root(), "apply    B:" + versions.toDebugStringUnlocked());
    auto s2    = versions.getSnapshot();
    auto entry = s2->version()->at(0);
    ASSERT_EQ(entry.checksum, 0x123UL);

    s1.reset();
    LOG_TRACE(&Logger::root(), "rel snap 1:" + versions.toDebugStringUnlocked());
    // VersionSet, size decrease to 1 when s1 release
    // VersionDeltaSet, size is 2 since we can not do a compaction on delta
    if constexpr (std::is_same_v<TypeParam, PageEntryMapVersionSet>)
        EXPECT_EQ(versions.size(), 1UL);
    else
        EXPECT_EQ(versions.size(), 2UL);

    s2.reset();
    LOG_TRACE(&Logger::root(), "rel snap 2:" + versions.toDebugStringUnlocked());
    EXPECT_EQ(versions.size(), 1UL);
}

TYPED_TEST_P(PageMapVersionSet_test, Restore)
{
    TypeParam versions;
    {
        auto s1 = versions.getSnapshot();

        typename TypeParam::BuilderType builder(s1->version(), true, &Poco::Logger::root());

        PageEntriesEdit edit;
        edit.put(1, PageEntry{.checksum = 123});
        edit.del(1);

        builder.apply(edit);
        versions.restore(builder.build());
    }
    auto s = versions.getSnapshot();
    ASSERT_EQ(s->version()->find(1), s->version()->end());
}

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

REGISTER_TYPED_TEST_CASE_P(PageMapVersionSet_test,
                           ApplyEdit,
                           ApplyEditWithReadLock,
                           ApplyEditWithReadLock2,
                           Restore,
                           GcConcurrencyDelPage,
                           GcPageMove,
                           GcConcurrencySetPage,
                           Snapshot);

using VersionSetTypes = ::testing::Types<PageEntryMapVersionSet, PageEntryMapDeltaVersionSet>;
INSTANTIATE_TYPED_TEST_CASE_P(VersionSetTypedTest, PageMapVersionSet_test, VersionSetTypes);


} // namespace tests
} // namespace DB
