#include <Common/Exception.h>
#include <Common/FmtUtils.h>
#include <IO/WriteHelpers.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <fmt/format.h>


namespace DB
{
namespace ErrorCodes
{
extern const int PS_ENTRY_NOT_EXISTS;
extern const int PS_ENTRY_NO_VALID_VERSION;
} // namespace ErrorCodes
namespace PS::V3::tests
{
String toString(const PageEntryV3 & entry)
{
    return fmt::format("PageEntry{{file: {}, offset: 0x{:X}, size: {}, checksum: 0x{:X}}}", entry.file_id, entry.offset, entry.size, entry.checksum);
}

inline bool isSameEntry(const PageEntryV3 & lhs, const PageEntryV3 & rhs)
{
    // Maybe need more fields check later
    return (lhs.file_id == rhs.file_id && lhs.offset == rhs.offset && lhs.size == rhs.size);
}

::testing::AssertionResult getEntryCompare(
    const char * expected_entry_expr,
    const char * dir_expr,
    const char * page_id_expr,
    const char * snap_expr,
    const PageEntryV3 & expected_entry,
    const PageDirectory & dir,
    const PageId page_id,
    const PageDirectorySnapshotPtr & snap)
{
    auto check_id_entry = [&](const PageIDAndEntryV3 & expected_id_entry, const PageIDAndEntryV3 & actual_id_entry) -> ::testing::AssertionResult {
        const auto & [pid, entry] = actual_id_entry;
        String err_msg;
        if (pid != expected_id_entry.first)
        {
            err_msg = fmt::format("Try to get entry [id={}] but get [id={}]", page_id_expr, pid);
            return ::testing::AssertionFailure(::testing::Message(err_msg.c_str()));
        }
        if (isSameEntry(entry, expected_entry))
        {
            return ::testing::AssertionSuccess();
        }
        // else not the expected entry we want
        auto actual_expr = fmt::format("Get entry [id={}] from {} with snap{}", page_id_expr, dir_expr, snap_expr);
        return testing::internal::EqFailure(
            expected_entry_expr,
            actual_expr.c_str(),
            toString(expected_entry),
            toString(entry),
            false);
    };
    String error;
    try
    {
        auto id_entry = dir.get(page_id, snap);
        return check_id_entry({page_id, expected_entry}, id_entry);
    }
    catch (DB::Exception & ex)
    {
        if (ex.code() == ErrorCodes::PS_ENTRY_NOT_EXISTS)
            error = fmt::format("Try to get entry [id={}] but not exists. Err message: {}", page_id_expr, ex.message());
        else if (ex.code() == ErrorCodes::PS_ENTRY_NO_VALID_VERSION)
            error = fmt::format("Try to get entry [id={}] with version {} from {} but failed. Err message: {}", page_id_expr, snap->sequence, snap_expr, ex.message());
        else
            error = ex.displayText();
        return ::testing::AssertionFailure(::testing::Message(error.c_str()));
    }
    catch (...)
    {
        error = getCurrentExceptionMessage(true);
    }
    return ::testing::AssertionFailure(::testing::Message(error.c_str()));
}
#define ASSERT_ENTRY_EQ(expected_entry, dir, pid, snap) \
    ASSERT_PRED_FORMAT4(getEntryCompare, expected_entry, dir, pid, snap)
#define EXPECT_ENTRY_EQ(expected_entry, dir, pid, snap) \
    EXPECT_PRED_FORMAT4(getEntryCompare, expected_entry, dir, pid, snap)

String toString(const PageIDAndEntriesV3 & entries)
{
    FmtBuffer buf;
    buf.append("[");
    buf.joinStr(
        entries.begin(),
        entries.end(),
        [](const PageIDAndEntryV3 & id_entry, FmtBuffer & buf) {
            buf.fmtAppend("<{},{}>", id_entry.first, toString(id_entry.second));
        },
        ", ");
    buf.append("]");
    return buf.toString();
}
::testing::AssertionResult getEntriesCompare(
    const char * expected_entries_expr,
    const char * dir_expr,
    const char * page_ids_expr,
    const char * snap_expr,
    const PageIDAndEntriesV3 & expected_entries,
    const PageDirectory & dir,
    const PageIds page_ids,
    const PageDirectorySnapshotPtr & snap)
{
    auto check_id_entries = [&](const PageIDAndEntriesV3 & expected_id_entries, const PageIDAndEntriesV3 & actual_id_entries) -> ::testing::AssertionResult {
        if (expected_id_entries.size() == actual_id_entries.size())
        {
            for (size_t idx = 0; idx == expected_id_entries.size(); ++idx)
            {
                const auto & expected_id_entry = expected_id_entries[idx];
                const auto & actual_id_entry = expected_id_entries[idx];
                if (actual_id_entry.first != expected_id_entry.first)
                {
                    auto err_msg = fmt::format("Try to get entry [id={}] but get [id={}] at [index={}]", expected_id_entry.first, actual_id_entry.first, idx);
                    return ::testing::AssertionFailure(::testing::Message(err_msg.c_str()));
                }
                if (!isSameEntry(expected_id_entry.second, actual_id_entry.second))
                {
                    // not the expected entry we want
                    String err_msg;
                    auto expect_expr = fmt::format("Entry at {} [index={}]", idx);
                    auto actual_expr = fmt::format("Get entries {} from {} with snap {} [index={}", page_ids_expr, dir_expr, snap_expr, idx);
                    return testing::internal::EqFailure(
                        expect_expr.c_str(),
                        actual_expr.c_str(),
                        toString(expected_id_entry.second),
                        toString(actual_id_entry.second),
                        false);
                }
            }
            return ::testing::AssertionSuccess();
        }

        // else not the expected entry we want
        auto expected_expr = fmt::format("Entries from {} [size={}]", expected_entries_expr, expected_entries.size());
        auto actual_expr = fmt::format("Get entries {} from {} with snap {}, [size={}]", page_ids_expr, dir_expr, snap_expr, actual_id_entries.size());
        return testing::internal::EqFailure(
            expected_expr.c_str(),
            actual_expr.c_str(),
            toString(expected_entries),
            toString(actual_id_entries),
            false);
    };
    String error;
    try
    {
        auto id_entries = dir.get(page_ids, snap);
        return check_id_entries(expected_entries, id_entries);
    }
    catch (DB::Exception & ex)
    {
        if (ex.code() == ErrorCodes::PS_ENTRY_NOT_EXISTS)
            error = fmt::format("Try to get entries with [ids={}] but not exists. Err message: {}", page_ids_expr, ex.message());
        else if (ex.code() == ErrorCodes::PS_ENTRY_NO_VALID_VERSION)
            error = fmt::format("Try to get entries with [ids={}] with version {} from {} but failed. Err message: {}", page_ids_expr, snap->sequence, snap_expr, ex.message());
        else
            error = ex.displayText();
        return ::testing::AssertionFailure(::testing::Message(error.c_str()));
    }
    catch (...)
    {
        error = getCurrentExceptionMessage(true);
    }
    return ::testing::AssertionFailure(::testing::Message(error.c_str()));
}
#define ASSERT_ENTRIES_EQ(expected_entries, dir, pid, snap) \
    ASSERT_PRED_FORMAT4(getEntriesCompare, expected_entries, dir, pid, snap)
#define EXPECT_ENTRIES_EQ(expected_entries, dir, pid, snap) \
    EXPECT_PRED_FORMAT4(getEntriesCompare, expected_entries, dir, pid, snap)

::testing::AssertionResult getEntryNotExist(
    const char * dir_expr,
    const char * page_id_expr,
    const char * snap_expr,
    const PageDirectory & dir,
    const PageId page_id,
    const PageDirectorySnapshotPtr & snap)
{
    String error;
    try
    {
        auto id_entry = dir.get(page_id, snap);
        error = fmt::format(
            "Expect entry [id={}] from {} with snap{} not exist, but got <{}, {}>",
            page_id_expr,
            dir_expr,
            snap_expr,
            id_entry.first,
            toString(id_entry.second));
    }
    catch (DB::Exception & ex)
    {
        if (ex.code() == ErrorCodes::PS_ENTRY_NOT_EXISTS || ex.code() == ErrorCodes::PS_ENTRY_NO_VALID_VERSION)
            return ::testing::AssertionSuccess();
        else
            error = ex.displayText();
        return ::testing::AssertionFailure(::testing::Message(error.c_str()));
    }
    catch (...)
    {
        error = getCurrentExceptionMessage(true);
    }
    return ::testing::AssertionFailure(::testing::Message(error.c_str()));
}
#define EXPECT_ENTRY_NOT_EXIST(dir, pid, snap) \
    EXPECT_PRED_FORMAT3(getEntryNotExist, dir, pid, snap)
::testing::AssertionResult getEntriesNotExist(
    const char * dir_expr,
    const char * page_ids_expr,
    const char * snap_expr,
    const PageDirectory & dir,
    const PageIds page_ids,
    const PageDirectorySnapshotPtr & snap)
{
    String error;
    try
    {
        auto id_entry = dir.get(page_ids, snap);
        error = fmt::format(
            "Expect entry [id={}] from {} with snap{} not exist, but got {}",
            page_ids_expr,
            dir_expr,
            snap_expr,
            toString(id_entry));
    }
    catch (DB::Exception & ex)
    {
        if (ex.code() == ErrorCodes::PS_ENTRY_NOT_EXISTS || ex.code() == ErrorCodes::PS_ENTRY_NO_VALID_VERSION)
            return ::testing::AssertionSuccess();
        else
            error = ex.displayText();
        return ::testing::AssertionFailure(::testing::Message(error.c_str()));
    }
    catch (...)
    {
        error = getCurrentExceptionMessage(true);
    }
    return ::testing::AssertionFailure(::testing::Message(error.c_str()));
}
#define EXPECT_ENTRIES_NOT_EXIST(dir, pids, snap) \
    EXPECT_PRED_FORMAT3(getEntriesNotExist, dir, pids, snap)

class PageDirectoryTest : public ::testing::Test
{
protected:
    PageDirectory dir;
};

TEST_F(PageDirectoryTest, ApplyPutRead)
try
{
    auto snap0 = dir.createSnapshot();
    EXPECT_ENTRY_NOT_EXIST(dir, 1, snap0);

    PageEntryV3 entry1{.file_id = 1, .size = 1024, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(1, entry1);
        dir.apply(std::move(edit));
    }

    auto snap1 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry1, dir, 1, snap1);

    PageEntryV3 entry2{.file_id = 2, .size = 1024, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(2, entry2);
        dir.apply(std::move(edit));
    }

    auto snap2 = dir.createSnapshot();
    EXPECT_ENTRY_NOT_EXIST(dir, 2, snap1); // creating snap2 won't affect the result of snap1
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap2);
    EXPECT_ENTRY_EQ(entry1, dir, 1, snap2);
    {
        PageIds ids{1, 2};
        PageIDAndEntriesV3 expected_entries{{1, entry1}, {2, entry2}};
        EXPECT_ENTRIES_EQ(expected_entries, dir, ids, snap2);
    }
}
CATCH

TEST_F(PageDirectoryTest, ApplyPutDelRead)
try
{
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry2{.file_id = 2, .size = 1024, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(1, entry1);
        edit.put(2, entry2);
        dir.apply(std::move(edit));
    }

    auto snap1 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry1, dir, 1, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);

    PageEntryV3 entry3{.file_id = 3, .size = 1024, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry4{.file_id = 4, .size = 1024, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.del(2);
        edit.put(3, entry3);
        edit.put(4, entry4);
        dir.apply(std::move(edit));
    }

    auto snap2 = dir.createSnapshot();
    // sanity check for snap1
    EXPECT_ENTRY_EQ(entry1, dir, 1, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_NOT_EXIST(dir, 3, snap1);
    EXPECT_ENTRY_NOT_EXIST(dir, 4, snap1);
    // check for snap2
    EXPECT_ENTRY_NOT_EXIST(dir, 2, snap2); // deleted
    EXPECT_ENTRY_EQ(entry1, dir, 1, snap2);
    EXPECT_ENTRY_EQ(entry3, dir, 3, snap2);
    EXPECT_ENTRY_EQ(entry4, dir, 4, snap2);
    {
        PageIds ids{1, 3, 4};
        PageIDAndEntriesV3 expected_entries{{1, entry1}, {3, entry3}, {4, entry4}};
        EXPECT_ENTRIES_EQ(expected_entries, dir, ids, snap2);
    }
}
CATCH

TEST_F(PageDirectoryTest, ApplyUpdateOnRefEntries)
try
{
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry2{.file_id = 2, .size = 1024, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(1, entry1);
        edit.put(2, entry2);
        dir.apply(std::move(edit));
    }

    { // Ref 3->2
        PageEntriesEdit edit;
        edit.ref(3, 2);
        dir.apply(std::move(edit));
    }
    auto snap1 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);

    // Update 3, 2 won't get updated. Update 2, 3 won't get updated.
    // Note that users should not rely on this behavior
    PageEntryV3 entry_updated{.file_id = 999, .size = 16, .offset = 0x123, .checksum = 0x123};
    {
        PageEntriesEdit edit;
        edit.put(3, entry_updated);
        dir.apply(std::move(edit));
    }
    auto snap2 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap2);
    EXPECT_ENTRY_EQ(entry_updated, dir, 3, snap2);

    PageEntryV3 entry_updated2{.file_id = 777, .size = 16, .offset = 0x123, .checksum = 0x123};
    {
        PageEntriesEdit edit;
        edit.put(2, entry_updated2);
        dir.apply(std::move(edit));
    }
    auto snap3 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap2);
    EXPECT_ENTRY_EQ(entry_updated, dir, 3, snap2);
    EXPECT_ENTRY_EQ(entry_updated2, dir, 2, snap3);
    EXPECT_ENTRY_EQ(entry_updated, dir, 3, snap3);
}
CATCH

TEST_F(PageDirectoryTest, ApplyDeleteOnRefEntries)
try
{
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry2{.file_id = 2, .size = 1024, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(1, entry1);
        edit.put(2, entry2);
        dir.apply(std::move(edit));
    }

    { // Ref 3->2
        PageEntriesEdit edit;
        edit.ref(3, 2);
        dir.apply(std::move(edit));
    }
    auto snap1 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);

    // Delete 3, 2 won't get deleted.
    {
        PageEntriesEdit edit;
        edit.del(3);
        dir.apply(std::move(edit));
    }
    auto snap2 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap2);
    EXPECT_ENTRY_NOT_EXIST(dir, 3, snap2);

    // Delete 2, 3 won't get deleted.
    {
        PageEntriesEdit edit;
        edit.del(2);
        dir.apply(std::move(edit));
    }
    auto snap3 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap2);
    EXPECT_ENTRY_NOT_EXIST(dir, 3, snap2);
    EXPECT_ENTRY_NOT_EXIST(dir, 2, snap3);
    EXPECT_ENTRY_NOT_EXIST(dir, 3, snap3);
}
CATCH

/// Put ref page to ref page, ref path collapse to normal page
TEST_F(PageDirectoryTest, ApplyRefOnRefEntries)
try
{
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry2{.file_id = 2, .size = 1024, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(1, entry1);
        edit.put(2, entry2);
        dir.apply(std::move(edit));
    }

    { // Ref 3->2
        PageEntriesEdit edit;
        edit.ref(3, 2);
        dir.apply(std::move(edit));
    }
    auto snap1 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);

    // Ref 4 -> 3
    {
        PageEntriesEdit edit;
        edit.ref(4, 3);
        dir.apply(std::move(edit));
    }
    auto snap2 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);
    EXPECT_ENTRY_NOT_EXIST(dir, 4, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap2);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap2);
    EXPECT_ENTRY_EQ(entry2, dir, 4, snap2);
}
CATCH

/// Put duplicated RefPages in different WriteBatch
TEST_F(PageDirectoryTest, ApplyDuplicatedRefEntries)
try
{
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry2{.file_id = 2, .size = 1024, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(1, entry1);
        edit.put(2, entry2);
        dir.apply(std::move(edit));
    }

    { // Ref 3->2
        PageEntriesEdit edit;
        edit.ref(3, 2);
        dir.apply(std::move(edit));
    }
    auto snap1 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);

    // Ref 3 -> 2
    {
        PageEntriesEdit edit;
        edit.ref(3, 2);
        dir.apply(std::move(edit));
    }
    auto snap2 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap2);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap2);
}
CATCH

/// Put duplicated RefPages due to ref-path-collapse
TEST_F(PageDirectoryTest, ApplyCollapseDuplicatedRefEntries)
try
{
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry2{.file_id = 2, .size = 1024, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(1, entry1);
        edit.put(2, entry2);
        dir.apply(std::move(edit));
    }

    { // Ref 3->2
        PageEntriesEdit edit;
        edit.ref(3, 2);
        dir.apply(std::move(edit));
    }
    auto snap1 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);


    { // Ref 4 -> 3, collapse to 4 -> 2
        PageEntriesEdit edit;
        edit.ref(4, 3);
        dir.apply(std::move(edit));
    }
    auto snap2 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap1);
    EXPECT_ENTRY_NOT_EXIST(dir, 4, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap2);
    EXPECT_ENTRY_EQ(entry2, dir, 3, snap2);
    EXPECT_ENTRY_EQ(entry2, dir, 4, snap2);
}
CATCH

TEST_F(PageDirectoryTest, ApplyRefToNotExistEntry)
try
{
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .offset = 0x123, .checksum = 0x4567};
    PageEntryV3 entry2{.file_id = 2, .size = 1024, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(1, entry1);
        edit.put(2, entry2);
        dir.apply(std::move(edit));
    }

    { // Ref 3-> 999
        PageEntriesEdit edit;
        edit.ref(3, 999);
        ASSERT_THROW({ dir.apply(std::move(edit)); }, DB::Exception);
    }
    auto snap1 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap1);
    EXPECT_ENTRY_NOT_EXIST(dir, 3, snap1);

    // TODO: restore, invalid ref page is filtered
}
CATCH
} // namespace PS::V3::tests
} // namespace DB
