#include <Common/Exception.h>
#include <IO/WriteHelpers.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <fmt/format.h>

#include "gtest/gtest.h"


namespace DB
{
namespace ErrorCodes
{
extern const int PS_PAGE_NOT_EXISTS;
extern const int PS_PAGE_NO_VALID_VERSION;
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
            err_msg = fmt::format("Try to get Page{} but get Page{}", page_id_expr, pid);
            return ::testing::AssertionFailure(::testing::Message(err_msg.c_str()));
        }
        if (isSameEntry(entry, expected_entry))
        {
            // Do not need a further check on the unsafe
            return ::testing::AssertionSuccess();
        }
        // else not the expected entry we want
        auto actual_expr = fmt::format("Get Page{} from {} with snap{}", page_id_expr, dir_expr, snap_expr);
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
        if (ex.code() == ErrorCodes::PS_PAGE_NOT_EXISTS)
            error = fmt::format("Try to get Page{} but not exists. Err message: {}", page_id_expr, ex.message());
        else if (ex.code() == ErrorCodes::PS_PAGE_NO_VALID_VERSION)
            error = fmt::format("Try to get Page{} with version {} from {} but failed. Err message: {}", page_id_expr, snap->sequence, snap_expr, ex.message());
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
            "Expect Page{} from {} with snap{} not exist, but got <{}, {}>",
            page_id_expr,
            dir_expr,
            snap_expr,
            id_entry.first,
            toString(id_entry.second));
    }
    catch (DB::Exception & ex)
    {
        if (ex.code() == ErrorCodes::PS_PAGE_NOT_EXISTS || ex.code() == ErrorCodes::PS_PAGE_NO_VALID_VERSION)
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

class PageDirectoryTest : public ::testing::Test
{
protected:
    PageDirectory dir;
};

TEST_F(PageDirectoryTest, ApplyRead)
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
    EXPECT_ENTRY_NOT_EXIST(dir, 2, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, 2, snap2);
}
CATCH

} // namespace PS::V3::tests
} // namespace DB
