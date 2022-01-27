#pragma once

#include <Common/Exception.h>
#include <Common/FmtUtils.h>
#include <IO/WriteHelpers.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace ErrorCodes
{
extern const int PS_ENTRY_NOT_EXISTS;
extern const int PS_ENTRY_NO_VALID_VERSION;
} // namespace ErrorCodes
namespace PS::V3::tests
{
inline String toString(const PageEntryV3 & entry)
{
    return fmt::format("PageEntry{{file: {}, offset: 0x{:X}, size: {}, checksum: 0x{:X}}}", entry.file_id, entry.offset, entry.size, entry.checksum);
}

inline String toString(const PageIDAndEntriesV3 & entries)
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


inline bool isSameEntry(const PageEntryV3 & lhs, const PageEntryV3 & rhs)
{
    // Maybe need more fields check later
    return (lhs.file_id == rhs.file_id && lhs.offset == rhs.offset && lhs.size == rhs.size);
}

inline ::testing::AssertionResult getEntryCompare(
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

inline ::testing::AssertionResult getEntriesCompare(
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

inline ::testing::AssertionResult getEntryNotExist(
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

inline ::testing::AssertionResult getEntriesNotExist(
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

using PageVersionAndEntryV3 = std::tuple<UInt64, UInt64, PageEntryV3>;
using PageVersionAndEntriesV3 = std::vector<PageVersionAndEntryV3>;

} // namespace PS::V3::tests
} // namespace DB
