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

TEST_F(PageDirectoryTest, ApplyPutWithIdenticalPages)
try
{
    // Put identical page in different `edit`
    PageId page_id = 50;

    auto snap0 = dir.createSnapshot();
    EXPECT_ENTRY_NOT_EXIST(dir, page_id, snap0);

    PageEntryV3 entry1{.file_id = 1, .size = 1024, .offset = 0x123, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(page_id, entry1);
        dir.apply(std::move(edit));
    }

    auto snap1 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry1, dir, page_id, snap1);

    PageEntryV3 entry2{.file_id = 1, .size = 1024, .offset = 0x1234, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(page_id, entry2);
        dir.apply(std::move(edit));
    }

    auto snap2 = dir.createSnapshot();
    EXPECT_ENTRY_EQ(entry1, dir, page_id, snap1);
    EXPECT_ENTRY_EQ(entry2, dir, page_id, snap2);
    {
        PageIds ids{page_id};
        PageIDAndEntriesV3 expected_entries{{page_id, entry2}};
        EXPECT_ENTRIES_EQ(expected_entries, dir, ids, snap2);
    }

    // Put identical page within one `edit`
    page_id++;
    PageEntryV3 entry3{.file_id = 1, .size = 1024, .offset = 0x12345, .checksum = 0x4567};
    {
        PageEntriesEdit edit;
        edit.put(page_id, entry1);
        edit.put(page_id, entry2);
        edit.put(page_id, entry3);

        // Should not be dead-lock
        dir.apply(std::move(edit));
    }
    auto snap3 = dir.createSnapshot();

    PageIds ids{page_id};
    PageIDAndEntriesV3 expected_entries{{page_id, entry3}};
    EXPECT_ENTRIES_EQ(expected_entries, dir, ids, snap3);
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

using PageVersionAndEntryV3 = std::tuple<UInt64, UInt64, PageEntryV3>;
using PageVersionAndEntriesV3 = std::vector<PageVersionAndEntryV3>;

class PageDirectoryGCTest : public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override
    {
        path = getTemporaryPath();
        DB::tests::TiFlashTestEnv::tryRemovePath(path);

        Poco::File file(path);
        if (!file.exists())
        {
            file.createDirectories();
        }

        file_provider = DB::tests::TiFlashTestEnv::getContext().getFileProvider();
        dir.blobstore = std::make_shared<BlobStore>(file_provider, path, config);
    }

    BlobStorePtr getBlobStore() const
    {
        return dir.blobstore;
    }

    void pushMvccSeqForword(size_t seq_nums, UInt64 get_snapshot = UINT64_MAX)
    {
        PageId page_id = UINT64_MAX - 100;
        [[maybe_unused]] PageVersionAndEntriesV3 meanless_seq_entries;

        for (size_t idx = 0; idx < seq_nums; idx++)
        {
            putInMvccAndBlobStore(page_id, fixed_test_buff_size, 1, meanless_seq_entries, 0, true, false);
            if (get_snapshot != UINT64_MAX && idx == get_snapshot)
            {
                snapshots_holder.emplace_back(dir.createSnapshot());
            }
        }
    }

    void putInMvccAndBlobStore(PageId page_id,
                               size_t buff_size,
                               size_t buff_nums,
                               PageVersionAndEntriesV3 & seq_entries,
                               UInt64 seq_start,
                               bool no_need_add = false,
                               bool copy_one_epoch = false)
    {
        char c_buff[buff_size * buff_nums];
        WriteBatch wb;
        for (size_t i = 0; i < buff_nums; ++i)
        {
            for (size_t j = 0; j < buff_size; ++j)
            {
                c_buff[j + i * buff_size] = static_cast<char>((j & 0xff) + i);
            }

            ReadBufferPtr buff = std::make_shared<ReadBufferFromMemory>(const_cast<char *>(c_buff + i * buff_size), buff_size);
            wb.putPage(page_id, /* tag */ 0, buff, buff_size);

            auto edit = getBlobStore()->write(wb, nullptr);
            const auto & record_last = edit.getRecords().rbegin();

            if (!no_need_add)
            {
                seq_entries.emplace_back(std::make_tuple(seq_start, 0, record_last->entry));
                if (copy_one_epoch)
                {
                    // If copy_one_epoch enable
                    // We will copy a new entry in new blobfile.
                    auto new_entry = record_last->entry;
                    new_entry.file_id += 1;
                    new_entry.offset = epoch_offset;
                    epoch_offset += buff_size;
                    seq_entries.emplace_back(std::make_tuple(seq_start, 1, new_entry));
                }
                seq_start++;
            }

            dir.apply(std::move(edit));
            wb.clear();
        }
    }

    void delInMvccAndBlobStore(PageId page_id)
    {
        WriteBatch wb;
        wb.delPage(page_id);
        auto edit = getBlobStore()->write(wb, nullptr);
        dir.apply(std::move(edit));
    }

protected:
    BlobStore::Config config;
    FileProviderPtr file_provider;
    String path{};
    PageDirectory dir;

    std::list<PageDirectorySnapshotPtr> snapshots_holder;
    size_t fixed_test_buff_size = 1024;

    size_t epoch_offset = 0;
};

::testing::AssertionResult getEntriesSeqCompare(
    const char * expected_entries_expr,
    const char * dir_expr,
    const char * page_ids_expr,
    const PageVersionAndEntriesV3 & expected_entries,
    const PageDirectory & dir,
    const PageId page_id)
{
    const auto & mvcc_table = dir.mvcc_table_directory;
    auto mvcc_it = mvcc_table.find(page_id);

    if (mvcc_it == mvcc_table.end())
    {
        return ::testing::AssertionFailure(::testing::Message(
            fmt::format(
                "Page [id={}] not exist in {}.",
                page_id,
                dir_expr)
                .c_str()));
    }

    auto & actual_entries_seq = mvcc_it->second->entries;

    if (actual_entries_seq.size() != expected_entries.size())
    {
        auto expect_expr = fmt::format("Entries with seq [page_id={}]",
                                       page_id);
        auto actual_expr = fmt::format("Get entries {} from {} with [page_id={}]",
                                       expected_entries_expr,
                                       dir_expr,
                                       page_ids_expr);
        return testing::internal::EqFailure(
            expect_expr.c_str(),
            actual_expr.c_str(),
            DB::toString(expected_entries.size()),
            DB::toString(actual_entries_seq.size()),
            false);
    }

    size_t idx = 0;
    for (auto & [actual_version_type, actual_entry] : actual_entries_seq)
    {
        const auto & expected_seq_entry = expected_entries[idx++];
        const auto & expected_seq = std::get<0>(expected_seq_entry);
        const auto & expected_epoch = std::get<1>(expected_seq_entry);
        const auto & expected_entry = std::get<2>(expected_seq_entry);

        if (actual_entry.is_delete)
        {
            auto expect_expr = fmt::format("Entry at {} [page_id={}, seq={}] ", idx - 1, page_id, actual_version_type.sequence);
            auto actual_expr = fmt::format("Get entries {} from {} with [page_id={}].",
                                           expected_entries_expr,
                                           dir_expr,
                                           page_ids_expr);
            return testing::internal::EqFailure(
                expect_expr.c_str(),
                actual_expr.c_str(),
                toString(expected_entry),
                toString(actual_entry.entry),
                false);
        }

        if (expected_seq != actual_version_type.sequence
            || expected_epoch != actual_version_type.epoch
            || !isSameEntry(expected_entry, actual_entry.entry))
        {
            // not the expected entry we want
            auto expect_expr = fmt::format("Entry at {} [page_id={}, seq={},epoch={}]", idx - 1, page_id, expected_seq, expected_epoch);
            auto actual_expr = fmt::format("Get entries {} from {} with [page_id={},seq={}, epoch={}].",
                                           expected_entries_expr,
                                           dir_expr,
                                           page_ids_expr,
                                           actual_version_type.sequence,
                                           actual_version_type.epoch);
            return testing::internal::EqFailure(
                expect_expr.c_str(),
                actual_expr.c_str(),
                toString(expected_entry),
                toString(actual_entry.entry),
                false);
        }
    }

    return ::testing::AssertionSuccess();
}

#define EXPECT_SEQ_ENTRIES_EQ(expected_entries, dir, pid) \
    EXPECT_PRED_FORMAT3(getEntriesSeqCompare, expected_entries, dir, pid)


TEST_F(PageDirectoryGCTest, TestPageDirectoryGCwithAlignSeq)
try
{
    /**
     * before GC => 
     *   pageid : 50
     *   entries: [v1...v5]
     *   hold_seq: [v3,v5]
     * after GC => 
     *   pageid : 50
     *   entries remain: [v3,v4,v5]
     *   snapshot remain: [v3,v5]
     */
    PageId page_id = 50;
    size_t buf_size = 1024;
    PageVersionAndEntriesV3 exp_seq_entries;

    // put v1 - v2
    putInMvccAndBlobStore(page_id, buf_size, 2, exp_seq_entries, 0, true);

    // put v3
    putInMvccAndBlobStore(page_id, buf_size, 1, exp_seq_entries, 3, false);
    auto snapshot_holder = dir.createSnapshot();

    // put v4
    putInMvccAndBlobStore(page_id, buf_size, 1, exp_seq_entries, 4, false);

    // put v5
    putInMvccAndBlobStore(page_id, buf_size, 1, exp_seq_entries, 5, false);
    auto snapshot_holder2 = dir.createSnapshot();

    dir.snapshotsGC();

    EXPECT_SEQ_ENTRIES_EQ(exp_seq_entries, dir, page_id);
}
CATCH

TEST_F(PageDirectoryGCTest, TestPageDirectoryGCwithUnalignSeq1)
try
{
    /**
     * case 1
     * before GC => 
     *   pageid : 50
     *   entries: [v3, v5, v10]
     *   lowest_seq: 1
     *   hold_seq: [v1, v3, v5, v10]
     * after GC => 
     *   pageid : 50
     *   entries: [v3, v5, v10]
     *   snapshot remain: [v3, v5, v10]
     */
    PageId page_id = 50;
    size_t buf_size = 1024;

    PageVersionAndEntriesV3 exp_seq_entries;

    // push v1 - v2
    pushMvccSeqForword(2, 1);

    // put v3
    putInMvccAndBlobStore(page_id, buf_size, 1, exp_seq_entries, 3);
    auto snapshot_holder1 = dir.createSnapshot();

    // push v4
    pushMvccSeqForword(1);

    // put v5
    putInMvccAndBlobStore(page_id, buf_size, 1, exp_seq_entries, 5);
    auto snapshot_holder2 = dir.createSnapshot();

    // push v6-v9
    pushMvccSeqForword(4);

    // put v10
    putInMvccAndBlobStore(page_id, buf_size, 1, exp_seq_entries, 10);
    auto snapshot_holder3 = dir.createSnapshot();

    dir.snapshotsGC();
    EXPECT_SEQ_ENTRIES_EQ(exp_seq_entries, dir, page_id);
}
CATCH

TEST_F(PageDirectoryGCTest, TestPageDirectoryGCwithUnalignSeq2)
try
{
    /**
     * case 2
     * before GC => 
     *   pageid : 50
     *   entries: [v2, v5, v10]
     *   lowest_seq: 3
     *   hold_seq: [v3, v5, v10]
     * after GC => 
     *   pageid : 50
     *   entries: [v5, v10]
     *   snapshot remain: [v2, v5, v10]
     */
    PageId page_id = 50;
    size_t buf_size = 1024;

    PageVersionAndEntriesV3 exp_seq_entries;

    // push v1 with anonymous entry
    pushMvccSeqForword(1);

    // put v2 without hold the snapshot.
    putInMvccAndBlobStore(page_id, buf_size, 1, exp_seq_entries, 2);

    // push v3, v4 with anonymous entry
    // Also hold v3 snapshot
    pushMvccSeqForword(2, 0);

    // put v5
    putInMvccAndBlobStore(page_id, buf_size, 1, exp_seq_entries, 5);
    auto snapshot_holder1 = dir.createSnapshot();

    // push v6-v9 with anonymous entry
    pushMvccSeqForword(4);

    // put v10
    putInMvccAndBlobStore(page_id, buf_size, 1, exp_seq_entries, 10);
    auto snapshot_holder2 = dir.createSnapshot();

    dir.snapshotsGC();
    EXPECT_SEQ_ENTRIES_EQ(exp_seq_entries, dir, page_id);
}
CATCH

TEST_F(PageDirectoryGCTest, TestPageDirectoryGCwithUnalignSeq3)
try
{
    /**
     * case 3
     * before GC => 
     *   pageid : 50
     *   entries: [v2, v5, v10]
     *   lowest_seq: 7
     *   hold_seq: [v7, v10]
     * after GC => 
     *   pageid : 50
     *   entries remain: [v5, v10]
     *   snapshot remain : [v5, v10]
     */
    PageId page_id = 50;
    size_t buf_size = 1024;

    PageVersionAndEntriesV3 exp_seq_entries;

    // push v1 with anonymous entry
    pushMvccSeqForword(1);

    // put v2 without hold the snapshot.
    // No need add v2 into `exp_seq_entries`
    putInMvccAndBlobStore(page_id, buf_size, 1, exp_seq_entries, 2, true);

    // push v3, v4 with anonymous entry
    pushMvccSeqForword(2);

    // put v5 without hold the snapshot.
    putInMvccAndBlobStore(page_id, buf_size, 1, exp_seq_entries, 5);

    // push v6-v9 with anonymous entry
    // Also hold v7 snapshot
    pushMvccSeqForword(4, 1);

    // put v10
    putInMvccAndBlobStore(page_id, buf_size, 1, exp_seq_entries, 10);
    auto snapshot_holder = dir.createSnapshot();

    dir.snapshotsGC();
    EXPECT_SEQ_ENTRIES_EQ(exp_seq_entries, dir, page_id);
}
CATCH

TEST_F(PageDirectoryGCTest, TestPageDirectoryGCwithUnalignSeq4)
try
{
    /**
     * case 4
     * before GC => 
     *   pageid : 50
     *   entries: [v2, v5, v10]
     *   lowest_seq: 11
     *   hold_seq: [11]
     * after GC => 
     *   pageid : 50
     *   entries remain: [v10]
     *   snapshot remain : [v10]
     */
    PageId page_id = 50;
    size_t buf_size = 1024;

    PageVersionAndEntriesV3 exp_seq_entries;

    // push v1 with anonymous entry
    pushMvccSeqForword(1);

    // put v2 without hold the snapshot.
    // No need add v2 into `exp_seq_entries`
    putInMvccAndBlobStore(page_id, buf_size, 1, exp_seq_entries, 2, true);

    // push v3, v4 with anonymous entry
    pushMvccSeqForword(2);

    // put v5 without hold the snapshot.
    // No need add v2 into `exp_seq_entries`
    putInMvccAndBlobStore(page_id, buf_size, 1, exp_seq_entries, 5, true);

    // push v6-v9 with anonymous entry
    pushMvccSeqForword(4);

    // put v10 without hold the snapshot.
    // add v2 into `exp_seq_entries`
    putInMvccAndBlobStore(page_id, buf_size, 1, exp_seq_entries, 10);

    // push v11 with anonymous entry
    // Also hold v11 snapshot
    pushMvccSeqForword(1, 0);

    dir.snapshotsGC();
    EXPECT_SEQ_ENTRIES_EQ(exp_seq_entries, dir, page_id);
}
CATCH

TEST_F(PageDirectoryGCTest, TestPageDirectoryGCwithDel)
try
{
    /**
     * before GC => 
     *   pageid : 50
     *   entries: [v2, v5, v10(del)]
     *   lowest_seq: 11
     *   hold_seq: [11]
     * after GC => 
     *   pageid : 50
     *   entries remain: [none]
     *   snapshot remain : [none]
     */
    PageId page_id = 50;
    size_t buf_size = 1024;

    [[maybe_unused]] PageVersionAndEntriesV3 meanless_seq_entries;

    // push v1 with anonymous entry
    pushMvccSeqForword(1);

    // put v2 without hold the snapshot.
    // No need add v2 into `exp_seq_entries`
    putInMvccAndBlobStore(page_id, buf_size, 1, meanless_seq_entries, 2, true);

    // push v3, v4 with anonymous entry
    pushMvccSeqForword(2);

    // put v5 without hold the snapshot.
    // No need add v2 into `exp_seq_entries`
    putInMvccAndBlobStore(page_id, buf_size, 1, meanless_seq_entries, 5, true);

    // push v6-v9 with anonymous entry
    pushMvccSeqForword(4);

    // put a writebatch with [type=del].
    // Then mvcc will push seq into v10
    delInMvccAndBlobStore(page_id);

    // push v11 with anonymous entry
    // Also hold v11 snapshot
    pushMvccSeqForword(1, 0);

    dir.snapshotsGC();
    auto snapshot = dir.createSnapshot();
    EXPECT_ENTRY_NOT_EXIST(dir, page_id, snapshot);
}
CATCH

TEST_F(PageDirectoryGCTest, TestfullGC)
{
    /**
     * PS. `v` means `sequence`, `e` means `epoch` 
     * before GC => 
     *   pageid: 50
     *   blobfile_id : 0
     *   entries: [v18-e1, v19-e1, v20-e1]
     *   valid_rate: 0.1
     *   total size: 20 * entries
     *   lowest_seq: 18
     * after GC => 
     *   pageid : 50
     *   blobfile_id(change to read only): 0
     *   entries: [v18-e1, v19-e1, v20-e1]
     *   blobfile change to read only
     *   blob file total size: 20 * entries
     *   -----
     *   pageid : 50
     *   blobfile_id: 1
     *   entries: [v18-e2, v19-e2, v20-e2]
     *   blob file total size: 3 * entries + 1 * anonymous entry
     */

    PageId page_id = 50;
    size_t buf_size = fixed_test_buff_size;

    PageVersionAndEntriesV3 exp_seq_entries;

    // push v1 with anonymous entry
    pushMvccSeqForword(17);

    // put v13
    // No need add v2 into `exp_seq_entries`
    putInMvccAndBlobStore(page_id, buf_size, 1, exp_seq_entries, 18, false, true);
    auto snapshot_holder = dir.createSnapshot();

    // put v14-v15
    // No need add v2 into `exp_seq_entries`
    putInMvccAndBlobStore(page_id, buf_size, 2, exp_seq_entries, 19, false, true);

    // do full gc
    dir.gc();

    auto & blob_stats = getBlobStore()->blob_stats;
    ASSERT_EQ(blob_stats.stats_map.size(), 2);

    auto it = blob_stats.stats_map.begin();
    auto & stat_0 = *it;
    auto & stat_1 = *++it;

    // Verify BlobStats is corrent after gc
    ASSERT_EQ(stat_0->sm_total_size, 20 * buf_size);
    ASSERT_EQ(stat_0->sm_valid_rate, 0.2);

    ASSERT_EQ(stat_1->sm_total_size, 4 * buf_size);
    ASSERT_EQ(stat_1->sm_valid_rate, 1);

    // Verify MVCC is corrent after gc

    EXPECT_SEQ_ENTRIES_EQ(exp_seq_entries, dir, page_id);
}

} // namespace PS::V3::tests
} // namespace DB
