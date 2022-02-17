#include <Common/Exception.h>
#include <Common/FmtUtils.h>
#include <Common/LogWithPrefix.h>
#include <Encryption/EncryptionPath.h>
#include <Encryption/FileProvider.h>
#include <Encryption/createReadBufferFromFileBaseByFileProvider.h>
#include <Encryption/createWriteBufferFromFileBaseByFileProvider.h>
#include <IO/WriteHelpers.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/LogFile/LogFormat.h>
#include <Storages/Page/V3/LogFile/LogReader.h>
#include <Storages/Page/V3/LogFile/LogWriter.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/WAL/WALReader.h>
#include <Storages/Page/V3/WAL/serialize.h>
#include <Storages/Page/V3/WALStore.h>
#include <Storages/Page/V3/tests/entries_helper.h>
#include <Storages/Page/WriteBatch.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <common/types.h>
#include <fmt/format.h>

#include <memory>

namespace DB
{
namespace PS::V3::tests
{
class CollapsingPageDirectoryTest : public DB::base::TiFlashStorageTestBasic
{
public:
    CollapsingPageDirectoryTest()
        : logger(&Poco::Logger::get("CollapsingPageDirectoryTest"))
    {}

    void SetUp() override
    {
        auto path = getTemporaryPath();
        dropDataOnDisk(path);
        createIfNotExist(path);
    }

    std::unique_ptr<LogReader> dumpAndGetReader()
    {
        auto ctx = ::DB::tests::TiFlashTestEnv::getContext();
        auto provider = ctx.getFileProvider();

        String filename = fmt::format("{}/{}", getTemporaryPath(), "log_1_0");
        EncryptionPath enc{filename, ""};
        Format::LogNumberType log_num = 1;
        auto log_writer = std::make_unique<LogWriter>(
            WriteBufferByFileProviderBuilder(
                /*has_checksum=*/false,
                provider,
                filename,
                enc,
                true,
                nullptr)
                .with_buffer_size(Format::BLOCK_SIZE)
                .build(),
            log_num,
            /*recycle*/ false,
            /*manual_flush*/ true);
        dir.dumpTo(log_writer);
        log_writer->flush();
        log_writer.reset();

        return std::make_unique<LogReader>(
            createReadBufferFromFileBaseByFileProvider(
                provider,
                filename,
                enc,
                /*estimated_size*/ Format::BLOCK_SIZE,
                /*aio_threshold*/ 0,
                /*read_limiter*/ nullptr,
                /*buffer_size*/ Format::BLOCK_SIZE),
            &reporter,
            /*verify_checksum*/ true,
            log_num,
            WALRecoveryMode::TolerateCorruptedTailRecords,
            logger);
    }

private:
    ReportCollector reporter;

protected:
    Poco::Logger * logger;
    CollapsingPageDirectory dir;
};

#define INSERT_ENTRY_TO(PAGE_ID, VERSION, BLOB_FILE_ID) \
    PageEntryV3 entry_v##VERSION{                       \
        .file_id = (BLOB_FILE_ID),                      \
        .size = (VERSION),                              \
        .tag = 0,                                       \
        .offset = 0x123,                                \
        .checksum = 0x4567};                            \
    {                                                   \
        PageEntriesEdit edit;                           \
        edit.appendRecord(PageEntriesEdit::EditRecord{  \
            .type = EditRecordType::PUT,                \
            .page_id = (PAGE_ID),                       \
            .ori_page_id = 0,                           \
            .version = PageVersionType((VERSION)),      \
            .entry = entry_v##VERSION});                \
        dir.apply(std::move(edit));                     \
    }
#define INSERT_ENTRY(PAGE_ID, VERSION) INSERT_ENTRY_TO(PAGE_ID, VERSION, 1)
#define INSERT_DELETE(PAGE_ID, VERSION)                \
    {                                                  \
        PageEntriesEdit edit;                          \
        edit.appendRecord(PageEntriesEdit::EditRecord{ \
            .type = EditRecordType::DEL,               \
            .page_id = (PAGE_ID),                      \
            .ori_page_id = 0,                          \
            .version = PageVersionType((VERSION)),     \
            .entry = {}});                             \
        dir.apply(std::move(edit));                    \
    }
#define INSERT_REF(REF_ID, ORI_ID, VERSION)            \
    {                                                  \
        PageEntriesEdit edit;                          \
        edit.appendRecord(PageEntriesEdit::EditRecord{ \
            .type = EditRecordType::REF,               \
            .page_id = (REF_ID),                       \
            .ori_page_id = (ORI_ID),                   \
            .version = PageVersionType((VERSION)),     \
            .entry = {}});                             \
        dir.apply(std::move(edit));                    \
    }

TEST_F(CollapsingPageDirectoryTest, CollapseFromDifferentEdits)
try
{
    // multiple-puts
    PageId page_1 = 1;
    INSERT_ENTRY(page_1, 1);
    INSERT_ENTRY(page_1, 2);
    INSERT_ENTRY(page_1, 3);
    INSERT_ENTRY(page_1, 4);

    // put
    PageId page_2 = 2;
    INSERT_ENTRY(page_2, 88);
    INSERT_ENTRY(page_2, 78); // <78,0> is applied after version <88,0>

    // put & del
    PageId page_3 = 3;
    INSERT_ENTRY(page_3, 90);
    INSERT_DELETE(page_3, 91);

    // put & del
    PageId page_4 = 4;
    INSERT_ENTRY(page_4, 92);
    INSERT_DELETE(page_4, 90); // <90,0> is applied after version <92,0>

    // Should collapsed to the latest version
    auto checker = [&](const CollapsingPageDirectory & d, const char * scope) -> void {
        SCOPED_TRACE(scope);
        const auto ver1 = d.table_directory.find(page_1)->second;
        EXPECT_TRUE(isSameEntry(ver1.entry, entry_v4));
        EXPECT_EQ(ver1.ver, PageVersionType(4, 0)) << fmt::format("{}", ver1.ver);

        const auto ver2 = d.table_directory.find(page_2)->second;
        EXPECT_TRUE(isSameEntry(ver2.entry, entry_v88));
        EXPECT_EQ(ver2.ver, PageVersionType(88, 0)) << fmt::format("{}", ver2.ver);

        EXPECT_EQ(d.table_directory.find(page_3), d.table_directory.end());

        const auto ver4 = d.table_directory.find(page_4)->second;
        EXPECT_TRUE(isSameEntry(ver4.entry, entry_v92));
        EXPECT_EQ(ver4.ver, PageVersionType(92, 0)) << fmt::format("{}", ver4.ver);

        // Check the max applied version
        EXPECT_EQ(d.max_applied_ver, PageVersionType(92, 0));
    };
    checker(dir, "before restore");

    // Dump to a log file and get a reader from that file to verify.
    // The collapsed state should be:
    //   page 1(ver=4), page 2(ver=88), page 4(ver=92)
    //   1->1, 2->2, 4->4
    CollapsingPageDirectory another_dir;
    auto reader = dumpAndGetReader();
    size_t num_edits_read = 0;
    while (true)
    {
        auto [ok, record] = reader->readRecord();
        if (!ok)
            break;
        ++num_edits_read;

        auto edit = ser::deserializeFrom(record);
        EXPECT_EQ(edit.size(), 6);
        for (auto iter = edit.getRecords().begin(); iter != edit.getRecords().end(); ++iter)
        {
            if (iter->type == EditRecordType::COLLAPSED_ENTRY)
            {
                if (iter->page_id == 1)
                {
                    EXPECT_EQ(iter->version, PageVersionType(4));
                    EXPECT_TRUE(isSameEntry(iter->entry, entry_v4)) << toString(iter->entry);
                }
                else if (iter->page_id == 2)
                {
                    EXPECT_EQ(iter->version, PageVersionType(88));
                    EXPECT_TRUE(isSameEntry(iter->entry, entry_v88)) << toString(iter->entry);
                }
                else if (iter->page_id == 4)
                {
                    EXPECT_EQ(iter->version, PageVersionType(92));
                    EXPECT_TRUE(isSameEntry(iter->entry, entry_v92)) << toString(iter->entry);
                }
                else
                {
                    ASSERT_TRUE(false) << fmt::format("unknown id {}", iter->page_id);
                }
            }
            else if (iter->type == EditRecordType::COLLAPSED_MAPPING)
            {
                if (iter->page_id == 1)
                {
                    EXPECT_EQ(iter->version, PageVersionType(4));
                    EXPECT_EQ(iter->page_id, iter->ori_page_id);
                }
                else if (iter->page_id == 2)
                {
                    EXPECT_EQ(iter->version, PageVersionType(88));
                    EXPECT_EQ(iter->page_id, iter->ori_page_id);
                }
                else if (iter->page_id == 4)
                {
                    EXPECT_EQ(iter->version, PageVersionType(92));
                    EXPECT_EQ(iter->page_id, iter->ori_page_id);
                }
                else
                {
                    ASSERT_TRUE(false) << fmt::format("unknown id {}", iter->page_id);
                }
            }
            else
            {
                ASSERT_TRUE(false); // unknown type
            }
        }
        another_dir.apply(std::move(edit));
    }
    EXPECT_EQ(num_edits_read, 1);

    //
    checker(dir, "after restore");
}
CATCH

TEST_F(CollapsingPageDirectoryTest, CollapseRef)
try
{
    // put
    PageId page_1 = 1;
    INSERT_ENTRY(page_1, 4);

    // put, ref
    PageId page_2 = 2;
    PageId page_4 = 4; // ref 4 -> 2
    INSERT_ENTRY(page_2, 88);
    INSERT_REF(page_4, page_2, 89);

    // put & del
    PageId page_3 = 3;
    INSERT_ENTRY(page_3, 90);
    INSERT_DELETE(page_3, 91);

    // put, ref, del
    PageId page_5 = 5;
    PageId page_6 = 6; // ref 6 -> 5
    INSERT_ENTRY(page_5, 108);
    INSERT_REF(page_6, page_5, 109);
    INSERT_DELETE(page_5, 109);

    // Should collapsed to the latest version
    auto checker = [&](const CollapsingPageDirectory & d, const char * scope) -> void {
        SCOPED_TRACE(scope);
        const auto ver1 = d.table_directory.find(page_1)->second;
        EXPECT_TRUE(isSameEntry(ver1.entry, entry_v4));
        EXPECT_EQ(ver1.ver, PageVersionType(4, 0)) << fmt::format("{}", ver1.ver);

        const auto ver2 = d.table_directory.find(page_2)->second;
        EXPECT_TRUE(isSameEntry(ver2.entry, entry_v88));
        EXPECT_EQ(ver2.ver, PageVersionType(88, 0)) << fmt::format("{}", ver2.ver);

        EXPECT_EQ(d.table_directory.find(page_3), d.table_directory.end());

        // 4 is a ref to 2
        const auto ver4 = d.id_mapping.find(page_4)->second;
        EXPECT_EQ(ver4.second, page_2);
        EXPECT_EQ(ver4.first, PageVersionType(89, 0)) << fmt::format("{}", ver4.first);

        // Check the max applied version
        EXPECT_EQ(d.max_applied_ver, PageVersionType(109, 0));
    };
    checker(dir, "before restore");

    // Dump to a log file and get a reader from that file to verify.
    // The collapsed state should be:
    //   1(ver=4), page 2(ver=88), page 5(ver=108)
    //   1->1, 2->2, 4->2, 6->5
    CollapsingPageDirectory another_dir;
    auto reader = dumpAndGetReader();
    size_t num_edits_read = 0;
    while (true)
    {
        auto [ok, record] = reader->readRecord();
        if (!ok)
            break;
        ++num_edits_read;

        auto edit = ser::deserializeFrom(record);
        EXPECT_EQ(edit.size(), 7);
        for (auto iter = edit.getRecords().begin(); iter != edit.getRecords().end(); ++iter)
        {
            if (iter->type == EditRecordType::COLLAPSED_ENTRY)
            {
                if (iter->page_id == 1)
                {
                    EXPECT_EQ(iter->version, PageVersionType(4));
                    EXPECT_TRUE(isSameEntry(iter->entry, entry_v4));
                }
                else if (iter->page_id == 2)
                {
                    EXPECT_EQ(iter->version, PageVersionType(88));
                    EXPECT_TRUE(isSameEntry(iter->entry, entry_v88));
                }
                else if (iter->page_id == 5)
                {
                    EXPECT_EQ(iter->version, PageVersionType(108));
                    EXPECT_TRUE(isSameEntry(iter->entry, entry_v108));
                }
                else
                {
                    ASSERT_TRUE(false) << fmt::format("unknown id {}", iter->page_id);
                }
            }
            else if (iter->type == EditRecordType::COLLAPSED_MAPPING)
            {
                if (iter->page_id == 1)
                {
                    EXPECT_EQ(iter->version, PageVersionType(4));
                    EXPECT_EQ(iter->page_id, iter->ori_page_id);
                }
                else if (iter->page_id == 2)
                {
                    EXPECT_EQ(iter->version, PageVersionType(88));
                    EXPECT_EQ(iter->page_id, iter->ori_page_id);
                }
                else if (iter->page_id == 4)
                {
                    EXPECT_EQ(iter->version, PageVersionType(89));
                    EXPECT_EQ(iter->ori_page_id, page_2);
                }
                else if (iter->page_id == 6)
                {
                    EXPECT_EQ(iter->version, PageVersionType(109));
                    EXPECT_EQ(iter->ori_page_id, page_5);
                }
                else
                {
                    ASSERT_TRUE(false) << fmt::format("unknown id {}", iter->page_id);
                }
            }
            else
            {
                ASSERT_TRUE(false); // unknown type
            }
        }
        another_dir.apply(std::move(edit)); // apply to `another_dir`
    }
    EXPECT_EQ(num_edits_read, 1);

    //
    checker(dir, "after restore");
}
CATCH

#undef INSERT_ENTRY_TO
#undef INSERT_ENTRY
#undef INSERT_DELETE

} // namespace PS::V3::tests
} // namespace DB
