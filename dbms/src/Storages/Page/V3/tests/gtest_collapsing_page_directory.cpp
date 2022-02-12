#include <Common/Exception.h>
#include <Common/FmtUtils.h>
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

protected:
    CollapsingPageDirectory dir;

private:
    ReportCollector reporter;
    Poco::Logger * logger;
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
#define INSERT_PUT_EXTERNAL(PAGE_ID, VERSION)          \
    {                                                  \
        PageEntriesEdit edit;                          \
        edit.appendRecord(PageEntriesEdit::EditRecord{ \
            .type = EditRecordType::PUT_EXTERNAL,      \
            .page_id = (PAGE_ID),                      \
            .ori_page_id = 0,                          \
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
    auto checker = [&](const CollapsingPageDirectory & d) -> void {
        const auto [ver1, entry1] = d.table_directory.find(page_1)->second;
        EXPECT_TRUE(isSameEntry(entry1, entry_v4));
        EXPECT_EQ(ver1, PageVersionType(4, 0)) << fmt::format("{}", ver1);

        const auto [ver2, entry2] = d.table_directory.find(page_2)->second;
        EXPECT_TRUE(isSameEntry(entry2, entry_v88));
        EXPECT_EQ(ver2, PageVersionType(88, 0)) << fmt::format("{}", ver2);

        EXPECT_EQ(d.table_directory.find(page_3), d.table_directory.end());

        const auto [ver4, entry4] = d.table_directory.find(page_4)->second;
        EXPECT_TRUE(isSameEntry(entry4, entry_v92));
        EXPECT_EQ(ver4, PageVersionType(92, 0)) << fmt::format("{}", ver4);

        // Check the max applied version
        EXPECT_EQ(d.max_applied_ver, PageVersionType(92, 0));
    };
    checker(dir);

    // Dump to a log file and get a reader from that file to verify.
    // Should collapsed to page 1(ver=4), page 2(ver=88), page 4(ver=92)
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
        EXPECT_EQ(edit.size(), 3);
        for (auto iter = edit.getRecords().begin(); iter != edit.getRecords().end(); ++iter)
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
            else if (iter->page_id == 4)
            {
                EXPECT_EQ(iter->version, PageVersionType(92));
                EXPECT_TRUE(isSameEntry(iter->entry, entry_v92));
            }
        }
        another_dir.apply(std::move(edit));
    }
    EXPECT_EQ(num_edits_read, 1);

    //
    checker(another_dir);
}
CATCH

TEST_F(CollapsingPageDirectoryTest, CollapseRef)
try
{
    // put
    PageId page_1 = 1;
    INSERT_ENTRY(page_1, 4);

    // put, ref, del
    PageId page_2 = 2;
    PageId page_4 = 4; // ref 4 -> 2
    INSERT_ENTRY(page_2, 88);
    INSERT_REF(page_4, page_2, 89);

    // put & del
    PageId page_3 = 3;
    INSERT_ENTRY(page_3, 90);
    INSERT_DELETE(page_3, 91);

    // putExternal & ref
    PageId page_5 = 5;
    PageId page_6 = 6;
    PageId page_7 = 7;
    INSERT_PUT_EXTERNAL(page_5, 95);
    INSERT_REF(page_6, page_5, 96);
    INSERT_REF(page_7, page_6, 97);

    // putExternal & ref
    PageId page_8 = 8;
    PageId page_9 = 9;
    PageId page_10 = 10;
    INSERT_PUT_EXTERNAL(page_8, 105);
    INSERT_REF(page_9, page_8, 106);
    INSERT_REF(page_10, page_9, 107);
    INSERT_DELETE(page_8, 107);
    INSERT_DELETE(page_9, 107);

    // Should collapsed to the latest version
    auto checker = [&](const CollapsingPageDirectory & d, bool restored) -> void {
        const auto [ver1, entry1] = d.table_directory.find(page_1)->second;
        EXPECT_TRUE(isSameEntry(entry1, entry_v4));
        EXPECT_EQ(ver1, PageVersionType(4, 0)) << fmt::format("{}", ver1);

        const auto [ver2, entry2] = d.table_directory.find(page_2)->second;
        EXPECT_TRUE(isSameEntry(entry2, entry_v88));
        EXPECT_EQ(ver2, PageVersionType(88, 0)) << fmt::format("{}", ver2);

        EXPECT_EQ(d.table_directory.find(page_3), d.table_directory.end());

        // 4 is a entry copy of 2
        const auto [ver4, entry4] = d.table_directory.find(page_4)->second;
        EXPECT_TRUE(isSameEntry(entry4, entry_v88));
        EXPECT_EQ(ver4, PageVersionType(89, 0)) << fmt::format("{}", ver2);

        // 5 is an external page, 6 is ref to 5, 7 is ref to 5
        EXPECT_EQ(page_5, d.external_directory.getNormalPageId(page_5, 95));
        EXPECT_EQ(page_5, d.external_directory.getNormalPageId(page_6, 96));
        EXPECT_EQ(page_5, d.external_directory.getNormalPageId(page_7, 97));
        EXPECT_EQ(d.table_directory.count(page_5), 0);
        EXPECT_EQ(d.table_directory.count(page_6), 0);
        EXPECT_EQ(d.table_directory.count(page_7), 0);

        // 8 is an deleted external page,
        if (!restored)
        {
            EXPECT_EQ(page_8, d.external_directory.getNormalPageId(page_8, 105));
            EXPECT_EQ(page_8, d.external_directory.getNormalPageId(page_9, 106));
        }
        else
        {
            // else after restored, external page 8, 9 is deleted
            EXPECT_ANY_THROW(d.external_directory.getNormalPageId(page_8, 105));
            EXPECT_ANY_THROW(d.external_directory.getNormalPageId(page_9, 106));
        }
        EXPECT_ANY_THROW(d.external_directory.getNormalPageId(page_8, 107));
        EXPECT_ANY_THROW(d.external_directory.getNormalPageId(page_9, 107));
        EXPECT_EQ(page_8, d.external_directory.getNormalPageId(page_10, 107));
        EXPECT_EQ(page_8, d.external_directory.getNormalPageId(page_10, 108));

        // Check the max applied version
        EXPECT_EQ(d.max_applied_ver, PageVersionType(107, 0));
    };
    checker(dir, false);

    // Dump to a log file and get a reader from that file to verify.
    // Should collapsed to page 1(ver=4), page 2(ver=88), page 4(ver=89),
    // (external) page5, 6->5, 7->5, 10->8
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
            if (iter->page_id == 1)
            {
                EXPECT_EQ(iter->type, EditRecordType::PUT);
                EXPECT_EQ(iter->version, PageVersionType(4));
                EXPECT_TRUE(isSameEntry(iter->entry, entry_v4));
            }
            else if (iter->page_id == 2)
            {
                EXPECT_EQ(iter->type, EditRecordType::PUT);
                EXPECT_EQ(iter->version, PageVersionType(88));
                EXPECT_TRUE(isSameEntry(iter->entry, entry_v88));
            }
            else if (iter->page_id == 4)
            {
                EXPECT_EQ(iter->type, EditRecordType::PUT);
                EXPECT_EQ(iter->version, PageVersionType(89));
                EXPECT_TRUE(isSameEntry(iter->entry, entry_v88));
            }
            else if (iter->page_id == 5)
            {
                EXPECT_EQ(iter->type, EditRecordType::REF_EXTERNAL);
                EXPECT_EQ(iter->ori_page_id, 5);
                EXPECT_EQ(iter->version, PageVersionType(95));
            }
            else if (iter->page_id == 6)
            {
                EXPECT_EQ(iter->type, EditRecordType::REF_EXTERNAL);
                EXPECT_EQ(iter->ori_page_id, 5);
                EXPECT_EQ(iter->version, PageVersionType(96));
            }
            else if (iter->page_id == 7)
            {
                EXPECT_EQ(iter->type, EditRecordType::REF_EXTERNAL);
                EXPECT_EQ(iter->ori_page_id, 5);
                EXPECT_EQ(iter->version, PageVersionType(97));
            }
            else if (iter->page_id == 10)
            {
                EXPECT_EQ(iter->type, EditRecordType::REF_EXTERNAL);
                EXPECT_EQ(iter->ori_page_id, 8);
                EXPECT_EQ(iter->version, PageVersionType(107));
            }
        }
        another_dir.apply(std::move(edit)); // apply to `another_dir`
    }
    EXPECT_EQ(num_edits_read, 1);

    //
    checker(another_dir, true);
}
CATCH

#undef INSERT_ENTRY_TO
#undef INSERT_ENTRY
#undef INSERT_DELETE

} // namespace PS::V3::tests
} // namespace DB
