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
            filename,
            provider,
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
            .type = WriteBatch::WriteType::PUT,         \
            .page_id = (PAGE_ID),                       \
            .version = PageVersionType((VERSION)),      \
            .entry = entry_v##VERSION});                \
        dir.apply(std::move(edit));                     \
    }
#define INSERT_ENTRY(PAGE_ID, VERSION) INSERT_ENTRY_TO(PAGE_ID, VERSION, 1)
#define INSERT_DELETE(PAGE_ID, VERSION)                \
    {                                                  \
        PageEntriesEdit edit;                          \
        edit.appendRecord(PageEntriesEdit::EditRecord{ \
            .type = WriteBatch::WriteType::DEL,        \
            .page_id = (PAGE_ID),                      \
            .version = PageVersionType((VERSION)),     \
            .entry = {}});                             \
        dir.apply(std::move(edit));                    \
    }

TEST_F(CollapsingPageDirectoryTest, CollapseFromDifferentEdits)
try
{
    NamespaceId ns_id = 100;
    // multiple-puts
    PageIdV3Internal page_1 = combine(ns_id, 1);
    INSERT_ENTRY(page_1, 1);
    INSERT_ENTRY(page_1, 2);
    INSERT_ENTRY(page_1, 3);
    INSERT_ENTRY(page_1, 4);

    // put
    PageIdV3Internal page_2 = combine(ns_id, 2);
    INSERT_ENTRY(page_2, 88);
    INSERT_ENTRY(page_2, 78); // <78,0> is applied after version <88,0>

    // put & del
    PageIdV3Internal page_3 = combine(ns_id, 3);
    INSERT_ENTRY(page_3, 90);
    INSERT_DELETE(page_3, 91);

    // put & del
    PageIdV3Internal page_4 = combine(ns_id, 4);
    INSERT_ENTRY(page_4, 92);
    INSERT_DELETE(page_4, 90); // <90,0> is applied after version <92,0>

    // Should collapsed to the latest version
    const auto [ver1, entry1] = dir.table_directory.find(page_1)->second;
    EXPECT_SAME_ENTRY(entry1, entry_v4);
    EXPECT_EQ(ver1, PageVersionType(4, 0)) << fmt::format("{}", ver1);

    const auto [ver2, entry2] = dir.table_directory.find(page_2)->second;
    EXPECT_SAME_ENTRY(entry2, entry_v88);
    EXPECT_EQ(ver2, PageVersionType(88, 0)) << fmt::format("{}", ver2);

    EXPECT_EQ(dir.table_directory.find(page_3), dir.table_directory.end());

    const auto [ver4, entry4] = dir.table_directory.find(page_4)->second;
    EXPECT_SAME_ENTRY(entry4, entry_v92);
    EXPECT_EQ(ver4, PageVersionType(92, 0)) << fmt::format("{}", ver4);

    // Check the max applied version
    EXPECT_EQ(dir.max_applied_ver, PageVersionType(92, 0));

    // Dump to a log file and get a reader from that file to verify.
    // Should collapsed to page 1(ver=4), page 2(ver=88), page 4(ver=92)
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
                EXPECT_SAME_ENTRY(iter->entry, entry_v4);
            }
            else if (iter->page_id == 2)
            {
                EXPECT_EQ(iter->version, PageVersionType(88));
                EXPECT_SAME_ENTRY(iter->entry, entry_v88);
            }
            else if (iter->page_id == 4)
            {
                EXPECT_EQ(iter->version, PageVersionType(92));
                EXPECT_SAME_ENTRY(iter->entry, entry_v92);
            }
        }
    }
    EXPECT_EQ(num_edits_read, 1);
}
CATCH

#undef INSERT_ENTRY_TO
#undef INSERT_ENTRY
#undef INSERT_DELETE

} // namespace PS::V3::tests
} // namespace DB
