#include <Poco/Logger.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageFile.h>
#include <TestUtils/TiFlashTestBasic.h>

#include "gtest/gtest.h"

namespace DB
{
namespace tests
{

TEST(PageFile_test, Compare)
{
    // clean up
    const String path = TiFlashTestEnv::getTemporaryPath() + "/page_file_test";
    {
        if (Poco::File p(path); p.exists())
        {
            Poco::File file(Poco::Path(path).parent());
            file.remove(true);
        }
    }

    const auto     file_provider = TiFlashTestEnv::getContext().getFileProvider();
    Poco::Logger * log           = &Poco::Logger::get("PageFile");

    {
        // Create files for tests
        PageFile checkpoint_pf = PageFile::newPageFile(55, 0, path, file_provider, PageFile::Type::Temp, log);
        auto     writer        = checkpoint_pf.createWriter(false, true);
        checkpoint_pf.setCheckpoint();
        PageFile pf0 = PageFile::newPageFile(2, 0, path, file_provider, PageFile::Type::Formal, log);
        writer       = pf0.createWriter(false, true);
        PageFile pf1 = PageFile::newPageFile(55, 1, path, file_provider, PageFile::Type::Formal, log);
        writer       = pf1.createWriter(false, true);
    }

    PageFile checkpoint_pf = PageFile::openPageFileForRead(55, 0, path, file_provider, PageFile::Type::Checkpoint, log);
    PageFile pf0           = PageFile::openPageFileForRead(2, 0, path, file_provider, PageFile::Type::Formal, log);
    PageFile pf1           = PageFile::openPageFileForRead(55, 1, path, file_provider, PageFile::Type::Formal, log);

    PageFile::Comparator comp;
    ASSERT_EQ(comp(pf0, pf1), true);
    ASSERT_EQ(comp(pf1, pf0), false);

    // Checkpoin file is less than formal file
    ASSERT_EQ(comp(checkpoint_pf, pf0), true);
    ASSERT_EQ(comp(pf0, checkpoint_pf), false);

    // Test compare in `PageFileSet`
    PageFileSet pf_set;
    pf_set.emplace(pf0);
    pf_set.emplace(pf1);
    pf_set.emplace(checkpoint_pf);

    ASSERT_EQ(pf_set.begin()->getType(), PageFile::Type::Checkpoint);
    ASSERT_EQ(pf_set.begin()->fileIdLevel(), checkpoint_pf.fileIdLevel());
    ASSERT_TRUE(pf_set.begin()->isExist());
    ASSERT_EQ(pf_set.rbegin()->getType(), PageFile::Type::Formal);
    ASSERT_EQ(pf_set.rbegin()->fileIdLevel(), pf1.fileIdLevel());
    ASSERT_TRUE(pf_set.rbegin()->isExist());

    // Test `isPageFileExist`
    ASSERT_TRUE(PageFile::isPageFileExist(checkpoint_pf.fileIdLevel(), path, file_provider, PageFile::Type::Checkpoint, log));
    ASSERT_TRUE(PageFile::isPageFileExist(pf0.fileIdLevel(), path, file_provider, PageFile::Type::Formal, log));
    ASSERT_TRUE(PageFile::isPageFileExist(pf1.fileIdLevel(), path, file_provider, PageFile::Type::Formal, log));
    ASSERT_FALSE(PageFile::isPageFileExist(pf1.fileIdLevel(), path, file_provider, PageFile::Type::Legacy, log));
    // set pf1 to legacy and check exist
    pf1.setLegacy();
    ASSERT_FALSE(PageFile::isPageFileExist(pf1.fileIdLevel(), path, file_provider, PageFile::Type::Formal, log));
    ASSERT_TRUE(PageFile::isPageFileExist(pf1.fileIdLevel(), path, file_provider, PageFile::Type::Legacy, log));
}

TEST(Page_test, GetField)
{
    const size_t buf_sz = 1024;
    char         c_buff[buf_sz];
    for (size_t i = 0; i < buf_sz; ++i)
        c_buff[i] = i % 0xff;

    Page page;
    page.data = ByteBuffer(c_buff, c_buff + buf_sz);
    std::set<Page::FieldOffset> fields{// {field_index, data_offset}
                                       {2, 0},
                                       {3, 20},
                                       {9, 99},
                                       {10086, 1000}};
    page.field_offsets = fields;

    ASSERT_EQ(page.data.size(), buf_sz);
    auto data = page.getFieldData(2);
    ASSERT_EQ(data.size(), fields.find(3)->offset - fields.find(2)->offset);
    for (size_t i = 0; i < data.size(); ++i)
    {
        auto field_offset = fields.find(2)->offset;
        EXPECT_EQ(*(data.begin() + i), static_cast<char>((field_offset + i) % 0xff)) //
            << "field index: 2, offset: " << i                                       //
            << ", offset inside page: " << field_offset + i;
    }

    data = page.getFieldData(3);
    ASSERT_EQ(data.size(), fields.find(9)->offset - fields.find(3)->offset);
    for (size_t i = 0; i < data.size(); ++i)
    {
        auto field_offset = fields.find(3)->offset;
        EXPECT_EQ(*(data.begin() + i), static_cast<char>((field_offset + i) % 0xff)) //
            << "field index: 3, offset: " << i                                       //
            << ", offset inside page: " << field_offset + i;
    }

    data = page.getFieldData(9);
    ASSERT_EQ(data.size(), fields.find(10086)->offset - fields.find(9)->offset);
    for (size_t i = 0; i < data.size(); ++i)
    {
        auto field_offset = fields.find(9)->offset;
        EXPECT_EQ(*(data.begin() + i), static_cast<char>((field_offset + i) % 0xff)) //
            << "field index: 9, offset: " << i                                       //
            << ", offset inside page: " << field_offset + i;
    }

    data = page.getFieldData(10086);
    ASSERT_EQ(data.size(), buf_sz - fields.find(10086)->offset);
    for (size_t i = 0; i < data.size(); ++i)
    {
        auto field_offset = fields.find(10086)->offset;
        EXPECT_EQ(*(data.begin() + i), static_cast<char>((field_offset + i) % 0xff)) //
            << "field index: 10086, offset: " << i                                   //
            << ", offset inside page: " << field_offset + i;
    }

    ASSERT_THROW({ page.getFieldData(0); }, DB::Exception);
}

TEST(PageEntry_test, GetFieldInfo)
{
    PageEntry                entry;
    PageFieldOffsetChecksums field_offsets{{0, 0}, {20, 0}, {64, 0}, {99, 0}, {1024, 0}};
    entry.size          = 40000;
    entry.field_offsets = field_offsets;

    size_t beg, end;
    std::tie(beg, end) = entry.getFieldOffsets(0);
    ASSERT_EQ(beg, 0UL);
    ASSERT_EQ(end, 20UL);
    ASSERT_EQ(entry.getFieldSize(0), 20UL - 0);

    std::tie(beg, end) = entry.getFieldOffsets(1);
    ASSERT_EQ(beg, 20UL);
    ASSERT_EQ(end, 64UL);
    ASSERT_EQ(entry.getFieldSize(1), 64UL - 20);

    std::tie(beg, end) = entry.getFieldOffsets(2);
    ASSERT_EQ(beg, 64UL);
    ASSERT_EQ(end, 99UL);
    ASSERT_EQ(entry.getFieldSize(2), 99UL - 64);

    std::tie(beg, end) = entry.getFieldOffsets(3);
    ASSERT_EQ(beg, 99UL);
    ASSERT_EQ(end, 1024UL);
    ASSERT_EQ(entry.getFieldSize(3), 1024UL - 99);

    std::tie(beg, end) = entry.getFieldOffsets(4);
    ASSERT_EQ(beg, 1024UL);
    ASSERT_EQ(end, entry.size);
    ASSERT_EQ(entry.getFieldSize(4), entry.size - 1024);

    ASSERT_THROW({ entry.getFieldOffsets(5); }, DB::Exception);
    ASSERT_THROW({ entry.getFieldSize(5); }, DB::Exception);
}

} // namespace tests
} // namespace DB
