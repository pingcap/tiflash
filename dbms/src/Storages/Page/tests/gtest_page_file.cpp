#include <Poco/Logger.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageFile.h>

#include "gtest/gtest.h"

namespace DB
{
namespace tests
{

TEST(PageFile_test, Compare)
{
    PageFile pf0 = PageFile::openPageFileForRead(0, 0, ".", PageFile::Type::Formal, &Poco::Logger::get("PageFile"));
    PageFile pf1 = PageFile::openPageFileForRead(0, 1, ".", PageFile::Type::Formal, &Poco::Logger::get("PageFile"));

    PageFile::Comparator comp;
    ASSERT_EQ(comp(pf0, pf1), true);
    ASSERT_EQ(comp(pf1, pf0), false);
}

TEST(Page_test, GetField)
{
    const size_t buf_sz = 1024;
    char         c_buff[buf_sz];
    for (size_t i = 0; i < buf_sz; ++i)
        c_buff[i] = i % 0xff;

    Page page;
    page.data = ByteBuffer(c_buff, c_buff + buf_sz);
    // {field_index, data_offset}
    // {2, 0},
    // {3, 20},
    // {9, 99},
    // {10086, 1000}};
    PageFieldOffsets offsets = {0, 20, 99, 1000};
    page.field_offsets       = offsets;

    ASSERT_EQ(page.data.size(), buf_sz);
    auto data = page.getFieldDataByRequestIndex(0);
    ASSERT_EQ(data.size(), offsets[1] - offsets[0]);
    for (size_t i = 0; i < data.size(); ++i)
    {
        auto field_offset = offsets[0];
        EXPECT_EQ(*(data.begin() + i), static_cast<char>((field_offset + i) % 0xff)) //
            << "field index: 2, offset: " << i                                       //
            << ", offset inside page: " << field_offset + i;
    }

    data = page.getFieldDataByRequestIndex(1);
    ASSERT_EQ(data.size(), offsets[2] - offsets[1]);
    for (size_t i = 0; i < data.size(); ++i)
    {
        auto field_offset = offsets[1];
        EXPECT_EQ(*(data.begin() + i), static_cast<char>((field_offset + i) % 0xff)) //
            << "field index: 3, offset: " << i                                       //
            << ", offset inside page: " << field_offset + i;
    }

    data = page.getFieldDataByRequestIndex(2);
    ASSERT_EQ(data.size(), offsets[3] - offsets[2]);
    for (size_t i = 0; i < data.size(); ++i)
    {
        auto field_offset = offsets[2];
        EXPECT_EQ(*(data.begin() + i), static_cast<char>((field_offset + i) % 0xff)) //
            << "field index: 9, offset: " << i                                       //
            << ", offset inside page: " << field_offset + i;
    }

    data = page.getFieldDataByRequestIndex(3);
    ASSERT_EQ(data.size(), buf_sz - offsets[3]);
    for (size_t i = 0; i < data.size(); ++i)
    {
        auto field_offset = offsets[3];
        EXPECT_EQ(*(data.begin() + i), static_cast<char>((field_offset + i) % 0xff)) //
            << "field index: 10086, offset: " << i                                   //
            << ", offset inside page: " << field_offset + i;
    }

    // Try to access not requested field
    ASSERT_THROW({ page.getFieldDataByRequestIndex(4); }, DB::Exception);
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
