#include <Encryption/PosixRandomAccessFile.h>
#include <Encryption/PosixWritableFile.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <Poco/Logger.h>
#include <Storages/Page/PageUtil.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

namespace DB
{
namespace FailPoints
{
extern const char force_split_io_size_4k[];
} // namespace FailPoints

namespace tests
{
static const std::string testFilename = "page_util_test";

TEST(PageUtils_test, ReadWriteFile)
{
    ::remove(testFilename.c_str());

    size_t buff_size = 1024;
    char buff_write[buff_size];

    for (size_t i = 0; i < buff_size; i++)
    {
        buff_write[i] = i % 0xFF;
    }
    WritableFilePtr file_for_write = std::make_shared<PosixWritableFile>(testFilename, true, -1, 0666);
#ifndef NDEBUG
    PageUtil::writeFile(file_for_write, 0, buff_write, buff_size, nullptr, true);
#else
    PageUtil::writeFile(file_for_write, 0, buff_write, buff_size, nullptr);
#endif
    PageUtil::syncFile(file_for_write);
    file_for_write->close();

    char buff_read[buff_size];
    RandomAccessFilePtr file_for_read = std::make_shared<PosixRandomAccessFile>(testFilename, -1, nullptr);
    PageUtil::readFile(file_for_read, 0, buff_read, buff_size, nullptr);
    ASSERT_EQ(strcmp(buff_write, buff_read), 0);

    ::remove(testFilename.c_str());
}

TEST(PageUtils_test, FileNotExists)
{
    ::remove(testFilename.c_str());

    int fd = PageUtil::openFile<true, false>(testFilename);
    ASSERT_EQ(fd, 0);
}

TEST(PageUtils_test, BigReadWriteFile)
{
    ::remove(testFilename.c_str());

    FailPointHelper::enableFailPoint(FailPoints::force_split_io_size_4k);
    try
    {
        WritableFilePtr file_for_write = std::make_shared<PosixWritableFile>(testFilename, true, -1, 0666);
        size_t buff_size = 13 * 1024 + 123;
        char buff_write[buff_size];
        char buff_read[buff_size];

        for (size_t i = 0; i < buff_size; i++)
        {
            buff_write[i] = i % 0xFF;
        }

#ifndef NDEBUG
        PageUtil::writeFile(file_for_write, 0, buff_write, buff_size, nullptr, false);
#else
        PageUtil::writeFile(file_for_write, 0, buff_write, buff_size, nullptr);
#endif
        PageUtil::syncFile(file_for_write);
        file_for_write->close();

        RandomAccessFilePtr file_for_read = std::make_shared<PosixRandomAccessFile>(testFilename, -1, nullptr);
        PageUtil::readFile(file_for_read, 0, buff_read, buff_size, nullptr);
        ASSERT_EQ(strcmp(buff_write, buff_read), 0);

        ::remove(testFilename.c_str());
        FailPointHelper::disableFailPoint(FailPoints::force_split_io_size_4k);
    }
    catch (DB::Exception & e)
    {
        ::remove(testFilename.c_str());
        FailPointHelper::disableFailPoint(FailPoints::force_split_io_size_4k);
        FAIL() << e.getStackTrace().toString();
    }
}

/// Test PageUtil::get(ReadBuffer * read_buffer, T * const result)
TEST(PageUtils_test, GetStrFromBuffer)
{
    ::remove(testFilename.c_str());

    using ValueType = size_t;

    const std::vector<ValueType> values = {789, 123, 456, 9876, 54321};
    {
        // prepare data
        WritableFilePtr file = std::make_shared<PosixWritableFile>(testFilename, true, -1, 0666);
        size_t offset = 0;
        for (auto v : values)
        {
            PageUtil::writeFile(
                file,
                offset,
                reinterpret_cast<char *>(&v),
                sizeof(v),
                nullptr
#ifndef NDEBUG
                ,
                false
#endif
            );
            offset += sizeof(v);
        }
        PageUtil::syncFile(file);
        file->close();
    }

    int fd2 = PageUtil::openFile<true, true>(testFilename);
    ASSERT_GT(fd2, 0);

    size_t buffer_size = 10;
    auto buffer = std::make_unique<ReadBufferFromFileDescriptor>(fd2, buffer_size);

    ValueType read_value = 0;
    size_t read_offset = 0;
    for (const auto expect_val : values)
    {
        ASSERT_TRUE(PageUtil::get<ValueType>(buffer.get(), &read_value));
        ASSERT_EQ(read_value, expect_val);
        read_offset += sizeof(read_value);
        ASSERT_EQ(buffer->count(), read_offset);
    }
    ASSERT_FALSE(buffer->hasPendingData());
    ::close(fd2);
}

} // namespace tests
} // namespace DB
