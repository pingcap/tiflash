#include <Encryption/PosixRandomAccessFile.h>
#include <Encryption/PosixWritableFile.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <Poco/Logger.h>
#include <Storages/Page/PageUtil.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace FailPoints
{
extern const char force_split_io_size_4k[];
} // namespace FailPoints

namespace tests
{
static const std::string FileName = "page_util_test";

TEST(PageUtils_test, ReadWriteFile)
{
    ::remove(FileName.c_str());

    size_t buff_size = 1024;
    char buff_write[buff_size];

    for (size_t i = 0; i < buff_size; i++)
    {
        buff_write[i] = i % 0xFF;
    }
    WritableFilePtr file_for_write = std::make_shared<PosixWritableFile>(FileName, true, -1, 0666);
#ifndef NDEBUG
    PageUtil::writeFile(file_for_write, 0, buff_write, buff_size, nullptr, true);
#else
    PageUtil::writeFile(file_for_write, 0, buff_write, buff_size, nullptr);
#endif
    PageUtil::syncFile(file_for_write);
    file_for_write->close();

    char buff_read[buff_size];
    RandomAccessFilePtr file_for_read = std::make_shared<PosixRandomAccessFile>(FileName, -1, nullptr);
    PageUtil::readFile(file_for_read, 0, buff_read, buff_size, nullptr);
    ASSERT_EQ(strcmp(buff_write, buff_read), 0);

    ::remove(FileName.c_str());
}

TEST(PageUtils_test, FileNotExists)
{
    ::remove(FileName.c_str());

    int fd = PageUtil::openFile<true, false>(FileName);
    ASSERT_EQ(fd, 0);
}

TEST(PageUtils_test, BigReadWriteFile)
{
    ::remove(FileName.c_str());

    FailPointHelper::enableFailPoint(FailPoints::force_split_io_size_4k);
    try
    {
        WritableFilePtr file_for_write = std::make_shared<PosixWritableFile>(FileName, true, -1, 0666);
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

        RandomAccessFilePtr file_for_read = std::make_shared<PosixRandomAccessFile>(FileName, -1, nullptr);
        PageUtil::readFile(file_for_read, 0, buff_read, buff_size, nullptr);
        ASSERT_EQ(strcmp(buff_write, buff_read), 0);

        ::remove(FileName.c_str());
        FailPointHelper::disableFailPoint(FailPoints::force_split_io_size_4k);
    }
    catch (DB::Exception & e)
    {
        ::remove(FileName.c_str());
        FailPointHelper::disableFailPoint(FailPoints::force_split_io_size_4k);
        FAIL() << e.getStackTrace().toString();
    }
}

TEST(PageUtils_test, GetStrFromBuffer)
{
    ::remove(FileName.c_str());

    // data prepare
    WritableFilePtr file = std::make_shared<PosixWritableFile>(FileName, true, -1, 0666);

    char        file_write[]  = "I want to do excellent jobs.";
    std::string data_to_write = file_write;
    int         length        = data_to_write.length();
#ifndef NDEBUG
    PageUtil::writeFile(file, 0, data_to_write.data(), 28, nullptr, true);
    PageUtil::writeFile(file, 28, reinterpret_cast<char *>(&length), sizeof(int), nullptr, true);
#else
    PageUtil::writeFile(file, 0, data_to_write.data(), 28, nullptr);
    PageUtil::writeFile(file, 28, reinterpret_cast<char *>(&length), sizeof(int), nullptr);
#endif
    PageUtil::syncFile(file);
    file->close();

    int fd2 = PageUtil::openFile<true, true>(FileName);
    ASSERT_GT(fd2, 0);

    size_t                                        buffer_size = 10;
    std::unique_ptr<ReadBufferFromFileDescriptor> buffer      = std::make_unique<ReadBufferFromFileDescriptor>(fd2, buffer_size);

    // real test
    char result[28];
    bool success = PageUtil::get<char[28]>(buffer.get(), &result);
    ASSERT_TRUE(success);
    for (int i = 0; i < 28; ++i)
    {
        ASSERT_EQ(result[i], file_write[i]);
    }
    ASSERT_EQ(buffer->count(), (long unsigned int)28);
    ASSERT_EQ(buffer->offset(), (long unsigned int)(28 % buffer_size));

    int result2;
    success = PageUtil::get<int>(buffer.get(), &result2);
    ASSERT_TRUE(success);
    ASSERT_EQ(result2, length);
    ASSERT_TRUE(!buffer->hasPendingData());
    ASSERT_EQ(buffer->count(), (long unsigned int)(28 + sizeof(int)));
    ASSERT_EQ(buffer->offset(), (long unsigned int)((28 + sizeof(int)) % buffer_size));
    ::close(fd2);
}

} // namespace tests
} // namespace DB
