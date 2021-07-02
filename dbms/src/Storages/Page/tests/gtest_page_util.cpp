#include <Encryption/PosixWritableFile.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <Poco/Logger.h>
#include <Storages/Page/PageUtil.h>

#include "gtest/gtest.h"

namespace DB
{
namespace tests
{
static const std::string FileName = "page_util_test";

TEST(PageUtils_test, ReadWriteFile)
{
    ::remove(FileName.c_str());

    WritableFilePtr file = std::make_shared<PosixWritableFile>(FileName, true, -1, 0666);

    std::string data_to_write = "123";
#ifndef NDEBUG
    PageUtil::writeFile(file, 0, data_to_write.data(), 3, nullptr, true);
#else
    PageUtil::writeFile(file, 0, data_to_write.data(), 3, nullptr);
#endif
    PageUtil::syncFile(file);
    file->close();

    int fd2 = PageUtil::openFile<true, true>(FileName);
    ASSERT_GT(fd2, 0);
    ::close(fd2);

    ::remove(FileName.c_str());
}

TEST(PageUtils_test, FileNotExists)
{
    ::remove(FileName.c_str());

    int fd = PageUtil::openFile<true, false>(FileName);
    ASSERT_EQ(fd, 0);
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

    size_t        buffer_size = 10;
    ReadBufferPtr buffer      = std::make_shared<ReadBufferFromFileDescriptor>(fd2, buffer_size);

    // real test
    char result[28];
    bool success = PageUtil::get<char[28]>(buffer, &result);
    ASSERT(success)
    for (int i = 0; i < 28; ++i)
    {
        ASSERT_EQ(result[i], file_write[i]);
    }
    ASSERT_EQ(buffer->count(), (long unsigned int)28);
    ASSERT_EQ(buffer->offset(), (long unsigned int)(28 % buffer_size));

    int result2;
    success = PageUtil::get<int>(buffer, &result2);
    ASSERT(success);
    ASSERT_EQ(result2, length);
    ASSERT(!buffer->hasPendingData());
    ASSERT_EQ(buffer->count(), (long unsigned int)(28 + sizeof(int)));
    ASSERT_EQ(buffer->offset(), (long unsigned int)((28 + sizeof(int)) % buffer_size));
    ::close(fd2);
}

} // namespace tests
} // namespace DB
