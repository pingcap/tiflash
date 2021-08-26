#include <Encryption/PosixRandomAccessFile.h>
#include <Encryption/PosixWritableFile.h>
#include <Poco/Logger.h>
#include <Storages/Page/PageUtil.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace tests
{
static const std::string FileName = "page_util_test";

TEST(PageUtils_test, ReadWriteFile)
{
    ::remove(FileName.c_str());

    size_t buff_size = 1024;
    char   buff_write[buff_size];

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

    char                buff_read[buff_size];
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
    char * buff;
    try
    {
        WritableFilePtr file_for_write = std::make_shared<PosixWritableFile>(FileName, true, -1, 0666);
        size_t          buff_size      = 3ULL * 1024 * 1024 * 1024 + 123;
        // Stack may not allow to alloc so large data in some platform
        // If malloc failed, give up this test
        buff = (char *)malloc(buff_size);
        if (buff == nullptr)
        {
            return;
        }
#ifndef NDEBUG
        PageUtil::writeFile(file_for_write, 0, buff, buff_size, nullptr, false);
#else
        PageUtil::writeFile(file_for_write, 0, buff, buff_size, nullptr);
#endif
        PageUtil::syncFile(file_for_write);
        file_for_write->close();

        RandomAccessFilePtr file_for_read = std::make_shared<PosixRandomAccessFile>(FileName, -1, nullptr);
        PageUtil::readFile(file_for_read, 0, buff, buff_size, nullptr);
        ::remove(FileName.c_str());
        free(buff);
    }
    catch (DB::Exception & e)
    {
        ::remove(FileName.c_str());
        free(buff);
        FAIL() << e.getStackTrace().toString();
    }
}

TEST(PageUtils_test, DISABLED_BigReadWriteFileWithCheck)
{
    ::remove(FileName.c_str());

    char * buff_write;
    char * buff_read;
    try
    {
        WritableFilePtr file_for_write = std::make_shared<PosixWritableFile>(FileName, true, -1, 0666);
        size_t          buff_size      = 3ULL * 1024 * 1024 * 1024 + 123;
        buff_write                     = (char *)malloc(buff_size);
        if (buff_write == nullptr)
        {
            return;
        }

        buff_read = (char *)malloc(buff_size);
        if (buff_read == nullptr)
        {
            return;
        }

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
        free(buff_write);
        free(buff_read);
    }
    catch (DB::Exception & e)
    {
        ::remove(FileName.c_str());
        free(buff_write);
        free(buff_read);
        FAIL() << e.getStackTrace().toString();
    }
}

} // namespace tests
} // namespace DB
