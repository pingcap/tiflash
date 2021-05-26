#include "gtest/gtest.h"

#include <Encryption/PosixWritableFile.h>
#include <Poco/Logger.h>

#include <Storages/Page/PageUtil.h>

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
<<<<<<< HEAD
    PageUtil::writeFile(file, 0, data_to_write.data(), 3);
=======
#ifndef NDEBUG
    PageUtil::writeFile(file, 0, data_to_write.data(), 3, nullptr, true);
#elif
    PageUtil::writeFile(file, 0, data_to_write.data(), 3, nullptr);
#endif
>>>>>>> f9d94d5d5... Fix the bug that incomplete write batches are not truncated (#1934)
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

} // namespace tests
} // namespace DB
