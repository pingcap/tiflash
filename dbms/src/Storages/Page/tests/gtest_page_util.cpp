#include "gtest/gtest.h"

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

    int fd = PageUtil::openFile<false, false>(FileName);
    PageUtil::writeFile(fd, 0, "123", 3, FileName);
    PageUtil::syncFile(fd, FileName);
    ::close(fd);

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
