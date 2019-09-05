#include "gtest/gtest.h"

#include <Poco/Logger.h>

#include <Storages/Page/PageUtil.h>

namespace DB
{
namespace tests
{
static String filename = "page_util_test";

TEST(PageUtils_test, ReadWriteFile)
{
    ::remove(filename.c_str());

    int fd = openFile<false, false>(filename);
    writeFile(fd, 0, "123", 3, filename);
    syncFile(fd, filename);
    ::close(fd);

    int fd2 = openFile<true, true>(filename);
    ASSERT_GT(fd2, 0);
    ::close(fd2);

    ::remove(filename.c_str());
}

TEST(PageUtils_test, FileNotExists)
{
    ::remove(filename.c_str());

    int fd = openFile<true, false>(filename);
    ASSERT_EQ(fd, 0);
}

} // namespace tests
} // namespace DB
