#include "gtest/gtest.h"

#include <Poco/Logger.h>

#include <Storages/Page/PageFile.h>

namespace DB
{
namespace tests
{

TEST(PageFile_test, Compare)
{
    PageFile pf0 = PageFile::openPageFileForRead(0, 0, ".", &Poco::Logger::get("PageFile"));
    PageFile pf1 = PageFile::openPageFileForRead(0, 1, ".", &Poco::Logger::get("PageFile"));

    PageFile::Comparator comp;
    ASSERT_EQ(comp(pf0, pf1), true);
    ASSERT_EQ(comp(pf1, pf0), false);
}

} // namespace tests
} // namespace DB
