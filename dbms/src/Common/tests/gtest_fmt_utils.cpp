#include <Common/FmtUtils.h>
#include <Common/joinStr.h>
#include <gtest/gtest.h>

namespace DB::tests
{
TEST(FmtUtilsTest, TestFmtBuffer)
{
    FmtBuffer buffer;
    buffer.append("{").append("test").append("}");
    ASSERT_EQ(buffer.toString(), "{test}");

    buffer.fmtAppend(" fmt append {}", "test");
    ASSERT_EQ(buffer.toString(), "{test} fmt append test");
}

TEST(FmtUtilsTest, TestJoinStr)
{
    FmtBuffer buffer;
    std::vector<std::string> v{"a", "b", "c"};
    DB::joinStr(v.cbegin(), v.cend(), buffer, [](const auto & s, FmtBuffer & fb) { fb.append(s); });
    ASSERT_EQ(buffer.toString(), "a, b, c");
}
} // namespace DB::tests