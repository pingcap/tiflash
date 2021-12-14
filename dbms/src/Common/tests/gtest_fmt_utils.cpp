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

    buffer.fmtAppend(" ptr var: {}", nullptr);
    ASSERT_EQ(buffer.toString(), "{test} fmt append test ptr var: 0x0");

    void * ptr = reinterpret_cast<void *>(0x123456);
    buffer.fmtAppend(" ptr var2: {}", ptr);
    ASSERT_EQ(buffer.toString(), "{test} fmt append test ptr var: 0x0 ptr var2: 0x123456");
}

TEST(FmtUtilsTest, TestJoinStr)
{
    FmtBuffer buffer;
    std::vector<std::string> v{"a", "b", "c"};
    DB::joinStr(v.cbegin(), v.cend(), buffer, [](const auto & s, FmtBuffer & fb) { fb.append(s); });
    ASSERT_EQ(buffer.toString(), "a, b, c");

    buffer.clear();
    v.clear();
    DB::joinStr(v.cbegin(), v.cend(), buffer, [](const auto & s, FmtBuffer & fb) { fb.append(s); });
    ASSERT_EQ(buffer.toString(), "");

    buffer.clear();
    v.push_back("a");
    DB::joinStr(v.cbegin(), v.cend(), buffer, [](const auto & s, FmtBuffer & fb) { fb.append(s); });
    ASSERT_EQ(buffer.toString(), "a");
}
} // namespace DB::tests
