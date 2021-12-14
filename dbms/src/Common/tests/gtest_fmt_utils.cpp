#include <Common/FmtUtils.h>
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
    buffer.joinStr(v.cbegin(), v.cend(), ", ");
    ASSERT_EQ(buffer.toString(), "a, b, c");

    buffer.clear();
    v.clear();
    buffer.joinStr(v.cbegin(), v.cend());
    ASSERT_EQ(buffer.toString(), "");

    buffer.clear();
    v.push_back("a");
    buffer.joinStr(v.cbegin(), v.cend())
        .joinStr(
            v.cbegin(),
            v.cend(),
            [](const auto & s, FmtBuffer & fb) { fb.append(s); fb.append("t"); },
            ", ");
    ASSERT_EQ(buffer.toString(), "aat");

    buffer.clear();
    v.clear();
    v.push_back("a");
    v.push_back(("b"));
    buffer.joinStr(v.cbegin(), v.cend(), "+");
    ASSERT_EQ(buffer.toString(), "a+b");

    buffer.clear();
    v.clear();
    v.push_back("a");
    v.push_back(("b"));
    buffer.joinStr(v.cbegin(), v.cend(), "+").joinStr(v.cbegin(), v.cend(), "-");
    ASSERT_EQ(buffer.toString(), "a+ba-b");
}
} // namespace DB::tests