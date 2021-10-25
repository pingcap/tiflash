#include <Common/FmtUtils.h>
#include <gtest/gtest.h>

namespace DB
{
namespace tests
{
TEST(FmtUtils_test, TestFmtBuffer)
{
    FmtBuffer buffer;
    buffer.append("{").append("test").append("}");
    ASSERT_EQ(buffer.toString(), "{test}");

    buffer.fmtAppend(" fmt append {}", "test");
    ASSERT_EQ(buffer.toString(), "{test} fmt append test");
}

} // namespace tests
} // namespace DB
