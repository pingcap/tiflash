#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>

#include <Common/Exception.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>

namespace DB
{
namespace tests
{

TEST(WriteBufferFromOwnString, TestFinalize)
{
    WriteBufferFromOwnString buffer;
    buffer << "abc";
    buffer << "1234";
    buffer << 'd';
    buffer << "    5678";

    std::string str = buffer.str();
    EXPECT_EQ(str, "abc1234d    5678");
    EXPECT_EQ(buffer.count(), str.size());
}

} // namespace tests
} // namespace DB

