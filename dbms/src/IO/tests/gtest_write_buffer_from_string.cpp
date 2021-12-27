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

TEST(WriteBufferFromOwnString, TestFinalize_ShortBuffer)
{
    WriteBufferFromOwnString buffer;
    buffer << "a";

    std::string str = buffer.str();
    EXPECT_EQ(str, "a");
    EXPECT_EQ(buffer.count(), str.size());
}

TEST(WriteBufferFromOwnString, TestFinalize_LongBuffer)
{
    std::string expect;
    WriteBufferFromOwnString buffer;

    /// 100 is long enough to trigger next
    for (size_t i = 0; i < 100; ++i)
    {
        char c = 'a' + i % 26;
        expect.push_back(c);
        buffer.write(c);
    }

    std::string str = buffer.str();
    EXPECT_EQ(str, expect);
    EXPECT_EQ(buffer.count(), str.size());
}

} // namespace tests
} // namespace DB

