#include <IO/IOSWrapper.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <gtest/gtest.h>
#pragma GCC diagnostic pop

#include <limits>
#include <random>

namespace DB
{
TEST(IOSWrapper, Streaming)
{
    auto buffer = WriteBufferFromOwnString{};
    {
        auto wrapper = OutputStreamWrapper{buffer};
        wrapper << 1234 << std::endl;
        wrapper << '@' << std::endl;
        wrapper << "4321" << std::endl;
    }
    auto result = buffer.releaseStr();
    auto reader = ReadBufferFromString{result};
    auto stream = InputStreamWrapper{reader};
    {
        int t;
        stream >> t;
        ASSERT_EQ(t, 1234);
    }
    {
        char t;
        stream.ignore(); // new line
        stream >> t;
        ASSERT_EQ(t, '@');
    }
    {
        std::string t;
        stream.ignore(); // new line
        stream >> t;
        ASSERT_EQ(t, "4321");
    }
}

TEST(IOSWrapper, MassiveStreaming)
{

    std::random_device dev;
    auto seed = dev();
    std::mt19937_64 eng{seed};
    std::uniform_int_distribution<int> dist{
        std::numeric_limits<int>::min(),
        std::numeric_limits<int>::max(),
    };

    auto buffer = WriteBufferFromOwnString{};
    std::vector<int> data;
    {
        auto wrapper = OutputStreamWrapper{buffer};
        for (auto i = 0; i < 1000'0000; ++i)
        {
            data.push_back(dist(eng));
            wrapper << data.back() << std::endl;
        }
    }
    auto result = buffer.releaseStr();
    auto reader = ReadBufferFromString{result};
    auto stream = InputStreamWrapper{reader};
    for (auto i : data) {
        int j;
        stream >> j;
        ASSERT_EQ(i, j);
    }
}

} // namespace DB