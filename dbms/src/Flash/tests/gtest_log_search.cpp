#include <Flash/LogSearch.h>
#include <gtest/gtest.h>

namespace DB
{
namespace tests
{
class LogSearch_Test : public ::testing::Test
{
public:
    void SetUp() override {}
};

TEST_F(LogSearch_Test, LogSearch)
{
    std::string s = "[2020/04/23 13:11:02.329 +08:00] [DEBUG] [\"Application : Load metadata done.\"]\n";
    std::string s_bad1 = "[2020/4/4 13:11:02.329 +08:00] [DEBUG] [\"Application : Load metadata done.\"]\n";
    std::string s_bad2 = "[2020/04/23 13:11:02.329 +08:00] [\"Application : Load metadata done.\"]\n";
    s = s + s;
    s.resize(s.size() - 1); // Trim \n

    int milli_second;
    int timezone_hour, timezone_min;
    int year, month, day, hour, minute, second;
    size_t loglevel_size;
    size_t loglevel_s;
    ASSERT_FALSE(LogIterator::read_date(s_bad1.size(), s_bad1.data(), year, month, day, hour, minute, second, milli_second, timezone_hour, timezone_min)
                 && LogIterator::read_level(s_bad1.size(), s_bad1.data(), loglevel_s, loglevel_size));
    ASSERT_FALSE(LogIterator::read_date(s_bad2.size(), s_bad2.data(), year, month, day, hour, minute, second, milli_second, timezone_hour, timezone_min)
                 && LogIterator::read_level(s_bad2.size(), s_bad2.data(), loglevel_s, loglevel_size));
    ASSERT_TRUE(LogIterator::read_date(s.size(), s.data(), year, month, day, hour, minute, second, milli_second, timezone_hour, timezone_min)
                && LogIterator::read_level(s.size(), s.data(), loglevel_s, loglevel_size));
    EXPECT_EQ(year, 2020);
    EXPECT_EQ(month, 4);
    EXPECT_EQ(day, 23);
    EXPECT_EQ(hour, 13);
    EXPECT_EQ(minute, 11);
    EXPECT_EQ(second, 2);
    EXPECT_EQ(milli_second, 329);
    EXPECT_EQ(timezone_hour, 8);
    EXPECT_EQ(timezone_min, 0);
    EXPECT_EQ((int)loglevel_size, 5);
    for (int i = 0; i < 3; i++)
    {
        auto in = std::unique_ptr<std::istringstream>(new std::istringstream(s));
        LogIterator itr(0l, 1587830400000l, {::diagnosticspb::LogLevel::Debug}, {}, std::move(in));
        {
            auto log = itr.next();
            ASSERT_TRUE(log.has_value());
            EXPECT_EQ(log->level(), ::diagnosticspb::LogLevel::Debug);
            EXPECT_EQ(log->time(), 1587618662329);
            EXPECT_EQ(log->message(), "[\"Application : Load metadata done.\"]");
        }
    }
}

} // namespace tests
} // namespace DB
