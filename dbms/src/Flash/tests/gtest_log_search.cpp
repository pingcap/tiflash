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
    std::string s = "2020.04.23 13:11:02.329014 [ 1 ] <Debug> Application: Load metadata done.\n2020.04.23 13:11:02.329014 [ 1 ] <Debug> "
                    "Application: Load metadata done again.";
    auto in = std::shared_ptr<std::istringstream>(new std::istringstream(s));

    LogIterator itr(0l, 1587830400000l, {::diagnosticspb::LogLevel::Debug}, {}, in);
    {
        auto log = itr.next();
        if (log)
        {
            EXPECT_EQ(log->level(), ::diagnosticspb::LogLevel::Debug);
            EXPECT_EQ(log->time(), 1587618662329);
            EXPECT_EQ(log->message(), "Application: Load metadata done.");
        }
    }

    {
        auto log = itr.next();
        if (log)
        {
            EXPECT_EQ(log->level(), ::diagnosticspb::LogLevel::Debug);
            EXPECT_EQ(log->time(), 1587618662329);
            EXPECT_EQ(log->message(), "Application: Load metadata done again.");
        }
    }
}

TEST_F(LogSearch_Test, LogSearchMultiLine)
{
    std::string s = "2020.04.23 13:11:02.329014 [ 1 ] <Debug> Application: \nLoad metadata done.\n2020.04.23 13:11:02.329014 [ 1 ] <Debug> "
                    "Application: Load metadata done again.";
    auto in = std::shared_ptr<std::istringstream>(new std::istringstream(s));

    LogIterator itr(0l, 1587830400000l, {::diagnosticspb::LogLevel::Debug}, {}, in);
    {
        auto log = itr.next();
        if (log)
        {
            EXPECT_EQ(log->level(), ::diagnosticspb::LogLevel::Debug);
            EXPECT_EQ(log->time(), 1587618662329);
            EXPECT_EQ(log->message(), "Application: \nLoad metadata done.");
        }
    }

    {
        auto log = itr.next();
        if (log)
        {
            EXPECT_EQ(log->level(), ::diagnosticspb::LogLevel::Debug);
            EXPECT_EQ(log->time(), 1587618662329);
            EXPECT_EQ(log->message(), "Application: Load metadata done again.");
        }
    }
}

} // namespace tests
} // namespace DB
