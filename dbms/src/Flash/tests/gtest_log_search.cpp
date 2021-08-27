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
    s = s + s;
    s.resize(s.size() - 1); // Trim \n

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
