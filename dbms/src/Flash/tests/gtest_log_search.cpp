#include <Flash/LogSearch.h>
#include <Poco/DeflatingStream.h>
#include <gtest/gtest.h>

#include <ext/scope_guard.h>

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
    {
        auto in = std::istringstream(s);
        LogIterator itr(0l, 1587830400000l, {::diagnosticspb::LogLevel::Debug}, {}, in);
        {
            auto log = itr.next();
            ASSERT_TRUE(log.has_value());
            EXPECT_EQ(log->level(), ::diagnosticspb::LogLevel::Debug);
            EXPECT_EQ(log->time(), 1587618662329);
            EXPECT_EQ(log->message(), "[\"Application : Load metadata done.\"]");
        }
    }
    {
        auto in = std::istringstream(s);
        LogIterator itr(0l, 1587618662329ll - 10, {::diagnosticspb::LogLevel::Debug}, {}, in);
        {
            auto log = itr.next();
            ASSERT_FALSE(log.has_value());
        }
    }
    {
        auto in = std::istringstream(s);
        LogIterator itr(0l, 1587830400000l, {::diagnosticspb::LogLevel::Info}, {}, in);
        {
            auto log = itr.next();
            ASSERT_FALSE(log.has_value());
        }
    }
}

TEST_F(LogSearch_Test, SearchDir)
{
    ASSERT_FALSE(FilterFileByDatetime("/1/2.log", "/1/test-err.log", 0));
    ASSERT_FALSE(FilterFileByDatetime("/1/2.log", "", 0));
    ASSERT_TRUE(FilterFileByDatetime("/1/test-err.log", "/1/test-err.log", 0));
    ASSERT_FALSE(FilterFileByDatetime("/1/2.log.123.gz", "/1/test-err.log", 0));
    ASSERT_FALSE(FilterFileByDatetime("/1/server.log.2021-10-09-14:50:55.....gz", "/1/test-err.log", 0));
    ASSERT_TRUE(FilterFileByDatetime("/1/server.log.2021-10-09-14:50:55.481.gz", "/1/test-err.log", 1633855377000)); // 1633855377000 : 2021-10-10 16:42:57
    ASSERT_FALSE(FilterFileByDatetime("/1/server.log.2021-10-10-16:43:57.123.gz", "/1/test-err.log", 1633855377000));

    ASSERT_TRUE(FilterFileByDatetime("/1/proxy.log.2021-10-09-14:50:55.123456789", "/1/test-err.log", 1633855377000));

    {
        const std::string example_data = "[2020/04/23 13:11:02.329 +08:00] [DEBUG] [\"Application : Load metadata done.\"]\n";
        std::string_view example = example_data;
        {
            std::string log_file_path = "/tmp/LogSearch_Test_SearchDir.gz";
            SCOPE_EXIT({
                Poco::File f(log_file_path);
                if (f.exists())
                {
                    f.remove();
                }
            });
            {
                // test read from .gz file

                std::ofstream ss(log_file_path, std::ios::binary);
                ASSERT_TRUE(ss);
                Poco::DeflatingOutputStream ds(ss, Poco::DeflatingStreamBuf::STREAM_GZIP);
                ds << example;
                ds.close();
                ss.close();

                ReadLogFile(log_file_path, [&](std::istream & istream) {
                    ASSERT_TRUE(istream);
                    LogIterator itr(0l, 1587830400000l, {::diagnosticspb::LogLevel::Debug}, {}, istream);
                    {
                        auto log = itr.next();
                        ASSERT_TRUE(log.has_value());
                        EXPECT_EQ(log->level(), ::diagnosticspb::LogLevel::Debug);
                        EXPECT_EQ(log->time(), 1587618662329);
                        EXPECT_EQ(log->message(), "[\"Application : Load metadata done.\"]");
                    }
                });
            }

            {
                // test read from broken .gz file

                std::ofstream ss(log_file_path, std::ios::binary);
                ASSERT_TRUE(ss);
                ss << example;
                ss.close();

                std::string data;
                ReadLogFile(log_file_path, [&](std::istream & istream) {
                    ASSERT_TRUE(istream);
                    LogIterator itr(0l, 1587830400000l, {::diagnosticspb::LogLevel::Debug}, {}, istream);
                    {
                        auto log = itr.next();
                        ASSERT_FALSE(log);
                    }
                    ASSERT_FALSE(istream);
                });
            }
        }

        {
            // test read from normal file
            std::string log_file_path = "/tmp/LogSearch_Test_SearchDir.log";
            std::ofstream ss(log_file_path, std::ios::binary);
            ASSERT_TRUE(ss);
            ss << example;
            ss.close();
            SCOPE_EXIT({
                Poco::File f(log_file_path);
                if (f.exists())
                {
                    f.remove();
                }
            });

            std::string data;
            ReadLogFile(log_file_path, [&](std::istream & istream) {
                ASSERT_TRUE(istream);
                std::getline(istream, data);
            });
            ASSERT_EQ(data, example.substr(0, example.size() - 1));
        }
    }
}

} // namespace tests
} // namespace DB
