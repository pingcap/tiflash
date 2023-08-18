// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Flash/LogSearch.h>
#include <Poco/DeflatingStream.h>
#include <common/types.h>
#include <gtest/gtest.h>

#include <ext/scope_guard.h>

namespace DB
{
namespace tests
{
class LogSearchTest : public ::testing::Test
{
public:
    void SetUp() override {}
};

inline Int64 getTimezoneAndOffset(int tz_sign, int tz_hour, int tz_min)
{
    time_t t = time(nullptr);
    struct tm lt = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, nullptr};
    localtime_r(&t, &lt);
    Int64 offset = tz_sign * (60 * 60 * tz_hour + 60 * tz_min) - lt.tm_gmtoff;
    offset *= 1000;
    return offset;
}

inline void getTimezoneString(char * tzs, int tz_sign, int tz_hour, int tz_min)
{
    snprintf(tzs, 10, "%c%02d:%02d", tz_sign > 0 ? '+' : '-', tz_hour, tz_min);
}

TEST_F(LogSearchTest, LogSearch)
{
    int tz_sign = 1, tz_hour = 8, tz_min = 0;
    Int64 offset = getTimezoneAndOffset(tz_sign, tz_hour, tz_min);
    char tzs[10];
    getTimezoneString(tzs, tz_sign, tz_hour, tz_min);
    std::string s
        = "[2020/04/23 13:11:02.329 " + std::string(tzs) + "] [DEBUG] [\"Application : Load metadata done.\"]\n";
    std::string s_bad1
        = "[2020/4/4 13:11:02.329 " + std::string(tzs) + "] [DEBUG] [\"Application : Load metadata done.\"]\n";
    std::string s_bad2 = "[2020/04/23 13:11:02.329 " + std::string(tzs) + "] [\"Application : Load metadata done.\"]\n";
    s = s + s;
    s.resize(s.size() - 1); // Trim \n

    int milli_second;
    int timezone_hour, timezone_min;
    int year, month, day, hour, minute, second;
    size_t loglevel_size;
    size_t loglevel_s;
    ASSERT_FALSE(
        LogIterator::readDate(
            s_bad1.size(),
            s_bad1.data(),
            year,
            month,
            day,
            hour,
            minute,
            second,
            milli_second,
            timezone_hour,
            timezone_min)
        && LogIterator::readLevel(s_bad1.size(), s_bad1.data(), loglevel_s, loglevel_size));
    ASSERT_FALSE(
        LogIterator::readDate(
            s_bad2.size(),
            s_bad2.data(),
            year,
            month,
            day,
            hour,
            minute,
            second,
            milli_second,
            timezone_hour,
            timezone_min)
        && LogIterator::readLevel(s_bad2.size(), s_bad2.data(), loglevel_s, loglevel_size));
    ASSERT_TRUE(
        LogIterator::readDate(
            s.size(),
            s.data(),
            year,
            month,
            day,
            hour,
            minute,
            second,
            milli_second,
            timezone_hour,
            timezone_min)
        && LogIterator::readLevel(s.size(), s.data(), loglevel_s, loglevel_size));
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
            EXPECT_EQ(log->time(), 1587618662329 + offset);
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

TEST_F(LogSearchTest, SearchDir)
{
    time_t t = time(nullptr);
    struct tm lt = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, nullptr};
    localtime_r(&t, &lt);
    Int64 offset = 60 * 60 * 8 * 1000 - lt.tm_gmtoff * 1000;

    ASSERT_FALSE(FilterFileByDatetime("/1/2.log", {"/1/test-err.log"}, 0));
    ASSERT_FALSE(FilterFileByDatetime("/1/2.log", {"", ""}, 0));
    ASSERT_FALSE(FilterFileByDatetime("/1/2.log", {""}, 0));
    ASSERT_FALSE(FilterFileByDatetime("/1/2.log", {}, 0));
    ASSERT_TRUE(FilterFileByDatetime("/1/test-err.log", {"/1/test-err.log"}, 0));
    ASSERT_FALSE(FilterFileByDatetime("/1/2.log.123.gz", {"/1/test-err.log"}, 0));
    ASSERT_FALSE(FilterFileByDatetime("/1/server.log.2021-10-09-14:50:55.....gz", {"/1/test-err.log"}, 0));
    ASSERT_TRUE(FilterFileByDatetime(
        "/1/server.log.2021-10-09-14:50:55.481.gz",
        {"/1/test-err.log"},
        1633855377000)); // 1633855377000 : 2021-10-10 16:42:57
    ASSERT_FALSE(FilterFileByDatetime("/1/server.log.2021-10-10-16:43:57.123.gz", {"/1/test-err.log"}, 1633855377000));

    ASSERT_TRUE(
        FilterFileByDatetime("/1/tiflash_tikv.2021-10-09T14-50-55.123.log", {"/1/test-err.log"}, 1633855377000));

    {
        const std::string example_data
            = "[2020/04/23 13:11:02.329 +08:00] [DEBUG] [\"Application : Load metadata done.\"]\n";
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
                        EXPECT_EQ(log->time(), 1587618662329 + offset);
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
