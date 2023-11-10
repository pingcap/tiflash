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


#include <Poco/AutoPtr.h>
#include <Poco/Channel.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/File.h>
#include <Poco/FileChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/PatternFormatter.h>
#include <common/StringRef.h>
#include <common/logger_useful.h>
#include <fmt/format.h>
#include <gtest/gtest.h>

#include <iomanip>
#include <string>

namespace tests
{
class LoggerUsefulTest : public ::testing::Test
{
public:
    static constexpr auto * log_file_path = "/tmp/logger_test";
    static void SetUpTestCase()
    {
        if (Poco::File f(log_file_path); f.exists())
            f.remove();
    }

    static Poco::Logger * getLogger()
    {
        Poco::AutoPtr<Poco::ConsoleChannel> channel = new Poco::ConsoleChannel(std::cout);
        // Poco::AutoPtr<Poco::FileChannel> channel(new Poco::FileChannel("/tmp/logger_test"));
        Poco::AutoPtr<Poco::PatternFormatter> formatter(
            new Poco::PatternFormatter("[%H:%M:%S.%i %Z] [%p] [%U(%u)]: %t"));
        Poco::AutoPtr<Poco::FormattingChannel> formatting_channel(new Poco::FormattingChannel(formatter, channel));
        Poco::Logger::root().setChannel(formatting_channel);
        Poco::Logger::root().setLevel(Poco::Message::PRIO_TRACE);
        return &Poco::Logger::get("LoggerUsefulTest");
    }
};


TEST_F(LoggerUsefulTest, Log)
{
    auto * log = getLogger();
    LOG_TRACE(log, "Trace log");
    LOG_DEBUG(log, "Debug log");
    LOG_INFO(log, "Info log");
    LOG_WARNING(log, "Warning log");
    LOG_ERROR(log, "Error log");

    LOG_ERROR(log, "Error log, num: ", 1);

    std::string msg_in_log;
    msg_in_log = "hello tiflash";
    LOG_DEBUG(log, msg_in_log);
}

TEST(FmtTest, StringRef)
{
    const char * str = "abcdefg\0\0\0\0";
    ASSERT_EQ("abc", fmt::format("{}", StringRef(str, 3)));
    ASSERT_EQ("abcdefg", fmt::format("{}", StringRef(str, 7)));
    ASSERT_EQ(std::string_view("abcdefg\0", 8), fmt::format("{}", StringRef(str, 8)));
}

} // namespace tests
