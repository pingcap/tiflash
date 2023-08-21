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

#include <Common/Logger.h>
#include <Common/UnifiedLogFormatter.h>
#include <Common/formatReadable.h>
#include <Common/tests/TestChannel.h>
#include <Poco/AutoPtr.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Message.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/logger_useful.h>

namespace DB
{
namespace tests
{

class LogMacroTest : public testing::Test
{
public:
    void SetUp() override
    {
        RUNTIME_CHECK(channel_backup == nullptr);
        Poco::AutoPtr<Poco::Formatter> formatter(new UnifiedLogFormatter<false>());
        Poco::AutoPtr<Poco::FormattingChannel> formatting_channel(new Poco::FormattingChannel(formatter, channel));
        channel_backup = Poco::Logger::root().getChannel();
        Poco::Logger::root().setChannel(formatting_channel);
        Poco::Logger::root().setLevel(Poco::Message::PRIO_TRACE);
    }

    void TearDown() override
    {
        Poco::Logger::root().setChannel(channel_backup);
        channel_backup = nullptr;
    }

protected:
    Poco::Channel * channel_backup = nullptr;
    Poco::AutoPtr<TestChannel> channel = Poco::AutoPtr<TestChannel>(new TestChannel());
};

TEST_F(LogMacroTest, Poco)
{
    auto * log = &Poco::Logger::get("LoggerTest");
    LOG_INFO(
        log,
        "float-number: {0:.4f}, {0:.5f}, size: {1}",
        3.1415926,
        formatReadableSizeWithBinarySuffix(9ULL * 1024 * 1024 * 1024 + 8 * 1024 * 1024 + 7 * 1024));

    ASSERT_EQ(
        channel->getLastMessage().getText().substr(32), // length of timestamp is 32
        R"raw( [INFO] [gtest_logger.cpp:61] ["float-number: 3.1416, 3.14159, size: 9.01 GiB"] [source=LoggerTest] [thread_id=1])raw");
}

TEST_F(LogMacroTest, PropsLogger)
{
    auto log = Logger::get("props=foo");
    LOG_INFO(
        log,
        "float-number: {0:.4f}, {0:.5f}, size: {1}",
        3.1415926,
        formatReadableSizeWithBinarySuffix(9ULL * 1024 * 1024 * 1024 + 8 * 1024 * 1024 + 7 * 1024));

    ASSERT_EQ(
        channel->getLastMessage().getText().substr(32), // length of timestamp is 32
        R"raw( [INFO] [gtest_logger.cpp:75] ["float-number: 3.1416, 3.14159, size: 9.01 GiB"] [source="props=foo"] [thread_id=1])raw");
}

TEST_F(LogMacroTest, PureMessage)
{
    auto log = Logger::get();
    LOG_INFO(log, "some arbitrary message {");

    ASSERT_EQ(
        channel->getLastMessage().getText().substr(32), // length of timestamp is 32
        R"raw( [INFO] [gtest_logger.cpp:85] ["some arbitrary message {"] [thread_id=1])raw");
}

TEST(LogIdTest, Basic)
{
    auto log = Logger::get("MyTestCase");
    ASSERT_EQ(log->name(), "MyTestCase");

    log = Logger::get("MyTestCase", "table_id=5", 128);
    ASSERT_EQ(log->name(), "MyTestCase table_id=5 128");

    log = Logger::get("MyTestCase", "", 128);
    ASSERT_EQ(log->name(), "MyTestCase 128");

    log = Logger::get("", "foo", 128, "");
    ASSERT_EQ(log->name(), "foo 128");
}

TEST(LogIdTest, GetChild)
{
    auto log = Logger::get();
    auto child = log->getChild();
    ASSERT_EQ(child->name(), "");

    log = Logger::get();
    child = log->getChild("table_id=10");
    ASSERT_EQ(child->name(), "table_id=10");

    log = Logger::get("table_id=15");
    child = log->getChild();
    ASSERT_EQ(child->name(), "table_id=15");

    log = Logger::get("table_id=15");
    child = log->getChild("query_id=x", "trace_id=100");
    ASSERT_EQ(child->name(), "table_id=15 query_id=x trace_id=100");
}

TEST(LogFormatTest, SourceSection)
{
    std::pair<int, int> beg{90, 0}, end{1024, 3}, min{1000, 0};
    auto text = fmt::format(
        "GC exit within {:.2f} sec. PageFiles from {}_{} to {}_{}, min writing {}_{}",
        1.2,
        beg.first,
        beg.second,
        end.first,
        end.second,
        min.first,
        min.second);
    Poco::Message msg(
        /*source*/ "log_name",
        /*text*/ text,
        /*prio*/ Poco::Message::PRIO_INFORMATION,
        /*file*/ "gtest_logger.cpp",
        /*line*/ 32);

    std::string formatted_text;
    UnifiedLogFormatter formatter;
    formatter.format(msg, formatted_text);
    ASSERT_EQ(
        formatted_text.substr(32), // length of timestamp is 32
        R"raw( [INFO] [gtest_logger.cpp:32] ["GC exit within 1.20 sec. PageFiles from 90_0 to 1024_3, min writing 1000_0"] [source=log_name] [thread_id=1])raw");
}

TEST(LogFormatTest, PlainText)
{
    Poco::Message msg(
        /*source*/ "",
        /*text*/ "Hello",
        /*prio*/ Poco::Message::PRIO_INFORMATION,
        /*file*/ "gtest_logger.cpp",
        /*line*/ 32);

    std::string formatted_text;
    UnifiedLogFormatter formatter;
    formatter.format(msg, formatted_text);
    ASSERT_EQ(
        formatted_text.substr(32), // length of timestamp is 32
        R"raw( [INFO] [gtest_logger.cpp:32] [Hello] [thread_id=1])raw");
}

TEST(LogFormatTest, QuoteButNotJSONEncode)
{
    Poco::Message msg(
        /*source*/ "",
        /*text*/ "Hello[abc]",
        /*prio*/ Poco::Message::PRIO_INFORMATION,
        /*file*/ "gtest_logger.cpp",
        /*line*/ 32);

    std::string formatted_text;
    UnifiedLogFormatter formatter;
    formatter.format(msg, formatted_text);
    ASSERT_EQ(
        formatted_text.substr(32), // length of timestamp is 32
        R"raw( [INFO] [gtest_logger.cpp:32] ["Hello[abc]"] [thread_id=1])raw");
}

TEST(LogFormatTest, JSONEncode)
{
    Poco::Message msg(
        /*source*/ "abc\tdef",
        /*text*/ "some string\n{\"foo\"}",
        /*prio*/ Poco::Message::PRIO_INFORMATION,
        /*file*/ "gtest_logger.cpp",
        /*line*/ 32);

    std::string formatted_text;
    UnifiedLogFormatter formatter;
    formatter.format(msg, formatted_text);
    ASSERT_EQ(
        formatted_text.substr(32), // length of timestamp is 32
        R"raw( [INFO] [gtest_logger.cpp:32] ["some string\n{\"foo\"}"] [source="abc\tdef"] [thread_id=1])raw");
}

TEST(LogFormatTest, JSONEncodeControlSeq)
{
    Poco::Message msg(
        /*source*/ "",
        /*text*/ "hello\x1Aworld 中文测试",
        /*prio*/ Poco::Message::PRIO_INFORMATION,
        /*file*/ "gtest_logger.cpp",
        /*line*/ 32);

    std::string formatted_text;
    UnifiedLogFormatter formatter;
    formatter.format(msg, formatted_text);
    ASSERT_EQ(
        formatted_text.substr(32), // length of timestamp is 32
        R"raw( [INFO] [gtest_logger.cpp:32] ["hello\u001aworld 中文测试"] [thread_id=1])raw");
}

} // namespace tests

} // namespace DB
