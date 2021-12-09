
#include <Poco/AutoPtr.h>
#include <Poco/Channel.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/File.h>
#include <Poco/FileChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/PatternFormatter.h>
#include <common/logger_fmt_useful.h>
#include <common/logger_useful.h>
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
        Poco::AutoPtr<Poco::PatternFormatter> formatter(new Poco::PatternFormatter("[%H:%M:%S.%i %Z] [%p] [%U(%u)]: %t"));
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
}

TEST_F(LoggerUsefulTest, LogFmt)
{
    auto * log = getLogger();
    LOG_FMT_TRACE(log, "Trace log");
    LOG_FMT_DEBUG(log, "Debug log");
    LOG_FMT_INFO(log, "Info log");
    LOG_FMT_WARNING(log, "Warning log");
    LOG_FMT_ERROR(log, "Error log");
}

} // namespace tests
