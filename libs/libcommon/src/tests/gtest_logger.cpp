
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

static constexpr size_t NUM_LONG_TEXT_LOG = 10000;

TEST_F(LoggerUsefulTest, DISABLED_LongTextOldStream)
{
    auto * log = getLogger();
    double elapsed_sec = 1.2;
    std::pair<int, int> beg{90, 0}, end{1024, 3}, min{1000, 0};
    size_t num_files = 1024, num_legacy = 1003, num_compact = 2, num_removed = 80;
    for (size_t i = 0; i < NUM_LONG_TEXT_LOG; ++i)
    {
        LOG_INFO(
            log,
            " GC exit within " << std::setprecision(2) << elapsed_sec << " sec. PageFiles from " //
                               << beg.first << "_" << beg.second << " to "
                               << end.first << "_" << end.second //
                               << ", min writing " << min.first << "_" << min.second
                               << ", num files: " << num_files << ", num legacy:" << num_legacy
                               << ", compact legacy archive files: " << num_compact
                               << ", remove data files: " << num_removed);
    }
}

TEST_F(LoggerUsefulTest, DISABLED_LongTextOldFmt)
{
    auto * log = getLogger();
    double elapsed_sec = 1.2;
    std::pair<int, int> beg{90, 0}, end{1024, 3}, min{1000, 0};
    size_t num_files = 1024, num_legacy = 1003, num_compact = 2, num_removed = 80;
    for (size_t i = 0; i < NUM_LONG_TEXT_LOG; ++i)
    {
        LOG_FMT_INFO(
            log,
            fmt::format(" GC exit within {:.2f} sec. PageFiles from {}_{} to {}_{}, min writing {}_{}"
                        ", num files: {}, num legacy:{}, compact legacy archive files: {}, remove data files: {}",
                        elapsed_sec,
                        beg.first,
                        beg.second,
                        end.first,
                        end.second,
                        min.first,
                        min.second,
                        num_files,
                        num_legacy,
                        num_compact,
                        num_removed));
    }
}

TEST_F(LoggerUsefulTest, DISABLED_LongTextFmt)
{
    auto * log = getLogger();
    double elapsed_sec = 1.2;
    std::pair<int, int> beg{90, 0}, end{1024, 3}, min{1000, 0};
    size_t num_files = 1024, num_legacy = 1003, num_compact = 2, num_removed = 80;
    for (size_t i = 0; i < NUM_LONG_TEXT_LOG; ++i)
    {
        LOG_FMT_INFO(
            log,
            " GC exit within {:.2f} sec. PageFiles from {}_{} to {}_{}, min writing {}_{}"
            ", num files: {}, num legacy:{}, compact legacy archive files: {}, remove data files: {}",
            elapsed_sec,
            beg.first,
            beg.second,
            end.first,
            end.second,
            min.first,
            min.second,
            num_files,
            num_legacy,
            num_compact,
            num_removed);
    }
}

} // namespace tests
