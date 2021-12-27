#include <Common/Exception.h>
#include <Common/LogWithPrefix.h>
#include <Common/UnifiedLogPatternFormatter.h>
#include <Common/formatReadable.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/logger_useful.h>
namespace tests
{
TEST(LogWithPrefixTest, LogFmt)
{
    auto log = &Poco::Logger::get("LogWithPrefixTest");
    LOG_INFO(log, fmt::format("float-number: {0:.4f}, {0:.5f}, size: {1}", 3.1415926, formatReadableSizeWithBinarySuffix(9ULL * 1024 * 1024 * 1024 + 8 * 1024 * 1024 + 7 * 1024)));

    auto log_with_prefix = std::make_shared<DB::LogWithPrefix>(log, "[name=log_fmt]");
    LOG_INFO(log_with_prefix, fmt::format("float-number: {0:.4f}, {0:.5f}, size: {1}", 3.1415926, formatReadableSizeWithBinarySuffix(9ULL * 1024 * 1024 * 1024 + 8 * 1024 * 1024 + 7 * 1024)));
    LOG_FMT_INFO(log_with_prefix, "float-number: {0:.4f}, {0:.5f}, size: {1}", 3.1415926, formatReadableSizeWithBinarySuffix(9ULL * 1024 * 1024 * 1024 + 8 * 1024 * 1024 + 7 * 1024));
}

TEST(LogFormatterTest, Fmt)
{
    std::pair<int, int> beg{90, 0}, end{1024, 3}, min{1000, 0};
    auto text = fmt::format(" GC exit within {:.2f} sec. PageFiles from {}_{} to {}_{}, min writing {}_{}",
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
        /*file*/ "gtest_log_with_prefix.cpp",
        /*line*/ 32);

    std::string formatted_text;
    DB::UnifiedLogPatternFormatter formatter;
    formatter.format(msg, formatted_text);
    ASSERT_EQ(
        formatted_text.substr(32), // length of timestamp is 32
        R"raw( [INFO] [gtest_log_with_prefix.cpp:32] ["log_name: GC exit within 1.20 sec. PageFiles from 90_0 to 1024_3, min writing 1000_0"] [thread_id=1])raw");
}

} // namespace tests
