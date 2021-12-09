#include <Common/Exception.h>
#include <Common/LogWithPrefix.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/logger_fmt_useful.h>
#include <common/logger_useful.h>
#include "Common/formatReadable.h"
namespace tests
{

TEST(LogWithPrefixTest, LogFmt)
{
    auto log = std::make_shared<DB::LogWithPrefix>(&Poco::Logger::get("LogWithPrefixTest"), "[case=log_fmt]");
    LOG_FMT_INFO(log, "float-number: {:.4f}, size: {}", 3.1415926, formatReadableSizeWithBinarySuffix(9ULL * 1024 * 1024 * 1024 + 8 * 1024 * 1024 + 7 * 1024));
}

} // namespace tests
