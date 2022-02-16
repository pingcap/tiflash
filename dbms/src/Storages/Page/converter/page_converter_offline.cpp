#include <Common/UnifiedLogPatternFormatter.h>
#include <Encryption/MockKeyManager.h>
#include <PageConverter.h>
#include <PageConverterOptions.h>
#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/PatternFormatter.h>
#include <TestUtils/MockDiskDelegator.h>

namespace DB
{
// Define is_background_thread for this binary
// It is required for `RateLimiter` but we do not link with `BackgroundProcessingPool`.
#if __APPLE__ && __clang__
__thread bool is_background_thread = false;
#else
thread_local bool is_background_thread = false;
#endif

} // namespace DB

void initGlobalLogger()
{
    // TODO : save log
    Poco::AutoPtr<Poco::ConsoleChannel> channel = new Poco::ConsoleChannel(std::cerr);
    Poco::AutoPtr<Poco::PatternFormatter> formatter(new DB::UnifiedLogPatternFormatter);
    Poco::AutoPtr<Poco::FormattingChannel> formatting_channel(new Poco::FormattingChannel(formatter, channel));
    Poco::Logger::root().setChannel(formatting_channel);
    Poco::Logger::root().setLevel("trace");
}

#include <fmt/format.h>

#include <fstream>

void test()
{
}

int main(int argc, char ** argv)
try
{
    initGlobalLogger();
    PageConverterOptions converter_opts = PageConverterOptions::parse(argc, argv);

    // TBD : Encryption not support
    DB::FileProviderPtr file_provider = std::make_shared<DB::FileProvider>(std::make_shared<DB::MockKeyManager>(false), false);
    DB::PSDiskDelegatorPtr delegator;

    if (converter_opts.paths.size() == 1)
    {
        delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(converter_opts.paths[0]);
    }
    else
    {
        delegator = std::make_shared<DB::tests::MockDiskDelegatorMulti>(converter_opts.paths);
    }

    DB::PageConverter page_converter(file_provider, delegator, converter_opts);
    page_converter.convertV2toV3();
    test();
    return 0;
}
catch (...)
{
    DB::tryLogCurrentException("");
    exit(-1);
}
