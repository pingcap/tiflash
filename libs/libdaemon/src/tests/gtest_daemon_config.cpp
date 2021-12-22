#include <Common/Config/TOMLConfiguration.h>
#include <Core/Types.h>
#include <Poco/Ext/LevelFilterChannel.h>
#include <Poco/Ext/ReloadableSplitterChannel.h>
#include <Poco/Ext/SourceFilterChannel.h>
#include <Poco/Ext/TiFlashLogFileChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <cpptoml.h>
#include <daemon/BaseDaemon.h>
#include <fmt/format.h>
#include <gtest/gtest.h>

class DaemonConfig_test : public ::testing::Test
{
public:
    DaemonConfig_test()
        : log(&Poco::Logger::get("DaemonConfig_test"))
    {
    }

    static void SetUpTestCase() {}

protected:
    Poco::Logger * log;
    void clearFiles(String path)
    {
        Poco::File file(path);
        if (file.exists())
            file.remove(true);
    }
};

static auto loadConfigFromString(const String & s)
{
    std::istringstream ss(s);
    cpptoml::parser p(ss);
    auto table = p.parse();
    Poco::AutoPtr<Poco::Util::LayeredConfiguration> config = new Poco::Util::LayeredConfiguration();
    config->add(new DB::TOMLConfiguration(table), false); // Take ownership of TOMLConfig
    return config;
}

static void verifyChannelConfig(Poco::Channel & channel, Poco::Util::AbstractConfiguration & config)
{
    if (typeid(channel) == typeid(Poco::TiFlashLogFileChannel))
    {
        Poco::TiFlashLogFileChannel * file_channel = dynamic_cast<Poco::TiFlashLogFileChannel *>(&channel);
        ASSERT_EQ(file_channel->getProperty(Poco::FileChannel::PROP_ROTATION), config.getRawString("logger.size", "100M"));
        ASSERT_EQ(file_channel->getProperty(Poco::FileChannel::PROP_PURGECOUNT), config.getRawString("logger.count", "10"));
        return;
    }
    if (typeid(channel) == typeid(Poco::LevelFilterChannel))
    {
        Poco::LevelFilterChannel * level_filter_channel = dynamic_cast<Poco::LevelFilterChannel *>(&channel);
        verifyChannelConfig(*level_filter_channel->getChannel(), config);
        return;
    }
    if (typeid(channel) == typeid(Poco::SourceFilterChannel))
    {
        Poco::SourceFilterChannel * source_filter_channel = dynamic_cast<Poco::SourceFilterChannel *>(&channel);
        verifyChannelConfig(*source_filter_channel->getChannel(), config);
        return;
    }
    if (typeid(channel) == typeid(Poco::FormattingChannel))
    {
        Poco::FormattingChannel * formatting_channel = dynamic_cast<Poco::FormattingChannel *>(&channel);
        verifyChannelConfig(*formatting_channel->getChannel(), config);
    }
}

TEST_F(DaemonConfig_test, ReloadLoggerConfig)
try
{
    DB::Strings tests = {
        R"(
[application]
runAsDaemon = false
[profiles]
[profiles.default]
max_rows_in_set = 455
dt_page_gc_low_write_prob = 0.2
[logger]
errorlog = "./tmp/log/tiflash_error.log"
tracing_log = "./tmp/log/tiflash_tracing.log"
level = "debug"
log = "./tmp/log/tiflash.log"
size = "1K"
        )",
        R"(
[profiles]
[profiles.default]
max_rows_in_set = 455
dt_page_gc_low_write_prob = 0.2
[logger]
count = 20
errorlog = "./tmp/log/tiflash_error.log"
tracing_log = "./tmp/log/tiflash_tracing.log"
level = "debug"
log = "./tmp/log/tiflash.log"
size = "1M"
        )",
        R"(
[application]
runAsDaemon = false
[profiles]
[profiles.default]
max_rows_in_set = 455
dt_page_gc_low_write_prob = 0.2
[logger]
errorlog = "./tmp/log/tiflash_error.log"
tracing_log = "./tmp/log/tiflash_tracing.log"
level = "debug"
log = "./tmp/log/tiflash.log"
size = "1K"
        )",
        R"(
[profiles]
[profiles.default]
max_rows_in_set = 455
dt_page_gc_low_write_prob = 0.2
[logger]
count = 1
errorlog = "./tmp/log/tiflash_error.log"
tracing_log = "./tmp/log/tiflash_tracing.log"
level = "debug"
log = "./tmp/log/tiflash.log"
size = "1"
        )",
    };
    BaseDaemon app;

    auto verifyLoggersConfig = [](size_t logger_num, Poco::Util::AbstractConfiguration & config) {
        for (size_t j = 0; j < logger_num; j++)
        {
            Poco::Logger & cur_logger = Poco::Logger::get(fmt::format("ReloadLoggerConfig_test{}", j));
            ASSERT_NE(cur_logger.getChannel(), nullptr);
            Poco::Channel * cur_logger_channel = cur_logger.getChannel();
            ASSERT_EQ(typeid(*cur_logger_channel), typeid(Poco::ReloadableSplitterChannel));
            Poco::ReloadableSplitterChannel * splitter_channel = dynamic_cast<Poco::ReloadableSplitterChannel *>(cur_logger_channel);
            splitter_channel->setPropertiesValidator(verifyChannelConfig);
            splitter_channel->validateProperties(config);
        };
    };

    for (size_t i = 0; i < tests.size(); ++i)
    {
        const auto & test_case = tests[i];
        auto config = loadConfigFromString(test_case);
        app.buildLoggers(*config);
        Poco::Logger::get(fmt::format("ReloadLoggerConfig_test{}", i));
        LOG_INFO(log, "parsing [index=" << i << "] [content=" << test_case << "]");
        verifyLoggersConfig(i + 1, *config);
    }
    clearFiles("./tmp/log");
}
CATCH
