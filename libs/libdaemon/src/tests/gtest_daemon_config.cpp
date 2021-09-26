#include <Common/Config/TOMLConfiguration.h>
#include <Core/Types.h>
#include <Poco/Ext/LevelFilterChannel.h>
#include <Poco/Ext/ReloadableSplitterChannel.h>
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
    if (typeid(channel) == typeid(DB::TiFlashLogFileChannel))
    {
        DB::TiFlashLogFileChannel * fileChannel = dynamic_cast<DB::TiFlashLogFileChannel *>(&channel);
        ASSERT_EQ(fileChannel->getProperty(Poco::FileChannel::PROP_ROTATION), config.getRawString("logger.size", "100M"));
        ASSERT_EQ(fileChannel->getProperty(Poco::FileChannel::PROP_PURGECOUNT), config.getRawString("logger.count", "1"));
        return;
    }
    if (typeid(channel) == typeid(Poco::LevelFilterChannel))
    {
        Poco::LevelFilterChannel * levelFilterChannel = dynamic_cast<Poco::LevelFilterChannel *>(&channel);
        verifyChannelConfig(*levelFilterChannel->getChannel(), config);
        return;
    }
    if (typeid(channel) == typeid(Poco::FormattingChannel))
    {
        Poco::FormattingChannel * formattingChannel = dynamic_cast<Poco::FormattingChannel *>(&channel);
        verifyChannelConfig(*formattingChannel->getChannel(), config);
    }
}

TEST_F(DaemonConfig_test, ReloadLoggerConfig)
try
{
    DB::Strings tests = {
        R"(
[profiles]
[profiles.default]
max_rows_in_set = 455
dt_page_gc_low_write_prob = 0.2
[logger]
count = 20
errorlog = "./tmp/log/tiflash_error.log"
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
count = 10
errorlog = "./tmp/log/tiflash_error.log"
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
            ASSERT_EQ(typeid(*cur_logger_channel), typeid(DB::ReloadableSplitterChannel));
            DB::ReloadableSplitterChannel * splitter_channel = dynamic_cast<DB::ReloadableSplitterChannel *>(cur_logger_channel);
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
