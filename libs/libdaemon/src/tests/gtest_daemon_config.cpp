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

#include <Common/Config/TOMLConfiguration.h>
#include <Core/Types.h>
#include <Poco/Ext/LevelFilterChannel.h>
#include <Poco/Ext/ReloadableSplitterChannel.h>
#include <Poco/Ext/SourceFilterChannel.h>
#include <Poco/Ext/TiFlashLogFileChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/config_common.h>
#include <cpptoml.h>
#include <daemon/BaseDaemon.h>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <limits.h>

#include <csignal>
class DaemonConfigTest : public ::testing::Test
{
public:
    DaemonConfigTest()
        : log(&Poco::Logger::get("DaemonConfigTest"))
    {}

    static void SetUpTestCase() {}

protected:
    Poco::Logger * log;
    static void clearFiles(String path)
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
        auto * file_channel = dynamic_cast<Poco::TiFlashLogFileChannel *>(&channel);
        ASSERT_EQ(
            file_channel->getProperty(Poco::FileChannel::PROP_ROTATION),
            config.getRawString("logger.size", "100M"));
        ASSERT_EQ(
            file_channel->getProperty(Poco::FileChannel::PROP_PURGECOUNT),
            config.getRawString("logger.count", "10"));
        return;
    }
    if (typeid(channel) == typeid(Poco::LevelFilterChannel))
    {
        auto * level_filter_channel = dynamic_cast<Poco::LevelFilterChannel *>(&channel);
        verifyChannelConfig(*level_filter_channel->getChannel(), config);
        return;
    }
    if (typeid(channel) == typeid(Poco::SourceFilterChannel))
    {
        auto * source_filter_channel = dynamic_cast<Poco::SourceFilterChannel *>(&channel);
        verifyChannelConfig(*source_filter_channel->getChannel(), config);
        return;
    }
    if (typeid(channel) == typeid(Poco::FormattingChannel))
    {
        auto * formatting_channel = dynamic_cast<Poco::FormattingChannel *>(&channel);
        verifyChannelConfig(*formatting_channel->getChannel(), config);
    }
}

namespace TiFlashUnwindTest
{
NO_INLINE int function_1()
{
    std::raise(SIGBUS);
    return 0;
}
NO_INLINE int function_2()
{
    int res = function_1();
    TIFLASH_NO_OPTIMIZE(res);
    return res;
}
NO_INLINE int function_3()
{
    int res = function_2();
    TIFLASH_NO_OPTIMIZE(res);
    return res;
}

} // namespace TiFlashUnwindTest

void removeFile(String path)
{
    Poco::File file(path);
    if (file.exists())
        file.remove(true);
}

struct TestDaemon : public BaseDaemon
{
    std::string config_path{};
    NO_INLINE int main(const std::vector<std::string> &) override
    {
        removeFile(config_path);
        TiFlashUnwindTest::function_3();
        return 0;
    }
};

TEST_F(DaemonConfigTest, ReloadLoggerConfig)
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

    auto verify_loggers_config = [](size_t logger_num, Poco::Util::AbstractConfiguration & config) {
        for (size_t j = 0; j < logger_num; j++)
        {
            Poco::Logger & cur_logger = Poco::Logger::get(fmt::format("ReloadLoggerConfig_test{}", j));
            ASSERT_NE(cur_logger.getChannel(), nullptr);
            Poco::Channel * cur_logger_channel = cur_logger.getChannel();
            ASSERT_EQ(typeid(*cur_logger_channel), typeid(Poco::ReloadableSplitterChannel));
            auto * splitter_channel = dynamic_cast<Poco::ReloadableSplitterChannel *>(cur_logger_channel);
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
        LOG_INFO(log, "parsing [index={}] [content={}]", i, test_case);
        verify_loggers_config(i + 1, *config);
    }
    clearFiles("./tmp/log");
}
CATCH

extern "C" char ** environ;

#if USE_UNWIND
TEST(BaseDaemon, StackUnwind)
{
    const auto * conf = R"(
[application]
runAsDaemon = false
[profiles]
[profiles.default]
max_rows_in_set = 455
dt_page_gc_low_write_prob = 0.2
[logger]
console = 1
level = "debug"
)";
    if (std::getenv("TIFLASH_DAEMON_TEST_UNWIND_CHILD"))
    {
        auto & listeners = ::testing::UnitTest::GetInstance()->listeners();
        delete listeners.Release(listeners.default_result_printer());
        TestDaemon app;
        auto config_name = fmt::format("/tmp/{}-{}.toml", reinterpret_cast<uintptr_t>(&app), ::time(nullptr));
        {
            auto config = std::ofstream(config_name);
            config << conf << std::endl;
        }
        char arg0[] = "";
        char arg1[] = "--config-file";
        char * argv[] = {arg0, arg1, config_name.data(), nullptr};
        app.config_path = config_name;
        app.run(3, argv);
        return;
    }
    char abs_path[PATH_MAX]{};
    if (ssize_t i_ret = ::readlink("/proc/self/exe", abs_path, sizeof(abs_path)); i_ret < 0)
        throw Poco::Exception("can not locate the abs path of binary");
    auto stdout_name = fmt::format("/tmp/{}-{}.stdout", reinterpret_cast<uintptr_t>(&abs_path), ::time(nullptr));
    auto stderr_name = fmt::format("/tmp/{}-{}.stderr", reinterpret_cast<uintptr_t>(&abs_path), ::time(nullptr));
    auto stdout_fd = ::open(stdout_name.c_str(), O_CREAT | O_TRUNC | O_RDWR, S_IRUSR | S_IWUSR);
    auto stderr_fd = ::open(stdout_name.c_str(), O_CREAT | O_TRUNC | O_RDWR, S_IRUSR | S_IWUSR);
    char arg[] = "--gtest_filter=BaseDaemon.StackUnwind";
    char * const argv[] = {abs_path, arg, nullptr};
    std::vector<char *> envs;
    std::string profile = "LLVM_PROFILE_FILE=default.profraw";
    std::string child_flag = "TIFLASH_DAEMON_TEST_UNWIND_CHILD=1";
    for (auto * i = environ; *i != nullptr; i++)
    {
        if (strstr(*i, "LLVM_PROFILE_FILE") != nullptr)
        {
            std::string file_path = *i;
            auto last = file_path.rfind('/');
            auto realpath = file_path.substr(0, last);
            profile = fmt::format("{}/daemon-child.profraw", realpath);
            std::cout << "LLVM profile: " << profile << std::endl;
        }
        else
        {
            envs.push_back(*i);
        }
    }
    envs.push_back(child_flag.data());
    envs.push_back(profile.data());
    envs.push_back(nullptr);
    auto pid = ::fork();
    if (pid == 0)
    {
        ::dup2(stdout_fd, STDOUT_FILENO);
        ::dup2(stderr_fd, STDERR_FILENO);
        ::execve(abs_path, argv, envs.data());
    }
    else
    {
        ::wait(nullptr);
        ::close(stdout_fd);
        ::close(stderr_fd);
        std::ifstream fin(stdout_name);
        std::stringstream stream{};
        stream << fin.rdbuf();
        auto data = stream.str();
        std::cout << data << std::endl;
        removeFile(stdout_name);
        removeFile(stderr_name);
        auto sigbus = data.find("Bus error");
        ASSERT_NE(sigbus, std::string::npos);
        auto function_1 = data.find("TiFlashUnwindTest::function_1()", sigbus + 1);
        ASSERT_NE(function_1, std::string::npos);
        auto function_2 = data.find("TiFlashUnwindTest::function_2()", function_1 + 1);
        ASSERT_NE(function_2, std::string::npos);
        auto function_3 = data.find("TiFlashUnwindTest::function_3()", function_2 + 1);
        ASSERT_NE(function_3, std::string::npos);
    }
}
#endif
