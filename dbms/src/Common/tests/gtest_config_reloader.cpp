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

#include <Common/Config/ConfigReloader.h>
#include <Poco/File.h>
#include <Poco/FileStream.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

#include <chrono>
#include <ext/singleton.h>
#include <memory>
#include <thread>

namespace DB
{
namespace tests
{
class ConfigReloaderTest : public ext::Singleton<ConfigReloaderTest>
{
};

TEST(ConfigReloaderTest, Basic)
{
    auto path = DB::tests::TiFlashTestEnv::getTemporaryPath("ConfigReloaderTest_Basic");
    Poco::File file(path);
    if (file.exists())
        file.remove();
    file.createFile();
    Poco::FileOutputStream stream(path);
    stream << R"(
[security]
ca_path="security/ca.pem"
cert_path="security/cert.pem"
key_path="security/key.pem"
cert_allowed_cn="tidb"
        )";

    int call_times = 0;
    {
        auto main_config_reloader = std::make_unique<ConfigReloader>(
            path,
            [&](ConfigurationPtr config [[maybe_unused]]) { ++call_times; },
            /* already_loaded = */ false);

        auto other_config_reloader = std::make_unique<ConfigReloader>(
            path,
            [&](ConfigurationPtr config [[maybe_unused]]) { ++call_times; },
            /* already_loaded = */ false,
            "otherCfgLoader");

        main_config_reloader->start();
        other_config_reloader->start();
        std::this_thread::sleep_for(std::chrono::seconds(3));
    }
    ASSERT_EQ(call_times, 2);
}

class TestConfigObject : public ConfigObject
{
public:
    bool fileUpdated() override
    {
        if (!inited)
        {
            inited = true;
            return true;
        }
        return false;
    }

private:
    bool inited = false;
};

TEST(ConfigReloaderTest, WithConfigObject)
{
    auto path = DB::tests::TiFlashTestEnv::getTemporaryPath("ConfigReloaderTest_WithConfigObject");
    Poco::File file(path);
    if (file.exists())
        file.remove();
    file.createFile();
    Poco::FileOutputStream stream(path);
    stream << R"(
        [profiles]
[profiles.default]
max_memory_usage = 0
        )";

    int call_times = 0;
    {
        auto main_config_reloader = std::make_unique<ConfigReloader>(
            path,
            [&](ConfigurationPtr config [[maybe_unused]]) { ++call_times; },
            /* already_loaded = */ false);
        main_config_reloader->addConfigObject(std::make_shared<TestConfigObject>());
        main_config_reloader->start();
        std::this_thread::sleep_for(std::chrono::seconds(3));
    }
    ASSERT_EQ(call_times, 2);
}
} // namespace tests
} // namespace DB
