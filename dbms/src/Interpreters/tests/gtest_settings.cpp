// Copyright 2026 PingCAP, Inc.
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

#include <Interpreters/Settings.h>
#include <TestUtils/ConfigTestUtils.h>
#include <gtest/gtest.h>

namespace DB::tests
{

TEST(SettingsConfigTest, RemovedProfileSetting)
{
    for (const auto & value : {"0", "true"})
    {
        SCOPED_TRACE(value);
        auto config = loadConfigFromString(fmt::format(
            R"(
[profiles.legacy]
use_uncompressed_cache = {}
[profiles.default]
profile = "legacy"
max_memory_usage = 123456
dt_enable_rough_set_filter = false
)",
            value));

        Settings settings;
        ASSERT_NO_THROW(settings.setProfile("default", *config));
        EXPECT_EQ(settings.max_memory_usage.getActualBytes(0), 123456UL);
        EXPECT_FALSE(settings.dt_enable_rough_set_filter);
    }
}

TEST(SettingsConfigTest, UnknownConfigSetting)
{
    auto config = loadConfigFromString(R"(
[settings]
a_unknown_setting = true
max_memory_usage = 123456
)");

    Settings settings;
    ASSERT_NO_THROW(settings.loadSettingsFromConfig("settings", *config));
    EXPECT_EQ(settings.max_memory_usage.getActualBytes(0), 123456UL);
}

TEST(SettingsConfigTest, InvalidKnownSetting)
{
    auto config = loadConfigFromString(R"(
[profiles.default]
dt_enable_rough_set_filter = "invalid"
)");

    Settings settings;
    EXPECT_THROW(settings.setProfile("default", *config), Exception);
    EXPECT_THROW(settings.loadSettingsFromConfig("profiles.default", *config), Exception);
}

} // namespace DB::tests
