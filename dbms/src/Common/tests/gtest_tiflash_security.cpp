// Copyright 2022 PingCAP, Ltd.
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

#include <Common/TiFlashSecurity.h>
#include <TestUtils/ConfigTestUtils.h>
#include <gtest/gtest.h>

#include <ext/singleton.h>

namespace DB
{
namespace tests
{
class TestTiFlashSecurity : public ext::Singleton<TestTiFlashSecurity>
{
};

TEST(TestTiFlashSecurity, Config)
{
    TiFlashSecurityConfig tiflash_config;
    const auto log = Logger::get();
    tiflash_config.setLog(log);

    tiflash_config.parseAllowedCN(String("[abc,efg]"));
    ASSERT_EQ((int)tiflash_config.allowedCommonNames().count("abc"), 1);
    ASSERT_EQ((int)tiflash_config.allowedCommonNames().count("efg"), 1);

    tiflash_config.allowedCommonNames().clear();

    tiflash_config.parseAllowedCN(String(R"(["abc","efg"])"));
    ASSERT_EQ((int)tiflash_config.allowedCommonNames().count("abc"), 1);
    ASSERT_EQ((int)tiflash_config.allowedCommonNames().count("efg"), 1);

    tiflash_config.allowedCommonNames().clear();

    tiflash_config.parseAllowedCN(String("[ abc , efg ]"));
    ASSERT_EQ((int)tiflash_config.allowedCommonNames().count("abc"), 1);
    ASSERT_EQ((int)tiflash_config.allowedCommonNames().count("efg"), 1);

    tiflash_config.allowedCommonNames().clear();

    tiflash_config.parseAllowedCN(String(R"([ "abc", "efg" ])"));
    ASSERT_EQ((int)tiflash_config.allowedCommonNames().count("abc"), 1);
    ASSERT_EQ((int)tiflash_config.allowedCommonNames().count("efg"), 1);

    String test =
        R"(
[security]
ca_path="security/ca.pem"
cert_path="security/cert.pem"
key_path="security/key.pem"
cert_allowed_cn="tidb"
        )";
    auto new_config = loadConfigFromString(test);
    tiflash_config.update(*new_config);
    ASSERT_EQ((int)tiflash_config.allowedCommonNames().count("tidb"), 1);

    test =
        R"(
[security]
cert_allowed_cn="tidb"
        )";
    new_config = loadConfigFromString(test);
    auto new_tiflash_config = TiFlashSecurityConfig(*new_config, log);
    ASSERT_EQ((int)new_tiflash_config.allowedCommonNames().count("tidb"), 0);
}

TEST(TestTiFlashSecurity, Update)
{
    String test =
        R"(
[security]
cert_allowed_cn="tidb"
        )";

    auto config = loadConfigFromString(test);
    const auto log = Logger::get();

    TiFlashSecurityConfig tiflash_config(*config, log); // no TLS config is set
    const auto * new_test =
        R"(
[security]
ca_path="security/ca.pem"
cert_path="security/cert.pem"
key_path="security/key.pem"
cert_allowed_cn="tidb"
        )";
    config = loadConfigFromString(new_test);
    ASSERT_EQ(tiflash_config.update(*config), false); // can't add tls config online

    config = loadConfigFromString(new_test);
    TiFlashSecurityConfig tiflash_config_new(*config, log);
    test =
        R"(
        )";
    config = loadConfigFromString(test);
    ASSERT_EQ(tiflash_config.update(*config), false); // can't remove security config online
}
} // namespace tests
} // namespace DB
