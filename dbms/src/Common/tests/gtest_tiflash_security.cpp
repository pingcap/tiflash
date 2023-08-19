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

#include <Common/TiFlashSecurity.h>
#include <TestUtils/ConfigTestUtils.h>
#include <gtest/gtest.h>

#include <ext/singleton.h>

namespace DB
{
namespace tests
{
class TiFlashSecurityTest : public ext::Singleton<TiFlashSecurityTest>
{
};

TEST(TiFlashSecurityTest, Config)
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
    auto new_tiflash_config = TiFlashSecurityConfig(log);
    new_tiflash_config.init(*new_config);
    ASSERT_EQ((int)new_tiflash_config.allowedCommonNames().count("tidb"), 0);
}

TEST(TiFlashSecurityTest, Update)
{
    String test =
        R"(
[security]
cert_allowed_cn="tidb"
        )";

    auto config = loadConfigFromString(test);
    const auto log = Logger::get();

    TiFlashSecurityConfig tiflash_config(log); // no TLS config is set
    tiflash_config.init(*config);
    test =
        R"(
[security]
ca_path="security/ca.pem"
cert_path="security/cert.pem"
key_path="security/key.pem"
cert_allowed_cn="tidb"
        )";
    config = loadConfigFromString(test);
    ASSERT_FALSE(tiflash_config.update(*config)); // Can't add tls config online
    ASSERT_FALSE(tiflash_config.hasTlsConfig());
    config = loadConfigFromString(test);
    TiFlashSecurityConfig tiflash_config_1(log);
    tiflash_config_1.init(*config);
    test =
        R"(
        )";
    config = loadConfigFromString(test);
    ASSERT_FALSE(tiflash_config_1.update(*config)); // Can't remove security config online
    ASSERT_TRUE(tiflash_config_1.hasTlsConfig());

    test =
        R"(
[security]
cert_allowed_cn="tidb"
        )";
    config = loadConfigFromString(test);
    ASSERT_FALSE(tiflash_config_1.update(*config)); // Can't remove tls config online
    ASSERT_TRUE(tiflash_config_1.hasTlsConfig());

    test =
        R"(
[security]
ca_path="security/ca_new.pem"
cert_path="security/cert_new.pem"
key_path="security/key_new.pem"
cert_allowed_cn="tidb"
        )";
    config = loadConfigFromString(test);
    ASSERT_TRUE(tiflash_config_1.update(*config));
    auto paths = tiflash_config_1.getPaths();
    ASSERT_EQ(std::get<0>(paths), "security/ca_new.pem");
    ASSERT_EQ(std::get<1>(paths), "security/cert_new.pem");
    ASSERT_EQ(std::get<2>(paths), "security/key_new.pem");
    ASSERT_EQ((int)tiflash_config_1.allowedCommonNames().count("tidb"), 1);
    ASSERT_EQ((int)tiflash_config_1.allowedCommonNames().count("tiflash"), 0);

    // add cert allowed cn
    test =
        R"(
[security]
ca_path="security/ca_new.pem"
cert_path="security/cert_new.pem"
key_path="security/key_new.pem"
cert_allowed_cn="[tidb, tiflash]"
        )";
    config = loadConfigFromString(test);
    ASSERT_FALSE(tiflash_config_1.update(*config));
    paths = tiflash_config_1.getPaths();
    ASSERT_EQ(std::get<0>(paths), "security/ca_new.pem");
    ASSERT_EQ(std::get<1>(paths), "security/cert_new.pem");
    ASSERT_EQ(std::get<2>(paths), "security/key_new.pem");
    ASSERT_EQ((int)tiflash_config_1.allowedCommonNames().count("tidb"), 1);
    ASSERT_EQ((int)tiflash_config_1.allowedCommonNames().count("tiflash"), 1);

    // Without security config
    test =
        R"(
        )";
    config = loadConfigFromString(test);
    TiFlashSecurityConfig tiflash_config_2(log);
    tiflash_config_2.init(*config);

    test =
        R"(
[security]
cert_allowed_cn="[tidb, tiflash]"
        )";

    config = loadConfigFromString(test);
    ASSERT_FALSE(tiflash_config_2.update(*config)); //Can't add security config online
    ASSERT_TRUE(tiflash_config_2.allowedCommonNames().empty());
    ASSERT_FALSE(tiflash_config_2.hasTlsConfig());

    test =
        R"(
[security]
ca_path="security/ca_new.pem"
cert_path="security/cert_new.pem"
key_path="security/key_new.pem"
cert_allowed_cn="[tidb, tiflash]"
redact_info_log=false
        )";
    config = loadConfigFromString(test);
    ASSERT_FALSE(tiflash_config_2.update(*config)); // Can't add security config online
    ASSERT_TRUE(tiflash_config_2.allowedCommonNames().empty());
    ASSERT_FALSE(tiflash_config_2.hasTlsConfig());
}
} // namespace tests
} // namespace DB
