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

#include <Common/Exception.h>
#include <Common/TiFlashSecurity.h>
#include <Poco/File.h>
#include <Poco/FileStream.h>
#include <TestUtils/ConfigTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

#include <ext/singleton.h>

namespace DB::tests
{

TEST(TiFlashSecurityTest, Config)
{
    {
        auto cns = TiFlashSecurityConfig::parseAllowedCN(String("[abc,efg]"));
        ASSERT_EQ(cns.count("abc"), 1);
        ASSERT_EQ(cns.count("efg"), 1);
    }

    {
        auto cns = TiFlashSecurityConfig::parseAllowedCN(String(R"(["abc","efg"])"));
        ASSERT_EQ(cns.count("abc"), 1);
        ASSERT_EQ(cns.count("efg"), 1);
    }

    {
        auto cns = TiFlashSecurityConfig::parseAllowedCN(String("[ abc , efg ]"));
        ASSERT_EQ(cns.count("abc"), 1);
        ASSERT_EQ(cns.count("efg"), 1);
    }

    {
        auto cns = TiFlashSecurityConfig::parseAllowedCN(String(R"([ "abc", "efg" ])"));
        ASSERT_EQ(cns.count("abc"), 1);
        ASSERT_EQ(cns.count("efg"), 1);
    }

    const auto log = Logger::get();

    {
        auto new_config = loadConfigFromString(R"(
[security]
ca_path="security/ca.pem"
cert_path="security/cert.pem"
key_path="security/key.pem"
cert_allowed_cn="tidb"
        )");
        TiFlashSecurityConfig tiflash_config(log);
        tiflash_config.init(*new_config);
        ASSERT_TRUE(tiflash_config.hasTlsConfig());
        ASSERT_EQ(tiflash_config.allowedCommonNames().count("tidb"), 1);
    }

    {
        auto new_config = loadConfigFromString(R"(
[security]
cert_allowed_cn="tidb"
        )");
        auto new_tiflash_config = TiFlashSecurityConfig(log);
        new_tiflash_config.init(*new_config);
        ASSERT_FALSE(new_tiflash_config.hasTlsConfig());
        // allowed common names is ignore when tls is not enabled
        ASSERT_EQ(new_tiflash_config.allowedCommonNames().count("tidb"), 0);
    }
}

TEST(TiFlashSecurityTest, EmptyConfig)
try
{
    const auto log = Logger::get();

    for (const auto & c : Strings{
             // empty strings
             R"([security]
ca_path=""
cert_path=""
key_path="")",
             // non-empty strings with space only
             R"([security]
ca_path="  "
cert_path=""
key_path="")",
         })
    {
        TiFlashSecurityConfig tiflash_config(log);
        auto new_config = loadConfigFromString(c);
        tiflash_config.init(*new_config);
        ASSERT_FALSE(tiflash_config.hasTlsConfig()) << fmt::format("case: {}", c);
    }
}
CATCH

TEST(TiFlashSecurityTest, InvalidConfig)
try
{
    const auto log = Logger::get();

    for (const auto & c : Strings{
             // only a part of ssl path is set
             R"([security]
ca_path="security/ca.pem"
cert_path=""
key_path="")",
             R"([security]
ca_path=""
cert_path="security/cert.pem"
key_path="")",
             R"([security]
ca_path=""
cert_path=""
key_path="security/key.pem")",
             R"([security]
ca_path=""
cert_path="security/cert.pem"
key_path="security/key.pem")",
             // comment out
             R"([security]
ca_path="security/ca.pem"
#cert_path="security/cert.pem"
key_path="security/key.pem")",
         })
    {
        TiFlashSecurityConfig tiflash_config(log);
        auto new_config = loadConfigFromString(c);
        try
        {
            tiflash_config.init(*new_config);
            ASSERT_FALSE(true) << fmt::format("should raise exception, case: {}", c);
        }
        catch (Exception & e)
        {
            // has_tls remain false when exception raise
            ASSERT_FALSE(tiflash_config.hasTlsConfig()) << fmt::format("case: {}", c);
            // the error code must be INVALID_CONFIG_PARAMETER
            ASSERT_EQ(e.code(), ErrorCodes::INVALID_CONFIG_PARAMETER) << fmt::format("case: {}", c);
        }
    }
}
CATCH

TEST(TiFlashSecurityTest, RedactLogConfig)
{
    for (const auto & [input, expect] : std::vector<std::pair<String, RedactMode>>{
             {"marker", RedactMode::Marker},
             {"Marker", RedactMode::Marker},
             {"MARKER", RedactMode::Marker},
             {"true", RedactMode::Enable},
             {"True", RedactMode::Enable},
             {"TRUE", RedactMode::Enable},
             {"yes", RedactMode::Enable},
             {"on", RedactMode::Enable},
             {"1", RedactMode::Enable},
             {"2", RedactMode::Enable},
             {"false", RedactMode::Disable},
             {"False", RedactMode::Disable},
             {"FALSE", RedactMode::Disable},
             {"no", RedactMode::Disable},
             {"off", RedactMode::Disable},
             {"0", RedactMode::Disable},
         })
    {
        EXPECT_EQ(TiFlashSecurityConfig::parseRedactLog(input), expect);
    }
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

String createConfigString(String & ca_path, String & cert_path, String & key_path)
{
    String ret =
        R"(
[security]
ca_path=")";
    ret += ca_path;
    ret += R"("
cert_path=")";
    ret += cert_path;
    ret += R"("
key_path=")";
    ret += key_path;
    ret += R"("
)";
    return ret;
}

TEST(TiFlashSecurityTest, readAndCacheSslCredentialOptions)
{
    const auto & test_info = testing::UnitTest::GetInstance()->current_test_info();
    assert(test_info);
    String file_name = test_info->file();
    auto pos = file_name.find_last_of('/');
    auto file_path = file_name.substr(0, pos);
    // these certs are copied from https://github.com/grpc/grpc/tree/v1.64.0/src/python/grpcio_tests/tests/unit/credentials
    auto ca_path = file_path + "/tls/ca.crt";
    auto cert_path = file_path + "/tls/cert.crt";
    auto key_path = file_path + "/tls/key.pem";
    auto test = createConfigString(ca_path, cert_path, key_path);
    auto config = loadConfigFromString(test);
    const auto log = Logger::get();
    TiFlashSecurityConfig tiflash_config(log);
    tiflash_config.init(*config);
    // first read will return a valid options
    auto options = tiflash_config.readAndCacheSslCredentialOptions();
    ASSERT_TRUE(options.has_value());
    // not return valid options if cert is not changed
    auto new_options = tiflash_config.readAndCacheSslCredentialOptions();
    ASSERT_FALSE(new_options.has_value());
    ca_path = file_path + "/tls/ca.crt.new";
    cert_path = file_path + "/tls/cert.crt.new";
    key_path = file_path + "/tls/key.pem.new";
    {
        Poco::FileOutputStream ca_fos(ca_path);
        ca_fos << options.value().pem_root_certs;
        Poco::FileOutputStream cert_fos(cert_path);
        cert_fos << options.value().pem_cert_chain;
        Poco::FileOutputStream key_fos(key_path);
        key_fos << options.value().pem_private_key;
    }
    test = createConfigString(ca_path, cert_path, key_path);
    config = loadConfigFromString(test);
    ASSERT_TRUE(tiflash_config.update(*config));
    // return a valid options if cert is changed
    options = tiflash_config.readAndCacheSslCredentialOptions();
    ASSERT_TRUE(options.has_value());
    new_options = tiflash_config.readAndCacheSslCredentialOptions();
    ASSERT_FALSE(new_options.has_value());
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(1s);
    {
        // change the modified time
        Poco::FileOutputStream fos(ca_path);
        fos << options.value().pem_root_certs;
    }
    new_options = tiflash_config.readAndCacheSslCredentialOptions();
    // no aware of the change since `update` is not called
    ASSERT_FALSE(new_options.has_value());
    ASSERT_TRUE(tiflash_config.update(*config));
    // aware of the change and return a new options
    new_options = tiflash_config.readAndCacheSslCredentialOptions();
    ASSERT_TRUE(new_options.has_value());
    Poco::File ca_file(ca_path);
    ca_file.remove(false);
    Poco::File cert_file(cert_path);
    cert_file.remove(false);
    Poco::File key_file(key_path);
    key_file.remove(false);
}

} // namespace DB::tests
