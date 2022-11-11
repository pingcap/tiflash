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
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

#include <ext/singleton.h>

namespace DB
{
namespace tests
{
class TiFlashSecurityConfigTest : public ::testing::Test
{
public:
    TiFlashSecurityConfigTest()
        : log(Logger::get("TiFlashSecurityConfigTest"))
    {}

    static void SetUpTestCase() {}

protected:
    LoggerPtr log;
};

TEST_F(TiFlashSecurityConfigTest, Config)
{
    TiFlashSecurityConfig config;
    config.parseAllowedCN(String("[abc,efg]"));
    ASSERT_EQ((int)config.allowed_common_names.count("abc"), 1);
    ASSERT_EQ((int)config.allowed_common_names.count("efg"), 1);

    config.allowed_common_names.clear();

    config.parseAllowedCN(String("[\"abc\",\"efg\"]"));
    ASSERT_EQ((int)config.allowed_common_names.count("abc"), 1);
    ASSERT_EQ((int)config.allowed_common_names.count("efg"), 1);

    config.allowed_common_names.clear();

    config.parseAllowedCN(String("[ abc , efg ]"));
    ASSERT_EQ((int)config.allowed_common_names.count("abc"), 1);
    ASSERT_EQ((int)config.allowed_common_names.count("efg"), 1);

    config.allowed_common_names.clear();

    config.parseAllowedCN(String("[ \"abc\", \"efg\" ]"));
    ASSERT_EQ((int)config.allowed_common_names.count("abc"), 1);
    ASSERT_EQ((int)config.allowed_common_names.count("efg"), 1);
}

TEST_F(TiFlashSecurityConfigTest, ShouldUpdate)
try
{
    Strings tests = {
        // Deprecated style
        R"(
[security]
ca_path="security/ca.pem"
cert_path="security/cert.pem"
key_path="security/key.pem"
        )",
    };

    TiFlashSecurityConfig old_security_config;
    old_security_config.ca_path = "security/ca2.pem";
    old_security_config.cert_path = "test/security/cert2.pem";
    old_security_config.key_path = "security/key2.pem";
    old_security_config.inited = true;
    old_security_config.options.pem_root_certs = "1.1";
    old_security_config.options.pem_cert_chain = "1.2";
    old_security_config.options.pem_private_key = "1.3";

    for (size_t i = 0; i < tests.size(); ++i)
    {
        const auto & test_case = tests[i];
        auto config = loadConfigFromString(test_case);

        LOG_INFO(log, "parsing [index={}] [content={}]", i, test_case);

        TiFlashSecurityConfig security_config = TiFlashSecurityConfig(*config, log);
        security_config.inited = true;
        security_config.options.pem_root_certs = "2.1";
        security_config.options.pem_cert_chain = "2.2";
        security_config.options.pem_private_key = "2.3";
        grpc::SslCredentialsOptions options = security_config.readSecurityInfo();

        LOG_INFO(log, "read cert option [root_certs={}] [cert_chain={}] [private_key={}]", options.pem_root_certs, options.pem_cert_chain, options.pem_private_key);
    }
}
CATCH
} // namespace tests
} // namespace DB
