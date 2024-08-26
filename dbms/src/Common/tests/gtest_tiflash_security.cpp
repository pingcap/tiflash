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
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

#include <ext/singleton.h>

namespace DB::tests
{

TEST(TestTiFlashSecurity, Config)
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
        SCOPED_TRACE(fmt::format("case: {}", c));
        auto new_config = loadConfigFromString(c);
        TiFlashSecurityConfig tiflash_config(new_config, log);
        ASSERT_FALSE(tiflash_config.hasTlsConfig());
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
        SCOPED_TRACE(fmt::format("case: {}", c));
        auto new_config = loadConfigFromString(c);
        try
        {
            TiFlashSecurityConfig tiflash_config(new_config, log);
            ASSERT_FALSE(true) << "should raise exception";
        }
        catch (Exception & e)
        {
            // has_tls remains false when an exception raise
            ASSERT_FALSE(tiflash_config.hasTlsConfig());
            // the error code must be INVALID_CONFIG_PARAMETER
            ASSERT_EQ(e.code(), ErrorCodes::INVALID_CONFIG_PARAMETER);
        }
    }
}
CATCH

} // namespace DB::tests
