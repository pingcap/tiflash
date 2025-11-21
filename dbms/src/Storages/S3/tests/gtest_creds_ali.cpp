// Copyright 2025 PingCAP, Inc.
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

#include <Storages/S3/CredentialsAliCloud.h>
#include <common/logger_useful.h>
#include <gtest/gtest.h>

#include <magic_enum.hpp>

namespace DB::S3::tests
{

TEST(ECSRAMRoleCredsTest, ParseResp)
{
    String resp_body = R"({
        "AccessKeyId" : "STS.test-access-key-id",
        "AccessKeySecret" : "test-access-key-secret",
        "Expiration" : "2025-10-10T13:55:03Z",
        "SecurityToken" : "test-security-token",
        "LastUpdated" : "2025-10-10T07:41:09Z",
        "Code" : "Success"
    })";

    auto [cred, err_msg] = DB::S3::AlibabaCloud::ECSRAMRoleCredentialsProvider::parseFromResponse(resp_body);
    ASSERT_TRUE(err_msg.empty()) << err_msg;
    ASSERT_EQ(cred.GetAWSAccessKeyId(), "STS.test-access-key-id");
    ASSERT_EQ(cred.GetAWSSecretKey(), "test-access-key-secret");
    ASSERT_EQ(cred.GetSessionToken(), "test-security-token");
    ASSERT_EQ(cred.GetExpiration().ToGmtString(Aws::Utils::DateFormat::ISO_8601), "2025-10-10T13:55:03Z");

    std::vector<String> invalid_resps = {
        R"({invalid json})",
        R"({"Code" : "Failed"})", //
        R"({"AccessKeyId" : "STS.test-access-key-id"})", // missing fields
        R"({"AccessKeyId" : "", "AccessKeySecret" : "test-access-key-secret", "Expiration" : "2025-10-10T13:55:03Z", "SecurityToken" : "test-security-token", "LastUpdated" : "2025-10-10T07:41:09Z", "Code" : "Success"})", // empty AccessKeyId
    };

    for (const auto & resp : invalid_resps)
    {
        auto [cred, err_msg] = DB::S3::AlibabaCloud::ECSRAMRoleCredentialsProvider::parseFromResponse(resp);
        ASSERT_FALSE(err_msg.empty()) << resp;
        ASSERT_TRUE(cred.IsEmpty()) << resp;
        LOG_INFO(Logger::get(), "Expected error: {} while parsing {}", err_msg, resp);
    }
}

} // namespace DB::S3::tests
