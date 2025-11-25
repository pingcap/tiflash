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

#include <Storages/S3/S3Common.h>
#include <aws/core/http/Scheme.h>
#include <common/logger_useful.h>
#include <gtest/gtest.h>

#include <magic_enum.hpp>

namespace DB::S3::tests
{

TEST(S3CommonTest, updateRegionByEndpoint)
{
    LoggerPtr log = Logger::get();


    struct TestCase
    {
        String endpoint;
        String expected_region;
        Aws::Http::Scheme expected_scheme;
        bool use_virtual_address = true;
    };

    std::vector<TestCase> cases{
        // AWS endpoint
        TestCase{
            "http://s3.us-east-1.amazonaws.com",
            "us-east-1",
            Aws::Http::Scheme::HTTP,
        },
        TestCase{
            "https://s3.us-west-1.amazonaws.com",
            "us-west-1",
            Aws::Http::Scheme::HTTPS,
        },
        // AWS dualstack endpoint
        TestCase{
            "http://s3.dualstack.us-east-1.amazonaws.com",
            "us-east-1",
            Aws::Http::Scheme::HTTP,
        },
        // AWS fips endpoint
        TestCase{
            "https://s3-fips.us-east-1.amazonaws.com",
            "us-east-1",
            Aws::Http::Scheme::HTTPS,
        },
        // AWS fips dualstack endpoint
        TestCase{
            "https://s3-fips.dualstack.us-east-1.amazonaws.com",
            "us-east-1",
            Aws::Http::Scheme::HTTPS,
        },
        // Alibaba Cloud endpoint (internal)
        TestCase{
            "http://oss-ap-southeast-1-internal.aliyuncs.com",
            "ap-southeast-1",
            Aws::Http::Scheme::HTTP,
        },
        TestCase{
            "https://oss-eu-central-1-internal.aliyuncs.com",
            "eu-central-1",
            Aws::Http::Scheme::HTTPS,
        },
        // Alibaba Cloud endpoint (external)
        TestCase{
            "http://oss-ap-southeast-1.aliyuncs.com",
            "ap-southeast-1",
            Aws::Http::Scheme::HTTP,
        },
        TestCase{
            "https://oss-na-south-1.aliyuncs.com",
            "na-south-1",
            Aws::Http::Scheme::HTTPS,
        },
        // non-AWS endpoint
        TestCase{
            "minio.mydomain.com",
            "us-west-2",
            Aws::Http::Scheme::HTTP,
        },
        TestCase{
            "10.0.0.1:9000",
            "us-west-2",
            Aws::Http::Scheme::HTTP,
            false,
        },
    };

    String default_test_region = "us-west-2";
    for (const auto & c : cases)
    {
        Aws::Client::ClientConfiguration cfg(true, "standard", true);
        cfg.region = default_test_region;
        cfg.endpointOverride = c.endpoint;
        bool use_virtual_address = updateRegionByEndpoint(cfg, log);
        ASSERT_EQ(cfg.region, c.expected_region) << c.endpoint;
        ASSERT_EQ(cfg.scheme, c.expected_scheme) << c.endpoint;
        ASSERT_EQ(cfg.verifySSL, c.expected_scheme == Aws::Http::Scheme::HTTPS) << c.endpoint;
        ASSERT_EQ(use_virtual_address, c.use_virtual_address) << c.endpoint;
    }
}

} // namespace DB::S3::tests
