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
    };

    std::vector<TestCase> cases{
        // AWS endpoint
        TestCase{
            "http://s3.us-east-1.amazonaws.com",
            "us-east-1",
        },
        TestCase{
            "https://s3.us-west-1.amazonaws.com",
            "us-west-1",
        },
        // AWS dualstack endpoint
        TestCase{
            "http://s3.dualstack.us-east-1.amazonaws.com",
            "us-east-1",
        },
        // AWS fips endpoint
        TestCase{
            "https://s3-fips.us-east-1.amazonaws.com",
            "us-east-1",
        },
        // AWS fips dualstack endpoint
        TestCase{
            "https://s3-fips.dualstack.us-east-1.amazonaws.com",
            "us-east-1",
        },
        // Alibaba Cloud endpoint (internal)
        TestCase{
            "http://oss-ap-southeast-1-internal.aliyuncs.com",
            "ap-southeast-1",
        },
        TestCase{
            "https://oss-eu-central-1-internal.aliyuncs.com",
            "eu-central-1",
        },
        // Alibaba Cloud endpoint (external)
        TestCase{
            "http://oss-ap-southeast-1.aliyuncs.com",
            "ap-southeast-1",
        },
        TestCase{
            "https://oss-na-south-1.aliyuncs.com",
            "na-south-1",
        },
        // non-AWS endpoint
        TestCase{
            "minio.mydomain.com",
            "us-west-2",
        },
    };

    String default_test_region = "us-west-2";
    for (const auto & c : cases)
    {
        Aws::Client::ClientConfiguration cfg;
        cfg.region = default_test_region;
        cfg.endpointOverride = c.endpoint;
        updateRegionByEndpoint(cfg, log);
        ASSERT_EQ(cfg.region, c.expected_region) << c.endpoint;
    }
}

} // namespace DB::S3::tests
