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

#include <Common/Config/ConfigProcessor.h>
#include <Common/Exception.h>
#include <Poco/Environment.h>
#include <Poco/Logger.h>
#include <Server/RaftConfigParser.h>
#include <TestUtils/ConfigTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <cpptoml.h>

namespace DB::tests
{

class RaftConfigTest : public ::testing::Test
{
public:
    RaftConfigTest()
        : log(Logger::get())
    {}

    static void SetUpTestCase() {}

protected:
    LoggerPtr log;
};

TEST_F(RaftConfigTest, SimpleSinglePath)
try
{
    Strings tests = {
        R"(
flash.service_addr = "0.0.0.0:3930"
        )",
        R"(
flash.service_addr = "0.0.0.0:3930"
flash.proxy.engine-addr = "172.16.5.85:3930"
        )",
        R"(
flash.service_addr = "0.0.0.0:3930"
flash.proxy.engine-addr = "172.16.5.85:3930"
flash.proxy.advertise-engine-addr = "h85:3930"
        )",
    };

    for (size_t i = 0; i < tests.size(); ++i)
    {
        const auto & test_case = tests[i];
        auto config = loadConfigFromString(test_case);
        LOG_INFO(log, "parsing, index={} content={}", i, test_case);

        const TiFlashRaftConfig raft_config = TiFlashRaftConfig::parseSettings(*config, log);
        switch (i)
        {
        case 0:
        {
            ASSERT_EQ(raft_config.flash_server_addr, "0.0.0.0:3930");
            ASSERT_EQ(raft_config.advertise_engine_addr, "0.0.0.0:3930");
            break;
        }
        case 1:
        {
            ASSERT_EQ(raft_config.flash_server_addr, "0.0.0.0:3930");
            ASSERT_EQ(raft_config.advertise_engine_addr, "172.16.5.85:3930");
            break;
        }
        case 2:
        {
            ASSERT_EQ(raft_config.flash_server_addr, "0.0.0.0:3930");
            ASSERT_EQ(raft_config.advertise_engine_addr, "h85:3930");
            break;
        }
        default:
            RUNTIME_CHECK(false, test_case);
        }
    }
}
CATCH


} // namespace DB::tests
