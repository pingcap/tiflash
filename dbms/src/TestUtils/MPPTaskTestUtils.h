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

#pragma once

#include <Server/FlashGrpcServerHolder.h>
#include <TestUtils/ExecutorTestUtils.h>

namespace DB::tests
{

class MPPTaskTestUtils : public ExecutorTest
{
public:
    static void SetUpTestCase()
    {
        ExecutorTest::SetUpTestCase();
        TiFlashSecurityConfig security_config;
        TiFlashRaftConfig raft_config;
        raft_config.flash_server_addr = "0.0.0.0:3930"; // TODO:: each FlashGrpcServer should have unique addr.
        Poco::AutoPtr<Poco::Util::LayeredConfiguration> config = new Poco::Util::LayeredConfiguration;
        log_ptr = Logger::get("compute_test");
        compute_server_ptr = std::make_unique<FlashGrpcServerHolder>(TiFlashTestEnv::getGlobalContext(), *config, security_config, raft_config, log_ptr);
    }

    static void TearDownTestCase()
    {
        compute_server_ptr.reset();
    }

protected:
    // TODO: Mock a simple storage layer to store test input.
    // Currently the lifetime of a server is held in this scope.
    // TODO: Add ComputeServerManager to maintain the lifetime of a bunch of servers.
    // Note: May go through GRPC fail number 14 --> socket closed,
    // if you start a server, send a request to the server using pingcap::kv::RpcClient,
    // then close the server and start the server using the same addr,
    // then send a request to the new server using pingcap::kv::RpcClient.
    static std::unique_ptr<FlashGrpcServerHolder> compute_server_ptr;
    static LoggerPtr log_ptr;
};

std::unique_ptr<FlashGrpcServerHolder> MPPTaskTestUtils::compute_server_ptr = nullptr;
LoggerPtr MPPTaskTestUtils::log_ptr = nullptr;


#define ASSERT_MPPTASK_EQUAL(tasks, expect_cols)                                          \
    TiFlashTestEnv::getGlobalContext().setColumnsForTest(context.executorIdColumnsMap()); \
    TiFlashTestEnv::getGlobalContext().setMPPTest();                                      \
    ASSERT_COLUMNS_EQ_UR(executeMPPTasks(tasks), expected_cols);

} // namespace DB::tests
