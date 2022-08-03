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

#include <cstddef>
#include <memory>
#include <unordered_map>

#include "Core/ColumnsWithTypeAndName.h"

namespace DB::tests
{

struct MockServerConfig
{
    String name;
    String addr;
};
class MockComputeServerManager
{
public:
    void addServerConfig(MockServerConfig config)
    {
        server_config_map[config.name] = config;
    }

    void startAllServer(const LoggerPtr & log_ptr)
    {
        for (const auto & kv : server_config_map)
        {
            TiFlashSecurityConfig security_config;
            TiFlashRaftConfig raft_config;
            raft_config.flash_server_addr = kv.second.addr;
            Poco::AutoPtr<Poco::Util::LayeredConfiguration> config = new Poco::Util::LayeredConfiguration;
            addServer(kv.first, std::make_unique<FlashGrpcServerHolder>(TiFlashTestEnv::getGlobalContext(), *config, security_config, raft_config, log_ptr));
        }
    }

    void reset()
    {
        server_map.clear();
    }

private:
    void addServer(String name, std::unique_ptr<FlashGrpcServerHolder> server)
    {
        server_map[name] = std::move(server);
    }

private:
    std::unordered_map<String, std::unique_ptr<FlashGrpcServerHolder>> server_map;
    std::unordered_map<String, MockServerConfig> server_config_map;
};

class MockStorage
{
    // std::vector<std::unordered_map<String, ColumnsWithTypeAndName>> splitColumnsByPartitionNum(Int64 table_id, size_t mpp_partition_num)
    // {
    //     std::vector<std::unordered_map<String, ColumnsWithTypeAndName>> res;


    //     for (auto kv : columns_for_test_map)
    //     {
    //         for (size_t i = 0; i < mpp_partition_num; ++i)
    //         {
    //             ColumnsWithTypeAndName columns_for_each_partition;
    //             for (auto col : kv.second)
    //             {
    //                 columns_for_each_partition.push_back(
    //                     ColumnWithTypeAndName(
    //                         col.column->cut(start, row_for_current_stream),
    //                         col.type,
    //                         col.name));
    //             }
    //         }
    //     }


    //     return res;
    // }

private:

    std::unordered_map<String, ColumnsWithTypeAndName> columns_for_test_map; /// <exector_id, columns>, for multiple sources
};

class MPPTaskTestUtils : public ExecutorTest
{
public:
    static void SetUpTestCase()
    {
        ExecutorTest::SetUpTestCase();
        log_ptr = Logger::get("compute_test");
        auto size = std::thread::hardware_concurrency();
        GRPCCompletionQueuePool::global_instance = std::make_unique<GRPCCompletionQueuePool>(size);
    }

    static void TearDownTestCase()
    {
        server_manager.reset();
    }

protected:
    // TODO: Mock a simple storage layer to store test input.
    // Currently the lifetime of a server is held in this scope.
    // TODO: Add ComputeServerManager to maintain the lifetime of a bunch of servers.
    // Note: May go through GRPC fail number 14 --> socket closed,
    // if you start a server, send a request to the server using pingcap::kv::RpcClient,
    // then close the server and start the server using the same addr,
    // then send a request to the new server using pingcap::kv::RpcClient.
    static LoggerPtr log_ptr;
    static MockComputeServerManager server_manager;
};


// std::unique_ptr<FlashGrpcServerHolder> MPPTaskTestUtils::compute_server_ptr = nullptr;
LoggerPtr MPPTaskTestUtils::log_ptr = nullptr;
MockComputeServerManager MPPTaskTestUtils::server_manager;


#define ASSERT_MPPTASK_EQUAL(tasks, expect_cols)                                          \
    TiFlashTestEnv::getGlobalContext().setColumnsForTest(context.executorIdColumnsMap()); \
    TiFlashTestEnv::getGlobalContext().setMPPTest();                                      \
    ASSERT_COLUMNS_EQ_UR(executeMPPTasks(tasks), expected_cols);

} // namespace DB::tests
