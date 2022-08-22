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

#include <Debug/MockComputeServerManager.h>
#include <Debug/MockStorage.h>
#include <Server/FlashGrpcServerHolder.h>
#include <TestUtils/ExecutorTestUtils.h>

namespace DB::tests
{
class MockTimeStampGenerator : public ext::Singleton<MockTimeStampGenerator>
{
public:
    Int64 nextTs()
    {
        return ++current_ts;
    }

private:
    std::atomic<UInt64> current_ts = 0;
};

class MockServerAddrGenerator : public ext::Singleton<MockServerAddrGenerator>
{
public:
    String nextAddr()
    {
        if (port >= port_upper_bound)
        {
            throw Exception("Failed to get next server addr");
        }
        return "0.0.0.0:" + std::to_string(port++);
    }

    void reset()
    {
        port = 3931;
    }

private:
    const Int64 port_upper_bound = 65536;
    std::atomic<Int64> port = 3931;
};

DAGProperties getDAGPropertiesForTest(int server_num)
{
    DAGProperties properties;
    // enable mpp
    properties.is_mpp_query = true;
    properties.mpp_partition_num = server_num;
    properties.start_ts = MockTimeStampGenerator::instance().nextTs();
    return properties;
}

class MPPTaskTestUtils : public ExecutorTest
{
public:
    static void SetUpTestCase()
    {
        ExecutorTest::SetUpTestCase();
        log_ptr = Logger::get("compute_test");
        server_num = 1;
    }

    static void TearDownTestCase() // NOLINT(readability-identifier-naming))
    {
        MockComputeServerManager::instance().reset();
    }

    void startServers()
    {
        startServers(server_num);
    }

    void startServers(size_t server_num_)
    {
        server_num = server_num_;
        MockComputeServerManager::instance().reset();
        auto size = std::thread::hardware_concurrency();
        GRPCCompletionQueuePool::global_instance = std::make_unique<GRPCCompletionQueuePool>(size);
        for (size_t i = 0; i < server_num; ++i)
        {
            MockComputeServerManager::instance().addServer(MockServerAddrGenerator::instance().nextAddr());
        }
        MockComputeServerManager::instance().startServers(log_ptr, TiFlashTestEnv::getGlobalContext());
        MockServerAddrGenerator::instance().reset();
    }

    size_t serverNum() const
    {
        return server_num;
    }

protected:
    static LoggerPtr log_ptr;
    static size_t server_num;
};

LoggerPtr MPPTaskTestUtils::log_ptr = nullptr;
size_t MPPTaskTestUtils::server_num = 0;

#define ASSERT_MPPTASK_EQUAL(tasks, properties, expect_cols)                                                                                \
    do                                                                                                                                      \
    {                                                                                                                                       \
        TiFlashTestEnv::getGlobalContext().setMPPTest();                                                                                    \
        MockComputeServerManager::instance().setMockStorage(context.mockStorage());                                                         \
        ASSERT_COLUMNS_EQ_UR(executeMPPTasks(tasks, properties, MockComputeServerManager::instance().getServerConfigMap()), expected_cols); \
    } while (0)

#define ASSERT_MPPTASK_EQUAL_WITH_SERVER_NUM(builder, properties, expect_cols) \
    do                                                                         \
    {                                                                          \
        for (size_t i = 1; i <= serverNum(); ++i)                              \
        {                                                                      \
            (properties).mpp_partition_num = i;                                \
            MockComputeServerManager::instance().resetMockMPPServerInfo(i);    \
            auto tasks = (builder).buildMPPTasks(context, properties);         \
            ASSERT_MPPTASK_EQUAL(tasks, properties, expect_cols);              \
        }                                                                      \
    } while (0)

#define ASSERT_MPPTASK_EQUAL_PLAN_AND_RESULT(builder, expected_strings, expected_cols) \
    do                                                                                 \
    {                                                                                  \
        auto properties = getDAGPropertiesForTest(serverNum());                        \
        auto tasks = (builder).buildMPPTasks(context, properties);                     \
        size_t task_size = tasks.size();                                               \
        for (size_t i = 0; i < task_size; ++i)                                         \
        {                                                                              \
            ASSERT_DAGREQUEST_EQAUL((expected_strings)[i], tasks[i].dag_request);      \
        }                                                                              \
        ASSERT_MPPTASK_EQUAL_WITH_SERVER_NUM(                                          \
            builder,                                                                   \
            properties,                                                                \
            expect_cols);                                                              \
    } while (0)
} // namespace DB::tests
