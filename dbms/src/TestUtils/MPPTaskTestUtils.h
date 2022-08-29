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

DAGProperties getDAGPropertiesForTest(int server_num);
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

// Hold MPP test related infomation:
// 1. context_idx: indicate which global context this MPP test use.
// 2. server_num: indicate how many compute server the MPP test use.
struct MPPTestMeta
{
    size_t context_idx;
    size_t server_num;
};

class MPPTaskTestUtils : public ExecutorTest
{
public:
    static void SetUpTestCase();

    static void TearDownTestCase(); // NOLINT(readability-identifier-naming))


    static void startServers();

    static void startServers(size_t server_num_);
    static size_t serverNum();

    void injectCancel(DAGRequestBuilder builder);

    ColumnsWithTypeAndName exeucteMPPTasks(QueryTasks & tasks, const DAGProperties & properties, std::unordered_map<size_t, MockServerConfig> & server_config_map);

    BlockInputStreamPtr executeMPPTasksForCancel(QueryTasks & tasks, const DAGProperties & properties, std::unordered_map<size_t, MockServerConfig> & server_config_map);

protected:
    static LoggerPtr log_ptr;
    static size_t server_num;
    static MPPTestMeta test_meta;
};

#define ASSERT_MPPTASK_EQUAL(tasks, properties, expect_cols)                                                                                \
    do                                                                                                                                      \
    {                                                                                                                                       \
        TiFlashTestEnv::getGlobalContext().setMPPTest();                                                                                    \
        MockComputeServerManager::instance().setMockStorage(context.mockStorage());                                                         \
        ASSERT_COLUMNS_EQ_UR(exeucteMPPTasks(tasks, properties, MockComputeServerManager::instance().getServerConfigMap()), expected_cols); \
    } while (0)


#define ASSERT_MPPTASK_EQUAL_WITH_SERVER_NUM(builder, properties, expect_cols)       \
    do                                                                               \
    {                                                                                \
        for (size_t i = serverNum(); i <= serverNum(); ++i)                          \
        {                                                                            \
            (properties).mpp_partition_num = i;                                      \
            MockComputeServerManager::instance().resetMockMPPServerInfo(i);          \
            auto tasks = (builder).buildMPPTasks(context, properties);               \
            for (auto task : tasks)                                                  \
                std::cout << ExecutorSerializer().serialize(task.dag_request.get()); \
            ASSERT_MPPTASK_EQUAL(tasks, properties, expect_cols);                    \
        }                                                                            \
    } while (0)

#define ASSERT_MPPTASK_EQUAL_PLAN_AND_RESULT(builder, expected_strings, expected_cols) \
    do                                                                                 \
    {                                                                                  \
        auto properties = DB::tests::getDAGPropertiesForTest(serverNum());             \
        for (int i = 0; i < TiFlashTestEnv::globalContextSize(); i++)                  \
            TiFlashTestEnv::getGlobalContext(i).setMPPTest();                          \
        std::cout << "ywq test properties serverNum: " << serverNum() << std::endl;    \
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
