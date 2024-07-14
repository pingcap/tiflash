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

#pragma once

#include <Debug/MockComputeServerManager.h>
#include <Interpreters/Context.h>
#include <TestUtils/ExecutorTestUtils.h>

namespace DB::tests
{
DAGProperties getDAGPropertiesForTest(
    int server_num,
    int local_query_id = -1,
    int tidb_server_id = -1,
    int query_ts = -1,
    int gather_id = -1);
class MockTimeStampGenerator : public ext::Singleton<MockTimeStampGenerator>
{
public:
    Int64 nextTs() { return ++current_ts; }

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

    void reset() { port = 4931; }

private:
    const Int64 port_upper_bound = 65536;
    std::atomic<Int64> port = 4931;
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

    void TearDown() override;

    void startServers();

    void startServers(size_t server_num_);
    static size_t serverNum();

    // run mpp tasks which are ready to cancel, the return value is the start_ts of query.
    BlockInputStreamPtr prepareMPPStreams(DAGRequestBuilder builder, const DAGProperties & properties);

    // prepareMPPTasks is not thread safe, the builder's executor_index(which is ref to context's index) is updated during this process
    std::vector<QueryTask> prepareMPPTasks(DAGRequestBuilder builder, const DAGProperties & properties);

    // prepareMPPTasks is thread safe
    std::vector<QueryTask> prepareMPPTasks(
        std::function<DAGRequestBuilder()> & gen_builder,
        const DAGProperties & properties);

    static void setCancelTest();

    /// Keep stream not deconstructed until cancelGather invoked outside, so that the deconstruction progress won't block
    static ColumnsWithTypeAndName executeProblematicMPPTasks(
        QueryTasks & tasks,
        const DAGProperties & properties,
        BlockInputStreamPtr & stream);
    static ColumnsWithTypeAndName executeMPPTasks(QueryTasks & tasks, const DAGProperties & properties);
    ColumnsWithTypeAndName buildAndExecuteMPPTasks(DAGRequestBuilder builder);

    ColumnsWithTypeAndName executeCoprocessorTask(std::shared_ptr<tipb::DAGRequest> & dag_request);

    static ::testing::AssertionResult assertQueryCancelled(const MPPQueryId & query_id);
    static ::testing::AssertionResult assertQueryActive(const MPPQueryId & query_id);

    static ::testing::AssertionResult assertGatherCancelled(const MPPGatherId & gather_id);
    static ::testing::AssertionResult assertGatherActive(const MPPGatherId & gather_id);

    static String queryInfo(size_t server_id);

    static DB::ColumnsWithTypeAndName getResultBlocks(
            MockDAGRequestContext & context,
            DAGRequestBuilder & builder,
            size_t server_num)
    {
        auto properties = DB::tests::getDAGPropertiesForTest(server_num);
        for (int i = 0; i < TiFlashTestEnv::globalContextSize(); ++i)
            TiFlashTestEnv::getGlobalContext(i).setMPPTest();

        properties.mpp_partition_num = server_num;
        MockComputeServerManager::instance().resetMockMPPServerInfo(server_num);
        auto mpp_tasks = builder.buildMPPTasks(context, properties);

        TiFlashTestEnv::getGlobalContext().setMPPTest();
        MockComputeServerManager::instance().setMockStorage(context.mockStorage());
        return executeMPPTasks(mpp_tasks, properties);
    }

protected:
    static LoggerPtr log_ptr;
    static size_t server_num;
    static MPPTestMeta test_meta;
    std::mutex mu;
};

#define ASSERT_MPPTASK_EQUAL(tasks, properties, expected_cols)                      \
    do                                                                              \
    {                                                                               \
        TiFlashTestEnv::getGlobalContext().setMPPTest();                            \
        MockComputeServerManager::instance().setMockStorage(context.mockStorage()); \
        ASSERT_COLUMNS_EQ_UR(expected_cols, executeMPPTasks(tasks, properties));    \
    } while (0)


#define ASSERT_MPPTASK_EQUAL_WITH_SERVER_NUM(builder, properties, expect_cols) \
    do                                                                         \
    {                                                                          \
        for (size_t i = 1; i <= serverNum(); ++i)                              \
        {                                                                      \
            (properties).mpp_partition_num = i;                                \
            MockComputeServerManager::instance().resetMockMPPServerInfo(i);    \
            auto mpp_tasks = (builder).buildMPPTasks(context, properties);     \
            ASSERT_MPPTASK_EQUAL(mpp_tasks, properties, expect_cols);          \
        }                                                                      \
    } while (0)

#define ASSERT_MPPTASK_EQUAL_PLAN_AND_RESULT(builder, expected_strings, expected_cols)  \
    do                                                                                  \
    {                                                                                   \
        auto properties = DB::tests::getDAGPropertiesForTest(serverNum());              \
        for (int i = 0; i < TiFlashTestEnv::globalContextSize(); ++i)                   \
            TiFlashTestEnv::getGlobalContext(i).setMPPTest();                           \
        auto tasks = (builder).buildMPPTasks(context, properties);                      \
        size_t task_size = tasks.size();                                                \
        ASSERT_EQ(task_size, (expected_strings).size());                                \
        ASSERT_MPPTASK_EQUAL_WITH_SERVER_NUM((builder), (properties), (expected_cols)); \
    } while (0)
} // namespace DB::tests
