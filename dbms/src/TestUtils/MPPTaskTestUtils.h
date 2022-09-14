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

#include <Common/FmtUtils.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <Debug/MockComputeServerManager.h>
#include <Debug/MockStorage.h>
#include <Flash/Coprocessor/ExecutionSummary.h>
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


class ExecutionSummaryCollector
{
public:
    void collect(std::vector<BlockInputStreamPtr> & streams)
    {
        for (size_t i = 0; i < streams.size(); ++i)
        {
            auto * exchange_receiver_stream = dynamic_cast<ExchangeReceiverInputStream *>(streams[i].get());
            assert(exchange_receiver_stream);
            for (size_t j = 0; j < exchange_receiver_stream->getSourceNum(); ++j)
            {
                for (auto kv : *exchange_receiver_stream->getRemoteExecutionSummaries(j))
                {
                    ExecutionSummary summary{kv.second.time_processed_ns, kv.second.num_produced_rows, kv.second.num_iterations, kv.second.concurrency};

                    if (execution_summaries.find(kv.first) == execution_summaries.end())
                    {
                        execution_summaries[kv.first] = summary;
                    }
                    else
                    {
                        execution_summaries[kv.first].merge(summary, false);
                    }
                }
            }
        }
    }

    auto toString() const -> std::string
    {
        FmtBuffer buf;
        buf.joinStr(
            execution_summaries.begin(),
            execution_summaries.end(),
            [&](const auto & summary, FmtBuffer &) {
                buf.fmtAppend("executor id: {}, time_processed_ns: {}, num_produced_rows: {}, num_iterations: {}, concurrency: {}",
                              summary.first,
                              summary.second.time_processed_ns,
                              summary.second.num_produced_rows,
                              summary.second.num_iterations,
                              summary.second.concurrency);
            },
            "\n");
        return buf.toString();
    }

    bool empty()
    {
        if (execution_summaries.empty())
            return true;

        for (auto summary : execution_summaries)
        {
            if (summary.second.time_processed_ns == 0 || summary.second.num_produced_rows == 0 || summary.second.num_iterations == 0 || summary.second.concurrency == 0)
                return true;
        }

        return false;
    }

private:
    std::unordered_map<String, ExecutionSummary> execution_summaries{}; // <executor_id, ExecutionSummary>
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

    static void startServers();

    static void startServers(size_t server_num_);
    static size_t serverNum();

    // run mpp tasks which are ready to cancel, the return value is the start_ts of query.
    std::tuple<size_t, std::vector<BlockInputStreamPtr>> prepareMPPStreams(DAGRequestBuilder builder);

    // return execution result cols, is_empty which indicate execution summaries contain info or not.
    std::pair<ColumnsWithTypeAndName, bool> exeucteMPPTasks(QueryTasks & tasks, const DAGProperties & properties, std::unordered_map<size_t, MockServerConfig> & server_config_map);
    static ::testing::AssertionResult assertQueryCancelled(size_t start_ts);
    static ::testing::AssertionResult assertQueryActive(size_t start_ts);
    static String queryInfo(size_t server_id);

protected:
    static LoggerPtr log_ptr;
    static size_t server_num;
    static MPPTestMeta test_meta;
};

#define ASSERT_MPPTASK_EQUAL(tasks, properties, expect_cols)                                                                   \
    do                                                                                                                         \
    {                                                                                                                          \
        TiFlashTestEnv::getGlobalContext().setMPPTest();                                                                       \
        MockComputeServerManager::instance().setMockStorage(context.mockStorage());                                            \
        auto [cols, is_empty] = exeucteMPPTasks(tasks, properties, MockComputeServerManager::instance().getServerConfigMap()); \
        ASSERT_COLUMNS_EQ_UR(cols, expected_cols);                                                                             \
        ASSERT_EQ(is_empty, false);                                                                                            \
    } while (0)


#define ASSERT_MPPTASK_EQUAL_WITH_SERVER_NUM(builder, properties, expect_cols) \
    do                                                                         \
    {                                                                          \
        for (size_t i = 1; i <= serverNum(); ++i)                    \
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
        auto properties = DB::tests::getDAGPropertiesForTest(serverNum());             \
        for (int i = 0; i < TiFlashTestEnv::globalContextSize(); ++i)                  \
            TiFlashTestEnv::getGlobalContext(i).setMPPTest();                          \
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

template <>
struct fmt::formatter<DB::tests::ExecutionSummaryCollector> : formatter<string_view>
{
    template <typename FormatContext>
    auto format(const DB::tests::ExecutionSummaryCollector & c, FormatContext & ctx) const
    {
        return formatter<string_view>::format(c.toString(), ctx);
    }
};
