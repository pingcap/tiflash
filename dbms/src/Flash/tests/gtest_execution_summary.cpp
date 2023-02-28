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

#include <Flash/Coprocessor/ExecutionSummaryCollector.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/mockExecutor.h>

namespace DB
{
namespace tests
{
class ExecutionSummaryTestRunner : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
        context.addMockTable({"test_db", "test_table"},
                             {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}},
                             {toNullableVec<String>("s1", {"banana", {}, "banana", "banana", {}, "banana", "banana", {}, "banana", "banana", {}, "banana"}),
                              toNullableVec<String>("s2", {"apple", {}, "banana", "apple", {}, "banana", "apple", {}, "banana", "apple", {}, "banana"})});
        context.addExchangeReceiver("test_exchange",
                                    {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}},
                                    {toNullableVec<String>("s1", {"banana", {}, "banana", "banana", {}, "banana", "banana", {}, "banana", "banana", {}, "banana"}),
                                     toNullableVec<String>("s2", {"apple", {}, "banana", "apple", {}, "banana", "apple", {}, "banana", "apple", {}, "banana"})});
    }

    static constexpr size_t concurrency = 10;
    static constexpr int not_check_rows = -1;
    // <rows, concurrency>
    using ProfileInfo = std::pair<int, size_t>;
    using Expect = std::unordered_map<String, ProfileInfo>;
    void testForExecutionSummary(
        const std::shared_ptr<tipb::DAGRequest> & request,
        const Expect & expect)
    {
        request->set_collect_execution_summaries(true);
        DAGContext dag_context(*request, "test_execution_summary", concurrency);
        executeStreams(&dag_context);
        ASSERT_EQ(dag_context.getProfileStreamsMap().size(), expect.size());
        ASSERT_TRUE(dag_context.collect_execution_summaries);
        ExecutionSummaryCollector summary_collector(dag_context);
        auto summaries = summary_collector.genExecutionSummaryResponse().execution_summaries();
        ASSERT_EQ(summaries.size(), expect.size());
        for (const auto & summary : summaries)
        {
            ASSERT_TRUE(summary.has_executor_id());
            auto it = expect.find(summary.executor_id());
            ASSERT_TRUE(it != expect.end()) << fmt::format("unknown executor_id: {}", summary.executor_id());
            if (it->second.first != not_check_rows)
                ASSERT_EQ(summary.num_produced_rows(), it->second.first) << fmt::format("executor_id: {}", summary.executor_id());
            ASSERT_EQ(summary.concurrency(), it->second.second) << fmt::format("executor_id: {}", summary.executor_id());
            // time_processed_ns, num_iterations and tiflash_scan_context are not checked here.
        }
    }

    void testForPipelineExecutionSummary(
        const std::shared_ptr<tipb::DAGRequest> & request,
        const Expect & expect)
    {
        request->set_collect_execution_summaries(true);
        DAGContext dag_context(*request, "test_execution_summary", concurrency);
        enablePipeline(true);
        executeStreams(&dag_context);
        // ASSERT_EQ(dag_context.getProfileStreamsMap().size(), expect.size());
        ASSERT_TRUE(dag_context.collect_execution_summaries);
        ExecutionSummaryCollector summary_collector(dag_context);
        auto summaries = summary_collector.genExecutionSummaryResponseForPipeline().execution_summaries();
        ASSERT_EQ(summaries.size(), expect.size());
        enablePipeline(false);

        for (const auto & summary : summaries)
        {
            ASSERT_TRUE(summary.has_executor_id());
            auto it = expect.find(summary.executor_id());

            std::cout << summary.executor_id() << std::endl;
            ASSERT_TRUE(it != expect.end()) << fmt::format("unknown executor_id: {}", summary.executor_id());
            std::cout << "produced rows: " << summary.num_produced_rows() << std::endl;
            if (it->second.first != not_check_rows)
                ASSERT_EQ(summary.num_produced_rows(), it->second.first) << fmt::format("executor_id: {}", summary.executor_id());
            ASSERT_EQ(summary.concurrency(), it->second.second) << fmt::format("executor_id: {}", summary.executor_id());
            // time_processed_ns, num_iterations and tiflash_scan_context are not checked here.
        }
    }
};

TEST_F(ExecutionSummaryTestRunner, test)
try
{
    {
        auto request = context
                           .scan("test_db", "test_table")
                           .filter(eq(col("s1"), col("s2")))
                           .build(context);
        Expect expect{{"table_scan_0", {12, concurrency}}, {"selection_1", {4, concurrency}}};
        testForPipelineExecutionSummary(request, expect);
        testForExecutionSummary(request, expect);
    }
    {
        auto request = context
                           .scan("test_db", "test_table")
                           .limit(5)
                           .build(context);
        Expect expect{{"table_scan_0", {not_check_rows, concurrency}}, {"limit_1", {5, 1}}};
        Expect expect1{{"table_scan_0", {not_check_rows, concurrency}}, {"limit_1", {5, 10}}};
        testForPipelineExecutionSummary(request, expect1);

        testForExecutionSummary(request, expect);
    }
    {
        auto request = context
                           .scan("test_db", "test_table")
                           .topN("s1", true, 5)
                           .build(context);
        Expect expect{{"table_scan_0", {not_check_rows, concurrency}}, {"topn_1", {5, 1}}};
        // testForPipelineExecutionSummary(request, expect);

        testForExecutionSummary(request, expect);
    }
    {
        auto request = context
                           .scan("test_db", "test_table")
                           .project({col("s2")})
                           .build(context);
        Expect expect{{"table_scan_0", {12, concurrency}}, {"project_1", {12, concurrency}}};
        testForPipelineExecutionSummary(request, expect);

        testForExecutionSummary(request, expect);
    }
    {
        auto request = context
                           .scan("test_db", "test_table")
                           .aggregation({col("s2")}, {col("s2")})
                           .build(context);
        Expect expect{{"table_scan_0", {12, concurrency}}, {"aggregation_1", {3, concurrency}}};
        testForExecutionSummary(request, expect);
    }
    {
        auto t1 = context.scan("test_db", "test_table");
        auto t2 = context.scan("test_db", "test_table");
        auto request = t1.join(t2, tipb::JoinType::TypeInnerJoin, {col("s1")}).build(context);
        Expect expect{{"table_scan_0", {12, concurrency}}, {"table_scan_1", {12, concurrency}}, {"Join_2", {64, concurrency}}};
        testForExecutionSummary(request, expect);
    }
    {
        auto request = context
                           .receive("test_exchange")
                           .exchangeSender(tipb::Hash)
                           .build(context);
        Expect expect{{"exchange_receiver_0", {12, concurrency}}, {"exchange_sender_1", {12, concurrency}}};
        testForExecutionSummary(request, expect);
    }
    {
        auto request = context
                           .receive("test_exchange")
                           .sort({{"s1", false}, {"s2", false}, {"s1", false}, {"s2", false}}, true)
                           .window(RowNumber(), {"s1", false}, {"s2", false}, buildDefaultRowsFrame())
                           .build(context);
        Expect expect{{"exchange_receiver_0", {12, concurrency}}, {"sort_1", {12, 1}}, {"window_2", {12, 1}}};
        testForExecutionSummary(request, expect);
    }
}
CATCH

} // namespace tests
} // namespace DB
