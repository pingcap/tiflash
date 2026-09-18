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

#include <Flash/Mpp/MPPTaskStatistics.h>
#include <Flash/Mpp/MPPTunnelSet.h>
#include <Flash/Statistics/ExecutorStatisticsCollector.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <Interpreters/Context.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/mockExecutor.h>
#include <kvproto/resource_manager.pb.h>

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

        context.addMockTable(
            {"test_db", "test_table"},
            {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}},
            {toNullableVec<String>(
                 "s1",
                 {"banana", {}, "banana", "banana", {}, "banana", "banana", {}, "banana", "banana", {}, "banana"}),
             toNullableVec<String>(
                 "s2",
                 {"apple", {}, "banana", "apple", {}, "banana", "apple", {}, "banana", "apple", {}, "banana"})});

        context.addExchangeReceiver(
            "test_exchange",
            {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}},
            {toNullableVec<String>(
                 "s1",
                 {"banana", {}, "banana", "banana", {}, "banana", "banana", {}, "banana", "banana", {}, "banana"}),
             toNullableVec<String>(
                 "s2",
                 {"apple", {}, "banana", "apple", {}, "banana", "apple", {}, "banana", "apple", {}, "banana"})});

        context.addMockTable(
            {"test_db", "empty_table"},
            {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}},
            {toNullableVec<Int32>("s1", {}), toNullableVec<String>("s2", {})});
    }

    static constexpr size_t concurrency = 10;

    void testHashTableStats(bool enable_join_v2)
    {
        context.context->getSettingsRef().enable_hash_join_v2 = enable_join_v2;
        enablePipeline(enable_join_v2);

        auto t1 = context.scan("test_db", "test_table");
        auto t2 = context.scan("test_db", "test_table");
        auto request = t1.join(t2, tipb::JoinType::TypeInnerJoin, {col("s1")}).build(context);
        request->set_collect_execution_summaries(true);

        DAGContext dag_context(*request, "test_execution_summary", concurrency);
        executeStreams(&dag_context);
        ExecutorStatisticsCollector statistics_collector("test_execution_summary", true);
        statistics_collector.initialize(&dag_context);
        statistics_collector.setLocalRUConsumption(
            RUConsumption{.cpu_ru = 0.0, .cpu_time_ns = 0, .read_ru = 0.0, .read_bytes = 0});
        const auto summaries = statistics_collector.genExecutionSummaryResponse().execution_summaries();

        const tipb::ExecutorExecutionSummary * join_summary = nullptr;
        for (const auto & summary : summaries)
        {
            if (summary.has_executor_id() && summary.executor_id() == "Join_2")
            {
                join_summary = &summary;
                break;
            }
        }
        ASSERT_NE(join_summary, nullptr);
        ASSERT_TRUE(join_summary->has_tiflash_hash_table_stats());
        ASSERT_EQ(join_summary->tiflash_hash_table_stats().size(), enable_join_v2 ? 8 : 1);
        ASSERT_EQ(
            join_summary->tiflash_hash_table_stats().size_kind(),
            enable_join_v2 ? tipb::TIFLASH_HASH_TABLE_SIZE_KIND_BUILD_ROW_COUNT
                           : tipb::TIFLASH_HASH_TABLE_SIZE_KIND_DISTINCT_KEY_COUNT);
        ASSERT_GT(join_summary->tiflash_hash_table_stats().memory_bytes(), 0);
    }

    void testHashTableStatsForMultipleJoins(bool enable_join_v2)
    {
        context.context->getSettingsRef().enable_hash_join_v2 = enable_join_v2;
        enablePipeline(enable_join_v2);

        auto t1 = context.scan("test_db", "test_table");
        auto t2 = context.scan("test_db", "test_table");
        auto t3 = context.scan("test_db", "test_table");
        auto request = t1.join(t2, tipb::JoinType::TypeInnerJoin, {col("s1")})
                           .join(t3, tipb::JoinType::TypeInnerJoin, {col("s1")})
                           .build(context);
        request->set_collect_execution_summaries(true);

        std::vector<String> join_executor_ids;
        traverseExecutors(request.get(), [&](const tipb::Executor & executor) {
            if (executor.has_join())
                join_executor_ids.push_back(executor.executor_id());
            return true;
        });
        ASSERT_EQ(join_executor_ids.size(), 2);

        DAGContext dag_context(*request, "test_execution_summary", concurrency);
        executeStreams(&dag_context);
        ExecutorStatisticsCollector statistics_collector("test_execution_summary", true);
        statistics_collector.initialize(&dag_context);
        statistics_collector.setLocalRUConsumption(
            RUConsumption{.cpu_ru = 0.0, .cpu_time_ns = 0, .read_ru = 0.0, .read_bytes = 0});
        const auto summaries = statistics_collector.genExecutionSummaryResponse().execution_summaries();

        for (const auto & join_executor_id : join_executor_ids)
        {
            const tipb::ExecutorExecutionSummary * join_summary = nullptr;
            for (const auto & summary : summaries)
            {
                if (summary.has_executor_id() && summary.executor_id() == join_executor_id)
                {
                    join_summary = &summary;
                    break;
                }
            }
            ASSERT_NE(join_summary, nullptr);
            ASSERT_TRUE(join_summary->has_tiflash_hash_table_stats());
            ASSERT_EQ(join_summary->tiflash_hash_table_stats().size(), enable_join_v2 ? 8 : 1);
            ASSERT_EQ(
                join_summary->tiflash_hash_table_stats().size_kind(),
                enable_join_v2 ? tipb::TIFLASH_HASH_TABLE_SIZE_KIND_BUILD_ROW_COUNT
                               : tipb::TIFLASH_HASH_TABLE_SIZE_KIND_DISTINCT_KEY_COUNT);
            ASSERT_GT(join_summary->tiflash_hash_table_stats().memory_bytes(), 0);
        }
    }

    void testHashAggTableStats(bool enable_pipeline, bool force_two_level)
    {
        enablePipeline(enable_pipeline);
        const auto old_two_level_threshold = context.context->getSettingsRef().group_by_two_level_threshold;
        const auto old_two_level_threshold_bytes = context.context->getSettingsRef().group_by_two_level_threshold_bytes;
        if (force_two_level)
        {
            context.context->setSetting("group_by_two_level_threshold", Field(static_cast<UInt64>(1)));
            context.context->setSetting("group_by_two_level_threshold_bytes", Field(static_cast<UInt64>(1)));
        }

        auto request = context.scan("test_db", "test_table").aggregation({col("s2")}, {col("s2")}).build(context);
        request->set_collect_execution_summaries(true);

        DAGContext dag_context(*request, "test_execution_summary", concurrency);
        ExecutorStatisticsCollector statistics_collector("test_execution_summary", true);
        // MPP initializes the collector before queryExecute builds the physical plan.
        // Keep the same ordering here to verify that aggregation profile lookup is
        // deferred until execution summaries are generated.
        statistics_collector.initialize(&dag_context);
        executeStreams(&dag_context);
        statistics_collector.setLocalRUConsumption(
            RUConsumption{.cpu_ru = 0.0, .cpu_time_ns = 0, .read_ru = 0.0, .read_bytes = 0});
        const auto summaries = statistics_collector.genExecutionSummaryResponse().execution_summaries();

        // Restore the test settings before validating the result so an assertion failure
        // cannot leak the forced two-level thresholds into subsequent test cases.
        context.context->setSetting(
            "group_by_two_level_threshold",
            Field(static_cast<UInt64>(old_two_level_threshold)));
        context.context->setSetting(
            "group_by_two_level_threshold_bytes",
            Field(static_cast<UInt64>(old_two_level_threshold_bytes)));

        const tipb::ExecutorExecutionSummary * aggregation_summary = nullptr;
        for (const auto & summary : summaries)
        {
            if (summary.has_executor_id() && summary.executor_id() == "aggregation_1")
            {
                aggregation_summary = &summary;
                break;
            }
        }
        ASSERT_NE(aggregation_summary, nullptr);
        ASSERT_TRUE(aggregation_summary->has_tiflash_hash_table_stats());
        ASSERT_EQ(
            aggregation_summary->tiflash_hash_table_stats().size_kind(),
            tipb::TIFLASH_HASH_TABLE_SIZE_KIND_DISTINCT_KEY_COUNT);
        // Ten build workers own independent maps, so the reported total is not the final merged NDV.
        ASSERT_GE(aggregation_summary->tiflash_hash_table_stats().size(), 3);
        ASSERT_GT(aggregation_summary->tiflash_hash_table_stats().memory_bytes(), 0);
    }

#define WRAP_FOR_EXCUTION_SUMMARY_TEST_BEGIN                                      \
    std::vector<DAGRequestType> type{DAGRequestType::tree, DAGRequestType::list}; \
    std::vector<bool> pipeline_bools{false, true};                                \
    for (auto enable_pipeline : pipeline_bools)                                   \
    {                                                                             \
        enablePipeline(enable_pipeline);                                          \
        for (const auto t : type)                                                 \
        {
#define WRAP_FOR_EXCUTION_SUMMARY_TEST_END \
    }                                      \
    }
};

TEST_F(ExecutionSummaryTestRunner, testBasic)
try
{
    WRAP_FOR_EXCUTION_SUMMARY_TEST_BEGIN
    {
        auto request = context.scan("test_db", "test_table").filter(eq(col("s1"), col("s2"))).build(context, t);
        Expect expect{{"table_scan_0", {12, concurrency}}, {"selection_1", {4, concurrency}}};
        testForExecutionSummary(request, expect);
    }
    {
        auto request
            = context.scan("test_db", "test_table").filter(eq(col("s1"), col("s2"))).limit(2).build(context, t);
        Expect expect{
            {"table_scan_0", {not_check_rows, concurrency}},
            {"selection_1", {not_check_rows, concurrency}},
            {"limit_2",
             {2, enable_pipeline ? concurrency : 1}}}; // for pipeline mode, limit can be executed in parallel.

        testForExecutionSummary(request, expect);
    }
    {
        auto request = context.scan("test_db", "test_table").limit(5).build(context, t);
        Expect expect{
            {"table_scan_0", {not_check_rows, concurrency}},
            {"limit_1",
             {5, enable_pipeline ? concurrency : 1}}}; // for pipeline mode, limit can be executed in parallel.
        testForExecutionSummary(request, expect);
    }
    {
        auto request = context.scan("test_db", "test_table").topN("s1", true, 5).build(context, t);
        Expect expect{
            {"table_scan_0", {not_check_rows, concurrency}},
            {"topn_1",
             {not_check_rows,
              enable_pipeline ? concurrency : 1}}}; // for pipeline mode, topn can be executed in parallel.
        testForExecutionSummary(request, expect);
    }

    {
        auto request = context.scan("test_db", "test_table")
                           .filter(eq(col("s1"), col("s2")))
                           .topN("s1", true, 12)
                           .build(context, t);
        Expect expect{
            {"table_scan_0", {not_check_rows, concurrency}},
            {"selection_1", {4, concurrency}},
            {"topn_2", {4, not_check_concurrency}}};
        testForExecutionSummary(request, expect);
    }

    {
        auto request = context.scan("test_db", "test_table")
                           .aggregation({col("s2")}, {col("s2")})
                           .topN("s2", true, 12)
                           .build(context, t);
        Expect expect{
            {"table_scan_0", {12, concurrency}},
            {"aggregation_1", {3, not_check_concurrency}},
            {"topn_2", {3, not_check_concurrency}}};
        testForExecutionSummary(request, expect);
    }

    WRAP_FOR_EXCUTION_SUMMARY_TEST_END
}
CATCH

TEST_F(ExecutionSummaryTestRunner, join)
try
{
    {
        auto t1 = context.scan("test_db", "test_table");
        auto t2 = context.scan("test_db", "test_table");
        auto request = t1.join(t2, tipb::JoinType::TypeInnerJoin, {col("s1")}).build(context);
        Expect expect{
            {"table_scan_0", {12, concurrency}},
            {"table_scan_1", {12, concurrency}},
            {"Join_2", {64, concurrency}}};
        testForExecutionSummary(request, expect);
    }
}
CATCH

TEST_F(ExecutionSummaryTestRunner, hashTableStats)
try
{
    testHashTableStats(false);
    testHashTableStats(true);
}
CATCH

TEST_F(ExecutionSummaryTestRunner, hashTableStatsForMultipleJoins)
try
{
    testHashTableStatsForMultipleJoins(false);
    testHashTableStatsForMultipleJoins(true);
}
CATCH

TEST_F(ExecutionSummaryTestRunner, hashAggTableStats)
try
{
    testHashAggTableStats(false, false);
    testHashAggTableStats(true, false);
    testHashAggTableStats(true, true);
}
CATCH

TEST_F(ExecutionSummaryTestRunner, genMPPTaskExecutionInfoWithoutSetRUInfo)
try
{
    auto request = context.scan("test_db", "test_table").limit(1).exchangeSender(tipb::PassThrough).build(context);
    request->set_collect_execution_summaries(true);

    mpp::TaskMeta meta;
    meta.set_gather_id(1);
    meta.set_task_id(1);
    meta.set_query_ts(1);
    meta.set_local_query_id(1);
    meta.set_server_id(1);
    meta.set_start_ts(1);

    DAGContext dag_context(*request, meta, false);
    // ExchangeSenderStatistics reads tunnel_set while initializing the mock MPP DAG.
    dag_context.tunnel_set = std::make_shared<MPPTunnelSet>("test-host");
    MPPTaskStatistics task_statistics(MPPTaskId(meta), "test-host");
    task_statistics.initializeExecutorDAG(&dag_context);

    auto execution_info = task_statistics.genTiFlashExecutionInfo();
    const auto root_executor_id = dag_context.dag_request.rootExecutorID();
    const tipb::ExecutorExecutionSummary * root_summary = nullptr;
    for (const auto & summary : execution_info.execution_summaries())
    {
        if (summary.executor_id() == root_executor_id)
        {
            root_summary = &summary;
            break;
        }
    }

    ASSERT_NE(root_summary, nullptr);
    ASSERT_TRUE(root_summary->has_ru_consumption());
    resource_manager::Consumption ru_consumption;
    ASSERT_TRUE(ru_consumption.ParseFromString(root_summary->ru_consumption()));
    ASSERT_DOUBLE_EQ(0.0, ru_consumption.r_r_u());
    ASSERT_DOUBLE_EQ(0.0, ru_consumption.read_bytes());
    ASSERT_DOUBLE_EQ(0.0, ru_consumption.total_cpu_time_ms());
}
CATCH

TEST_F(ExecutionSummaryTestRunner, treeBased)
try
{
    WRAP_FOR_TEST_BEGIN
    {
        auto request = context.scan("test_db", "test_table")
                           .filter(eq(col("s1"), col("s2")))
                           .limit(2)
                           .project({col("s1")})
                           .build(context);
        Expect expect{
            {"table_scan_0", {not_check_rows, concurrency}},
            {"selection_1", {not_check_rows, concurrency}},
            {"limit_2",
             {2, enable_pipeline ? concurrency : 1}}, // for pipeline mode, limit can be executed in parallel.
            {"project_3", {2, concurrency}}};
        testForExecutionSummary(request, expect);
    }

    {
        auto request = context.scan("test_db", "test_table").topN("s1", true, 5).project({col("s2")}).build(context);
        Expect expect{
            {"table_scan_0", {not_check_rows, concurrency}},
            {"topn_1",
             {not_check_rows,
              enable_pipeline ? concurrency : 1}}, // for pipeline mode, topn can be executed in parallel.
            {"project_2", {not_check_rows, concurrency}}};
        Expect expect_pipeline{
            {"table_scan_0", {not_check_rows, concurrency}},
            {"topn_1", {not_check_rows, concurrency}},
            {"project_2", {not_check_rows, concurrency}}};

        testForExecutionSummary(request, expect);
    }
    {
        auto request = context.scan("test_db", "test_table").project({col("s2")}).build(context);
        Expect expect{{"table_scan_0", {12, concurrency}}, {"project_1", {12, concurrency}}};

        testForExecutionSummary(request, expect);
    }

    {
        auto request = context.scan("test_db", "test_table").project({col("s2")}).project({col("s2")}).build(context);
        Expect expect{
            {"table_scan_0", {12, concurrency}},
            {"project_1", {12, concurrency}},
            {"project_2", {12, concurrency}}};
        testForExecutionSummary(request, expect);
    }

    {
        auto request = context.scan("test_db", "empty_table").project({col("s2")}).build(context);
        Expect expect{{"table_scan_0", {0, concurrency}}, {"project_1", {0, concurrency}}};

        testForExecutionSummary(request, expect);
    }

    {
        auto request = context.scan("test_db", "empty_table").project({col("s2")}).topN("s2", true, 12).build(context);
        Expect expect{
            {"table_scan_0", {0, concurrency}},
            {"project_1", {0, concurrency}},
            {"topn_2", {0, enable_pipeline ? concurrency : 1}}}; // for pipeline mode, topn can be executed in parallel.
        testForExecutionSummary(request, expect);
    }

    {
        auto request = context.scan("test_db", "test_table")
                           .aggregation({col("s2")}, {col("s2")})
                           .project({col("s2")})
                           .build(context);
        Expect expect{
            {"table_scan_0", {12, concurrency}},
            {"aggregation_1", {3, not_check_concurrency}},
            // for pipeline mode, the concurrency of agg convergent is determined by the amount of data. Because the amount of data is small, the concurrency here is 1.
            {"project_2", {3, enable_pipeline ? 1 : concurrency}}};

        testForExecutionSummary(request, expect);
    }

    {
        auto request = context.scan("test_db", "test_table")
                           .aggregation({}, {col("s2")})
                           .project({col("s2")})
                           .limit(2)
                           .build(context);

        Expect expect{
            {"table_scan_0", {12, concurrency}},
            {"aggregation_1", {3, not_check_concurrency}},
            // for pipeline mode, the concurrency of agg convergent is determined by the amount of data. Because the amount of data is small, the concurrency here is 1.
            {"project_2", {not_check_rows, enable_pipeline ? 1 : concurrency}},
            {"limit_3", {2, 1}}};

        testForExecutionSummary(request, expect);
    }

    WRAP_FOR_TEST_END
}
CATCH

TEST_F(ExecutionSummaryTestRunner, expand)
try
{
    WRAP_FOR_TEST_BEGIN
    {
        auto request = context.scan("test_db", "test_table")
                           .expand(MockVVecColumnNameVec{
                               MockVecColumnNameVec{
                                   MockColumnNameVec{"s1"},
                               },
                               MockVecColumnNameVec{
                                   MockColumnNameVec{"s2"},
                               },
                           })
                           .build(context);

        Expect expect{{"table_scan_0", {12, concurrency}}, {"expand_1", {24, concurrency}}};
        testForExecutionSummary(request, expect);
    }
    WRAP_FOR_TEST_END
}
CATCH

TEST_F(ExecutionSummaryTestRunner, agg)
try
{
    WRAP_FOR_EXCUTION_SUMMARY_TEST_BEGIN
    {
        auto request = context.scan("test_db", "test_table").aggregation({col("s2")}, {col("s2")}).build(context, t);
        Expect expect{{"table_scan_0", {12, concurrency}}, {"aggregation_1", {3, not_check_concurrency}}};
        testForExecutionSummary(request, expect);
    }
    WRAP_FOR_EXCUTION_SUMMARY_TEST_END
}
CATCH

#undef WRAP_FOR_EXCUTION_SUMMARY_TEST_BEGIN
#undef WRAP_FOR_EXCUTION_SUMMARY_TEST_END

} // namespace tests
} // namespace DB
