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

#include <Interpreters/Context.h>
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
