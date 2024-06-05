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

#include <TestUtils/InterpreterTestUtils.h>
#include <TestUtils/mockExecutor.h>

namespace DB
{
namespace tests
{
class PlannerInterpreterExecuteTest : public DB::tests::InterpreterTestUtils
{
public:
    void initializeContext() override
    {
        InterpreterTestUtils::initializeContext();

        enablePlanner(true);
        enablePipeline(false);

        // The following steps update the expected results of cases in bulk
        // 1. manually delete the gtest_planner_interpreter.out
        // 2. call setRecord()
        // 3. ./gtests_dbms --gtest_filter=PlannerInterpreterExecuteTest.*
        // setRecord();

        context.addMockTable({"test_db", "test_table"}, {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}});
        context.addMockTable(
            {"test_db", "test_table_1"},
            {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}, {"s3", TiDB::TP::TypeString}});
        context.addMockTable(
            {"test_db", "r_table"},
            {{"r_a", TiDB::TP::TypeLong}, {"r_b", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}});
        context.addMockTable(
            {"test_db", "l_table"},
            {{"l_a", TiDB::TP::TypeLong}, {"l_b", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}});
        context.addExchangeReceiver(
            "sender_1",
            {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}, {"s3", TiDB::TP::TypeString}});
        context.addExchangeReceiver(
            "sender_l",
            {{"l_a", TiDB::TP::TypeLong}, {"l_b", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}});
        context.addExchangeReceiver(
            "sender_r",
            {{"r_a", TiDB::TP::TypeLong}, {"r_b", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}});
    }
};

TEST_F(PlannerInterpreterExecuteTest, StrangeQuery)
try
{
    auto request = context.scan("test_db", "test_table_1")
                       .filter(eq(col("s2"), col("s3")))
                       .filter(eq(col("s1"), col("s3")))
                       .filter(eq(col("s1"), col("s2")))
                       .build(context);
    runAndAssert(request, 10);

    request = context.scan("test_db", "test_table_1").limit(10).limit(9).limit(8).build(context);
    runAndAssert(request, 10);

    request = context.scan("test_db", "test_table_1")
                  .topN("s3", false, 10)
                  .topN("s2", false, 9)
                  .topN("s1", false, 8)
                  .build(context);
    runAndAssert(request, 10);
}
CATCH

TEST_F(PlannerInterpreterExecuteTest, SingleQueryBlock)
try
{
    auto request = context.scan("test_db", "test_table_1")
                       .filter(eq(col("s2"), col("s3")))
                       .aggregation({Max(col("s1"))}, {col("s2"), col("s3")})
                       .filter(eq(col("s2"), col("s3")))
                       .topN("s2", false, 10)
                       .build(context);
    runAndAssert(request, 10);

    request = context.scan("test_db", "test_table_1")
                  .filter(eq(col("s2"), col("s3")))
                  .aggregation({Max(col("s1"))}, {col("s2"), col("s3")})
                  .filter(eq(col("s2"), col("s3")))
                  .limit(10)
                  .build(context);
    runAndAssert(request, 10);
}
CATCH

TEST_F(PlannerInterpreterExecuteTest, ParallelQuery)
try
{
    /// executor with table scan
    auto request = context.scan("test_db", "test_table_1").limit(10).build(context);
    runAndAssert(request, 1);
    runAndAssert(request, 5);

    request = context.scan("test_db", "test_table_1").project({"s1", "s2", "s3"}).build(context);
    runAndAssert(request, 1);
    runAndAssert(request, 5);

    request
        = context.scan("test_db", "test_table_1").aggregation({Max(col("s1"))}, {col("s2"), col("s3")}).build(context);
    runAndAssert(request, 1);
    runAndAssert(request, 5);

    request = context.scan("test_db", "test_table_1").topN("s2", false, 10).build(context);
    runAndAssert(request, 1);
    runAndAssert(request, 5);

    request = context.scan("test_db", "test_table_1").filter(eq(col("s2"), col("s3"))).build(context);
    runAndAssert(request, 1);
    runAndAssert(request, 5);

    /// other cases
    request = context.scan("test_db", "test_table_1")
                  .limit(10)
                  .project({"s1", "s2", "s3"})
                  .aggregation({Max(col("s1"))}, {col("s2"), col("s3")})
                  .build(context);
    runAndAssert(request, 10);
    runAndAssert(request, 1);

    request = context.scan("test_db", "test_table_1")
                  .topN("s2", false, 10)
                  .project({"s1", "s2", "s3"})
                  .aggregation({Max(col("s1"))}, {col("s2"), col("s3")})
                  .build(context);
    runAndAssert(request, 10);
    runAndAssert(request, 1);

    request = context.scan("test_db", "test_table_1")
                  .aggregation({Max(col("s1"))}, {col("s2"), col("s3")})
                  .project({"s2", "s3"})
                  .aggregation({Max(col("s2"))}, {col("s3")})
                  .build(context);
    runAndAssert(request, 10);
    runAndAssert(request, 1);

    request = context.scan("test_db", "test_table_1")
                  .aggregation({Max(col("s1"))}, {col("s2"), col("s3")})
                  .exchangeSender(tipb::PassThrough)
                  .build(context);
    runAndAssert(request, 10);
    runAndAssert(request, 1);

    request = context.scan("test_db", "test_table_1")
                  .topN("s2", false, 10)
                  .exchangeSender(tipb::PassThrough)
                  .build(context);
    runAndAssert(request, 10);
    runAndAssert(request, 1);

    request = context.scan("test_db", "test_table_1").limit(10).exchangeSender(tipb::PassThrough).build(context);
    runAndAssert(request, 10);
    runAndAssert(request, 1);

    DAGRequestBuilder table1 = context.scan("test_db", "r_table");
    DAGRequestBuilder table2 = context.scan("test_db", "l_table");
    request = table1.join(table2.limit(1), tipb::JoinType::TypeLeftOuterJoin, {col("join_c")}).build(context);
    runAndAssert(request, 10);
}
CATCH

TEST_F(PlannerInterpreterExecuteTest, MultipleQueryBlockWithSource)
try
{
    auto request = context.scan("test_db", "test_table_1")
                       .project({"s1", "s2", "s3"})
                       .project({"s1", "s2"})
                       .project({"s1"})
                       .build(context);
    runAndAssert(request, 10);

    request = context.scan("test_db", "test_table_1")
                  .project({"s1", "s2", "s3"})
                  .topN({{"s1", true}, {"s2", false}}, 10)
                  .project({"s1", "s2"})
                  .build(context);
    runAndAssert(request, 10);

    request = context.scan("test_db", "test_table_1")
                  .project({"s1", "s2", "s3"})
                  .topN({{"s1", true}, {"s2", false}}, 10)
                  .project({"s1", "s2"})
                  .aggregation({Max(col("s1"))}, {col("s1"), col("s2")})
                  .project({"max(s1)", "s1", "s2"})
                  .build(context);
    runAndAssert(request, 10);

    request = context.scan("test_db", "test_table_1")
                  .project({"s1", "s2", "s3"})
                  .topN({{"s1", true}, {"s2", false}}, 10)
                  .project({"s1", "s2"})
                  .aggregation({Max(col("s1"))}, {col("s1"), col("s2")})
                  .project({"max(s1)", "s1", "s2"})
                  .filter(eq(col("s1"), col("s2")))
                  .project({"max(s1)", "s1"})
                  .limit(10)
                  .build(context);
    runAndAssert(request, 10);

    request
        = context.receive("sender_1").project({"s1", "s2", "s3"}).project({"s1", "s2"}).project({"s1"}).build(context);
    runAndAssert(request, 10);

    request = context.receive("sender_1")
                  .project({"s1", "s2", "s3"})
                  .project({"s1", "s2"})
                  .project({"s1"})
                  .exchangeSender(tipb::Broadcast)
                  .build(context);
    runAndAssert(request, 10);
}
CATCH

TEST_F(PlannerInterpreterExecuteTest, Window)
try
{
    auto request = context.scan("test_db", "test_table")
                       .sort({{"s1", true}, {"s2", false}}, true)
                       .window(RowNumber(), {"s1", true}, {"s2", false}, buildDefaultRowsFrame())
                       .build(context);
    runAndAssert(request, 10);

    request = context.scan("test_db", "test_table")
                  .sort({{"s1", true}, {"s2", false}}, true)
                  .window(RowNumber(), {"s1", true}, {"s2", false}, buildDefaultRowsFrame())
                  .project({"s1", "s2", "RowNumber()"})
                  .build(context);
    runAndAssert(request, 10);

    request = context.scan("test_db", "test_table_1")
                  .sort({{"s1", true}, {"s2", false}}, true)
                  .project({"s1", "s2", "s3"})
                  .window(RowNumber(), {"s1", true}, {"s1", false}, buildDefaultRowsFrame())
                  .project({"s1", "s2", "s3", "RowNumber()"})
                  .build(context);
    runAndAssert(request, 10);
}
CATCH

TEST_F(PlannerInterpreterExecuteTest, FineGrainedShuffle)
try
{
    // fine-grained shuffle is enabled.
    const uint64_t enable = 8;
    const uint64_t disable = 0;
    auto request = context.receive("sender_1", enable)
                       .sort({{"s1", true}, {"s2", false}}, true, enable)
                       .window(RowNumber(), {"s1", true}, {"s2", false}, buildDefaultRowsFrame(), enable)
                       .build(context);
    runAndAssert(request, 10);

    auto topn_request = context.receive("sender_1").topN("s2", false, 10).build(context);
    runAndAssert(topn_request, 10);

    // fine-grained shuffle is disabled.
    request = context.receive("sender_1", disable)
                  .sort({{"s1", true}, {"s2", false}}, true, disable)
                  .window(RowNumber(), {"s1", true}, {"s2", false}, buildDefaultRowsFrame(), disable)
                  .build(context);
    runAndAssert(request, 10);

    topn_request = context.receive("sender_1").topN("s2", false, 10).build(context);
    runAndAssert(topn_request, 10);
}
CATCH

TEST_F(PlannerInterpreterExecuteTest, FineGrainedShuffleJoin)
try
{
    // fine-grained shuffle is enabled.
    const uint64_t enable = 8;
    const uint64_t disable = 0;
    {
        // Join Source.
        DAGRequestBuilder receiver1 = context.receive("sender_l");
        DAGRequestBuilder receiver2 = context.receive("sender_r", enable);

        auto request
            = receiver1.join(receiver2, tipb::JoinType::TypeLeftOuterJoin, {col("join_c")}, enable).build(context);
        runAndAssert(request, 10);
    }
    {
        // Join Source.
        DAGRequestBuilder receiver1 = context.receive("sender_l");
        DAGRequestBuilder receiver2 = context.receive("sender_r", disable);

        auto request
            = receiver1.join(receiver2, tipb::JoinType::TypeLeftOuterJoin, {col("join_c")}, disable).build(context);
        runAndAssert(request, 10);
    }
}
CATCH

TEST_F(PlannerInterpreterExecuteTest, FineGrainedShuffleAgg)
try
{
    // fine-grained shuffle is enabled.
    const uint64_t enable = 8;
    const uint64_t disable = 0;
    {
        DAGRequestBuilder receiver1 = context.receive("sender_1", enable);
        auto request = receiver1.aggregation({Max(col("s1"))}, {col("s2")}, enable).build(context);
        runAndAssert(request, 10);
    }

    {
        DAGRequestBuilder receiver1 = context.receive("sender_1", disable);
        auto request = receiver1.aggregation({Max(col("s1"))}, {col("s2")}, disable).build(context);
        runAndAssert(request, 10);
    }
}
CATCH

TEST_F(PlannerInterpreterExecuteTest, Join)
try
{
    // TODO: Find a way to write the request easier.
    {
        // Join Source.
        DAGRequestBuilder table1 = context.scan("test_db", "r_table");
        DAGRequestBuilder table2 = context.scan("test_db", "l_table");
        DAGRequestBuilder table3 = context.scan("test_db", "r_table");
        DAGRequestBuilder table4 = context.scan("test_db", "l_table");

        auto request = table1
                           .join(
                               table2.join(
                                   table3.join(table4, tipb::JoinType::TypeLeftOuterJoin, {col("join_c")}),
                                   tipb::JoinType::TypeLeftOuterJoin,
                                   {col("join_c")}),
                               tipb::JoinType::TypeLeftOuterJoin,
                               {col("join_c")})
                           .build(context);

        runAndAssert(request, 10);
    }

    {
        // only join + ExchangeReceiver
        DAGRequestBuilder receiver1 = context.receive("sender_l");
        DAGRequestBuilder receiver2 = context.receive("sender_r");
        DAGRequestBuilder receiver3 = context.receive("sender_l");
        DAGRequestBuilder receiver4 = context.receive("sender_r");

        auto request = receiver1
                           .join(
                               receiver2.join(
                                   receiver3.join(receiver4, tipb::JoinType::TypeLeftOuterJoin, {col("join_c")}),
                                   tipb::JoinType::TypeLeftOuterJoin,
                                   {col("join_c")}),
                               tipb::JoinType::TypeLeftOuterJoin,
                               {col("join_c")})
                           .build(context);
        runAndAssert(request, 10);
    }

    {
        // join + receiver + sender
        DAGRequestBuilder receiver1 = context.receive("sender_l");
        DAGRequestBuilder receiver2 = context.receive("sender_r");
        DAGRequestBuilder receiver3 = context.receive("sender_l");
        DAGRequestBuilder receiver4 = context.receive("sender_r");

        auto request = receiver1
                           .join(
                               receiver2.join(
                                   receiver3.join(receiver4, tipb::JoinType::TypeLeftOuterJoin, {col("join_c")}),
                                   tipb::JoinType::TypeLeftOuterJoin,
                                   {col("join_c")}),
                               tipb::JoinType::TypeLeftOuterJoin,
                               {col("join_c")})
                           .exchangeSender(tipb::PassThrough)
                           .build(context);
        runAndAssert(request, 10);
    }
}
CATCH

TEST_F(PlannerInterpreterExecuteTest, JoinThenAgg)
try
{
    {
        // Left Join.
        DAGRequestBuilder table1 = context.scan("test_db", "r_table");
        DAGRequestBuilder table2 = context.scan("test_db", "l_table");

        auto request = table1.join(table2, tipb::JoinType::TypeLeftOuterJoin, {col("join_c")})
                           .aggregation({Max(col("r_a"))}, {col("join_c")})
                           .build(context);
        runAndAssert(request, 10);
    }

    {
        // Right Join
        DAGRequestBuilder table1 = context.scan("test_db", "r_table");
        DAGRequestBuilder table2 = context.scan("test_db", "l_table");

        auto request = table1.join(table2, tipb::JoinType::TypeRightOuterJoin, {col("join_c")})
                           .aggregation({Max(col("r_a"))}, {col("join_c")})
                           .build(context);
        runAndAssert(request, 10);
    }

    {
        // Right join + receiver + sender
        DAGRequestBuilder receiver1 = context.receive("sender_l");
        DAGRequestBuilder receiver2 = context.receive("sender_r");

        auto request = receiver1.join(receiver2, tipb::JoinType::TypeRightOuterJoin, {col("join_c")})
                           .aggregation({Sum(col("r_a"))}, {col("join_c")})
                           .limit(10)
                           .exchangeSender(tipb::PassThrough)
                           .build(context);
        runAndAssert(request, 20);
    }
}
CATCH

TEST_F(PlannerInterpreterExecuteTest, ListBase)
try
{
    {
        auto request = context.scan("test_db", "test_table")
                           .filter(eq(col("s1"), col("s2")))
                           .aggregation(Max(col("s1")), col("s2"))
                           .filter(eq(col("s2"), lit(Field("1", 1))))
                           .limit(10)
                           .build(context, DAGRequestType::list);
        runAndAssert(request, 1);
    }

    {
        auto request = context.scan("test_db", "test_table")
                           .filter(eq(col("s1"), col("s2")))
                           .aggregation(Max(col("s1")), col("s2"))
                           .filter(eq(col("s2"), lit(Field("1", 1))))
                           .topN("s2", false, 10)
                           .build(context, DAGRequestType::list);
        runAndAssert(request, 20);
    }
}
CATCH


TEST_F(PlannerInterpreterExecuteTest, ExpandPlan)
try
{
    {
        auto request = context.receive("sender_1")
                           .aggregation({Count(col("s1"))}, {col("s2")})
                           .expand(MockVVecColumnNameVec{
                               MockVecColumnNameVec{
                                   MockColumnNameVec{"count(s1)"},
                               },
                               MockVecColumnNameVec{
                                   MockColumnNameVec{"s2"},
                               },
                           })
                           .join(
                               context.scan("test_db", "test_table").project({"s2"}),
                               tipb::JoinType::TypeInnerJoin,
                               {col("s2")})
                           .project({"count(s1)", "groupingID"})
                           .topN({{"groupingID", true}}, 2)
                           .build(context);
        runAndAssert(request, 10);
    }
}
CATCH

TEST_F(PlannerInterpreterExecuteTest, Expand2Plan)
try
{
    std::vector<tipb::FieldType> fields(3);
    fields[0].set_tp(TiDB::TypeString);
    fields[1].set_tp(TiDB::TypeString);
    fields[2].set_tp(TiDB::TypeLongLong);
    fields[2].set_flag(TiDB::ColumnFlagNotNull | TiDB::ColumnFlagUnsigned);
    {
        auto request = context.receive("sender_1")
                           .aggregation({Count(col("s1"))}, {col("s2")})
                           .expand2(
                               std::vector<MockAstVec>{
                                   {col("count(s1)"), lit(Field(Null())), lit(Field(static_cast<UInt64>(1)))},
                                   {lit(Field(Null())), col("s2"), lit(Field(static_cast<UInt64>(2)))}},
                               std::vector<String>{"grouping_id"},
                               fields)
                           .join(
                               context.scan("test_db", "test_table").project({"s2"}),
                               tipb::JoinType::TypeInnerJoin,
                               {col("s2")})
                           .project({"count(s1)", "grouping_id"})
                           .topN({{"grouping_id", true}}, 2)
                           .build(context);
        runAndAssert(request, 10);
    }
}
CATCH

TEST_F(PlannerInterpreterExecuteTest, autoPassThroughAgg)
try
{
    auto request = context.scan("test_db", "test_table_1")
                  .aggregation({Max(col("s1"))}, {col("s2"), col("s3")}, 0, true)
                  .exchangeSender(tipb::PassThrough)
                  .build(context);
    runAndAssert(request, 10);
    runAndAssert(request, 1);
}
CATCH
} // namespace tests
} // namespace DB
