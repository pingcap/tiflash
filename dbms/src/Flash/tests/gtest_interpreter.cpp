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

#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/mockExecutor.h>

namespace DB
{
namespace tests
{
class InterpreterExecuteTest : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();

        enablePlanner(false);

        context.addMockTable({"test_db", "test_table"}, {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}});
        context.addMockTable({"test_db", "test_table_1"}, {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}, {"s3", TiDB::TP::TypeString}});
        context.addMockTable({"test_db", "r_table"}, {{"r_a", TiDB::TP::TypeLong}, {"r_b", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}});
        context.addMockTable({"test_db", "l_table"}, {{"l_a", TiDB::TP::TypeLong}, {"l_b", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}});
        context.addExchangeRelationSchema("sender_1", {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}, {"s3", TiDB::TP::TypeString}});
        context.addExchangeRelationSchema("sender_l", {{"l_a", TiDB::TP::TypeLong}, {"l_b", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}});
        context.addExchangeRelationSchema("sender_r", {{"r_a", TiDB::TP::TypeLong}, {"r_b", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}});
    }
};

TEST_F(InterpreterExecuteTest, SingleQueryBlock)
try
{
    auto request = context.scan("test_db", "test_table_1")
                       .filter(eq(col("s2"), col("s3")))
                       .aggregation({Max(col("s1"))}, {col("s2"), col("s3")})
                       .filter(eq(col("s2"), col("s3")))
                       .topN("s2", false, 10)
                       .build(context);
    {
        String expected = R"(
Union: <for test>
 SharedQuery x 10: <restore concurrency>
  Expression: <final projection>
   MergeSorting, limit = 10
    Union: <for partial order>
     PartialSorting x 10: limit = 10
      Expression: <before order and select>
       Filter: <execute having>
        SharedQuery: <restore concurrency>
         ParallelAggregating, max_threads: 10, final: true
          Expression x 10: <before aggregation>
           Filter: <execute where>
            MockTableScan)";
        ASSERT_BLOCKINPUTSTREAM_EQAUL(expected, request, 10);
    }

    request = context.scan("test_db", "test_table_1")
                  .filter(eq(col("s2"), col("s3")))
                  .aggregation({Max(col("s1"))}, {col("s2"), col("s3")})
                  .filter(eq(col("s2"), col("s3")))
                  .limit(10)
                  .build(context);

    {
        String expected = R"(
Union: <for test>
 SharedQuery x 10: <restore concurrency>
  Limit, limit = 10
   Union: <for partial limit>
    Limit x 10, limit = 10
     Expression: <final projection>
      Expression: <before order and select>
       Filter: <execute having>
        SharedQuery: <restore concurrency>
         ParallelAggregating, max_threads: 10, final: true
          Expression x 10: <before aggregation>
           Filter: <execute where>
            MockTableScan)";
        ASSERT_BLOCKINPUTSTREAM_EQAUL(expected, request, 10);
    }
}
CATCH

TEST_F(InterpreterExecuteTest, MultipleQueryBlockWithSource)
try
{
    auto request = context.scan("test_db", "test_table_1")
                       .project({"s1", "s2", "s3"})
                       .project({"s1", "s2"})
                       .project({"s1"})
                       .build(context);
    {
        String expected = R"(
Union: <for test>
 Expression x 10: <final projection>
  Expression: <projection>
   Expression: <final projection>
    Expression: <projection>
     Expression: <final projection>
      Expression: <projection>
       Expression: <final projection>
        MockTableScan)";
        ASSERT_BLOCKINPUTSTREAM_EQAUL(expected, request, 10);
    }

    request = context.scan("test_db", "test_table_1")
                  .project({"s1", "s2", "s3"})
                  .topN({{"s1", true}, {"s2", false}}, 10)
                  .project({"s1", "s2"})
                  .build(context);
    {
        String expected = R"(
Union: <for test>
 Expression x 10: <final projection>
  Expression: <projection>
   SharedQuery: <restore concurrency>
    Expression: <final projection>
     MergeSorting, limit = 10
      Union: <for partial order>
       PartialSorting x 10: limit = 10
        Expression: <projection>
         Expression: <final projection>
          MockTableScan)";
        ASSERT_BLOCKINPUTSTREAM_EQAUL(expected, request, 10);
    }

    request = context.scan("test_db", "test_table_1")
                  .project({"s1", "s2", "s3"})
                  .topN({{"s1", true}, {"s2", false}}, 10)
                  .project({"s1", "s2"})
                  .aggregation({Max(col("s1"))}, {col("s1"), col("s2")})
                  .project({"max(s1)", "s1", "s2"})
                  .build(context);
    {
        String expected = R"(
Union: <for test>
 Expression x 10: <final projection>
  Expression: <projection>
   Expression: <final projection>
    SharedQuery: <restore concurrency>
     ParallelAggregating, max_threads: 10, final: true
      Expression x 10: <projection>
       SharedQuery: <restore concurrency>
        Expression: <final projection>
         MergeSorting, limit = 10
          Union: <for partial order>
           PartialSorting x 10: limit = 10
            Expression: <projection>
             Expression: <final projection>
              MockTableScan)";
        ASSERT_BLOCKINPUTSTREAM_EQAUL(expected, request, 10);
    }

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
    {
        String expected = R"(
Union: <for test>
 SharedQuery x 10: <restore concurrency>
  Limit, limit = 10
   Union: <for partial limit>
    Limit x 10, limit = 10
     Expression: <final projection>
      Expression: <projection>
       Expression: <final projection>
        Expression: <before order and select>
         Filter: <execute where>
          Expression: <projection>
           Expression: <final projection>
            SharedQuery: <restore concurrency>
             ParallelAggregating, max_threads: 10, final: true
              Expression x 10: <projection>
               SharedQuery: <restore concurrency>
                Expression: <final projection>
                 MergeSorting, limit = 10
                  Union: <for partial order>
                   PartialSorting x 10: limit = 10
                    Expression: <projection>
                     Expression: <final projection>
                      MockTableScan)";
        ASSERT_BLOCKINPUTSTREAM_EQAUL(expected, request, 10);
    }

    request = context.receive("sender_1")
                  .project({"s1", "s2", "s3"})
                  .project({"s1", "s2"})
                  .project({"s1"})
                  .build(context);
    {
        String expected = R"(
Union: <for test>
 Expression x 10: <final projection>
  Expression: <projection>
   Expression: <final projection>
    Expression: <projection>
     Expression: <final projection>
      Expression: <projection>
       Expression: <final projection>
        MockExchangeReceiver)";
        ASSERT_BLOCKINPUTSTREAM_EQAUL(expected, request, 10);
    }

    request = context.receive("sender_1")
                  .project({"s1", "s2", "s3"})
                  .project({"s1", "s2"})
                  .project({"s1"})
                  .exchangeSender(tipb::Broadcast)
                  .build(context);
    {
        String expected = R"(
Union: <for test>
 MockExchangeSender x 10
  Expression: <final projection>
   Expression: <projection>
    Expression: <final projection>
     Expression: <projection>
      Expression: <final projection>
       Expression: <projection>
        Expression: <final projection>
         MockExchangeReceiver)";
        ASSERT_BLOCKINPUTSTREAM_EQAUL(expected, request, 10);
    }
}
CATCH

TEST_F(InterpreterExecuteTest, Window)
try
{
    auto request = context
                       .scan("test_db", "test_table")
                       .sort({{"s1", true}, {"s2", false}}, true)
                       .window(RowNumber(), {"s1", true}, {"s2", false}, buildDefaultRowsFrame())
                       .build(context);
    {
        String expected = R"(
Union: <for test>
 Expression x 10: <final projection>
  Expression: <before order and select>
   SharedQuery: <restore concurrency>
    Window, function: {row_number}, frame: {type: Rows, boundary_begin: Current, boundary_end: Current}
     Expression: <final projection>
      MergeSorting, limit = 0
       Union: <for partial order>
        PartialSorting x 10: limit = 0
         Expression: <final projection>
          MockTableScan)";
        ASSERT_BLOCKINPUTSTREAM_EQAUL(expected, request, 10);
    }

    request = context.scan("test_db", "test_table")
                  .sort({{"s1", true}, {"s2", false}}, true)
                  .window(RowNumber(), {"s1", true}, {"s2", false}, buildDefaultRowsFrame())
                  .project({"s1", "s2", "RowNumber()"})
                  .build(context);
    {
        String expected = R"(
Union: <for test>
 Expression x 10: <final projection>
  Expression: <before order and select>
   Expression: <projection>
    Expression: <final projection>
     SharedQuery: <restore concurrency>
      Window, function: {row_number}, frame: {type: Rows, boundary_begin: Current, boundary_end: Current}
       Expression: <final projection>
        MergeSorting, limit = 0
         Union: <for partial order>
          PartialSorting x 10: limit = 0
           Expression: <final projection>
            MockTableScan)";
        ASSERT_BLOCKINPUTSTREAM_EQAUL(expected, request, 10);
    }

    request = context.scan("test_db", "test_table_1")
                  .sort({{"s1", true}, {"s2", false}}, true)
                  .project({"s1", "s2", "s3"})
                  .window(RowNumber(), {"s1", true}, {"s1", false}, buildDefaultRowsFrame())
                  .project({"s1", "s2", "s3", "RowNumber()"})
                  .build(context);
    {
        String expected = R"(
Union: <for test>
 Expression x 10: <final projection>
  Expression: <before order and select>
   Expression: <projection>
    Expression: <final projection>
     SharedQuery: <restore concurrency>
      Window, function: {row_number}, frame: {type: Rows, boundary_begin: Current, boundary_end: Current}
       Union: <merge into one for window input>
        Expression x 10: <final projection>
         Expression: <projection>
          SharedQuery: <restore concurrency>
           Expression: <final projection>
            MergeSorting, limit = 0
             Union: <for partial order>
              PartialSorting x 10: limit = 0
               Expression: <final projection>
                MockTableScan)";
        ASSERT_BLOCKINPUTSTREAM_EQAUL(expected, request, 10);
    }
}
CATCH

TEST_F(InterpreterExecuteTest, FineGrainedShuffle)
try
{
    // fine-grained shuffle is enabled.
    const uint64_t enable = 8;
    const uint64_t disable = 0;
    auto request = context
                       .receive("sender_1", enable)
                       .sort({{"s1", true}, {"s2", false}}, true, enable)
                       .window(RowNumber(), {"s1", true}, {"s2", false}, buildDefaultRowsFrame(), enable)
                       .build(context);
    {
        String expected = R"(
Union: <for test>
 Expression x 10: <final projection>
  Expression: <before order and select>
   Window: <enable fine grained shuffle>, function: {row_number}, frame: {type: Rows, boundary_begin: Current, boundary_end: Current}
    Expression: <final projection>
     MergeSorting: <enable fine grained shuffle>, limit = 0
      PartialSorting: <enable fine grained shuffle>: limit = 0
       Expression: <final projection>
        MockExchangeReceiver
        )";
        ASSERT_BLOCKINPUTSTREAM_EQAUL(expected, request, 10);
    }

    auto topn_request = context
                            .receive("sender_1")
                            .topN("s2", false, 10)
                            .build(context);
    String topn_expected = R"(
Union: <for test>
 SharedQuery x 10: <restore concurrency>
  Expression: <final projection>
   MergeSorting, limit = 10
    Union: <for partial order>
     PartialSorting x 10: limit = 10
      MockExchangeReceiver
    )";
    ASSERT_BLOCKINPUTSTREAM_EQAUL(topn_expected, topn_request, 10);

    // fine-grained shuffle is disabled.
    request = context
                  .receive("sender_1", disable)
                  .sort({{"s1", true}, {"s2", false}}, true, disable)
                  .window(RowNumber(), {"s1", true}, {"s2", false}, buildDefaultRowsFrame(), disable)
                  .build(context);
    {
        String expected = R"(
Union: <for test>
 Expression x 10: <final projection>
  Expression: <before order and select>
   SharedQuery: <restore concurrency>
    Window, function: {row_number}, frame: {type: Rows, boundary_begin: Current, boundary_end: Current}
     Expression: <final projection>
      MergeSorting, limit = 0
       Union: <for partial order>
        PartialSorting x 10: limit = 0
         Expression: <final projection>
          MockExchangeReceiver
        )";
        ASSERT_BLOCKINPUTSTREAM_EQAUL(expected, request, 10);
    }

    topn_request = context
                       .receive("sender_1")
                       .topN("s2", false, 10)
                       .build(context);
    ASSERT_BLOCKINPUTSTREAM_EQAUL(topn_expected, topn_request, 10);
}
CATCH

TEST_F(InterpreterExecuteTest, Join)
try
{
    // TODO: Find a way to write the request easier.
    {
        // Join Source.
        DAGRequestBuilder table1 = context.scan("test_db", "r_table");
        DAGRequestBuilder table2 = context.scan("test_db", "l_table");
        DAGRequestBuilder table3 = context.scan("test_db", "r_table");
        DAGRequestBuilder table4 = context.scan("test_db", "l_table");

        auto request = table1.join(
                                 table2.join(
                                     table3.join(table4,
                                                 tipb::JoinType::TypeLeftOuterJoin,
                                                 {col("join_c")}),
                                     tipb::JoinType::TypeLeftOuterJoin,
                                     {col("join_c")}),
                                 tipb::JoinType::TypeLeftOuterJoin,
                                 {col("join_c")})
                           .build(context);

        String expected = R"(
CreatingSets
 Union: <for join>
  HashJoinBuild x 10: <join build, build_side_root_executor_id = table_scan_3>, join_kind = Left
   Expression: <append join key and join filters for build side>
    Expression: <final projection>
     MockTableScan
 Union x 2: <for join>
  HashJoinBuild x 10: <join build, build_side_root_executor_id = Join_4>, join_kind = Left
   Expression: <append join key and join filters for build side>
    Expression: <final projection>
     Expression: <remove useless column after join>
      HashJoinProbe: <join probe, join_executor_id = Join_4>
       Expression: <final projection>
        MockTableScan
 Union: <for test>
  Expression x 10: <final projection>
   Expression: <remove useless column after join>
    HashJoinProbe: <join probe, join_executor_id = Join_6>
     Expression: <final projection>
      MockTableScan)";
        ASSERT_BLOCKINPUTSTREAM_EQAUL(expected, request, 10);
    }

    {
        // only join + ExchangeReceiver
        DAGRequestBuilder receiver1 = context.receive("sender_l");
        DAGRequestBuilder receiver2 = context.receive("sender_r");
        DAGRequestBuilder receiver3 = context.receive("sender_l");
        DAGRequestBuilder receiver4 = context.receive("sender_r");

        auto request = receiver1.join(
                                    receiver2.join(
                                        receiver3.join(receiver4,
                                                       tipb::JoinType::TypeLeftOuterJoin,
                                                       {col("join_c")}),
                                        tipb::JoinType::TypeLeftOuterJoin,
                                        {col("join_c")}),
                                    tipb::JoinType::TypeLeftOuterJoin,
                                    {col("join_c")})
                           .build(context);

        String expected = R"(
CreatingSets
 Union: <for join>
  HashJoinBuild x 10: <join build, build_side_root_executor_id = exchange_receiver_3>, join_kind = Left
   Expression: <append join key and join filters for build side>
    Expression: <final projection>
     MockExchangeReceiver
 Union x 2: <for join>
  HashJoinBuild x 10: <join build, build_side_root_executor_id = Join_4>, join_kind = Left
   Expression: <append join key and join filters for build side>
    Expression: <final projection>
     Expression: <remove useless column after join>
      HashJoinProbe: <join probe, join_executor_id = Join_4>
       Expression: <final projection>
        MockExchangeReceiver
 Union: <for test>
  Expression x 10: <final projection>
   Expression: <remove useless column after join>
    HashJoinProbe: <join probe, join_executor_id = Join_6>
     Expression: <final projection>
      MockExchangeReceiver)";
        ASSERT_BLOCKINPUTSTREAM_EQAUL(expected, request, 10);
    }

    {
        // join + receiver + sender
        DAGRequestBuilder receiver1 = context.receive("sender_l");
        DAGRequestBuilder receiver2 = context.receive("sender_r");
        DAGRequestBuilder receiver3 = context.receive("sender_l");
        DAGRequestBuilder receiver4 = context.receive("sender_r");

        auto request = receiver1.join(
                                    receiver2.join(
                                        receiver3.join(receiver4,
                                                       tipb::JoinType::TypeLeftOuterJoin,
                                                       {col("join_c")}),
                                        tipb::JoinType::TypeLeftOuterJoin,
                                        {col("join_c")}),
                                    tipb::JoinType::TypeLeftOuterJoin,
                                    {col("join_c")})
                           .exchangeSender(tipb::PassThrough)
                           .build(context);

        String expected = R"(
CreatingSets
 Union: <for join>
  HashJoinBuild x 10: <join build, build_side_root_executor_id = exchange_receiver_3>, join_kind = Left
   Expression: <append join key and join filters for build side>
    Expression: <final projection>
     MockExchangeReceiver
 Union x 2: <for join>
  HashJoinBuild x 10: <join build, build_side_root_executor_id = Join_4>, join_kind = Left
   Expression: <append join key and join filters for build side>
    Expression: <final projection>
     Expression: <remove useless column after join>
      HashJoinProbe: <join probe, join_executor_id = Join_4>
       Expression: <final projection>
        MockExchangeReceiver
 Union: <for test>
  MockExchangeSender x 10
   Expression: <final projection>
    Expression: <remove useless column after join>
     HashJoinProbe: <join probe, join_executor_id = Join_6>
      Expression: <final projection>
       MockExchangeReceiver)";
        ASSERT_BLOCKINPUTSTREAM_EQAUL(expected, request, 10);
    }
}
CATCH

TEST_F(InterpreterExecuteTest, JoinThenAgg)
try
{
    {
        // Left Join.
        DAGRequestBuilder table1 = context.scan("test_db", "r_table");
        DAGRequestBuilder table2 = context.scan("test_db", "l_table");

        auto request = table1.join(
                                 table2,
                                 tipb::JoinType::TypeLeftOuterJoin,
                                 {col("join_c")})
                           .aggregation({Max(col("r_a"))}, {col("join_c")})
                           .build(context);
        String expected = R"(
CreatingSets
 Union: <for join>
  HashJoinBuild x 10: <join build, build_side_root_executor_id = table_scan_1>, join_kind = Left
   Expression: <append join key and join filters for build side>
    Expression: <final projection>
     MockTableScan
 Union: <for test>
  Expression x 10: <final projection>
   SharedQuery: <restore concurrency>
    ParallelAggregating, max_threads: 10, final: true
     Expression x 10: <remove useless column after join>
      HashJoinProbe: <join probe, join_executor_id = Join_2>
       Expression: <final projection>
        MockTableScan)";
        ASSERT_BLOCKINPUTSTREAM_EQAUL(expected, request, 10);
    }

    {
        // Right Join
        DAGRequestBuilder table1 = context.scan("test_db", "r_table");
        DAGRequestBuilder table2 = context.scan("test_db", "l_table");

        auto request = table1.join(
                                 table2,
                                 tipb::JoinType::TypeRightOuterJoin,
                                 {col("join_c")})
                           .aggregation({Max(col("r_a"))}, {col("join_c")})
                           .build(context);
        String expected = R"(
CreatingSets
 Union: <for join>
  HashJoinBuild x 10: <join build, build_side_root_executor_id = table_scan_1>, join_kind = Right
   Expression: <append join key and join filters for build side>
    Expression: <final projection>
     MockTableScan
 Union: <for test>
  Expression x 10: <final projection>
   SharedQuery: <restore concurrency>
    ParallelAggregating, max_threads: 10, final: true
     Expression x 10: <remove useless column after join>
      HashJoinProbe: <join probe, join_executor_id = Join_2>
       Expression: <append join key and join filters for probe side>
        Expression: <final projection>
         MockTableScan
     Expression x 10: <remove useless column after join>
      NonJoined: <add stream with non_joined_data if full_or_right_join>)";
        ASSERT_BLOCKINPUTSTREAM_EQAUL(expected, request, 10);
    }

    {
        // Right join + receiver + sender
        DAGRequestBuilder receiver1 = context.receive("sender_l");
        DAGRequestBuilder receiver2 = context.receive("sender_r");

        auto request = receiver1.join(
                                    receiver2,
                                    tipb::JoinType::TypeRightOuterJoin,
                                    {col("join_c")})
                           .aggregation({Sum(col("r_a"))}, {col("join_c")})
                           .exchangeSender(tipb::PassThrough)
                           .limit(10)
                           .build(context);
        String expected = R"(
CreatingSets
 Union: <for join>
  HashJoinBuild x 20: <join build, build_side_root_executor_id = exchange_receiver_1>, join_kind = Right
   Expression: <append join key and join filters for build side>
    Expression: <final projection>
     MockExchangeReceiver
 Union: <for test>
  MockExchangeSender x 20
   SharedQuery: <restore concurrency>
    Limit, limit = 10
     Union: <for partial limit>
      Limit x 20, limit = 10
       Expression: <final projection>
        Expression: <before order and select>
         SharedQuery: <restore concurrency>
          ParallelAggregating, max_threads: 20, final: true
           Expression x 20: <remove useless column after join>
            HashJoinProbe: <join probe, join_executor_id = Join_2>
             Expression: <append join key and join filters for probe side>
              Expression: <final projection>
               MockExchangeReceiver
           Expression x 20: <remove useless column after join>
            NonJoined: <add stream with non_joined_data if full_or_right_join>)";
        ASSERT_BLOCKINPUTSTREAM_EQAUL(expected, request, 20);
    }
}
CATCH

TEST_F(InterpreterExecuteTest, ListBase)
try
{
    {
        auto request = context
                           .scan("test_db", "test_table")
                           .filter(eq(col("s1"), col("s2")))
                           .aggregation(Max(col("s1")), col("s2"))
                           .limit(10)
                           .build(context, DAGRequestType::list);
        String expected = R"(
Limit, limit = 10
 Expression: <final projection>
  Aggregating
   Concat
    Expression: <before aggregation>
     Filter: <execute where>
      MockTableScan)";
        ASSERT_BLOCKINPUTSTREAM_EQAUL(expected, request, 1);
    }

    {
        auto request = context
                           .scan("test_db", "test_table")
                           .filter(eq(col("s1"), col("s2")))
                           .aggregation(Max(col("s1")), col("s2"))
                           .topN("s2", false, 10)
                           .build(context, DAGRequestType::list);
        String expected = R"(
Union: <for test>
 SharedQuery x 20: <restore concurrency>
  Expression: <final projection>
   MergeSorting, limit = 10
    Union: <for partial order>
     PartialSorting x 20: limit = 10
      SharedQuery: <restore concurrency>
       ParallelAggregating, max_threads: 20, final: true
        Expression x 20: <before aggregation>
         Filter: <execute where>
          MockTableScan)";
        ASSERT_BLOCKINPUTSTREAM_EQAUL(expected, request, 20);
    }
}
CATCH

} // namespace tests
} // namespace DB
