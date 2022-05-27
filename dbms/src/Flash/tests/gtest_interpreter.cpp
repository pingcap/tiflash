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

#include <TestUtils/InterpreterTestUtils.h>
#include <TestUtils/mockExecutor.h>

namespace DB
{
namespace tests
{
class InterpreterExecuteTest : public DB::tests::InterpreterTest
{
public:
    void initializeContext() override
    {
        InterpreterTest::initializeContext();

        context.addMockTable({"test_db", "test_table"}, {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}});
        context.addMockTable({"test_db", "test_table_1"}, {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}, {"s3", TiDB::TP::TypeString}});
        context.addMockTable({"test_db", "r_table"}, {{"r_a", TiDB::TP::TypeLong}, {"r_b", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}});
        context.addMockTable({"test_db", "l_table"}, {{"l_a", TiDB::TP::TypeLong}, {"l_b", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}});
        context.addExchangeRelationSchema("sender_1", {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}, {"s3", TiDB::TP::TypeString}});
        context.addExchangeRelationSchema("sender_l", {{"l_a", TiDB::TP::TypeString}, {"l_b", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}});
        context.addExchangeRelationSchema("sender_r", {{"r_a", TiDB::TP::TypeString}, {"r_b", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}});
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
Union: <for mpp>
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
Union: <for mpp>
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
                       .project("s1")
                       .build(context);
    {
        String expected = R"(
Union: <for mpp>
 Expression x 10: <final projection>
  Expression: <projection>
   Expression: <before projection>
    Expression: <final projection>
     Expression: <projection>
      Expression: <before projection>
       Expression: <final projection>
        Expression: <projection>
         Expression: <before projection>
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
Union: <for mpp>
 Expression x 10: <final projection>
  Expression: <projection>
   Expression: <before projection>
    SharedQuery: <restore concurrency>
     Expression: <final projection>
      MergeSorting, limit = 10
       Union: <for partial order>
        PartialSorting x 10: limit = 10
         Expression: <projection>
          Expression: <before projection>
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
Union: <for mpp>
 Expression x 10: <final projection>
  Expression: <projection>
   Expression: <before projection>
    Expression: <final projection>
     SharedQuery: <restore concurrency>
      ParallelAggregating, max_threads: 10, final: true
       Expression x 10: <before aggregation>
        Expression: <projection>
         Expression: <before projection>
          SharedQuery: <restore concurrency>
           Expression: <final projection>
            MergeSorting, limit = 10
             Union: <for partial order>
              PartialSorting x 10: limit = 10
               Expression: <projection>
                Expression: <before projection>
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
Union: <for mpp>
 SharedQuery x 10: <restore concurrency>
  Limit, limit = 10
   Union: <for partial limit>
    Limit x 10, limit = 10
     Expression: <final projection>
      Expression: <projection>
       Expression: <before projection>
        Expression: <final projection>
         Expression: <before order and select>
          Filter: <execute where>
           Expression: <projection>
            Expression: <before projection>
             Expression: <final projection>
              SharedQuery: <restore concurrency>
               ParallelAggregating, max_threads: 10, final: true
                Expression x 10: <before aggregation>
                 Expression: <projection>
                  Expression: <before projection>
                   SharedQuery: <restore concurrency>
                    Expression: <final projection>
                     MergeSorting, limit = 10
                      Union: <for partial order>
                       PartialSorting x 10: limit = 10
                        Expression: <projection>
                         Expression: <before projection>
                          Expression: <final projection>
                           MockTableScan)";
        ASSERT_BLOCKINPUTSTREAM_EQAUL(expected, request, 10);
    }

    // Join Source.
    DAGRequestBuilder table1 = context.scan("test_db", "r_table");
    DAGRequestBuilder table2 = context.scan("test_db", "l_table");
    DAGRequestBuilder table3 = context.scan("test_db", "r_table");
    DAGRequestBuilder table4 = context.scan("test_db", "l_table");

    request = table1.join(
                        table2.join(
                            table3.join(table4,
                                        {col("join_c")},
                                        ASTTableJoin::Kind::Left),
                            {col("join_c")},
                            ASTTableJoin::Kind::Left),
                        {col("join_c")},
                        ASTTableJoin::Kind::Left)
                  .build(context);
    {
        String expected = R"(
CreatingSets
 Union: <for join>
  HashJoinBuildBlockInputStream x 10: <join build, build_side_root_executor_id = table_scan_3>, join_kind = Left
   Expression: <append join key and join filters for build side>
    Expression: <final projection>
     MockTableScan
 Union x 2: <for join>
  HashJoinBuildBlockInputStream x 10: <join build, build_side_root_executor_id = Join_4>, join_kind = Left
   Expression: <append join key and join filters for build side>
    Expression: <final projection>
     Expression: <remove useless column after join>
      HashJoinProbe: <join probe, join_executor_id = Join_4>
       Expression: <final projection>
        MockTableScan
 Union: <for mpp>
  Expression x 10: <final projection>
   Expression: <remove useless column after join>
    HashJoinProbe: <join probe, join_executor_id = Join_6>
     Expression: <final projection>
      MockTableScan)";
        ASSERT_BLOCKINPUTSTREAM_EQAUL(expected, request, 10);
    }

    request = context.receive("sender_1")
                  .project({"s1", "s2", "s3"})
                  .project({"s1", "s2"})
                  .project("s1")
                  .build(context);
    {
        String expected = R"(
Union: <for mpp>
 Expression x 10: <final projection>
  Expression: <projection>
   Expression: <before projection>
    Expression: <final projection>
     Expression: <projection>
      Expression: <before projection>
       Expression: <final projection>
        Expression: <projection>
         Expression: <before projection>
          Expression: <final projection>
           MockExchangeReceiver)";
        ASSERT_BLOCKINPUTSTREAM_EQAUL(expected, request, 10);
    }

    request = context.receive("sender_1")
                  .project({"s1", "s2", "s3"})
                  .project({"s1", "s2"})
                  .project("s1")
                  .exchangeSender(tipb::Broadcast)
                  .build(context);
    {
        String expected = R"(
Union: <for mpp>
 MockExchangeSender x 10
  Expression: <final projection>
   Expression: <projection>
    Expression: <before projection>
     Expression: <final projection>
      Expression: <projection>
       Expression: <before projection>
        Expression: <final projection>
         Expression: <projection>
          Expression: <before projection>
           Expression: <final projection>
            MockExchangeReceiver)";
        ASSERT_BLOCKINPUTSTREAM_EQAUL(expected, request, 10);
    }

    // only join + ExchangeReceiver
    DAGRequestBuilder receiver1 = context.receive("sender_l");
    DAGRequestBuilder receiver2 = context.receive("sender_r");
    DAGRequestBuilder receiver3 = context.receive("sender_l");
    DAGRequestBuilder receiver4 = context.receive("sender_r");

    request = receiver1.join(
                           receiver2.join(
                               receiver3.join(receiver4,
                                              {col("join_c")},
                                              ASTTableJoin::Kind::Left),
                               {col("join_c")},
                               ASTTableJoin::Kind::Left),
                           {col("join_c")},
                           ASTTableJoin::Kind::Left)
                  .build(context);
    {
        String expected = R"(
CreatingSets
 Union: <for join>
  HashJoinBuildBlockInputStream x 10: <join build, build_side_root_executor_id = exchange_receiver_3>, join_kind = Left
   Expression: <append join key and join filters for build side>
    Expression: <final projection>
     MockExchangeReceiver
 Union x 2: <for join>
  HashJoinBuildBlockInputStream x 10: <join build, build_side_root_executor_id = Join_4>, join_kind = Left
   Expression: <append join key and join filters for build side>
    Expression: <final projection>
     Expression: <remove useless column after join>
      HashJoinProbe: <join probe, join_executor_id = Join_4>
       Expression: <final projection>
        MockExchangeReceiver
 Union: <for mpp>
  Expression x 10: <final projection>
   Expression: <remove useless column after join>
    HashJoinProbe: <join probe, join_executor_id = Join_6>
     Expression: <final projection>
      MockExchangeReceiver)";
        ASSERT_BLOCKINPUTSTREAM_EQAUL(expected, request, 10);
    }

    // join + receiver + sender
    // TODO: Find a way to write the request easier.
    DAGRequestBuilder receiver5 = context.receive("sender_l");
    DAGRequestBuilder receiver6 = context.receive("sender_r");
    DAGRequestBuilder receiver7 = context.receive("sender_l");
    DAGRequestBuilder receiver8 = context.receive("sender_r");
    request = receiver5.join(
                           receiver6.join(
                               receiver7.join(receiver8,
                                              {col("join_c")},
                                              ASTTableJoin::Kind::Left),
                               {col("join_c")},
                               ASTTableJoin::Kind::Left),
                           {col("join_c")},
                           ASTTableJoin::Kind::Left)
                  .exchangeSender(tipb::PassThrough)
                  .build(context);
    {
        String expected = R"(
CreatingSets
 Union: <for join>
  HashJoinBuildBlockInputStream x 10: <join build, build_side_root_executor_id = exchange_receiver_3>, join_kind = Left
   Expression: <append join key and join filters for build side>
    Expression: <final projection>
     MockExchangeReceiver
 Union x 2: <for join>
  HashJoinBuildBlockInputStream x 10: <join build, build_side_root_executor_id = Join_4>, join_kind = Left
   Expression: <append join key and join filters for build side>
    Expression: <final projection>
     Expression: <remove useless column after join>
      HashJoinProbe: <join probe, join_executor_id = Join_4>
       Expression: <final projection>
        MockExchangeReceiver
 Union: <for mpp>
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

} // namespace tests
} // namespace DB