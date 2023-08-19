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
Union
 SharedQuery x 10
  Expression
   MergeSorting
    Union
     PartialSorting x 10
      Expression
       Filter
        SharedQuery
         ParallelAggregating
          Expression x 10
           Filter
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
Union
 SharedQuery x 10
  Limit
   Union
    Limit x 10
     Expression
      Expression
       Filter
        SharedQuery
         ParallelAggregating
          Expression x 10
           Filter
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
Union
 Expression x 10
  Expression
   Expression
    Expression
     Expression
      Expression
       Expression
        Expression
         Expression
          Expression
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
Union
 Expression x 10
  Expression
   Expression
    SharedQuery
     Expression
      MergeSorting
       Union
        PartialSorting x 10
         Expression
          Expression
           Expression
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
Union
 Expression x 10
  Expression
   Expression
    Expression
     SharedQuery
      ParallelAggregating
       Expression x 10
        Expression
         Expression
          SharedQuery
           Expression
            MergeSorting
             Union
              PartialSorting x 10
               Expression
                Expression
                 Expression
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
Union
 SharedQuery x 10
  Limit
   Union
    Limit x 10
     Expression
      Expression
       Expression
        Expression
         Expression
          Filter
           Expression
            Expression
             Expression
              SharedQuery
               ParallelAggregating
                Expression x 10
                 Expression
                  Expression
                   SharedQuery
                    Expression
                     MergeSorting
                      Union
                       PartialSorting x 10
                        Expression
                         Expression
                          Expression
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
 Union
  HashJoinBuildBlockInputStream x 10
   Expression
    Expression
     MockTableScan
 Union x 2
  HashJoinBuildBlockInputStream x 10
   Expression
    Expression
     Expression
      HashJoinProbe
       Expression
        MockTableScan
 Union
  Expression x 10
   Expression
    HashJoinProbe
     Expression
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
Union
 Expression x 10
  Expression
   Expression
    Expression
     Expression
      Expression
       Expression
        Expression
         Expression
          Expression
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
Union
 MockExchangeSender x 10
  Expression
   Expression
    Expression
     Expression
      Expression
       Expression
        Expression
         Expression
          Expression
           Expression
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
 Union
  HashJoinBuildBlockInputStream x 10
   Expression
    Expression
     MockExchangeReceiver
 Union x 2
  HashJoinBuildBlockInputStream x 10
   Expression
    Expression
     Expression
      HashJoinProbe
       Expression
        MockExchangeReceiver
 Union
  Expression x 10
   Expression
    HashJoinProbe
     Expression
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
 Union
  HashJoinBuildBlockInputStream x 10
   Expression
    Expression
     MockExchangeReceiver
 Union x 2
  HashJoinBuildBlockInputStream x 10
   Expression
    Expression
     Expression
      HashJoinProbe
       Expression
        MockExchangeReceiver
 Union
  MockExchangeSender x 10
   Expression
    Expression
     HashJoinProbe
      Expression
       MockExchangeReceiver)";
        ASSERT_BLOCKINPUTSTREAM_EQAUL(expected, request, 10);
    }
}
CATCH

} // namespace tests
} // namespace DB