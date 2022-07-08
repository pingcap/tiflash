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

#include <Flash/Coprocessor/InterpreterUtils.h>
#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/CreatingSetsBlockInputStream.h>
#include <Flash/Planner/PhysicalPlan.h>
#include <Flash/Planner/PhysicalPlanVisitor.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/mockExecutor.h>

namespace DB
{
namespace tests
{
class PhysicalPlanTestRunner : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
        context.addMockTable({"test_db", "test_table"},
                             {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}},
                             {toNullableVec<String>("s1", {"banana", {}, "banana"}),
                              toNullableVec<String>("s2", {"apple", {}, "banana"})});
        context.addExchangeReceiver("exchange1",
                                    {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}},
                                    {toNullableVec<String>("s1", {"banana", {}, "banana"}),
                                     toNullableVec<String>("s2", {"apple", {}, "banana"})});

        context.addExchangeReceiver("exchange_r_table",
                                    {{"s1", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
                                    {toNullableVec<String>("s", {"banana", "banana"}),
                                     toNullableVec<String>("join_c", {"apple", "banana"})});
        context.addExchangeReceiver("exchange_l_table",
                                    {{"s1", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
                                    {toNullableVec<String>("s", {"banana", "banana"}),
                                     toNullableVec<String>("join_c", {"apple", "banana"})});
    }

    void execute(
        const std::shared_ptr<tipb::DAGRequest> & request,
        const String & expected_physical_plan,
        const String & expected_streams,
        const ColumnsWithTypeAndName & expect_columns)
    {
        // TODO support multi-streams.
        size_t max_streams = 1;
        DAGContext dag_context(*request, "executor_test", max_streams);
        dag_context.setColumnsForTest(context.executorIdColumnsMap());
        context.context.setDAGContext(&dag_context);

        PhysicalPlan physical_plan{context.context, log->identifier()};
        assert(request);
        physical_plan.build(request.get());
        physical_plan.outputAndOptimize();

        ASSERT_EQ(Poco::trim(expected_physical_plan), Poco::trim(physical_plan.toString()));

        BlockInputStreamPtr final_stream;
        {
            DAGPipeline pipeline;
            physical_plan.transform(pipeline, context.context, max_streams);
            if (pipeline.streams.size() == 1 && pipeline.streams_with_non_joined_data.empty() && !dag_context.hasSubquery())
            {
                final_stream = pipeline.firstStream();
            }
            else // for join
            {
                // for non joined probe streams.
                BlockInputStreams inputs{};
                inputs.insert(inputs.end(), pipeline.streams.cbegin(), pipeline.streams.cend());
                inputs.insert(inputs.end(), pipeline.streams_with_non_joined_data.cbegin(), pipeline.streams_with_non_joined_data.cend());
                auto probe_stream = std::make_shared<ConcatBlockInputStream>(inputs, log->identifier());
                
                // for join build side streams
                assert(dag_context.hasSubquery());
                const Settings & settings = context.context.getSettingsRef();
                final_stream = std::make_shared<CreatingSetsBlockInputStream>(
                    probe_stream,
                    std::move(dag_context.moveSubqueries()),
                    SizeLimits(settings.max_rows_to_transfer, settings.max_bytes_to_transfer, settings.transfer_overflow_mode),
                    dag_context.log->identifier());
            }
            FmtBuffer fb;
            final_stream->dumpTree(fb);
            ASSERT_EQ(Poco::trim(expected_streams), Poco::trim(fb.toString()));
        }

        readAndAssertBlock(final_stream, expect_columns);
    }

    LoggerPtr log = Logger::get("PhysicalPlanTestRunner", "test_physical_plan");
};

TEST_F(PhysicalPlanTestRunner, Filter)
try
{
    auto request = context.receive("exchange1")
                       .filter(eq(col("s1"), col("s2")))
                       .project({col("s1"), col("s2")})
                       .build(context);

    execute(
        request,
        /*expected_physical_plan=*/R"(
<Projection, project_2> | is_record_profile_streams: true, schema: <s1, Nullable(String)>, <s2, Nullable(String)>
 <Filter, selection_1> | is_record_profile_streams: true, schema: <s1, Nullable(String)>, <s2, Nullable(String)>
  <MockExchangeReceiver, exchange_receiver_0> | is_record_profile_streams: true, schema: <s1, Nullable(String)>, <s2, Nullable(String)>)",
        /*expected_streams=*/R"(
Expression: <projection>
 Filter
  MockExchangeReceiver)",
        {toNullableVec<String>({"banana"}),
         toNullableVec<String>({"banana"})});
}
CATCH

TEST_F(PhysicalPlanTestRunner, Limit)
try
{
    auto request = context.receive("exchange1")
                       .limit(1)
                       .build(context);

    execute(
        request,
        /*expected_physical_plan=*/R"(
<Limit, limit_1> | is_record_profile_streams: true, schema: <s1, Nullable(String)>, <s2, Nullable(String)>
 <MockExchangeReceiver, exchange_receiver_0> | is_record_profile_streams: true, schema: <s1, Nullable(String)>, <s2, Nullable(String)>)",
        /*expected_streams=*/R"(
Limit, limit = 1
 MockExchangeReceiver)",
        {toNullableVec<String>({"banana"}),
         toNullableVec<String>({"apple"})});
}
CATCH

TEST_F(PhysicalPlanTestRunner, TopN)
try
{
    auto request = context.receive("exchange1")
                       .topN("s2", false, 1)
                       .project({col("s1"), col("s2")})
                       .build(context);

    execute(
        request,
        /*expected_physical_plan=*/R"(
<Projection, project_2> | is_record_profile_streams: true, schema: <s1, Nullable(String)>, <s2, Nullable(String)>
 <TopN, topn_1> | is_record_profile_streams: true, schema: <s1, Nullable(String)>, <s2, Nullable(String)>
  <MockExchangeReceiver, exchange_receiver_0> | is_record_profile_streams: true, schema: <s1, Nullable(String)>, <s2, Nullable(String)>)",
        /*expected_streams=*/R"(
Expression: <projection>
 MergeSorting, limit = 1
  PartialSorting: limit = 1
   MockExchangeReceiver)",
        {toNullableVec<String>({{}}),
         toNullableVec<String>({{}})});
}
CATCH

// agg's schema = agg funcs + agg group bys
TEST_F(PhysicalPlanTestRunner, Aggregation)
try
{
    auto request = context.receive("exchange1")
                       .aggregation(Max(col("s2")), col("s1"))
                       .build(context);

    execute(
        request,
        /*expected_physical_plan=*/R"(
<Aggregation, aggregation_1> | is_record_profile_streams: false, schema: <max(s2)_collator_0 , Nullable(String)>, <s1, Nullable(String)>
 <MockExchangeReceiver, exchange_receiver_0> | is_record_profile_streams: true, schema: <s1, Nullable(String)>, <s2, Nullable(String)>)",
        /*expected_streams=*/R"(
Expression: <cast after aggregation>
 Aggregating
  Concat
   MockExchangeReceiver)",
        {toNullableVec<String>({{}, "banana"}),
         toNullableVec<String>({{}, "banana"})});
}
CATCH

TEST_F(PhysicalPlanTestRunner, Projection)
try
{
    auto request = context.receive("exchange1")
                       .project({concat(col("s1"), col("s2"))})
                       .build(context);

    execute(
        request,
        /*expected_physical_plan=*/R"(
<Projection, project_1> | is_record_profile_streams: true, schema: <tidbConcat(s1, s2)_collator_0 , Nullable(String)>
 <MockExchangeReceiver, exchange_receiver_0> | is_record_profile_streams: true, schema: <s1, Nullable(String)>, <s2, Nullable(String)>)",
        /*expected_streams=*/R"(
Expression: <projection>
 MockExchangeReceiver)",
        {toNullableVec<String>({"bananaapple", {}, "bananabanana"})});
}
CATCH

TEST_F(PhysicalPlanTestRunner, MockExchangeSender)
try
{
    auto request = context.receive("exchange1")
                       .exchangeSender(tipb::Hash)
                       .build(context);

    execute(
        request,
        /*expected_physical_plan=*/R"(
<MockExchangeSender, exchange_sender_1> | is_record_profile_streams: true, schema: <s1, Nullable(String)>, <s2, Nullable(String)>
 <MockExchangeReceiver, exchange_receiver_0> | is_record_profile_streams: true, schema: <s1, Nullable(String)>, <s2, Nullable(String)>)",
        /*expected_streams=*/R"(
MockExchangeSender
 MockExchangeReceiver)",
        {toNullableVec<String>({"banana", {}, "banana"}),
         toNullableVec<String>({"apple", {}, "banana"})});
}
CATCH

TEST_F(PhysicalPlanTestRunner, MockExchangeReceiver)
try
{
    auto request = context.receive("exchange1")
                       .build(context);

    execute(
        request,
        /*expected_physical_plan=*/R"(
<MockExchangeReceiver, exchange_receiver_0> | is_record_profile_streams: true, schema: <s1, Nullable(String)>, <s2, Nullable(String)>)",
        /*expected_streams=*/R"(
MockExchangeReceiver)",
        {toNullableVec<String>({"banana", {}, "banana"}),
         toNullableVec<String>({"apple", {}, "banana"})});
}
CATCH

TEST_F(PhysicalPlanTestRunner, Join)
try
{
    auto get_request = [&](const ASTTableJoin::Kind & kind) {
        return context
            .receive("exchange_l_table")
            .join(context.receive("exchange_r_table"), {col("join_c"), col("join_c")}, kind)
            .build(context);
    };

    auto request = get_request(ASTTableJoin::Kind::Left);
    execute(
        request,
        /*expected_physical_plan=*/R"(
<Join, Join_2> | is_record_profile_streams: true, schema: <Join_2_l_s, Nullable(String)>, <Join_2_l_join_c, Nullable(String)>, <Join_2_r_s, Nullable(String)>, <Join_2_r_join_c, Nullable(String)>
 <Projection, NonRootFinalProjection> | is_record_profile_streams: false, schema: <Join_2_r_s, Nullable(String)>, <Join_2_r_join_c, Nullable(String)>
  <MockExchangeReceiver, exchange_receiver_0> | is_record_profile_streams: true, schema: <s, Nullable(String)>, <join_c, Nullable(String)>
 <Projection, NonRootFinalProjection> | is_record_profile_streams: false, schema: <Join_2_l_s, Nullable(String)>, <Join_2_l_join_c, Nullable(String)>
  <MockExchangeReceiver, exchange_receiver_1> | is_record_profile_streams: true, schema: <s, Nullable(String)>, <join_c, Nullable(String)>)",
        /*expected_streams=*/R"(
CreatingSets
 HashJoinBuildBlockInputStream: <join build, build_side_root_executor_id = NonRootFinalProjection>, join_kind = Left
  Expression: <append join key and join filters for build side>
   Expression: <final projection>
    MockExchangeReceiver
 Concat
  Expression: <remove useless column after join>
   HashJoinProbe: <join probe, join_executor_id = Join_2>
    Expression: <append join key and join filters for probe side>
     Expression: <final projection>
      MockExchangeReceiver)",
        {toNullableVec<String>({"banana", "banana", "banana", "banana"}),
         toNullableVec<String>({"apple", "apple", "banana", "banana"}),
         toNullableVec<String>({"banana", "{}", "{}", "banana"}),
         toNullableVec<String>({"apple", "{}", "{}", "banana"})});

    request = get_request(ASTTableJoin::Kind::Right);
    execute(
        request,
        /*expected_physical_plan=*/R"(
<Join, Join_2> | is_record_profile_streams: true, schema: <Join_2_l_s, Nullable(String)>, <Join_2_l_join_c, Nullable(String)>, <Join_2_r_s, Nullable(String)>, <Join_2_r_join_c, Nullable(String)>
 <Projection, NonRootFinalProjection> | is_record_profile_streams: false, schema: <Join_2_r_s, Nullable(String)>, <Join_2_r_join_c, Nullable(String)>
  <MockExchangeReceiver, exchange_receiver_0> | is_record_profile_streams: true, schema: <s, Nullable(String)>, <join_c, Nullable(String)>
 <Projection, NonRootFinalProjection> | is_record_profile_streams: false, schema: <Join_2_l_s, Nullable(String)>, <Join_2_l_join_c, Nullable(String)>
  <MockExchangeReceiver, exchange_receiver_1> | is_record_profile_streams: true, schema: <s, Nullable(String)>, <join_c, Nullable(String)>)",
        /*expected_streams=*/R"(
CreatingSets
 HashJoinBuildBlockInputStream: <join build, build_side_root_executor_id = NonRootFinalProjection>, join_kind = Right
  Expression: <append join key and join filters for build side>
   Expression: <final projection>
    MockExchangeReceiver
 Concat
  Expression: <remove useless column after join>
   HashJoinProbe: <join probe, join_executor_id = Join_2>
    Expression: <append join key and join filters for probe side>
     Expression: <final projection>
      MockExchangeReceiver
  Expression: <remove useless column after join>
   NonJoined: <add stream with non_joined_data if full_or_right_join>)",
        {toNullableVec<String>({"banana", "banana", "banana", "banana"}),
         toNullableVec<String>({"apple", "apple", "banana", "banana"}),
         toNullableVec<String>({"banana", "banana", "banana", "banana"}),
         toNullableVec<String>({"apple", "banana", "apple", "banana"})});

    request = get_request(ASTTableJoin::Kind::Inner);
    execute(
        request,
        /*expected_physical_plan=*/R"(
<Join, Join_2> | is_record_profile_streams: true, schema: <Join_2_l_s, Nullable(String)>, <Join_2_l_join_c, Nullable(String)>, <Join_2_r_s, Nullable(String)>, <Join_2_r_join_c, Nullable(String)>
 <Projection, NonRootFinalProjection> | is_record_profile_streams: false, schema: <Join_2_r_s, Nullable(String)>, <Join_2_r_join_c, Nullable(String)>
  <MockExchangeReceiver, exchange_receiver_0> | is_record_profile_streams: true, schema: <s, Nullable(String)>, <join_c, Nullable(String)>
 <Projection, NonRootFinalProjection> | is_record_profile_streams: false, schema: <Join_2_l_s, Nullable(String)>, <Join_2_l_join_c, Nullable(String)>
  <MockExchangeReceiver, exchange_receiver_1> | is_record_profile_streams: true, schema: <s, Nullable(String)>, <join_c, Nullable(String)>)",
        /*expected_streams=*/R"(
CreatingSets
 HashJoinBuildBlockInputStream: <join build, build_side_root_executor_id = NonRootFinalProjection>, join_kind = Inner
  Expression: <append join key and join filters for build side>
   Expression: <final projection>
    MockExchangeReceiver
 Concat
  Expression: <remove useless column after join>
   HashJoinProbe: <join probe, join_executor_id = Join_2>
    Expression: <append join key and join filters for probe side>
     Expression: <final projection>
      MockExchangeReceiver)",
        {toNullableVec<String>({"banana", "banana"}),
         toNullableVec<String>({"apple", "banana"}),
         toNullableVec<String>({"banana", "banana"}),
         toNullableVec<String>({"apple", "banana"})});
}
CATCH

} // namespace tests
} // namespace DB
