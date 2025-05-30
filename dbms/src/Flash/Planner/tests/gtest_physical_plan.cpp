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

#include <DataStreams/ConcatBlockInputStream.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Planner/PhysicalPlan.h>
#include <Flash/Planner/PhysicalPlanVisitor.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/mockExecutor.h>

#include <boost/algorithm/string/replace.hpp>

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

        context.addMockTable(
            {"test_db", "test_table"},
            {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}},
            {toNullableVec<String>("s1", {"banana", {}, "banana"}),
             toNullableVec<String>("s2", {"apple", {}, "banana"})});
        context.addExchangeReceiver(
            "exchange1",
            {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}},
            {toNullableVec<String>("s1", {"banana", {}, "banana"}),
             toNullableVec<String>("s2", {"apple", {}, "banana"})});

        context.addExchangeReceiver(
            "exchange2",
            {{"partition", TiDB::TP::TypeLongLong}, {"order", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2}),
             toNullableVec<Int64>("order", {1, 1, 2, 2, 1, 1, 2, 2})},
            1,
            {{"partition", TiDB::TP::TypeLongLong}});

        context.addExchangeReceiver(
            "exchange3",
            {{"s1", TiDB::TP::TypeString},
             {"s2", TiDB::TP::TypeString},
             {"s3", TiDB::TP::TypeLongLong},
             {"s4", TiDB::TP::TypeLongLong}},
            {toNullableVec<String>("s1", {"banana", {}, "banana"}),
             toNullableVec<String>("s2", {"apple", {}, "banana"}),
             toNullableVec<Int64>("s3", {1, {}, 1}),
             toNullableVec<Int64>("s4", {1, 1, {}})});

        context.addExchangeReceiver(
            "exchange_r_table",
            {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
            {toNullableVec<String>("s", {"banana", "banana"}), toNullableVec<String>("join_c", {"apple", "banana"})});
        context.addExchangeReceiver(
            "exchange_l_table",
            {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
            {toNullableVec<String>("s", {"banana", "banana"}), toNullableVec<String>("join_c", {"apple", "banana"})});

        context.addMockTable(
            {"multi_test", "t1"},
            {{"a", TiDB::TP::TypeLong, false}, {"b", TiDB::TP::TypeLong, false}, {"c", TiDB::TP::TypeLong, false}},
            {toVec<Int32>("a", {1, 3, 0}), toVec<Int32>("b", {2, 2, 0}), toVec<Int32>("c", {3, 2, 0})});
        context.addMockTable(
            {"multi_test", "t2"},
            {{"a", TiDB::TP::TypeLong, false}, {"b", TiDB::TP::TypeLong, false}, {"c", TiDB::TP::TypeLong, false}},
            {toVec<Int32>("a", {3, 3, 0}), toVec<Int32>("b", {4, 2, 0}), toVec<Int32>("c", {5, 3, 0})});
        context.addMockTable(
            {"multi_test", "t3"},
            {{"a", TiDB::TP::TypeLong, false}, {"b", TiDB::TP::TypeLong, false}},
            {toVec<Int32>("a", {1, 2, 0}), toVec<Int32>("b", {2, 2, 0})});
        context.addMockTable(
            {"multi_test", "t4"},
            {{"a", TiDB::TP::TypeLong, false}, {"b", TiDB::TP::TypeLong, false}},
            {toVec<Int32>("a", {3, 2, 0}), toVec<Int32>("b", {4, 2, 0})});
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
        TiFlashTestEnv::setUpTestContext(
            *context.context,
            &dag_context,
            context.mockStorage(),
            TestType::EXECUTOR_TEST);

        PhysicalPlan physical_plan{*context.context, log->identifier()};
        assert(request);
        physical_plan.build(request.get());
        physical_plan.outputAndOptimize();

        ASSERT_EQ(Poco::trim(expected_physical_plan), Poco::trim(physical_plan.toString()));

        BlockInputStreamPtr final_stream;
        {
            DAGPipeline pipeline;
            physical_plan.buildBlockInputStream(pipeline, *context.context, max_streams);
            executeCreatingSets(pipeline, *context.context, max_streams, log);
            final_stream = pipeline.firstStream();
            FmtBuffer fb;
            final_stream->dumpTree(fb);
            ASSERT_EQ(Poco::trim(expected_streams), Poco::trim(fb.toString()));
        }

        ASSERT_COLUMNS_EQ_UR(expect_columns, readBlock(final_stream));
    }

    std::tuple<DAGRequestBuilder, DAGRequestBuilder, DAGRequestBuilder, DAGRequestBuilder> multiTestScan()
    {
        return {
            context.scan("multi_test", "t1"),
            context.scan("multi_test", "t2"),
            context.scan("multi_test", "t3"),
            context.scan("multi_test", "t4")};
    }

    LoggerPtr log = Logger::get("PhysicalPlanTestRunner", "test_physical_plan");
};

String replaceStringName(String s)
{
    return boost::replace_all_copy(s, "{StringName}", DataTypeString::getDefaultName());
}

TEST_F(PhysicalPlanTestRunner, Filter)
try
{
    auto request = context.receive("exchange1").filter(eq(col("s1"), col("s2"))).build(context);

    execute(
        request,
        /*expected_physical_plan=*/replaceStringName(R"(
<Projection, selection_1> | is_tidb_operator: false, schema: <selection_1_s1, Nullable({StringName})>, <selection_1_s2, Nullable({StringName})>
 <Filter, selection_1> | is_tidb_operator: true, schema: <s1, Nullable({StringName})>, <s2, Nullable({StringName})>
  <MockExchangeReceiver, exchange_receiver_0> | is_tidb_operator: true, schema: <s1, Nullable({StringName})>, <s2, Nullable({StringName})>)"),
        /*expected_streams=*/R"(
Expression: <final projection>
 Filter
  MockExchangeReceiver)",
        {toNullableVec<String>({"banana"}), toNullableVec<String>({"banana"})});
}
CATCH

TEST_F(PhysicalPlanTestRunner, Limit)
try
{
    auto request = context.receive("exchange1").limit(1).build(context);

    execute(
        request,
        /*expected_physical_plan=*/replaceStringName(R"(
<Projection, limit_1> | is_tidb_operator: false, schema: <limit_1_s1, Nullable({StringName})>, <limit_1_s2, Nullable({StringName})>
 <Limit, limit_1> | is_tidb_operator: true, schema: <s1, Nullable({StringName})>, <s2, Nullable({StringName})>
  <MockExchangeReceiver, exchange_receiver_0> | is_tidb_operator: true, schema: <s1, Nullable({StringName})>, <s2, Nullable({StringName})>)"),
        /*expected_streams=*/R"(
Expression: <final projection>
 Limit, limit = 1
  MockExchangeReceiver)",
        {toNullableVec<String>({"banana"}), toNullableVec<String>({"apple"})});
}
CATCH

TEST_F(PhysicalPlanTestRunner, TopN)
try
{
    auto request = context.receive("exchange1").topN("s2", false, 1).build(context);

    execute(
        request,
        /*expected_physical_plan=*/replaceStringName(R"(
<Projection, topn_1> | is_tidb_operator: false, schema: <topn_1_s1, Nullable({StringName})>, <topn_1_s2, Nullable({StringName})>
 <TopN, topn_1> | is_tidb_operator: true, schema: <s1, Nullable({StringName})>, <s2, Nullable({StringName})>
  <MockExchangeReceiver, exchange_receiver_0> | is_tidb_operator: true, schema: <s1, Nullable({StringName})>, <s2, Nullable({StringName})>)"),
        /*expected_streams=*/R"(
Expression: <final projection>
 MergeSorting, limit = 1
  PartialSorting: limit = 1
   MockExchangeReceiver)",
        {toNullableVec<String>({{}}), toNullableVec<String>({{}})});
}
CATCH

// agg's schema = agg funcs + agg group bys
TEST_F(PhysicalPlanTestRunner, Aggregation)
try
{
    auto request = context.receive("exchange1").aggregation(Max(col("s2")), col("s1")).build(context);

    execute(
        request,
        /*expected_physical_plan=*/replaceStringName(R"(
<Projection, aggregation_1> | is_tidb_operator: false, schema: <aggregation_1_max(s2)_collator_46 , Nullable({StringName})>, <aggregation_1_first_row(s1)_collator_46 , Nullable({StringName})>
 <Aggregation, aggregation_1> | is_tidb_operator: true, schema: <max(s2)_collator_46 , Nullable({StringName})>, <first_row(s1)_collator_46 , Nullable({StringName})>
  <MockExchangeReceiver, exchange_receiver_0> | is_tidb_operator: true, schema: <s1, Nullable({StringName})>, <s2, Nullable({StringName})>)"),
        /*expected_streams=*/R"(
Expression: <final projection>
 Expression: <expr after aggregation>
  Aggregating
   MockExchangeReceiver)",
        {toNullableVec<String>({{}, "banana"}), toNullableVec<String>({{}, "banana"})});
}
CATCH

TEST_F(PhysicalPlanTestRunner, Projection)
try
{
    auto request = context.receive("exchange1").project({concat(col("s1"), col("s2"))}).build(context);

    execute(
        request,
        /*expected_physical_plan=*/replaceStringName(R"(
<Projection, project_1> | is_tidb_operator: false, schema: <project_1_tidbConcat(s1, s2)_collator_46 , Nullable({StringName})>
 <Projection, project_1> | is_tidb_operator: true, schema: <tidbConcat(s1, s2)_collator_46 , Nullable({StringName})>
  <MockExchangeReceiver, exchange_receiver_0> | is_tidb_operator: true, schema: <s1, Nullable({StringName})>, <s2, Nullable({StringName})>)"),
        /*expected_streams=*/R"(
Expression: <final projection>
 Expression: <projection>
  MockExchangeReceiver)",
        {toNullableVec<String>({"bananaapple", {}, "bananabanana"})});

    request = context.receive("exchange3")
                  .project(
                      {concat(col("s1"), col("s2")),
                       concat(col("s1"), col("s2")),
                       And(col("s3"), col("s4")),
                       NOT(col("s3"))})
                  .build(context);

    execute(
        request,
        /*expected_physical_plan=*/replaceStringName(R"(
<Projection, project_1> | is_tidb_operator: false, schema: <project_1_tidbConcat(s1, s2)_collator_46 , Nullable({StringName})>, <project_1_tidbConcat(s1, s2)_collator_46 _1, Nullable({StringName})>, <project_1_CAST(and(notEquals(s3, 0_Int64)_collator_0 , notEquals(s4, 0_Int64)_collator_0 )_collator_46 , Nullable(UInt64)_{StringName})_collator_0 , Nullable(UInt64)>, <project_1_CAST(not(notEquals(s3, 0_Int64)_collator_0 )_collator_46 , Nullable(UInt64)_{StringName})_collator_0 , Nullable(UInt64)>
 <Projection, project_1> | is_tidb_operator: true, schema: <tidbConcat(s1, s2)_collator_46 , Nullable({StringName})>, <tidbConcat(s1, s2)_collator_46 , Nullable({StringName})>, <CAST(and(notEquals(s3, 0_Int64)_collator_0 , notEquals(s4, 0_Int64)_collator_0 )_collator_46 , Nullable(UInt64)_{StringName})_collator_0 , Nullable(UInt64)>, <CAST(not(notEquals(s3, 0_Int64)_collator_0 )_collator_46 , Nullable(UInt64)_{StringName})_collator_0 , Nullable(UInt64)>
  <MockExchangeReceiver, exchange_receiver_0> | is_tidb_operator: true, schema: <s1, Nullable({StringName})>, <s2, Nullable({StringName})>, <s3, Nullable(Int64)>, <s4, Nullable(Int64)>)"),
        /*expected_streams=*/R"(
Expression: <final projection>
 Expression: <projection>
  MockExchangeReceiver)",
        {toNullableVec<String>({"bananaapple", {}, "bananabanana"}),
         toNullableVec<String>({"bananaapple", {}, "bananabanana"}),
         toNullableVec<UInt64>({1, {}, {}}),
         toNullableVec<UInt64>({0, {}, 0})});
}
CATCH

TEST_F(PhysicalPlanTestRunner, MockExchangeSender)
try
{
    auto request = context.receive("exchange1").exchangeSender(tipb::Hash).build(context);

    execute(
        request,
        /*expected_physical_plan=*/replaceStringName(R"(
<MockExchangeSender, exchange_sender_1> | is_tidb_operator: true, schema: <exchange_sender_1_s1, Nullable({StringName})>, <exchange_sender_1_s2, Nullable({StringName})>
 <Projection, exchange_receiver_0> | is_tidb_operator: false, schema: <exchange_sender_1_s1, Nullable({StringName})>, <exchange_sender_1_s2, Nullable({StringName})>
  <MockExchangeReceiver, exchange_receiver_0> | is_tidb_operator: true, schema: <s1, Nullable({StringName})>, <s2, Nullable({StringName})>)"),
        /*expected_streams=*/R"(
MockExchangeSender
 Expression: <final projection>
  MockExchangeReceiver)",
        {toNullableVec<String>({"banana", {}, "banana"}), toNullableVec<String>({"apple", {}, "banana"})});
}
CATCH

TEST_F(PhysicalPlanTestRunner, MockExchangeReceiver)
try
{
    auto request = context.receive("exchange1").build(context);

    execute(
        request,
        /*expected_physical_plan=*/replaceStringName(R"(
<Projection, exchange_receiver_0> | is_tidb_operator: false, schema: <exchange_receiver_0_s1, Nullable({StringName})>, <exchange_receiver_0_s2, Nullable({StringName})>
 <MockExchangeReceiver, exchange_receiver_0> | is_tidb_operator: true, schema: <s1, Nullable({StringName})>, <s2, Nullable({StringName})>)"),
        /*expected_streams=*/R"(
Expression: <final projection>
 MockExchangeReceiver)",
        {toNullableVec<String>({"banana", {}, "banana"}), toNullableVec<String>({"apple", {}, "banana"})});
}
CATCH

TEST_F(PhysicalPlanTestRunner, WindowFunction)
try
{
    auto get_request = [&](bool enable_fine_grained_shuffle) {
        static const uint64_t enable = 8;
        static const uint64_t disable = 0;
        bool fine_grained_shuffle_stream_count = enable_fine_grained_shuffle ? enable : disable;
        return context.receive("exchange2", fine_grained_shuffle_stream_count)
            .sort(
                {{"partition", false}, {"order", false}, {"partition", false}, {"order", false}},
                true,
                fine_grained_shuffle_stream_count)
            .window(
                RowNumber(),
                {"order", false},
                {"partition", false},
                buildDefaultRowsFrame(),
                fine_grained_shuffle_stream_count)
            .build(context);
    };

    auto request = get_request(false);
    execute(
        request,
        /*expected_physical_plan=*/replaceStringName(R"(
<Projection, window_2> | is_tidb_operator: false, schema: <window_2_window_2_partition, Nullable(Int64)>, <window_2_window_2_order, Nullable(Int64)>, <window_2_CAST(window_2_row_number()_collator , Nullable(Int64)_{StringName})_collator_0 , Nullable(Int64)>
 <Window, window_2> | is_tidb_operator: true, schema: <window_2_partition, Nullable(Int64)>, <window_2_order, Nullable(Int64)>, <window_2_row_number()_collator , Int64>
  <WindowSort, sort_1> | is_tidb_operator: true, schema: <partition, Nullable(Int64)>, <order, Nullable(Int64)>
   <MockExchangeReceiver, exchange_receiver_0> | is_tidb_operator: true, schema: <partition, Nullable(Int64)>, <order, Nullable(Int64)>)"),
        /*expected_streams=*/R"(
Expression: <final projection>
 Expression: <expr after window>
  Window, function: {row_number}, frame: {type: Rows, boundary_begin: Current, boundary_end: Current}
   MergeSorting, limit = 0
    PartialSorting: limit = 0
     MockExchangeReceiver)",
        {toNullableVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>("order", {1, 1, 2, 2, 1, 1, 2, 2}),
         toNullableVec<Int64>("row_number", {1, 2, 3, 4, 1, 2, 3, 4})});

    request = get_request(true);
    execute(
        request,
        /*expected_physical_plan=*/replaceStringName(R"(
<Projection, window_2> | is_tidb_operator: false, schema: <window_2_window_2_partition, Nullable(Int64)>, <window_2_window_2_order, Nullable(Int64)>, <window_2_CAST(window_2_row_number()_collator , Nullable(Int64)_{StringName})_collator_0 , Nullable(Int64)>
 <Window, window_2> | is_tidb_operator: true, schema: <window_2_partition, Nullable(Int64)>, <window_2_order, Nullable(Int64)>, <window_2_row_number()_collator , Int64>
  <WindowSort, sort_1> | is_tidb_operator: true, schema: <partition, Nullable(Int64)>, <order, Nullable(Int64)>
   <MockExchangeReceiver, exchange_receiver_0> | is_tidb_operator: true, schema: <partition, Nullable(Int64)>, <order, Nullable(Int64)>)"),
        /*expected_streams=*/R"(
Expression: <final projection>
 Expression: <expr after window>
  Window: <enable fine grained shuffle>, function: {row_number}, frame: {type: Rows, boundary_begin: Current, boundary_end: Current}
   MergeSorting: <enable fine grained shuffle>, limit = 0
    PartialSorting: <enable fine grained shuffle>: limit = 0
     MockExchangeReceiver)",
        {toNullableVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>("order", {1, 1, 2, 2, 1, 1, 2, 2}),
         toNullableVec<Int64>("row_number", {1, 2, 3, 4, 1, 2, 3, 4})});
}
CATCH

TEST_F(PhysicalPlanTestRunner, MockTableScan)
try
{
    auto request = context.scan("test_db", "test_table").build(context);

    execute(
        request,
        /*expected_physical_plan=*/replaceStringName(R"(
<Projection, table_scan_0> | is_tidb_operator: false, schema: <table_scan_0_s1, Nullable({StringName})>, <table_scan_0_s2, Nullable({StringName})>
 <MockTableScan, table_scan_0> | is_tidb_operator: true, schema: <s1, Nullable({StringName})>, <s2, Nullable({StringName})>)"),
        /*expected_streams=*/R"(
Expression: <final projection>
 MockTableScan)",
        {toNullableVec<String>({"banana", {}, "banana"}), toNullableVec<String>({"apple", {}, "banana"})});
}
CATCH

TEST_F(PhysicalPlanTestRunner, Join)
try
{
    // Simple Join
    {
        auto get_request = [&](const tipb::JoinType & join_type) {
            return context.receive("exchange_l_table")
                .join(context.receive("exchange_r_table"), join_type, {col("join_c"), col("join_c")})
                .build(context);
        };

        auto request = get_request(tipb::JoinType::TypeInnerJoin);
        execute(
            request,
            /*expected_physical_plan=*/replaceStringName(R"(
<Projection, Join_2> | is_tidb_operator: false, schema: <Join_2_Join_2_l_s, Nullable({StringName})>, <Join_2_Join_2_l_join_c, Nullable({StringName})>, <Join_2_Join_2_r_s, Nullable({StringName})>, <Join_2_Join_2_r_join_c, Nullable({StringName})>
 <Join, Join_2> | is_tidb_operator: true, schema: <Join_2_l_s, Nullable({StringName})>, <Join_2_l_join_c, Nullable({StringName})>, <Join_2_r_s, Nullable({StringName})>, <Join_2_r_join_c, Nullable({StringName})>
  <Projection, exchange_receiver_0> | is_tidb_operator: false, schema: <Join_2_l_s, Nullable({StringName})>, <Join_2_l_join_c, Nullable({StringName})>
   <MockExchangeReceiver, exchange_receiver_0> | is_tidb_operator: true, schema: <s, Nullable({StringName})>, <join_c, Nullable({StringName})>
  <Projection, exchange_receiver_1> | is_tidb_operator: false, schema: <Join_2_r_s, Nullable({StringName})>, <Join_2_r_join_c, Nullable({StringName})>
   <MockExchangeReceiver, exchange_receiver_1> | is_tidb_operator: true, schema: <s, Nullable({StringName})>, <join_c, Nullable({StringName})>)"),
            /*expected_streams=*/R"(
CreatingSets
 HashJoinBuild: <join build, build_side_root_executor_id = exchange_receiver_1>, join_kind = Inner
  Expression: <final projection>
   MockExchangeReceiver
 Expression: <final projection>
  HashJoinProbe: <join probe, join_executor_id = Join_2, scan_hash_map_after_probe = false>
   Expression: <final projection>
    MockExchangeReceiver)",
            {toNullableVec<String>({"banana", "banana"}),
             toNullableVec<String>({"apple", "banana"}),
             toNullableVec<String>({"banana", "banana"}),
             toNullableVec<String>({"apple", "banana"})});

        request = get_request(tipb::JoinType::TypeLeftOuterJoin);
        execute(
            request,
            /*expected_physical_plan=*/replaceStringName(R"(
<Projection, Join_2> | is_tidb_operator: false, schema: <Join_2_Join_2_l_s, Nullable({StringName})>, <Join_2_Join_2_l_join_c, Nullable({StringName})>, <Join_2_Join_2_r_s, Nullable({StringName})>, <Join_2_Join_2_r_join_c, Nullable({StringName})>
 <Join, Join_2> | is_tidb_operator: true, schema: <Join_2_l_s, Nullable({StringName})>, <Join_2_l_join_c, Nullable({StringName})>, <Join_2_r_s, Nullable({StringName})>, <Join_2_r_join_c, Nullable({StringName})>
  <Projection, exchange_receiver_0> | is_tidb_operator: false, schema: <Join_2_l_s, Nullable({StringName})>, <Join_2_l_join_c, Nullable({StringName})>
   <MockExchangeReceiver, exchange_receiver_0> | is_tidb_operator: true, schema: <s, Nullable({StringName})>, <join_c, Nullable({StringName})>
  <Projection, exchange_receiver_1> | is_tidb_operator: false, schema: <Join_2_r_s, Nullable({StringName})>, <Join_2_r_join_c, Nullable({StringName})>
   <MockExchangeReceiver, exchange_receiver_1> | is_tidb_operator: true, schema: <s, Nullable({StringName})>, <join_c, Nullable({StringName})>)"),
            /*expected_streams=*/R"(
CreatingSets
 HashJoinBuild: <join build, build_side_root_executor_id = exchange_receiver_1>, join_kind = Left
  Expression: <final projection>
   MockExchangeReceiver
 Expression: <final projection>
  HashJoinProbe: <join probe, join_executor_id = Join_2, scan_hash_map_after_probe = false>
   Expression: <final projection>
    MockExchangeReceiver)",
            {toNullableVec<String>({"banana", "banana"}),
             toNullableVec<String>({"apple", "banana"}),
             toNullableVec<String>({"banana", "banana"}),
             toNullableVec<String>({"apple", "banana"})});

        request = get_request(tipb::JoinType::TypeRightOuterJoin);
        execute(
            request,
            /*expected_physical_plan=*/replaceStringName(R"(
<Projection, Join_2> | is_tidb_operator: false, schema: <Join_2_Join_2_l_s, Nullable({StringName})>, <Join_2_Join_2_l_join_c, Nullable({StringName})>, <Join_2_Join_2_r_s, Nullable({StringName})>, <Join_2_Join_2_r_join_c, Nullable({StringName})>
 <Join, Join_2> | is_tidb_operator: true, schema: <Join_2_l_s, Nullable({StringName})>, <Join_2_l_join_c, Nullable({StringName})>, <Join_2_r_s, Nullable({StringName})>, <Join_2_r_join_c, Nullable({StringName})>
  <Projection, exchange_receiver_0> | is_tidb_operator: false, schema: <Join_2_l_s, Nullable({StringName})>, <Join_2_l_join_c, Nullable({StringName})>
   <MockExchangeReceiver, exchange_receiver_0> | is_tidb_operator: true, schema: <s, Nullable({StringName})>, <join_c, Nullable({StringName})>
  <Projection, exchange_receiver_1> | is_tidb_operator: false, schema: <Join_2_r_s, Nullable({StringName})>, <Join_2_r_join_c, Nullable({StringName})>
   <MockExchangeReceiver, exchange_receiver_1> | is_tidb_operator: true, schema: <s, Nullable({StringName})>, <join_c, Nullable({StringName})>)"),
            /*expected_streams=*/R"(
CreatingSets
 HashJoinBuild: <join build, build_side_root_executor_id = exchange_receiver_1>, join_kind = Right
  Expression: <final projection>
   MockExchangeReceiver
 Expression: <final projection>
  HashJoinProbe: <join probe, join_executor_id = Join_2, scan_hash_map_after_probe = true>
   Expression: <final projection>
    MockExchangeReceiver)",
            {toNullableVec<String>({"banana", "banana"}),
             toNullableVec<String>({"apple", "banana"}),
             toNullableVec<String>({"banana", "banana"}),
             toNullableVec<String>({"apple", "banana"})});
    }

    // MultiRightInnerJoin
    {
        auto [t1, t2, t3, t4] = multiTestScan();
        auto request = t1.join(t2, tipb::JoinType::TypeRightOuterJoin, {col("a")})
                           .join(
                               t3.join(t4, tipb::JoinType::TypeRightOuterJoin, {col("a")}),
                               tipb::JoinType::TypeInnerJoin,
                               {col("b")})
                           .build(context);
        execute(
            request,
            /*expected_physical_plan=*/replaceStringName(R"(
<Projection, Join_6> | is_tidb_operator: false, schema: <Join_6_Join_6_l_Join_4_l_a, Nullable(Int32)>, <Join_6_Join_6_l_Join_4_l_b, Nullable(Int32)>, <Join_6_Join_6_l_Join_4_l_c, Nullable(Int32)>, <Join_6_Join_6_l_Join_4_r_a, Int32>, <Join_6_Join_6_l_Join_4_r_b, Int32>, <Join_6_Join_6_l_Join_4_r_c, Int32>, <Join_6_Join_6_r_Join_5_l_a, Nullable(Int32)>, <Join_6_Join_6_r_Join_5_l_b, Nullable(Int32)>, <Join_6_Join_6_r_Join_5_r_a, Int32>, <Join_6_Join_6_r_Join_5_r_b, Int32>
 <Join, Join_6> | is_tidb_operator: true, schema: <Join_6_l_Join_4_l_a, Nullable(Int32)>, <Join_6_l_Join_4_l_b, Nullable(Int32)>, <Join_6_l_Join_4_l_c, Nullable(Int32)>, <Join_6_l_Join_4_r_a, Int32>, <Join_6_l_Join_4_r_b, Int32>, <Join_6_l_Join_4_r_c, Int32>, <Join_6_r_Join_5_l_a, Nullable(Int32)>, <Join_6_r_Join_5_l_b, Nullable(Int32)>, <Join_6_r_Join_5_r_a, Int32>, <Join_6_r_Join_5_r_b, Int32>
  <Projection, Join_4> | is_tidb_operator: false, schema: <Join_6_l_Join_4_l_a, Nullable(Int32)>, <Join_6_l_Join_4_l_b, Nullable(Int32)>, <Join_6_l_Join_4_l_c, Nullable(Int32)>, <Join_6_l_Join_4_r_a, Int32>, <Join_6_l_Join_4_r_b, Int32>, <Join_6_l_Join_4_r_c, Int32>
   <Join, Join_4> | is_tidb_operator: true, schema: <Join_4_l_a, Nullable(Int32)>, <Join_4_l_b, Nullable(Int32)>, <Join_4_l_c, Nullable(Int32)>, <Join_4_r_a, Int32>, <Join_4_r_b, Int32>, <Join_4_r_c, Int32>
    <Projection, table_scan_0> | is_tidb_operator: false, schema: <Join_4_l_a, Int32>, <Join_4_l_b, Int32>, <Join_4_l_c, Int32>
     <MockTableScan, table_scan_0> | is_tidb_operator: true, schema: <a, Int32>, <b, Int32>, <c, Int32>
    <Projection, table_scan_1> | is_tidb_operator: false, schema: <Join_4_r_a, Int32>, <Join_4_r_b, Int32>, <Join_4_r_c, Int32>
     <MockTableScan, table_scan_1> | is_tidb_operator: true, schema: <a, Int32>, <b, Int32>, <c, Int32>
  <Projection, Join_5> | is_tidb_operator: false, schema: <Join_6_r_Join_5_l_a, Nullable(Int32)>, <Join_6_r_Join_5_l_b, Nullable(Int32)>, <Join_6_r_Join_5_r_a, Int32>, <Join_6_r_Join_5_r_b, Int32>
   <Join, Join_5> | is_tidb_operator: true, schema: <Join_5_l_a, Nullable(Int32)>, <Join_5_l_b, Nullable(Int32)>, <Join_5_r_a, Int32>, <Join_5_r_b, Int32>
    <Projection, table_scan_2> | is_tidb_operator: false, schema: <Join_5_l_a, Int32>, <Join_5_l_b, Int32>
     <MockTableScan, table_scan_2> | is_tidb_operator: true, schema: <a, Int32>, <b, Int32>
    <Projection, table_scan_3> | is_tidb_operator: false, schema: <Join_5_r_a, Int32>, <Join_5_r_b, Int32>
     <MockTableScan, table_scan_3> | is_tidb_operator: true, schema: <a, Int32>, <b, Int32>)"),
            /*expected_streams=*/R"(
CreatingSets
 HashJoinBuild x 2: <join build, build_side_root_executor_id = table_scan_3>, join_kind = Right
  Expression: <final projection>
   MockTableScan
 HashJoinBuild: <join build, build_side_root_executor_id = Join_5>, join_kind = Inner
  Expression: <final projection>
   HashJoinProbe: <join probe, join_executor_id = Join_5, scan_hash_map_after_probe = true>
    Expression: <final projection>
     MockTableScan
 Expression: <final projection>
  HashJoinProbe: <join probe, join_executor_id = Join_6, scan_hash_map_after_probe = false>
   Expression: <final projection>
    HashJoinProbe: <join probe, join_executor_id = Join_4, scan_hash_map_after_probe = true>
     Expression: <final projection>
      MockTableScan)",
            {toNullableVec<Int32>({3, 3, 0}),
             toNullableVec<Int32>({2, 2, 0}),
             toNullableVec<Int32>({2, 2, 0}),
             toVec<Int32>({3, 3, 0}),
             toVec<Int32>({4, 2, 0}),
             toVec<Int32>({5, 3, 0}),
             toNullableVec<Int32>({2, 2, 0}),
             toNullableVec<Int32>({2, 2, 0}),
             toVec<Int32>({2, 2, 0}),
             toVec<Int32>({2, 2, 0})});
    }

    // MultiRightLeftJoin
    {
        auto [t1, t2, t3, t4] = multiTestScan();
        auto request = t1.join(t2, tipb::JoinType::TypeRightOuterJoin, {col("a")})
                           .join(
                               t3.join(t4, tipb::JoinType::TypeRightOuterJoin, {col("a")}),
                               tipb::JoinType::TypeLeftOuterJoin,
                               {col("b")})
                           .build(context);
        execute(
            request,
            /*expected_physical_plan=*/replaceStringName(R"(
<Projection, Join_6> | is_tidb_operator: false, schema: <Join_6_Join_6_l_Join_4_l_a, Nullable(Int32)>, <Join_6_Join_6_l_Join_4_l_b, Nullable(Int32)>, <Join_6_Join_6_l_Join_4_l_c, Nullable(Int32)>, <Join_6_Join_6_l_Join_4_r_a, Int32>, <Join_6_Join_6_l_Join_4_r_b, Int32>, <Join_6_Join_6_l_Join_4_r_c, Int32>, <Join_6_Join_6_r_Join_5_l_a, Nullable(Int32)>, <Join_6_Join_6_r_Join_5_l_b, Nullable(Int32)>, <Join_6_Join_6_r_Join_5_r_a, Nullable(Int32)>, <Join_6_Join_6_r_Join_5_r_b, Nullable(Int32)>
 <Join, Join_6> | is_tidb_operator: true, schema: <Join_6_l_Join_4_l_a, Nullable(Int32)>, <Join_6_l_Join_4_l_b, Nullable(Int32)>, <Join_6_l_Join_4_l_c, Nullable(Int32)>, <Join_6_l_Join_4_r_a, Int32>, <Join_6_l_Join_4_r_b, Int32>, <Join_6_l_Join_4_r_c, Int32>, <Join_6_r_Join_5_l_a, Nullable(Int32)>, <Join_6_r_Join_5_l_b, Nullable(Int32)>, <Join_6_r_Join_5_r_a, Nullable(Int32)>, <Join_6_r_Join_5_r_b, Nullable(Int32)>
  <Projection, Join_4> | is_tidb_operator: false, schema: <Join_6_l_Join_4_l_a, Nullable(Int32)>, <Join_6_l_Join_4_l_b, Nullable(Int32)>, <Join_6_l_Join_4_l_c, Nullable(Int32)>, <Join_6_l_Join_4_r_a, Int32>, <Join_6_l_Join_4_r_b, Int32>, <Join_6_l_Join_4_r_c, Int32>
   <Join, Join_4> | is_tidb_operator: true, schema: <Join_4_l_a, Nullable(Int32)>, <Join_4_l_b, Nullable(Int32)>, <Join_4_l_c, Nullable(Int32)>, <Join_4_r_a, Int32>, <Join_4_r_b, Int32>, <Join_4_r_c, Int32>
    <Projection, table_scan_0> | is_tidb_operator: false, schema: <Join_4_l_a, Int32>, <Join_4_l_b, Int32>, <Join_4_l_c, Int32>
     <MockTableScan, table_scan_0> | is_tidb_operator: true, schema: <a, Int32>, <b, Int32>, <c, Int32>
    <Projection, table_scan_1> | is_tidb_operator: false, schema: <Join_4_r_a, Int32>, <Join_4_r_b, Int32>, <Join_4_r_c, Int32>
     <MockTableScan, table_scan_1> | is_tidb_operator: true, schema: <a, Int32>, <b, Int32>, <c, Int32>
  <Projection, Join_5> | is_tidb_operator: false, schema: <Join_6_r_Join_5_l_a, Nullable(Int32)>, <Join_6_r_Join_5_l_b, Nullable(Int32)>, <Join_6_r_Join_5_r_a, Int32>, <Join_6_r_Join_5_r_b, Int32>
   <Join, Join_5> | is_tidb_operator: true, schema: <Join_5_l_a, Nullable(Int32)>, <Join_5_l_b, Nullable(Int32)>, <Join_5_r_a, Int32>, <Join_5_r_b, Int32>
    <Projection, table_scan_2> | is_tidb_operator: false, schema: <Join_5_l_a, Int32>, <Join_5_l_b, Int32>
     <MockTableScan, table_scan_2> | is_tidb_operator: true, schema: <a, Int32>, <b, Int32>
    <Projection, table_scan_3> | is_tidb_operator: false, schema: <Join_5_r_a, Int32>, <Join_5_r_b, Int32>
     <MockTableScan, table_scan_3> | is_tidb_operator: true, schema: <a, Int32>, <b, Int32>)"),
            /*expected_streams=*/R"(
CreatingSets
 HashJoinBuild x 2: <join build, build_side_root_executor_id = table_scan_3>, join_kind = Right
  Expression: <final projection>
   MockTableScan
 HashJoinBuild: <join build, build_side_root_executor_id = Join_5>, join_kind = Left
  Expression: <final projection>
   HashJoinProbe: <join probe, join_executor_id = Join_5, scan_hash_map_after_probe = true>
    Expression: <final projection>
     MockTableScan
 Expression: <final projection>
  HashJoinProbe: <join probe, join_executor_id = Join_6, scan_hash_map_after_probe = false>
   Expression: <final projection>
    HashJoinProbe: <join probe, join_executor_id = Join_4, scan_hash_map_after_probe = true>
     Expression: <final projection>
      MockTableScan)",
            {toNullableVec<Int32>({3, 3, 0}),
             toNullableVec<Int32>({2, 2, 0}),
             toNullableVec<Int32>({2, 2, 0}),
             toVec<Int32>({3, 3, 0}),
             toVec<Int32>({4, 2, 0}),
             toVec<Int32>({5, 3, 0}),
             toNullableVec<Int32>({2, 2, 0}),
             toNullableVec<Int32>({2, 2, 0}),
             toNullableVec<Int32>({2, 2, 0}),
             toNullableVec<Int32>({2, 2, 0})});
    }
}
CATCH

} // namespace tests
} // namespace DB
