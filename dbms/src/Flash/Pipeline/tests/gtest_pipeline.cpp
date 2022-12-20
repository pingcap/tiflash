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

#include <Flash/Pipeline/Pipeline.h>
#include <Flash/Planner/PhysicalPlan.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/mockExecutor.h>

namespace DB
{
namespace tests
{
class PipelineTestRunner : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();

        context.context.setExecutorTest();

        context.addMockTable({"test_db", "test_table"},
                             {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}},
                             {toNullableVec<String>("s1", {"banana", {}, "banana"}),
                              toNullableVec<String>("s2", {"apple", {}, "banana"})});
        context.addExchangeReceiver("exchange",
                                    {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}},
                                    {toNullableVec<String>("s1", {"banana", {}, "banana"}),
                                     toNullableVec<String>("s2", {"apple", {}, "banana"})});
    }

    void assertEquals(
        const std::shared_ptr<tipb::DAGRequest> & request,
        const String & expected_pipeline)
    {
        DAGContext dag_context(*request, "executor_test", /*concurrency=*/1);
        context.context.setDAGContext(&dag_context);
        context.context.setMockStorage(context.mockStorage());

        PhysicalPlan physical_plan{context.context, ""};
        assert(request);
        physical_plan.build(request.get());
        physical_plan.outputAndOptimize();
        auto pipelines = physical_plan.toPipelines();
        assert(!pipelines.empty());
        auto root_pipeline = pipelines[0];
        FmtBuffer buffer;
        root_pipeline->toTreeString(buffer);
        ASSERT_EQ(Poco::trim(expected_pipeline), Poco::trim(buffer.toString()));
    }
};

TEST_F(PipelineTestRunner, simple_pipeline)
try
{
    {
        auto request = context.receive("exchange")
                           .filter(eq(col("s1"), col("s2")))
                           .project({concat(col("s1"), col("s2"))})
                           .limit(1)
                           .exchangeSender(tipb::Hash)
                           .build(context);
        assertEquals(
            request,
            "[<MockExchangeSender, exchange_sender_4> | is_tidb_operator: true, schema: <exchange_sender_4_CAST(tidbConcat(s1, s2)_collator_46 , Nullable(UInt64)_String)_collator_0 , Nullable(UInt64)>] <== "
            "[<Projection, limit_3> | is_tidb_operator: false, schema: <exchange_sender_4_CAST(tidbConcat(s1, s2)_collator_46 , Nullable(UInt64)_String)_collator_0 , Nullable(UInt64)>] <== "
            "[<Limit, limit_3> | is_tidb_operator: true, schema: <tidbConcat(s1, s2)_collator_46 , Nullable(String)>] <== "
            "[<Projection, project_2> | is_tidb_operator: true, schema: <tidbConcat(s1, s2)_collator_46 , Nullable(String)>] <== "
            "[<Filter, selection_1> | is_tidb_operator: true, schema: <s1, Nullable(String)>, <s2, Nullable(String)>] <== "
            "[<MockExchangeReceiver, exchange_receiver_0> | is_tidb_operator: true, schema: <s1, Nullable(String)>, <s2, Nullable(String)>]");
    }

    {
        auto request = context.scan("test_db", "test_table").build(context);
        assertEquals(
            request,
            "[<Projection, table_scan_0> | is_tidb_operator: false, schema: <table_scan_0_s1, Nullable(String)>, <table_scan_0_s2, Nullable(String)>] <== "
            "[<MockTableScan, table_scan_0> | is_tidb_operator: true, schema: <s1, Nullable(String)>, <s2, Nullable(String)>]");
    }
}
CATCH

} // namespace tests
} // namespace DB
