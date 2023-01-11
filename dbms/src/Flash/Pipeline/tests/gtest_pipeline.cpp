// Copyright 2023 PingCAP, Ltd.
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
        auto root_pipeline = physical_plan.toPipeline();
        assert(root_pipeline);
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
            "pipeline#0: MockExchangeReceiver|exchange_receiver_0 -> Filter|selection_1 -> Projection|project_2 -> Limit|limit_3 -> Projection|NonTiDBOperator -> MockExchangeSender|exchange_sender_4");
    }

    {
        auto request = context.scan("test_db", "test_table").build(context);
        assertEquals(
            request,
            "pipeline#0: MockTableScan|table_scan_0 -> Projection|NonTiDBOperator");
    }
}
CATCH

} // namespace tests
} // namespace DB
