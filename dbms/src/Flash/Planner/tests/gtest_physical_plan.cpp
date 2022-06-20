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
#include <Flash/Planner/PhysicalPlanBuilder.h>
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
    }

    void testPhysicalPlan(
        const std::shared_ptr<tipb::DAGRequest> & request,
        const String & expected_physical_plan,
        const String & expected_streams,
        const ColumnsWithTypeAndName & expect_columns)
    {
        // TODO support multi-streams.
        size_t max_streams = 1;
        PhysicalPlanBuilder builder{context.context, log->identifier()};
        builder.build(request.get());
        auto physical_plan = builder.getResult();

        ASSERT_EQ(Poco::trim(expected_physical_plan), Poco::trim(PhysicalPlanVisitor::visitToString(physical_plan)));

        BlockInputStreamPtr final_stream;
        {
            DAGPipeline pipeline;
            physical_plan->transform(pipeline, context.context, max_streams);
            assert(pipeline.streams.size() == 1 && pipeline.streams_with_non_joined_data.size() == 1);
            final_stream = pipeline.firstStream();
            FmtBuffer fb;
            final_stream->dumpTree(fb);
            ASSERT_EQ(Poco::trim(expected_streams), Poco::trim(fb.toString()));
        }
        
        readAndAssertBlock(final_stream, expect_columns);
    }

protected:
    LoggerPtr log = Logger::get("PhysicalPlanTestRunner", "test_physical_plan");
};

TEST_F(ExecutorTestRunner, Filter)
try
{
    auto request = context.receive("exchange1")
                  .filter(eq(col("s1"), col("s2")))
                  .build(context);
    
    testPhysicalPlan(
        request.get(),
        "",
        "",
        {toNullableVec<String>({"banana"}),
         toNullableVec<String>({"banana"})}
    );
}
CATCH

TEST_F(ExecutorTestRunner, Limit)
try
{
    auto request = context.receive("exchange1")
                  .limit(1)
                  .build(context);
    
    testPhysicalPlan(
        request.get(),
        "",
        "",
        {toNullableVec<String>({"banana"}),
         toNullableVec<String>({"apple"})}
    );
}
CATCH

TEST_F(ExecutorTestRunner, TopN)
try
{
    auto request = context.receive("exchange1")
                  .topN("s2", false, 1)
                  .build(context);
    
    testPhysicalPlan(
        request.get(),
        "",
        "",
        {toNullableVec<String>({{}}),
         toNullableVec<String>({{}})}
    );
}
CATCH

TEST_F(ExecutorTestRunner, Aggregation)
try
{
    auto request = context.receive("exchange1")
                  .aggregation(Max(col("s2")), col("s1"))
                  .build(context);
    
    testPhysicalPlan(
        request.get(),
        "",
        "",
        {toNullableVec<String>({"banana"})}
    );
}
CATCH

TEST_F(ExecutorTestRunner, Projection)
try
{
    auto request = context.receive("exchange1")
                  .project({col("s1")})
                  .build(context);
    
    testPhysicalPlan(
        request.get(),
        "",
        "",
        {toNullableVec<String>({"banana", {}, "banana"})}
    );
}
CATCH

TEST_F(ExecutorTestRunner, MockExchangeSender)
try
{
    auto request = context.receive("exchange1")
                  .exchangeSender(tipb::Hash)
                  .build(context);
    
    testPhysicalPlan(
        request.get(),
        "",
        "",
        {toNullableVec<String>({"banana", {}, "banana"}),
         toNullableVec<String>({"apple", {}, "banana"})}
    );
}
CATCH

TEST_F(ExecutorTestRunner, MockExchangeReceiver)
try
{
    auto request = context.receive("exchange1")
                  .build(context);
    
    testPhysicalPlan(
        request.get(),
        "",
        "",
        {toNullableVec<String>({"banana", {}, "banana"}),
         toNullableVec<String>({"apple", {}, "banana"})}
    );
}
CATCH

} // namespace tests
} // namespace DB