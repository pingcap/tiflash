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

#include <Flash/Planner/PhysicalPlan.h>
#include <Flash/Planner/PhysicalPlanVisitor.h>
#include <Operators/OperatorBuilder.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/mockExecutor.h>

namespace DB::tests
{
namespace
{
class GetResultSink : public Sink
{
public:
    explicit GetResultSink(std::vector<Block> & blocks_)
        : blocks(blocks_)
    {}

    OperatorStatus write(Block && block) override
    {
        if (!block)
            return OperatorStatus::FINISHED;
        blocks.push_back(std::move(block));
        return OperatorStatus::PASS;
    }

    std::vector<Block> & blocks;
};
} // namespace

class SimpleOperatorTestRunner : public DB::tests::ExecutorTest
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
        context.addExchangeReceiver("exchange1",
                                    {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}},
                                    {toNullableVec<String>("s1", {"banana", {}, "banana"}),
                                     toNullableVec<String>("s2", {"apple", {}, "banana"})});
        context.addExchangeReceiver("exchange3",
                                    {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}, {"s3", TiDB::TP::TypeLongLong}, {"s4", TiDB::TP::TypeLongLong}},
                                    {toNullableVec<String>("s1", {"banana", {}, "banana"}),
                                     toNullableVec<String>("s2", {"apple", {}, "banana"}),
                                     toNullableVec<Int64>("s3", {1, {}, 1}),
                                     toNullableVec<Int64>("s4", {1, 1, {}})});
    }

    void executeAndAssert(
        const std::shared_ptr<tipb::DAGRequest> & request,
        const ColumnsWithTypeAndName & expect_columns)
    {
        DAGContext dag_context(*request, "operator_test", /*concurrency=*/1);
        context.context.setDAGContext(&dag_context);
        context.context.setMockStorage(context.mockStorage());

        PhysicalPlan physical_plan{context.context, ""};
        physical_plan.build(request.get());
        auto plan_tree = physical_plan.outputAndOptimize();

        OperatorsBuilder op_builder;
        PhysicalPlanVisitor::visitPostOrder(plan_tree, [&](const PhysicalPlanNodePtr & plan) {
            assert(plan);
            plan->transform(op_builder, context.context, /*concurrency=*/1);
        });

        std::vector<Block> blocks;
        op_builder.transform([&blocks](auto & builder) { builder.setSink(std::make_unique<GetResultSink>(blocks)); });
        auto result = op_builder.build();
        assert(result.size() == 1 && result.back().size() == 1);
        const auto & op_exec = result.back().back();
        while (op_exec->execute() != OperatorStatus::FINISHED)
        {
        }
        ASSERT_COLUMNS_EQ_UR(expect_columns, mergeBlocks(std::move(blocks)).getColumnsWithTypeAndName());
    }
};

TEST_F(SimpleOperatorTestRunner, Filter)
try
{
    auto request = context.receive("exchange1")
                       .filter(eq(col("s1"), col("s2")))
                       .build(context);

    executeAndAssert(
        request,
        {toNullableVec<String>({"banana"}),
         toNullableVec<String>({"banana"})});
}
CATCH

TEST_F(SimpleOperatorTestRunner, Limit)
try
{
    auto request = context.receive("exchange1")
                       .limit(1)
                       .build(context);

    executeAndAssert(
        request,
        {toNullableVec<String>({"banana"}),
         toNullableVec<String>({"apple"})});
}
CATCH

TEST_F(SimpleOperatorTestRunner, Projection)
try
{
    auto request = context.receive("exchange1")
                       .project({concat(col("s1"), col("s2"))})
                       .build(context);

    executeAndAssert(
        request,
        {toNullableVec<String>({"bananaapple", {}, "bananabanana"})});

    request = context.receive("exchange3")
                  .project({concat(col("s1"), col("s2")), concat(col("s1"), col("s2")), And(col("s3"), col("s4")), NOT(col("s3"))})
                  .build(context);

    executeAndAssert(
        request,
        {toNullableVec<String>({"bananaapple", {}, "bananabanana"}),
         toNullableVec<String>({"bananaapple", {}, "bananabanana"}),
         toNullableVec<UInt64>({1, {}, {}}),
         toNullableVec<UInt64>({0, {}, 0})});
}
CATCH

TEST_F(SimpleOperatorTestRunner, MockExchangeReceiver)
try
{
    auto request = context.receive("exchange1")
                       .build(context);

    executeAndAssert(
        request,
        {toNullableVec<String>({"banana", {}, "banana"}),
         toNullableVec<String>({"apple", {}, "banana"})});
}
CATCH

TEST_F(SimpleOperatorTestRunner, MockTableScan)
try
{
    auto request = context.scan("test_db", "test_table")
                       .build(context);

    executeAndAssert(
        request,
        {toNullableVec<String>({"banana", {}, "banana"}),
         toNullableVec<String>({"apple", {}, "banana"})});
}
CATCH

} // namespace DB::tests
