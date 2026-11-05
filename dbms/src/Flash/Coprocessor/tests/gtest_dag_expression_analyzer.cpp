// Copyright 2026 PingCAP, Inc.
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

#include <Common/TiFlashException.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <TestUtils/AggregationTestUtils.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TiDB/Schema/TiDB.h>

namespace DB::tests
{
namespace
{
template <typename F>
void expectBadRequest(F && action)
{
    try
    {
        action();
        FAIL() << "Expected malformed expression arguments to be rejected";
    }
    catch (const TiFlashException & e)
    {
        EXPECT_TRUE(e.getError().is(Errors::Coprocessor::BadRequest)) << e.standardText();
    }
}
} // namespace

class DAGExpressionAnalyzerTest : public FunctionTest
{
public:
    static void SetUpTestCase()
    {
        FunctionTest::SetUpTestCase();
        AggregationTest::SetUpTestCase();
    }
};

TEST_F(DAGExpressionAnalyzerTest, RejectsInWithoutLeftOperand)
{
    for (auto sig :
         {tipb::ScalarFuncSig::InInt,
          tipb::ScalarFuncSig::InReal,
          tipb::ScalarFuncSig::InString,
          tipb::ScalarFuncSig::InDecimal,
          tipb::ScalarFuncSig::InTime,
          tipb::ScalarFuncSig::InDuration})
    {
        SCOPED_TRACE(tipb::ScalarFuncSig_Name(sig));
        tipb::Expr expr;
        expr.set_tp(tipb::ExprType::ScalarFunc);
        expr.set_sig(sig);

        Block block;
        DAGExpressionAnalyzer analyzer(block, *context);
        auto actions = std::make_shared<ExpressionActions>(block.getColumnsWithTypeAndName());
        expectBadRequest([&] { analyzer.getActions(expr, actions); });
        expectBadRequest([&] { exprToString(expr, {}); });
    }
}

TEST_F(DAGExpressionAnalyzerTest, InWithEmptyAndNonEmptySet)
{
    for (bool empty_set : {true, false})
    {
        SCOPED_TRACE(empty_set);
        tipb::Expr expr;
        expr.set_tp(tipb::ExprType::ScalarFunc);
        expr.set_sig(tipb::ScalarFuncSig::InInt);
        expr.mutable_field_type()->set_tp(TiDB::TypeTiny);
        expr.mutable_field_type()->set_flag(TiDB::ColumnFlagUnsigned | TiDB::ColumnFlagNotNull);
        *expr.add_children() = constructInt64LiteralTiExpr(1);
        if (!empty_set)
            *expr.add_children() = constructInt64LiteralTiExpr(1);

        Block block({createColumn<UInt8>({0}, "dummy")});
        DAGExpressionAnalyzer analyzer(block, *context);
        auto actions = std::make_shared<ExpressionActions>(block.getColumnsWithTypeAndName());
        const auto result_name = analyzer.getActions(expr, actions);
        actions->execute(block);
        const auto & result = block.getByName(result_name).column;
        ASSERT_EQ(result->size(), 1);
        EXPECT_EQ(result->getUInt(0), empty_set ? 0 : 1);
        EXPECT_EQ(exprToString(expr, {}), empty_set ? "1 tidbIn ()" : "1 tidbIn (1)");
    }
}

TEST_F(DAGExpressionAnalyzerTest, RejectsGroupConcatWithoutValueAndSeparator)
{
    for (int child_count : {0, 1})
    {
        SCOPED_TRACE(child_count);
        for (bool has_order_by : {false, true})
        {
            SCOPED_TRACE(has_order_by);
            tipb::Aggregation aggregation;
            auto * expr = aggregation.add_agg_func();
            expr->set_tp(tipb::ExprType::GroupConcat);
            if (child_count == 1)
                *expr->add_children() = constructStringLiteralTiExpr(",");
            if (has_order_by)
                *expr->add_order_by()->mutable_expr() = constructInt64LiteralTiExpr(1);

            Block block;
            DAGExpressionAnalyzer analyzer(block, *context);
            auto actions = std::make_shared<ExpressionActions>(block.getColumnsWithTypeAndName());
            AggregateDescriptions aggregate_descriptions;
            NamesAndTypes aggregated_columns;
            expectBadRequest(
                [&] { analyzer.buildAggFuncs(aggregation, actions, aggregate_descriptions, aggregated_columns); });
        }
    }
}

TEST_F(DAGExpressionAnalyzerTest, GroupConcatWithValueAndSeparator)
{
    for (bool has_order_by : {false, true})
    {
        SCOPED_TRACE(has_order_by);
        tipb::Aggregation aggregation;
        auto * expr = aggregation.add_agg_func();
        expr->set_tp(tipb::ExprType::GroupConcat);
        *expr->add_children() = constructStringLiteralTiExpr("value");
        *expr->add_children() = constructStringLiteralTiExpr(",");
        if (has_order_by)
            *expr->add_order_by()->mutable_expr() = constructInt64LiteralTiExpr(1);
        WriteBufferFromOwnString buffer;
        encodeDAGUInt64(1024, buffer);
        expr->set_val(buffer.releaseStr());

        Block block;
        DAGExpressionAnalyzer analyzer(block, *context);
        auto actions = std::make_shared<ExpressionActions>(block.getColumnsWithTypeAndName());
        AggregateDescriptions aggregate_descriptions;
        NamesAndTypes aggregated_columns;
        analyzer.buildAggFuncs(aggregation, actions, aggregate_descriptions, aggregated_columns);
        ASSERT_EQ(aggregate_descriptions.size(), 1);
        EXPECT_EQ(aggregate_descriptions[0].argument_names.size(), 1);
        EXPECT_EQ(aggregated_columns.size(), 1);
    }
}
} // namespace DB::tests
