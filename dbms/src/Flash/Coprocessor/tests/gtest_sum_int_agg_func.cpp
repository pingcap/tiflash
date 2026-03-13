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

#include <AggregateFunctions/AggregateFunctionNull.h>
#include <AggregateFunctions/AggregateFunctionSum.h>
#include <Core/Types.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/Coprocessor/AggregationInterpreterHelper.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <gtest/gtest.h>

namespace DB
{
namespace tests
{
namespace
{
template <typename T>
using AggregateFunctionSumSimple = AggregateFunctionSum<
    T,
    typename NearestFieldType<T>::Type,
    AggregateFunctionSumData<typename NearestFieldType<T>::Type>>;

template <typename TInput, typename TExpectedOutput>
void checkSumIntReturnType(const String & expected_output, const String & expected_output_nullable)
{
    using NestedFunc = AggregateFunctionSumSimple<TInput>;
    auto nested = std::make_shared<NestedFunc>();
    ASSERT_EQ(nested->getReturnType()->getName(), expected_output);

    auto wrapped
        = std::make_shared<AggregateFunctionNullUnary</*result_is_nullable*/ true, /*input_is_nullable*/ true>>(nested);
    ASSERT_EQ(wrapped->getReturnType()->getName(), expected_output_nullable);
}
} // namespace

TEST(SumIntAggFuncTest, DagUtilsMappedToSum)
{
    tipb::Expr expr;
    expr.set_tp(tipb::ExprType::SumInt);

    ASSERT_TRUE(isAggFunctionExpr(expr));
    ASSERT_EQ(getAggFunctionName(expr), "sum");
    ASSERT_EQ(getFunctionName(expr), "sum");
}

TEST(SumIntAggFuncTest, ExprToStringIsSum)
{
    std::vector<NameAndTypePair> input_cols;
    input_cols.emplace_back("col0", std::make_shared<DataTypeInt32>());

    tipb::Expr col_ref;
    col_ref.set_tp(tipb::ExprType::ColumnRef);
    WriteBufferFromOwnString ss;
    encodeDAGInt64(0, ss);
    col_ref.set_val(ss.releaseStr());

    tipb::Expr sum_int;
    sum_int.set_tp(tipb::ExprType::SumInt);
    *sum_int.add_children() = col_ref;

    ASSERT_EQ(exprToString(sum_int, input_cols), "sum(col0)");
}

TEST(SumIntAggFuncTest, PartialStageIsTreatedAsSum)
{
    tipb::Expr expr;
    expr.set_tp(tipb::ExprType::SumInt);

    ASSERT_FALSE(AggregationInterpreterHelper::isSumOnPartialResults(expr));

    expr.set_aggfuncmode(tipb::AggFunctionMode::Partial1Mode);
    ASSERT_FALSE(AggregationInterpreterHelper::isSumOnPartialResults(expr));

    expr.set_aggfuncmode(tipb::AggFunctionMode::Partial2Mode);
    ASSERT_TRUE(AggregationInterpreterHelper::isSumOnPartialResults(expr));

    expr.set_aggfuncmode(tipb::AggFunctionMode::FinalMode);
    ASSERT_TRUE(AggregationInterpreterHelper::isSumOnPartialResults(expr));
}

TEST(SumIntAggFuncTest, ReturnTypeForIntegerInputs)
{
    checkSumIntReturnType<Int8, Int64>("Int64", "Nullable(Int64)");
    checkSumIntReturnType<Int16, Int64>("Int64", "Nullable(Int64)");
    checkSumIntReturnType<Int32, Int64>("Int64", "Nullable(Int64)");
    checkSumIntReturnType<Int64, Int64>("Int64", "Nullable(Int64)");

    checkSumIntReturnType<UInt8, UInt64>("UInt64", "Nullable(UInt64)");
    checkSumIntReturnType<UInt16, UInt64>("UInt64", "Nullable(UInt64)");
    checkSumIntReturnType<UInt32, UInt64>("UInt64", "Nullable(UInt64)");
    checkSumIntReturnType<UInt64, UInt64>("UInt64", "Nullable(UInt64)");
}

} // namespace tests
} // namespace DB
