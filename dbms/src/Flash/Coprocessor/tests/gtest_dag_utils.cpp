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

#include <Flash/Coprocessor/DAGUtils.h>
#include <gtest/gtest.h>

namespace DB
{
namespace tests
{

TEST(DAGUtilsTest, MappedToSum)
{
    tipb::Expr expr;
    expr.set_tp(tipb::ExprType::SumInt);

    ASSERT_TRUE(isAggFunctionExpr(expr));
    ASSERT_EQ(getAggFunctionName(expr), "sum");
    ASSERT_EQ(getFunctionName(expr), "sum");
}

TEST(DAGUtilsTest, MappedToTiDBUnaryMinus)
{
    tipb::Expr expr;
    expr.set_tp(tipb::ExprType::ScalarFunc);

    expr.set_sig(tipb::ScalarFuncSig::UnaryMinusInt);
    ASSERT_EQ(getFunctionName(expr), "tidbUnaryMinusInt");

    expr.set_sig(tipb::ScalarFuncSig::UnaryMinusReal);
    ASSERT_EQ(getFunctionName(expr), "tidbUnaryMinusReal");

    expr.set_sig(tipb::ScalarFuncSig::UnaryMinusDecimal);
    ASSERT_EQ(getFunctionName(expr), "tidbUnaryMinusDecimal");
}

} // namespace tests
} // namespace DB
