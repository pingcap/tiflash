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

#include <Common/Exception.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <TiDB/Schema/TiDBTypes.h>
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

TEST(DAGUtilsTest, RejectsMalformedVectorFloat32Literal)
{
    tipb::Expr expr;
    expr.set_tp(tipb::ExprType::TiDBVectorFloat32);
    expr.mutable_field_type()->set_tp(TiDB::TypeTiDBVectorFloat32);

    for (const auto & value : {String{}, String(3, '\0'), String{"\x01\0\0\0", 4}, String(4, '\xff')})
    {
        expr.set_val(value);
        EXPECT_THROW(decodeLiteral(expr), Exception);
    }
}

TEST(DAGUtilsTest, RejectsMalformedInt64Literal)
{
    tipb::Expr expr;
    expr.set_tp(tipb::ExprType::Int64);

    for (const auto & value : {String{}, String(4, '\0'), String{"\x01\x02\x03\x04\x05\x06\x07", 7}})
    {
        expr.set_val(value);
        EXPECT_THROW(decodeLiteral(expr), Exception);
    }
}

TEST(DAGUtilsTest, RejectsMalformedUInt64Literal)
{
    tipb::Expr expr;
    expr.set_tp(tipb::ExprType::Uint64);

    for (const auto & value : {String{}, String(4, '\0'), String{"\x01\x02\x03\x04\x05\x06\x07", 7}})
    {
        expr.set_val(value);
        EXPECT_THROW(decodeLiteral(expr), Exception);
    }
}

TEST(DAGUtilsTest, RejectsMalformedDecimalLiteral)
{
    tipb::Expr expr;
    expr.set_tp(tipb::ExprType::MysqlDecimal);

    for (const auto & value : {
             String{}, // no precision/scale header
             String{"\x05\x02", 2}, // header without body
             String{"\x05\x02\x01\x02", 4}, // body shorter than bin_size
         })
    {
        expr.set_val(value);
        EXPECT_THROW(decodeLiteral(expr), Exception);
    }
}

} // namespace tests
} // namespace DB
