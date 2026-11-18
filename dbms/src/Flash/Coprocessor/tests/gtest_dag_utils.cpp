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
#include <Common/MyTime.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <IO/Buffer/WriteBufferFromString.h>
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

TEST(DAGUtilsTest, RejectsOutOfRangeMysqlTimeLiteral)
{
    tipb::Expr expr;
    expr.set_tp(tipb::ExprType::MysqlTime);
    expr.mutable_field_type()->set_tp(TiDB::TypeDatetime);

    auto encodePacked = [](UInt64 packed) {
        String val;
        WriteBufferFromString buf(val);
        encodeDAGUInt64(packed, buf);
        val.resize(buf.count());
        return val;
    };

    // In-range packed values are accepted and roundtrip.
    for (const auto packed : {
             UInt64(0), // zero date
             MyDateTime(9999, 12, 31, 23, 59, 59, 999999).toPackedUInt(), // the max valid value
         })
    {
        expr.set_val(encodePacked(packed));
        EXPECT_EQ(decodeLiteral(expr).template safeGet<UInt64>(), packed);
    }

    // Out-of-range packed values can only come from untrusted requests and
    // must be rejected before they reach datetime formatting.
    for (const auto packed : {
             MyDateTime(9999, 12, 31, 23, 59, 59, 0).toPackedUInt() + (UInt64(13) << 46), // year > 9999
             MyDateTime(2020, 1, 1, 0, 0, 0, 999999).toPackedUInt() + 1, // micro_second > 999999
             ~UInt64(0), // all bits set
         })
    {
        expr.set_val(encodePacked(packed));
        EXPECT_THROW(decodeLiteral(expr), Exception);
    }
}

} // namespace tests
} // namespace DB
