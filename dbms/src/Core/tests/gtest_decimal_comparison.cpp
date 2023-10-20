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

#include <Core/DecimalComparison.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/arithmeticOverflow.h>

namespace DB
{
namespace tests
{
class TestDecimalComparisonUtils : public ::testing::Test
{
};

TEST_F(TestDecimalComparisonUtils, DecimalComparisonApply)
try
{
    {
        // case 1: original issue case, 1/2 < 3 ?
        using A = Decimal32;
        using B = Decimal32;
        A a(500'000'000);
        B b(3);
        int res = DecimalComparison<A, B, LessOp, true>::apply<false, true>(a, b, 1'000'000'000);
        ASSERT_EQ(res, 1);
    }

    {
        // case 2: original issue case, 1*10^64 <= 0.1 ?
        using A = Decimal256;
        using B = Decimal32;
        boost::multiprecision::checked_int256_t origin_a{
            "12345678911234567891123456789112345678911234567891123456789112345"};
        A a(origin_a);
        B b(1);
        int res = DecimalComparison<A, B, LessOrEqualsOp, true>::apply<true, false>(a, b, 10);
        ASSERT_EQ(res, 0);
    }

    {
        // case 3: path check, will apply<true, true> throw?
        using A = Decimal32;
        using B = Decimal32;
        A a(500'000'000);
        B b(3);
        auto func_call = [&] {
            DecimalComparison<A, B, LessOp, true>::apply<true, true>(a, b, 1'000'000'000);
        };
        ASSERT_THROW(func_call(), DB::Exception);
    }

    {
        // case 4: path check, x is positive and will overflow.
        using A = Decimal32;
        using B = Decimal32;
        using Type = DecimalComparison<A, B, LessOp, true>;
        A a(500'000'000);
        B b(0);
        Type::CompareInt scale = 1'000'000'000;

        int res = Type::apply<true, false>(a, b, scale);
        ASSERT_EQ(res, 0);

        Type::CompareInt a_promoted = static_cast<Type::CompareInt>(a);
        int overflowed = common::mulOverflow(a_promoted, scale, a_promoted);
        ASSERT_EQ(overflowed, 1);
    }

    {
        // case 5: path check, x is negative and will overflow.
        using A = Decimal32;
        using B = Decimal32;
        using Type = DecimalComparison<A, B, LessOp, true>;
        A a(-500'000'000);
        B b(0);
        Type::CompareInt scale = 1'000'000'000;

        int res = Type::apply<true, false>(a, b, scale);
        ASSERT_EQ(res, 1);

        Type::CompareInt a_promoted = static_cast<Type::CompareInt>(a);
        int overflowed = common::mulOverflow(a_promoted, scale, a_promoted);
        ASSERT_EQ(overflowed, 1);
    }

    {
        // case 6: path check, y is positive and will overflow.
        using A = Decimal32;
        using B = Decimal32;
        using Type = DecimalComparison<A, B, LessOp, true>;
        A a(-500'000'000);
        B b(500'000'000);
        Type::CompareInt scale = 1'000'000'000;

        int res = Type::apply<false, true>(a, b, scale);
        ASSERT_EQ(res, 1);

        Type::CompareInt b_promoted = static_cast<Type::CompareInt>(b);
        int overflowed = common::mulOverflow(b_promoted, scale, b_promoted);
        ASSERT_EQ(overflowed, 1);
    }

    {
        // case 7: path check, y is negative and will overflow.
        using A = Decimal32;
        using B = Decimal32;
        using Type = DecimalComparison<A, B, LessOp, true>;
        A a(-500'000'000);
        B b(-500'000'000);
        Type::CompareInt scale = 1'000'000'000;

        int res = Type::apply<false, true>(a, b, scale);
        ASSERT_EQ(res, 0);

        Type::CompareInt b_promoted = static_cast<Type::CompareInt>(b);
        int overflowed = common::mulOverflow(b_promoted, scale, b_promoted);
        ASSERT_EQ(overflowed, 1);
    }

    {
        // case 8: path check, no overflow.
        using A = Decimal64;
        using B = Decimal32;
        using Type = DecimalComparison<A, B, LessOp, true>;
        A a(500'000'000'000'000);
        B b(500'000'000);
        Type::CompareInt scale = 1'000'000'000;

        int res = Type::apply<false, true>(a, b, scale);
        ASSERT_EQ(res, 1);

        Type::CompareInt b_promoted = static_cast<Type::CompareInt>(b);
        int overflowed = common::mulOverflow(b_promoted, scale, b_promoted);
        ASSERT_EQ(overflowed, 0);
    }

    {
        // case 9: path check, overflow of int256.
        using A = Decimal256;
        using B = Decimal256;
        using Type = DecimalComparison<A, B, LessOp, true>;
        boost::multiprecision::checked_int256_t origin_a{
            "-12345678911234567891123456789112345678911234567891123456789112345"};
        boost::multiprecision::checked_int256_t origin_b{"114514"};
        boost::multiprecision::checked_int256_t origin_scale{"1000000000000000000000000000000000000000000000000"};
        A a(origin_a);
        B b(origin_b);
        Type::CompareInt scale(origin_scale);

        int res = Type::apply<true, false>(a, b, scale);
        ASSERT_EQ(res, 1);

        Type::CompareInt a_promoted = static_cast<Type::CompareInt>(a);
        int overflowed = common::mulOverflow(a_promoted, scale, a_promoted);
        ASSERT_EQ(overflowed, 1);
    }
}
CATCH

} // namespace tests

} // namespace DB