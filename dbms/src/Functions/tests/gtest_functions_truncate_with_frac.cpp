// Copyright 2025 PingCAP, Inc.
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

#include <Functions/FunctionsRound.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace tests
{
class TestFunctionsTruncateWithFrac : public DB::tests::FunctionTest
{
public:
    String func_name = "tidbTruncateWithFrac";

    auto getFrac(size_t size, const std::optional<Int64> & frac)
    {
        return createConstColumn<Nullable<Int64>>(size, {frac});
    }

    auto execute(const ColumnWithTypeAndName & input, const ColumnWithTypeAndName & frac)
    {
        return executeFunction(func_name, input, frac);
    }

    auto execute(const ColumnWithTypeAndName & input, std::optional<Int64> frac)
    {
        return execute(input, getFrac(input.column->size(), frac));
    }
};

template <typename T>
class TestFunctionsTruncateWithFracSigned : public TestFunctionsTruncateWithFrac
{
};

using TestFunctionsTruncateWithFracSignedTypes = ::testing::Types<Int8, Int16, Int32, Int64>;
TYPED_TEST_CASE(TestFunctionsTruncateWithFracSigned, TestFunctionsTruncateWithFracSignedTypes);

TYPED_TEST(TestFunctionsTruncateWithFracSigned, ConstFrac)
try
{
    using Int = TypeParam;

    int digits = std::numeric_limits<Int>::digits10;
    Int large = 5;
    for (int i = 0; i < digits - 1; ++i)
        large *= 10;

#define DATA                                                                                 \
    {                                                                                        \
        0, 1, -1, 4, 5, -4, -5, 49, 50, -49, -50, large - 1, large, -(large - 1), -large, {} \
    }

    auto input = createColumn<Nullable<Int>>(DATA);
    auto output = createColumn<Nullable<Int64>>(DATA);
#undef DATA

    ASSERT_COLUMN_EQ(output, this->execute(input, std::numeric_limits<Int64>::max()));
    for (int i = 0; i <= 100; ++i)
        ASSERT_COLUMN_EQ(output, this->execute(input, i)) << "i = " << i;

    const auto res = (large / 10) * 10;
    const auto res1 = ((large - 1) / 10) * 10;
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({0, 0, 0, 0, 0, 0, 0, 40, 50, -40, -50, res1, res, -res1, -res, {}}),
        this->execute(input, -1));
}
CATCH

TEST_F(TestFunctionsTruncateWithFrac, Float32)
{
    using Float = Float32;

    auto input = createColumn<Nullable<Float>>(
        {0.0, 2.5, -2.5, 0.25, -0.25, 0.125, -0.125, 25, -25, 250, -250, 2.6, 2.4, -2.6, -2.4, {}});

    // const frac
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({0, 2, -2, 0.0, -0.0, 0.0, -0.0, 25, -25, 250, -250, 2, 2, -2, -2, {}}),
        this->execute(input, 0));
    // truncate(2.6, 1) -> 2.6 is 2.599... in Float32, so the result is 2.5 for Float32.
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>(
            {0, 2.5, -2.5, 0.2, -0.2, 0.1, -0.1, 25, -25, 250, -250, 2.5, 2.4, -2.5, -2.4, {}}),
        this->execute(input, 1));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>(
            {0, 2.5, -2.5, 0.25, -0.25, 0.12, -0.12, 25, -25, 250, -250, 2.59, 2.4, -2.59, -2.4, {}}),
        this->execute(input, 2));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>(
            {0, 2.5, -2.5, 0.25, -0.25, 0.125, -0.125, 25, -25, 250, -250, 2.599, 2.4, -2.599, -2.4, {}}),
        this->execute(input, 3));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>(
            {0, 0.0, -0.0, 0.0, -0.0, 0.0, -0.0, 20, -20, 250, -250, 0.0, 0.0, -0.0, -0.0, {}}),
        this->execute(input, -1));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>(
            {0, 0.0, -0.0, 0.0, -0.0, 0.0, -0.0, 0.0, -0.0, 200, -200, 0.0, 0.0, -0.0, -0.0, {}}),
        this->execute(input, -2));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>(
            {0, 0.0, -0.0, 0.0, -0.0, 0.0, -0.0, 0.0, -0.0, 0.0, -0.0, 0.0, 0.0, -0.0, -0.0, {}}),
        this->execute(input, -3));

    // const input
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({0, 0.1, 0.12, 0.125, {}}),
        this->execute(createConstColumn<Float>(5, 0.125), createColumn<Nullable<Int64>>({0, 1, 2, 3, {}})));
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<Float64>>(5, {}),
        this->execute(createConstColumn<Nullable<Float>>(5, {}), createColumn<Nullable<Int64>>({0, 1, 2, 3, {}})));

    // const input & frac
    ASSERT_COLUMN_EQ(
        createConstColumn<Float64>(1, 0.12),
        this->execute(createConstColumn<Float>(1, 0.125), createConstColumn<Int64>(1, 2)));
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<Float64>>(1, {}),
        this->execute(createConstColumn<Float>(1, 0.125), createConstColumn<Nullable<Int64>>(1, {})));
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<Float64>>(1, {}),
        this->execute(createConstColumn<Nullable<Float>>(1, {}), createConstColumn<Int64>(1, 2)));
}

TEST_F(TestFunctionsTruncateWithFrac, Float64)
{
    using Float = Float64;

    auto input = createColumn<Nullable<Float>>(
        {0.0, 2.5, -2.5, 0.25, -0.25, 0.125, -0.125, 25, -25, 250, -250, 2.6, 2.4, -2.6, -2.4, {}});

    // const frac
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({0, 2, -2, 0.0, -0.0, 0.0, -0.0, 25, -25, 250, -250, 2, 2, -2, -2, {}}),
        this->execute(input, 0));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>(
            {0, 2.5, -2.5, 0.2, -0.2, 0.1, -0.1, 25, -25, 250, -250, 2.6, 2.4, -2.6, -2.4, {}}),
        this->execute(input, 1));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>(
            {0, 2.5, -2.5, 0.25, -0.25, 0.12, -0.12, 25, -25, 250, -250, 2.6, 2.4, -2.6, -2.4, {}}),
        this->execute(input, 2));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>(
            {0, 2.5, -2.5, 0.25, -0.25, 0.125, -0.125, 25, -25, 250, -250, 2.6, 2.4, -2.6, -2.4, {}}),
        this->execute(input, 3));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>(
            {0, 0.0, -0.0, 0.0, -0.0, 0.0, -0.0, 20, -20, 250, -250, 0.0, 0.0, -0.0, -0.0, {}}),
        this->execute(input, -1));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>(
            {0, 0.0, -0.0, 0.0, -0.0, 0.0, -0.0, 0.0, -0.0, 200, -200, 0.0, 0.0, -0.0, -0.0, {}}),
        this->execute(input, -2));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>(
            {0, 0.0, -0.0, 0.0, -0.0, 0.0, -0.0, 0.0, -0.0, 0.0, -0.0, 0.0, 0.0, -0.0, -0.0, {}}),
        this->execute(input, -3));

    // const input
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({0, 0.1, 0.12, 0.125, {}}),
        this->execute(createConstColumn<Float>(5, 0.125), createColumn<Nullable<Int64>>({0, 1, 2, 3, {}})));
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<Float64>>(5, {}),
        this->execute(createConstColumn<Nullable<Float>>(5, {}), createColumn<Nullable<Int64>>({0, 1, 2, 3, {}})));

    // const input & frac
    ASSERT_COLUMN_EQ(
        createConstColumn<Float64>(1, 0.12),
        this->execute(createConstColumn<Float>(1, 0.125), createConstColumn<Int64>(1, 2)));
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<Float64>>(1, {}),
        this->execute(createConstColumn<Float>(1, 0.125), createConstColumn<Nullable<Int64>>(1, {})));
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<Float64>>(1, {}),
        this->execute(createConstColumn<Nullable<Float>>(1, {}), createConstColumn<Int64>(1, 2)));
}

TEST_F(TestFunctionsTruncateWithFrac, DecimalUpgrade)
{
    // decimal upgrade
    // Decimal(2, 1) -> Decimal(11, 10)
    ASSERT_COLUMN_EQ(
        createConstColumn<Decimal64>(std::make_tuple(11, 10), 100, "9.9" + String(9, '0')),
        execute(createConstColumn<Decimal32>(std::make_tuple(2, 1), 100, "9.9"), createConstColumn<Int64>(100, 10)));

    // Decimal(5, 0) -> Decimal(35, 30)
    ASSERT_COLUMN_EQ(
        createConstColumn<Decimal128>(std::make_tuple(35, 30), 100, "99999." + String(30, '0')),
        execute(
            createConstColumn<Decimal32>(std::make_tuple(5, 0), 100, "99999"),
            createConstColumn<Int64>(100, 1000)));

    // Decimal(9, 0) -> Decimal(39, 30)
    ASSERT_COLUMN_EQ(
        createConstColumn<Decimal256>(std::make_tuple(39, 30), 100, "999999999." + String(30, '0')),
        execute(
            createConstColumn<Decimal32>(std::make_tuple(9, 0), 100, "999999999"),
            createConstColumn<UInt64>(100, 30)));
}

template <typename T>
class TestFunctionsTruncateWithFracDecimal : public TestFunctionsTruncateWithFrac
{
};

using TestFunctionsTruncateWithFracDecimalTypes = ::testing::Types<Decimal32, Decimal64, Decimal128, Decimal256>;
TYPED_TEST_CASE(TestFunctionsTruncateWithFracDecimal, TestFunctionsTruncateWithFracDecimalTypes);

TYPED_TEST(TestFunctionsTruncateWithFracDecimal, Basic)
try
{
    using Decimal = TypeParam;
    constexpr int max_prec = maxDecimalPrecision<Decimal>();

    auto column = [](std::tuple<PrecType, ScaleType> args, const std::vector<std::optional<String>> & data) {
        return createColumn<Nullable<Decimal>>(args, data);
    };

    auto constColumn = [](std::tuple<PrecType, ScaleType> args, size_t size, const std::optional<String> & data) {
        return createConstColumn<Nullable<Decimal>>(args, size, data);
    };

    // const frac
    {
        // Decimal(max_prec - 1, 3)
        constexpr int prec = max_prec - 1;
        auto large = String(prec - 3, '9') + ".999";
        auto truncated = String(prec - 3, '9');
        auto input = column(
            {prec, 3},
            {"0.000",
             "2.490",
             "-2.490",
             "2.500",
             "-2.500",
             "0.250",
             "-0.250",
             "25.000",
             "-25.000",
             large,
             "-" + large,
             {}});
        ASSERT_COLUMN_EQ(input, this->execute(input, 3));
        ASSERT_COLUMN_EQ(
            column(
                {prec - 1, 2},
                {"0.00",
                 "2.49",
                 "-2.49",
                 "2.50",
                 "-2.50",
                 "0.25",
                 "-0.25",
                 "25.00",
                 "-25.00",
                 truncated + ".99",
                 "-" + truncated + ".99",
                 {}}),
            this->execute(input, 2));
        ASSERT_COLUMN_EQ(
            column(
                {prec - 2, 1},
                {"0.0",
                 "2.4",
                 "-2.4",
                 "2.5",
                 "-2.5",
                 "0.2",
                 "-0.2",
                 "25.0",
                 "-25.0",
                 truncated + ".9",
                 "-" + truncated + ".9",
                 {}}),
            this->execute(input, 1));
    }

    // const input
    {
        auto frac = createColumn<Nullable<Int64>>({3, 2, 1, 0, -1, -2, -3, -4, -5, -6, {}});

        ASSERT_COLUMN_EQ(
            createColumn<Nullable<Decimal32>>(
                std::make_tuple(8, 3),
                {"98765.432",
                 "98765.430",
                 "98765.400",
                 "98765.000",
                 "98760.000",
                 "98700.000",
                 "98000.000",
                 "90000.000",
                 "0.000",
                 "0.000",
                 {}}),
            this->execute(constColumn({max_prec - 1, 3}, 11, "98765.432"), frac));
        ASSERT_COLUMN_EQ(
            constColumn({max_prec - 1, 3}, 11, {}),
            this->execute(constColumn({max_prec - 1, 3}, 11, {}), frac));
    }

    // const input & frac
    ASSERT_COLUMN_EQ(
        createConstColumn<Decimal32>(std::make_tuple(2, 2), 1, "0.02"),
        this->execute(constColumn({max_prec - 1, 3}, 1, "0.025"), createConstColumn<Int64>(1, 2)));
    ASSERT_COLUMN_EQ(
        constColumn({max_prec - 2, 2}, 1, {}),
        this->execute(constColumn({max_prec - 1, 3}, 1, {}), createConstColumn<Int64>(1, 2)));
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<Decimal32>>(std::make_tuple(1, 0), 1, {}, "", 0),
        this->execute(constColumn({max_prec - 1, 3}, 1, "0.025"), createConstColumn<Nullable<Int64>>(1, {})));
    ASSERT_COLUMN_EQ(
        createConstColumn<Decimal32>(std::make_tuple(6, 5), 100, "1." + String(5, '0')),
        this->execute(
            createConstColumn<Decimal>(std::make_tuple(max_prec - 5, 0), 100, "1"),
            createConstColumn<Int64>(100, 5)));
}
CATCH
} // namespace tests
} // namespace DB
