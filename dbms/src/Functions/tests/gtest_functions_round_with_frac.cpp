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

#include <Functions/FunctionsRound.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace tests
{
class TestFunctionsRoundWithFrac : public DB::tests::FunctionTest
{
public:
    String func_name = "tidbRoundWithFrac";

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

TEST_F(TestFunctionsRoundWithFrac, PrecisionInfer)
{
    constexpr auto infer = TiDBRoundPrecisionInferer::infer;
    using Result = std::tuple<PrecType, ScaleType>;

    EXPECT_EQ(infer(9, 3, 2, true), Result(9, 2));
    EXPECT_EQ(infer(9, 3, 1, true), Result(8, 1));
    EXPECT_EQ(infer(9, 3, 0, true), Result(7, 0));
    EXPECT_EQ(infer(9, 3, -1, true), Result(7, 0));
    EXPECT_EQ(infer(9, 3, -2, true), Result(7, 0));
    EXPECT_EQ(infer(9, 3, 40, true), Result(36, 30));
    EXPECT_EQ(infer(9, 3, -100, true), Result(7, 0));
    EXPECT_EQ(infer(9, 3, 0, false), Result(10, 3));
    EXPECT_EQ(infer(9, 3, 233, false), Result(10, 3));

    EXPECT_EQ(infer(18, 6, 2, true), Result(15, 2));
    EXPECT_EQ(infer(18, 6, 1, true), Result(14, 1));
    EXPECT_EQ(infer(18, 6, 0, true), Result(13, 0));
    EXPECT_EQ(infer(18, 6, -1, true), Result(13, 0));
    EXPECT_EQ(infer(18, 6, -2, true), Result(13, 0));
    EXPECT_EQ(infer(18, 6, 40, true), Result(42, 30));
    EXPECT_EQ(infer(18, 6, -100, true), Result(13, 0));
    EXPECT_EQ(infer(18, 6, 0, false), Result(19, 6));
    EXPECT_EQ(infer(18, 6, -233, false), Result(19, 6));

    EXPECT_EQ(infer(30, 30, 2, true), Result(3, 2));
    EXPECT_EQ(infer(30, 30, 1, true), Result(2, 1));
    EXPECT_EQ(infer(30, 30, 0, true), Result(1, 0));
    EXPECT_EQ(infer(30, 30, -1, true), Result(1, 0));
    EXPECT_EQ(infer(30, 30, -2, true), Result(1, 0));
    EXPECT_EQ(infer(30, 30, 40, true), Result(30, 30));
    EXPECT_EQ(infer(30, 30, -100, true), Result(1, 0));
    EXPECT_EQ(infer(30, 30, 0, false), Result(31, 30));
    EXPECT_EQ(infer(30, 30, 233, false), Result(31, 30));

    EXPECT_EQ(infer(64, 0, 40, true), Result(65, 30));
    EXPECT_EQ(infer(64, 0, -100, true), Result(65, 0));
    EXPECT_EQ(infer(64, 0, -62, true), Result(65, 0));
    EXPECT_EQ(infer(64, 0, -63, true), Result(65, 0));
    EXPECT_EQ(infer(64, 0, -64, true), Result(65, 0));
    EXPECT_EQ(infer(64, 0, -65, true), Result(65, 0));
    EXPECT_EQ(infer(64, 0, -66, true), Result(65, 0));
    EXPECT_EQ(infer(64, 0, 0, false), Result(65, 0));
    EXPECT_EQ(infer(64, 0, 233, false), Result(65, 0));

    EXPECT_EQ(infer(65, 30, 40, true), Result(65, 30));
    EXPECT_EQ(infer(65, 30, -100, true), Result(36, 0));
    EXPECT_EQ(infer(65, 30, 0, false), Result(65, 30));
    EXPECT_EQ(infer(65, 30, 233, false), Result(65, 30));

    EXPECT_EQ(infer(65, 0, 233, false), Result(65, 0));
    EXPECT_EQ(infer(65, 1, 233, false), Result(65, 1));
    EXPECT_EQ(infer(64, 0, 233, false), Result(65, 0));

    EXPECT_EQ(infer(18, 6, 4, true), Result(17, 4));
    EXPECT_EQ(infer(18, 6, 5, true), Result(18, 5));
    EXPECT_EQ(infer(18, 6, 6, true), Result(18, 6));
    EXPECT_EQ(infer(18, 6, 7, true), Result(19, 7));
    EXPECT_EQ(infer(18, 6, 8, true), Result(20, 8));
}

template <typename T>
class TestFunctionsRoundWithFracSigned : public TestFunctionsRoundWithFrac
{
};

using TestFunctionsRoundWithFracSignedTypes = ::testing::Types<Int8, Int16, Int32, Int64>;
TYPED_TEST_CASE(TestFunctionsRoundWithFracSigned, TestFunctionsRoundWithFracSignedTypes);

TYPED_TEST(TestFunctionsRoundWithFracSigned, ConstFrac)
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

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({0, 0, 0, 0, 10, 0, -10, 50, 50, -50, -50, large, large, -large, -large, {}}),
        this->execute(input, -1));

    int start = -2;
    if (digits > 2)
    {
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<Int64>>({0, 0, 0, 0, 0, 0, 0, 0, 100, 0, -100, large, large, -large, -large, {}}),
            this->execute(input, -2));

        start = -3;
    }
    for (int i = start; i >= -(digits - 1); --i)
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<Int64>>({0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, large, large, -large, -large, {}}),
            this->execute(input, i))
            << "i = " << i;

    if (digits > 2)
    {
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<Int64>>({0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, large * 2, 0, -large * 2, {}}),
            this->execute(input, -digits));
    }

    auto zeroes = createColumn<Nullable<Int64>>({0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, {}});
    for (int i = -(digits + 1); i >= -100; --i)
        ASSERT_COLUMN_EQ(zeroes, this->execute(input, i)) << "i = " << i;
    ASSERT_COLUMN_EQ(zeroes, this->execute(input, std::numeric_limits<Int64>::min()));
}
CATCH

template <typename T>
class TestFunctionsRoundWithFracUnsigned : public TestFunctionsRoundWithFrac
{
};

using TestFunctionsRoundWithFracUnsignedTypes = ::testing::Types<UInt8, UInt16, UInt32, UInt64>;
TYPED_TEST_CASE(TestFunctionsRoundWithFracUnsigned, TestFunctionsRoundWithFracUnsignedTypes);

TYPED_TEST(TestFunctionsRoundWithFracUnsigned, ConstFrac)
try
{
    using UInt = TypeParam;

    UInt large = 5;
    int digits = std::numeric_limits<UInt>::digits10;
    for (int i = 0; i < digits - 1; ++i)
        large *= 10;

#define DATA                                     \
    {                                            \
        0, 1, 4, 5, 49, 50, large - 1, large, {} \
    }

    auto input = createColumn<Nullable<UInt>>(DATA);
    auto output = createColumn<Nullable<UInt64>>(DATA);
#undef DATA

    ASSERT_COLUMN_EQ(output, this->execute(input, std::numeric_limits<Int64>::max()));
    for (int i = 0; i <= 100; ++i)
        ASSERT_COLUMN_EQ(output, this->execute(input, i)) << "i = " << i;

    ASSERT_COLUMN_EQ(createColumn<Nullable<UInt64>>({0, 0, 0, 10, 50, 50, large, large, {}}), this->execute(input, -1));

    int start = -2;
    if (digits > 2)
    {
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<UInt64>>({0, 0, 0, 0, 0, 100, large, large, {}}),
            this->execute(input, -2));

        start = -3;
    }
    for (int i = start; i >= -(digits - 1); --i)
        ASSERT_COLUMN_EQ(createColumn<Nullable<UInt64>>({0, 0, 0, 0, 0, 0, large, large, {}}), this->execute(input, i))
            << "i = " << i;

    if (digits > 2)
    {
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<UInt64>>({0, 0, 0, 0, 0, 0, 0, large * 2, {}}),
            this->execute(input, -digits));
    }

    auto zeroes = createColumn<Nullable<UInt64>>({0, 0, 0, 0, 0, 0, 0, 0, {}});
    for (int i = -(digits + 1); i >= -100; --i)
        ASSERT_COLUMN_EQ(zeroes, this->execute(input, i)) << "i = " << i;
    ASSERT_COLUMN_EQ(zeroes, this->execute(input, std::numeric_limits<Int64>::min()));
}
CATCH

TEST_F(TestFunctionsRoundWithFrac, Int64ConstFracOverflow)
{
    using Limits = std::numeric_limits<Int64>;

    {
        auto input = createColumn<Int64>({Limits::max(), Limits::min()});

        EXPECT_NO_THROW(execute(input, Limits::max()));
        for (int i = 0; i <= 100; ++i)
            EXPECT_NO_THROW(execute(input, 0)) << "i = " << i;
    }

    for (Int64 v : {Limits::max(), Limits::min()})
    {
        auto input = createColumn<Int64>({v});
        UInt64 value = v;

        for (int i = 0; i <= 100; ++i)
        {
            UInt64 digit = value % 10;
            value /= 10;

            auto message = fmt::format("value = {}, i = {}", value, i);
            if (digit >= 5)
                EXPECT_THROW(execute(input, -(i + 1)), Exception) << message;
            else
                EXPECT_NO_THROW(execute(input, -(i + 1))) << message;
        }

        EXPECT_NO_THROW(execute(input, Limits::min()));
    }
}

TEST_F(TestFunctionsRoundWithFrac, UInt64ConstFracOverflow)
{
    UInt64 value = std::numeric_limits<UInt64>::max();
    auto input = createColumn<UInt64>({value});

    EXPECT_NO_THROW(execute(input, std::numeric_limits<Int64>::max()));
    for (int i = 0; i <= 100; ++i)
        EXPECT_NO_THROW(execute(input, 0)) << "i = " << i;

    for (int i = 0; i <= 100; ++i)
    {
        UInt64 digit = value % 10;
        value /= 10;

        auto message = fmt::format("value = {}, i = {}", value, i);
        if (digit >= 5)
            EXPECT_THROW(execute(input, -(i + 1)), Exception) << message;
        else
            EXPECT_NO_THROW(execute(input, -(i + 1))) << message;
    }

    EXPECT_NO_THROW(execute(input, std::numeric_limits<Int64>::min()));
}

TEST_F(TestFunctionsRoundWithFrac, IntConstInput)
{
    InferredDataVector<Nullable<Int64>> frac_data{0, -1, -2, -3, -4, -5, -6, -7, -8, -9, -10, {}};
    size_t size = frac_data.size();

    Int32 int32_input = 2147398765;
    InferredDataVector<Nullable<Int64>> int32_result
        = {2147398765,
           2147398770,
           2147398800,
           2147399000,
           2147400000,
           2147400000,
           2147000000,
           2150000000,
           2100000000,
           2000000000,
           0,
           {}};

    UInt32 uint32_input = 4293876543;
    InferredDataVector<Nullable<UInt64>> uint32_result
        = {4293876543,
           4293876540,
           4293876500,
           4293877000,
           4293880000,
           4293900000,
           4294000000,
           4290000000,
           4300000000,
           4000000000,
           0,
           {}};

    auto frac = createColumn<Nullable<Int64>>(frac_data);

    {
        // const signed
        auto input = createConstColumn<Int32>(size, int32_input);
        auto result = createColumn<Nullable<Int64>>(int32_result);
        ASSERT_COLUMN_EQ(result, execute(input, frac));
    }

    {
        // const unsigned
        auto input = createConstColumn<UInt32>(size, uint32_input);
        auto result = createColumn<Nullable<UInt64>>(uint32_result);
        ASSERT_COLUMN_EQ(result, execute(input, frac));
    }

    {
        // null constant
        auto input = createConstColumn<Nullable<Int64>>(size, {});
        auto result = createConstColumn<Nullable<Int64>>(size, {});
        ASSERT_COLUMN_EQ(result, execute(input, frac));
    }

    {
        // const signed - const frac
        for (size_t i = 0; i < size; ++i)
        {
            bool frac_data_null = !frac_data[i].has_value();
            ASSERT_COLUMN_EQ(
                frac_data_null ? createConstColumn<Nullable<Int64>>(1, int32_result[i])
                               : createConstColumn<Int64>(1, int32_result[i].value()),
                execute(createConstColumn<Int32>(1, int32_input), createConstColumn<Nullable<Int64>>(1, frac_data[i])));
            ASSERT_COLUMN_EQ(
                frac_data_null ? createConstColumn<Nullable<UInt64>>(1, uint32_result[i])
                               : createConstColumn<UInt64>(1, uint32_result[i].value()),
                execute(
                    createConstColumn<UInt32>(1, uint32_input),
                    createConstColumn<Nullable<Int64>>(1, frac_data[i])));
            ASSERT_COLUMN_EQ(
                createConstColumn<Nullable<Int64>>(1, {}),
                execute(
                    createConstColumn<Nullable<Int64>>(1, {}),
                    createConstColumn<Nullable<Int64>>(1, frac_data[i])));
        }
    }
}

template <typename T>
class TestFunctionsRoundWithFracFloating : public TestFunctionsRoundWithFrac
{
};

using TestFunctionsRoundWithFracFloatingTypes = ::testing::Types<Float32, Float64>;
TYPED_TEST_CASE(TestFunctionsRoundWithFracFloating, TestFunctionsRoundWithFracFloatingTypes);

TYPED_TEST(TestFunctionsRoundWithFracFloating, All)
{
    using Float = TypeParam;

    auto input = createColumn<Nullable<Float>>(
        {0.0, 2.5, -2.5, 0.25, -0.25, 0.125, -0.125, 25, -25, 250, -250, 2.6, 2.4, -2.6, -2.4, {}});

    // const frac

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({0, 2, -2, 0.0, -0.0, 0.0, -0.0, 25, -25, 250, -250, 3, 2, -3, -2, {}}),
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

template <typename T>
class TestFunctionsRoundWithFracDecimal : public TestFunctionsRoundWithFrac
{
};

using TestFunctionsRoundWithFracDecimalTypes = ::testing::Types<Decimal32, Decimal64, Decimal128, Decimal256>;
TYPED_TEST_CASE(TestFunctionsRoundWithFracDecimal, TestFunctionsRoundWithFracDecimalTypes);

TYPED_TEST(TestFunctionsRoundWithFracDecimal, Basic)
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
        auto rounded = "1" + String(prec - 3, '0');
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
                {prec, 2},
                {"0.00",
                 "2.49",
                 "-2.49",
                 "2.50",
                 "-2.50",
                 "0.25",
                 "-0.25",
                 "25.00",
                 "-25.00",
                 rounded + ".00",
                 "-" + rounded + ".00",
                 {}}),
            this->execute(input, 2));
        ASSERT_COLUMN_EQ(
            column(
                {prec - 1, 1},
                {"0.0",
                 "2.5",
                 "-2.5",
                 "2.5",
                 "-2.5",
                 "0.3",
                 "-0.3",
                 "25.0",
                 "-25.0",
                 rounded + ".0",
                 "-" + rounded + ".0",
                 {}}),
            this->execute(input, 1));
        ASSERT_COLUMN_EQ(
            column({prec - 2, 0}, {"0", "2", "-2", "3", "-3", "0", "0", "25", "-25", rounded, "-" + rounded, {}}),
            this->execute(input, 0));
        ASSERT_COLUMN_EQ(
            column({prec - 2, 0}, {"0", "0", "0", "0", "0", "0", "0", "30", "-30", rounded, "-" + rounded, {}}),
            this->execute(input, -1));

        for (int i = -2; i >= -(prec - 3); --i)
            ASSERT_COLUMN_EQ(
                column({prec - 2, 0}, {"0", "0", "0", "0", "0", "0", "0", "0", "0", rounded, "-" + rounded, {}}),
                this->execute(input, i))
                << "i = " << i;

        ASSERT_COLUMN_EQ(
            column({prec - 2, 0}, {"0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", {}}),
            this->execute(input, -(prec - 2)));
        ASSERT_COLUMN_EQ(
            column({prec - 2, 0}, {"0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", {}}),
            this->execute(input, std::numeric_limits<Int64>::min()));
    }

    // const input

    {
        auto frac = createColumn<Nullable<Int64>>({3, 2, 1, 0, -1, -2, -3, -4, -5, -6, {}});

        ASSERT_COLUMN_EQ(
            createColumn<Nullable<Decimal32>>(
                std::make_tuple(9, 3),
                {"98765.432",
                 "98765.430",
                 "98765.400",
                 "98765.000",
                 "98770.000",
                 "98800.000",
                 "99000.000",
                 "100000.000",
                 "100000.000",
                 "0.000",
                 {}}),
            this->execute(constColumn({max_prec - 1, 3}, 11, "98765.432"), frac));
        ASSERT_COLUMN_EQ(
            constColumn({max_prec, 3}, 11, {}),
            this->execute(constColumn({max_prec - 1, 3}, 11, {}), frac));
    }

    // const input & frac

    ASSERT_COLUMN_EQ(
        createConstColumn<Decimal32>(std::make_tuple(3, 2), 1, "0.03"),
        this->execute(constColumn({max_prec - 1, 3}, 1, "0.025"), createConstColumn<Int64>(1, 2)));
    ASSERT_COLUMN_EQ(
        constColumn({max_prec - 1, 2}, 1, {}),
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

TEST_F(TestFunctionsRoundWithFrac, DecimalRound)
{
    // decimal downgrade

    {
        // Decimal(65, 30) -> Decimal(36, 0)
        auto small = "0.5" + String(29, '0');
        auto large = String(35, '9') + "." + String(30, '9');
        auto large_rounded = "1" + String(35, '0');
        ASSERT_COLUMN_EQ(
            createColumn<Decimal128>(std::make_tuple(36, 0), {"1", large_rounded}),
            execute(
                createColumn<Decimal256>(std::make_tuple(65, 30), {small, large}),
                createConstColumn<UInt64>(2, 0)));
    }

    {
        // Decimal(38, 30) -> Decimal(9, 0)
        auto small = "0.49" + String(28, '0');
        auto large = String(8, '9') + "." + String(30, '9');
        auto large_rounded = "1" + String(8, '0');
        ASSERT_COLUMN_EQ(
            createColumn<Decimal32>(std::make_tuple(9, 0), {"0", large_rounded}),
            execute(
                createColumn<Decimal128>(std::make_tuple(38, 30), {small, large}),
                createConstColumn<UInt64>(2, 0)));
    }

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

    // decimal overflow

    {
        auto large = String(65, '9');
        ASSERT_NO_THROW(execute(createColumn<Decimal256>(std::make_tuple(65, 0), {large}), 0));
        ASSERT_NO_THROW(execute(createColumn<Decimal256>(std::make_tuple(65, 0), {"0"}), 1));
        ASSERT_THROW(execute(createColumn<Decimal256>(std::make_tuple(65, 0), {large}), 1), TiFlashException);
        ASSERT_NO_THROW(execute(createColumn<Decimal256>(std::make_tuple(65, 0), {"0"}), -1));
        ASSERT_THROW(execute(createColumn<Decimal256>(std::make_tuple(65, 0), {large}), -1), TiFlashException);
        ASSERT_NO_THROW(execute(createColumn<Decimal256>(std::make_tuple(65, 0), {large}), createColumn<Int64>({1})));
        ASSERT_NO_THROW(execute(createColumn<Decimal256>(std::make_tuple(65, 0), {large}), createColumn<Int64>({0})));
        ASSERT_NO_THROW(execute(createColumn<Decimal256>(std::make_tuple(65, 0), {"0"}), createColumn<Int64>({-1})));
        ASSERT_THROW(
            execute(createColumn<Decimal256>(std::make_tuple(65, 0), {large}), createColumn<Int64>({-1})),
            TiFlashException);
    }

    {
        auto large = String(35, '9');
        ASSERT_NO_THROW(execute(createColumn<Decimal256>(std::make_tuple(35, 0), {large}), 30));
        ASSERT_NO_THROW(execute(createColumn<Decimal256>(std::make_tuple(35, 0), {large}), 100));
    }

    {
        auto large = String(36, '9');
        ASSERT_NO_THROW(execute(createColumn<Decimal256>(std::make_tuple(36, 0), {"0"}), 30));
        ASSERT_NO_THROW(execute(createColumn<Decimal256>(std::make_tuple(36, 0), {"0"}), 100));
        ASSERT_NO_THROW(execute(createColumn<Decimal256>(std::make_tuple(36, 0), {"0"}), 30));
        ASSERT_THROW(execute(createColumn<Decimal256>(std::make_tuple(36, 0), {large}), 30), TiFlashException);
        ASSERT_NO_THROW(execute(createColumn<Decimal256>(std::make_tuple(36, 0), {"0"}), 100));
        ASSERT_THROW(execute(createColumn<Decimal256>(std::make_tuple(36, 0), {large}), 100), TiFlashException);
        ASSERT_NO_THROW(execute(createColumn<Decimal256>(std::make_tuple(36, 0), {large}), createColumn<Int64>({30})));
        ASSERT_NO_THROW(execute(createColumn<Decimal256>(std::make_tuple(36, 0), {large}), createColumn<Int64>({100})));
    }
}

TEST_F(TestFunctionsRoundWithFrac, FracType)
{
    ASSERT_COLUMN_EQ(createColumn<Float64>({1.22}), execute(createColumn<Float32>({1.22}), createColumn<Int64>({2})));
    ASSERT_COLUMN_EQ(createColumn<Float64>({1.22}), execute(createColumn<Float32>({1.22}), createColumn<UInt64>({2})));
    ASSERT_COLUMN_EQ(createColumn<Float64>({1.22}), execute(createColumn<Float32>({1.22}), createColumn<Int32>({2})));
    ASSERT_COLUMN_EQ(createColumn<Float64>({1.22}), execute(createColumn<Float32>({1.22}), createColumn<UInt32>({2})));
    ASSERT_COLUMN_EQ(createColumn<Float64>({1.22}), execute(createColumn<Float32>({1.22}), createColumn<Int16>({2})));
    ASSERT_COLUMN_EQ(createColumn<Float64>({1.22}), execute(createColumn<Float32>({1.22}), createColumn<UInt16>({2})));
    ASSERT_COLUMN_EQ(createColumn<Float64>({1.22}), execute(createColumn<Float32>({1.22}), createColumn<Int8>({2})));
    ASSERT_COLUMN_EQ(createColumn<Float64>({1.22}), execute(createColumn<Float32>({1.22}), createColumn<UInt8>({2})));
}

} // namespace tests

} // namespace DB
