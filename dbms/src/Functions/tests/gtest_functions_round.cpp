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

#include <Common/FieldVisitors.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/registerFunctions.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace tests
{
namespace
{
template <typename T>
using Limits = std::numeric_limits<T>;

using DecimalField32 = DecimalField<Decimal32>;
using DecimalField64 = DecimalField<Decimal64>;
using DecimalField128 = DecimalField<Decimal128>;
using DecimalField256 = DecimalField<Decimal256>;

template <typename>
struct ToDecimalType;

template <typename T>
struct ToDecimalType<DecimalField<T>>
{
    using type = T;
};

// parse an array of strings into array of decimals.
template <typename DecimalField>
auto parseDecimalArray(PrecType prec, ScaleType scale, const std::vector<std::optional<std::string>> & literals)
{
    std::vector<std::optional<DecimalField>> result;

    result.reserve(literals.size());
    for (const auto & literal : literals)
    {
        if (literal.has_value())
            result.push_back(parseDecimal<typename ToDecimalType<DecimalField>::type>(literal.value(), prec, scale));
        else
            result.push_back(std::nullopt);
    }

    return result;
}

template <typename Input, typename Output>
struct TestData
{
    using InputType = Input;
    using OutputType = Output;

    PrecType input_prec;
    ScaleType input_scale;
    PrecType output_prec;
    ScaleType output_scale;

    std::vector<std::optional<Input>> input;
    std::vector<std::optional<Output>> output;
};

// `id` is used to distinguish between different test cases with same input and output types.
template <typename Input, typename Output, size_t id_ = 0>
struct TestCase
{
    using InputType = Input;
    using OutputType = Output;
    static constexpr size_t id = id_;
};

using TestCases = ::testing::Types<
    TestCase<Int64, Int64>,
    TestCase<UInt64, UInt64>,
    TestCase<Float64, Float64>,
    TestCase<DecimalField32, DecimalField32, 0>,
    TestCase<DecimalField32, DecimalField32, 1>,
    TestCase<DecimalField32, DecimalField32, 2>,
    TestCase<DecimalField256, DecimalField256>,
    TestCase<DecimalField128, DecimalField128>,
    TestCase<DecimalField64, DecimalField32>,
    TestCase<DecimalField128, DecimalField64>,
    TestCase<DecimalField256, DecimalField64>>;

template <typename InputType, typename OutputType, size_t id = 0>
auto getTestData();

template <>
auto getTestData<Int64, Int64>()
{
    return TestData<Int64, Int64>{
        0,
        0,
        0,
        0,
        {0, 1, -1, Limits<Int64>::max(), Limits<Int64>::min(), std::nullopt},
        {0, 1, -1, Limits<Int64>::max(), Limits<Int64>::min(), std::nullopt}};
};

template <>
auto getTestData<UInt64, UInt64>()
{
    return TestData<UInt64, UInt64>{
        0,
        0,
        0,
        0,
        {0, 1, Limits<UInt64>::max(), std::nullopt},
        {0, 1, Limits<UInt64>::max(), std::nullopt}};
}

template <>
auto getTestData<Float64, Float64>()
{
    double large_value = std::pow(10.0, 100.0);
    return TestData<Float64, Float64>{
        0,
        0,
        0,
        0,
        {-5.5,
         -4.5,
         -3.5,
         -2.5,
         -1.5,
         -0.6,
         -0.5,
         -0.4,
         0.0,
         0.4,
         0.5,
         0.6,
         1.5,
         2.5,
         3.5,
         4.5,
         5.5,
         large_value,
         std::nullopt},
        {-6.0,
         -4.0,
         -4.0,
         -2.0,
         -2.0,
         -1.0,
         0.0,
         0.0,
         0.0,
         0.0,
         0.0,
         1.0,
         2.0,
         2.0,
         4.0,
         4.0,
         6.0,
         large_value,
         std::nullopt}};
}

// Decimal(9, 0) -> Decimal(9, 0)
template <>
auto getTestData<DecimalField32, DecimalField32, 0>()
{
    return TestData<DecimalField32, DecimalField32>{
        9,
        0,
        9,
        0,
        parseDecimalArray<DecimalField32>(9, 0, {"0", "1", "-1", "999999999", "-999999999", std::nullopt}),
        parseDecimalArray<DecimalField32>(9, 0, {"0", "1", "-1", "999999999", "-999999999", std::nullopt})};
}

// Decimal(9, 1) -> Decimal(9, 0)
template <>
auto getTestData<DecimalField32, DecimalField32, 1>()
{
    return TestData<DecimalField32, DecimalField32>{
        9,
        1,
        9,
        0,
        parseDecimalArray<DecimalField32>(
            9,
            1,
            {"-2.5",
             "-1.5",
             "-0.6",
             "-0.5",
             "-0.4",
             "0.0",
             "0.4",
             "0.5",
             "0.6",
             "1.5",
             "2.5",
             "99999999.9",
             "-99999999.9",
             std::nullopt}),
        parseDecimalArray<DecimalField32>(
            9,
            0,
            {"-3", "-2", "-1", "-1", "0", "0", "0", "1", "1", "2", "3", "100000000", "-100000000", std::nullopt})};
}

// Decimal(9, 9) -> Decimal(1, 0)
template <>
auto getTestData<DecimalField32, DecimalField32, 2>()
{
    return TestData<DecimalField32, DecimalField32>{
        9,
        9,
        1,
        0,
        parseDecimalArray<DecimalField32>(
            9,
            9,
            {"0.000000000",
             "0.000000001",
             "-0.000000001",
             "0.500000000",
             "-0.500000000",
             "0.999999999",
             "-0.999999999",
             std::nullopt}),
        parseDecimalArray<DecimalField32>(1, 0, {"0", "0", "0", "1", "-1", "1", "-1", std::nullopt})};
}

// Decimal(65, 0) -> Decimal(65, 0)
template <>
auto getTestData<DecimalField256, DecimalField256>()
{
    std::string extreme(65, '9');

    return TestData<DecimalField256, DecimalField256>{
        65,
        0,
        65,
        0,
        parseDecimalArray<DecimalField256>(65, 0, {"0", "1", "-1", extreme, "-" + extreme, std::nullopt}),
        parseDecimalArray<DecimalField256>(65, 0, {"0", "1", "-1", extreme, "-" + extreme, std::nullopt})};
}

// Decimal(38, 1) -> Decimal(38, 0)
template <>
auto getTestData<DecimalField128, DecimalField128>()
{
    auto extreme = std::string(37, '9') + ".9";
    auto rounded_extreme = '1' + std::string(37, '0');

    return TestData<DecimalField128, DecimalField128>{
        38,
        1,
        38,
        0,
        parseDecimalArray<DecimalField128>(
            38,
            1,
            {"-2.5",
             "-1.5",
             "-0.6",
             "-0.5",
             "-0.4",
             "0.0",
             "0.4",
             "0.5",
             "0.6",
             "1.5",
             "2.5",
             extreme,
             "-" + extreme,
             std::nullopt}),
        parseDecimalArray<DecimalField128>(
            38,
            0,
            {"-3",
             "-2",
             "-1",
             "-1",
             "0",
             "0",
             "0",
             "1",
             "1",
             "2",
             "3",
             rounded_extreme,
             "-" + rounded_extreme,
             std::nullopt})};
}

// Decimal(18, 10) -> Decimal(9, 0)
template <>
auto getTestData<DecimalField64, DecimalField32>()
{
    auto zeros = '.' + std::string(10, '0');
    auto half = ".5" + std::string(9, '5');
    auto extreme = std::string(8, '9') + '.' + std::string(10, '9');
    auto rounded_extreme = '1' + std::string(8, '0');

    return TestData<DecimalField64, DecimalField32>{
        18,
        10,
        9,
        0,
        parseDecimalArray<DecimalField64>(
            18,
            10,
            {"0" + zeros, "0" + half, "-0" + half, extreme, "-" + extreme, std::nullopt}),
        parseDecimalArray<DecimalField32>(
            9,
            0,
            {"0", "1", "-1", rounded_extreme, "-" + rounded_extreme, std::nullopt})};
}

// Decimal(25, 10) -> Decimal(16, 0)
template <>
auto getTestData<DecimalField128, DecimalField64>()
{
    auto zeros = '.' + std::string(10, '0');
    auto half = ".5" + std::string(9, '5');
    auto extreme = std::string(15, '9') + '.' + std::string(10, '9');
    auto rounded_extreme = '1' + std::string(15, '0');

    return TestData<DecimalField128, DecimalField64>{
        25,
        10,
        16,
        0,
        parseDecimalArray<DecimalField128>(
            25,
            10,
            {"0" + zeros, "0" + half, "-0" + half, extreme, "-" + extreme, std::nullopt}),
        parseDecimalArray<DecimalField64>(
            16,
            0,
            {"0", "1", "-1", rounded_extreme, "-" + rounded_extreme, std::nullopt})};
}

// Decimal(40, 30) -> Decimal(11, 0)
template <>
auto getTestData<DecimalField256, DecimalField64>()
{
    auto zeros = '.' + std::string(30, '0');
    auto half = ".5" + std::string(29, '5');
    auto extreme = std::string(10, '9') + '.' + std::string(30, '9');
    auto rounded_extreme = '1' + std::string(10, '0');

    return TestData<DecimalField256, DecimalField64>{
        40,
        30,
        11,
        0,
        parseDecimalArray<DecimalField256>(
            40,
            30,
            {"0" + zeros, "0" + half, "-0" + half, extreme, "-" + extreme, std::nullopt}),
        parseDecimalArray<DecimalField64>(
            11,
            0,
            {"0", "1", "-1", rounded_extreme, "-" + rounded_extreme, std::nullopt})};
}

} // namespace

template <typename T>
class TestFunctionsRound : public DB::tests::FunctionTest
{
};

TYPED_TEST_CASE(TestFunctionsRound, TestCases);

TYPED_TEST(TestFunctionsRound, TiDBRound)
try
{
    // prepare test data.

    using InputType = typename TypeParam::InputType;
    using OutputType = typename TypeParam::OutputType;

    auto data = getTestData<InputType, OutputType, TypeParam::id>();
    size_t size = data.input.size();

    // determine data type.

    DataTypePtr data_type;
    if constexpr (isDecimalField<InputType>())
        data_type = std::make_shared<DataTypeDecimal<typename ToDecimalType<InputType>::type>>(
            data.input_prec,
            data.input_scale);
    else
        data_type = std::make_shared<DataTypeNumber<InputType>>();
    data_type = makeNullable(data_type);

    // construct argument columns: `input` and `frac`.

    auto input_column = data_type->createColumn();

    for (const auto & value : data.input)
    {
        if (value.has_value())
            input_column->insert(Field(value.value()));
        else
            input_column->insert(Null());
    }

    auto input = ColumnWithTypeAndName{std::move(input_column), data_type, "input"};

    auto frac_type = std::make_shared<DataTypeInt64>();
    auto frac_column = frac_type->createColumnConst(size, Field(static_cast<Int64>(0)));
    auto frac = ColumnWithTypeAndName{std::move(frac_column), frac_type, "frac"};

    // build function.

    const auto context = TiFlashTestEnv::getContext();
    auto & factory = FunctionFactory::instance();

    auto builder = factory.tryGet("tidbRoundWithFrac", *context);
    ASSERT_NE(builder, nullptr);

    auto function = builder->build({input, frac});
    ASSERT_NE(function, nullptr);

    // prepare block.

    Block block;
    block.insert(input);
    block.insert(frac);
    block.insert({nullptr, function->getReturnType(), "result"});

    // execute function.

    function->execute(
        block,
        {block.getPositionByName("input"), block.getPositionByName("frac")},
        block.getPositionByName("result"));

    // check result.

    auto result = block.getByName("result");
    ASSERT_NE(result.column, nullptr);

    if constexpr (isDecimalField<OutputType>())
    {
        auto result_type = result.type;
        if (auto actual = checkAndGetDataType<DataTypeNullable>(result_type.get()))
            result_type = actual->getNestedType();

        ASSERT_EQ(getDecimalPrecision(*result_type, 0), data.output_prec);
        ASSERT_EQ(getDecimalScale(*result_type, 0), data.output_scale);
    }

    ASSERT_EQ(block.rows(), size);

    Field result_field;
    for (size_t i = 0; i < size; ++i)
    {
        result.column->get(i, result_field);

        if (data.output[i].has_value())
        {
            ASSERT_FALSE(result_field.isNull()) << "index = " << i;

            auto got = result_field.safeGet<OutputType>();
            auto expected = data.output[i].value();

            if constexpr (isDecimalField<OutputType>())
            {
                ASSERT_EQ(got.getScale(), expected.getScale()) << "index = " << i;
                ASSERT_EQ(got.getValue(), expected.getValue()) << "index = " << i;
            }
            else
                ASSERT_EQ(got, expected) << "index = " << i;
        }
        else
            ASSERT_TRUE(result_field.isNull()) << "index = " << i;
    }
}
CATCH

} // namespace tests

} // namespace DB
