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

auto parseIntArray(const std::vector<std::optional<Int64>> & literals)
{
    std::vector<std::optional<Int64>> result;

    result.reserve(literals.size());
    for (const auto & literal : literals)
    {
        if (literal.has_value())
            result.push_back(literal.value());
        else
            result.push_back(std::nullopt);
    }

    return result;
}

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

    std::vector<std::optional<Input>> input;
    std::vector<std::optional<Output>> floor_output;
    std::vector<std::optional<Output>> ceil_output;
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
    TestCase<DecimalField32, Int64>,
    TestCase<DecimalField32, DecimalField32>,
    TestCase<DecimalField64, DecimalField64>,
    TestCase<DecimalField128, DecimalField128>,
    TestCase<DecimalField256, DecimalField256>>;

template <typename InputType, typename OutputType, size_t id = 0>
auto getTestData();

template <>
auto getTestData<DecimalField32, Int64>()
{
    return TestData<DecimalField32, Int64>{
        9,
        2,
        parseDecimalArray<DecimalField32>(9, 2, {"2.5", "999.5", "2", "999", std::nullopt}),
        parseIntArray({2, 999, 2, 999, std::nullopt}),
        parseIntArray({3, 1000, 2, 999, std::nullopt})};
}

template <>
auto getTestData<DecimalField32, DecimalField32>()
{
    return TestData<DecimalField32, DecimalField32>{
        9,
        2,
        parseDecimalArray<DecimalField32>(9, 2, {"2.5", "999.5", "2", "999", std::nullopt}),
        parseDecimalArray<DecimalField32>(9, 0, {"2", "999", "2", "999", std::nullopt}),
        parseDecimalArray<DecimalField32>(9, 0, {"3", "1000", "2", "999", std::nullopt})};
}

template <>
auto getTestData<DecimalField64, DecimalField64>()
{
    return TestData<DecimalField64, DecimalField64>{
        18,
        10,
        parseDecimalArray<DecimalField64>(18, 10, {"2.5", "999.5", "2", "999", std::nullopt}),
        parseDecimalArray<DecimalField64>(18, 0, {"2", "999", "2", "999", std::nullopt}),
        parseDecimalArray<DecimalField64>(18, 0, {"3", "1000", "2", "999", std::nullopt})};
}

template <>
auto getTestData<DecimalField128, DecimalField128>()
{
    return TestData<DecimalField128, DecimalField128>{
        38,
        30,
        parseDecimalArray<DecimalField128>(38, 30, {"2.5", "999.5", "2", "999", std::nullopt}),
        parseDecimalArray<DecimalField128>(38, 0, {"2", "999", "2", "999", std::nullopt}),
        parseDecimalArray<DecimalField128>(38, 0, {"3", "1000", "2", "999", std::nullopt})};
}

template <>
auto getTestData<DecimalField256, DecimalField256>()
{
    return TestData<DecimalField256, DecimalField256>{
        65,
        30,
        parseDecimalArray<DecimalField256>(65, 30, {"2.5", "999.5", "2", "999", std::nullopt}),
        parseDecimalArray<DecimalField256>(65, 0, {"2", "999", "2", "999", std::nullopt}),
        parseDecimalArray<DecimalField256>(65, 0, {"3", "1000", "2", "999", std::nullopt})};
}

} // namespace

template <typename T>
class TestFunctionsFloorAndCeil : public DB::tests::FunctionTest
{
};

TYPED_TEST_CASE(TestFunctionsFloorAndCeil, TestCases);

TYPED_TEST(TestFunctionsFloorAndCeil, Floor)
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

    auto input_column = data_type->createColumn();

    for (const auto & value : data.input)
    {
        if (value.has_value())
            input_column->insert(Field(value.value()));
        else
            input_column->insert(Null());
    }

    auto input = ColumnWithTypeAndName{std::move(input_column), data_type, "input"};

    // build function.

    const auto context = TiFlashTestEnv::getContext();
    auto & factory = FunctionFactory::instance();

    FunctionBuilderPtr builder;
    if constexpr (isDecimalField<OutputType>())
        builder = factory.tryGet("floor", *context);
    else
        builder = factory.tryGet("floorDecimalToInt", *context);
    ASSERT_NE(builder, nullptr);

    auto function = builder->build({input});
    ASSERT_NE(function, nullptr);

    // prepare block.

    Block block;
    block.insert(input);
    block.insert({nullptr, function->getReturnType(), "result"});

    // execute function.

    function->execute(block, {block.getPositionByName("input")}, block.getPositionByName("result"));

    // check result.

    auto result = block.getByName("result");
    ASSERT_NE(result.column, nullptr);

    if constexpr (isDecimalField<OutputType>())
    {
        auto result_type = result.type;
        if (auto actual = checkAndGetDataType<DataTypeNullable>(result_type.get()))
            result_type = actual->getNestedType();
    }

    ASSERT_EQ(block.rows(), size);

    Field result_field;
    for (size_t i = 0; i < size; ++i)
    {
        result.column->get(i, result_field);

        if (data.floor_output[i].has_value())
        {
            ASSERT_FALSE(result_field.isNull()) << "index = " << i;

            auto got = result_field.safeGet<OutputType>();
            auto expected = data.floor_output[i].value();

            if constexpr (isDecimalField<OutputType>())
            {
                ASSERT_EQ(got.getScale(), expected.getScale()) << "index = " << i;
                ASSERT_TRUE(got.getValue() == expected.getValue()) << "index = " << i;
            }
            else
                ASSERT_EQ(got, expected) << "index = " << i;
        }
        else
            ASSERT_TRUE(result_field.isNull()) << "index = " << i;
    }
}
CATCH

TYPED_TEST(TestFunctionsFloorAndCeil, Ceil)
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

    auto input_column = data_type->createColumn();

    for (const auto & value : data.input)
    {
        if (value.has_value())
            input_column->insert(Field(value.value()));
        else
            input_column->insert(Null());
    }

    auto input = ColumnWithTypeAndName{std::move(input_column), data_type, "input"};

    // build function.

    const auto context = TiFlashTestEnv::getContext();
    auto & factory = FunctionFactory::instance();

    FunctionBuilderPtr builder;
    if constexpr (isDecimalField<OutputType>())
        builder = factory.tryGet("ceil", *context);
    else
        builder = factory.tryGet("ceilDecimalToInt", *context);
    ASSERT_NE(builder, nullptr);

    auto function = builder->build({input});
    ASSERT_NE(function, nullptr);

    // prepare block.

    Block block;
    block.insert(input);
    block.insert({nullptr, function->getReturnType(), "result"});

    // execute function.

    function->execute(block, {block.getPositionByName("input")}, block.getPositionByName("result"));

    // check result.

    auto result = block.getByName("result");
    ASSERT_NE(result.column, nullptr);

    if constexpr (isDecimalField<OutputType>())
    {
        auto result_type = result.type;
        if (auto actual = checkAndGetDataType<DataTypeNullable>(result_type.get()))
            result_type = actual->getNestedType();
    }

    ASSERT_EQ(block.rows(), size);

    Field result_field;
    for (size_t i = 0; i < size; ++i)
    {
        result.column->get(i, result_field);

        if (data.ceil_output[i].has_value())
        {
            ASSERT_FALSE(result_field.isNull()) << "index = " << i;

            auto got = result_field.safeGet<OutputType>();
            auto expected = data.ceil_output[i].value();

            if constexpr (isDecimalField<OutputType>())
            {
                ASSERT_EQ(got.getScale(), expected.getScale()) << "index = " << i;
                ASSERT_TRUE(got.getValue() == expected.getValue()) << "index = " << i;
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
