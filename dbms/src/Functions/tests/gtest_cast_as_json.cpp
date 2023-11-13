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

#include <Columns/ColumnNullable.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/Decode/JsonBinary.h>

#include <string>
#include <vector>

namespace DB::tests
{
/**
  * Because all the functions that cast xx as json have 
  *    ```
  *    bool useDefaultImplementationForNulls() const override { return true; }
  *    bool useDefaultImplementationForConstants() const override { return true; }
  *    ```
  * there is no need to test const, null_value, and only value.
  *
  * CastStringAsJson and CastDurationAsJson rely on tipb::FieldType,
  * so they cannot be tested in unit tests.
  * They will be tested in integration tests.
  */
class TestCastAsJson : public DB::tests::FunctionTest
{
public:
    ColumnWithTypeAndName executeFunctionWithCast(const String & func_name, const ColumnsWithTypeAndName & columns)
    {
        auto json_column = executeFunction(func_name, columns);
        static auto json_return_type = std::make_shared<DataTypeString>();
        assert(json_return_type->equals(*json_column.type));
        // The `json_binary` should be cast as a string to improve readability.
        return executeFunction("cast_json_as_string", {json_column});
    }

    template <typename Input>
    void executeAndAssert(const String & func_name, const Input & input, const String & expect)
    {
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({expect}),
            executeFunctionWithCast(func_name, {createColumn<Input>({input})}));
    }

    template <typename Input>
    typename std::enable_if<IsDecimal<Input>, void>::type executeAndAssert(
        const String & func_name,
        const DecimalField<Input> & input,
        const String & expect)
    {
        auto meta = std::make_tuple(19, input.getScale());
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({expect}),
            executeFunctionWithCast(func_name, {createColumn<Input>(meta, {input})}));
    }
};

TEST_F(TestCastAsJson, CastJsonAsJson)
try
{
    /// prepare
    // []
    // clang-format off
    const UInt8 empty_array[] = {
        JsonBinary::TYPE_CODE_ARRAY, // array_type
        0x0, 0x0, 0x0, 0x0, // element_count
        0x8, 0x0, 0x0, 0x0}; // total_size
    // clang-format on
    ColumnWithTypeAndName json_column;
    {
        auto empty_array_json = ColumnString::create();
        empty_array_json->insertData(reinterpret_cast<const char *>(empty_array), sizeof(empty_array) / sizeof(UInt8));
        empty_array_json->insertData(reinterpret_cast<const char *>(empty_array), sizeof(empty_array) / sizeof(UInt8));
        json_column = ColumnWithTypeAndName(std::move(empty_array_json), std::make_shared<DataTypeString>());
    }

    auto gen_column_expect = [](const String & value) {
        // double for rows_count 2.
        return createColumn<Nullable<String>>({value, value});
    };

    auto res = executeFunctionWithCast("cast_json_as_json", {json_column});
    auto expect = gen_column_expect("[]");
    ASSERT_COLUMN_EQ(expect, res);
}
CATCH

TEST_F(TestCastAsJson, CastIntAsJson)
try
{
    const String func_name = "cast_int_as_json";

    /// UInt8
    executeAndAssert<UInt8>(func_name, 1, "true");
    executeAndAssert<UInt8>(func_name, std::numeric_limits<UInt8>::max(), "true");
    executeAndAssert<UInt8>(func_name, 0, "false");

    /// UInt64
    executeAndAssert<UInt64>(func_name, 0, "0");
    executeAndAssert<UInt64>(func_name, 999, "999");
    executeAndAssert<UInt64>(
        func_name,
        std::numeric_limits<UInt64>::max(),
        fmt::format("{}", std::numeric_limits<UInt64>::max()));

    /// Int64
    executeAndAssert<Int64>(func_name, 0, "0");
    executeAndAssert<Int64>(func_name, 999, "999");
    executeAndAssert<Int64>(func_name, -999, "-999");
    executeAndAssert<Int64>(
        func_name,
        std::numeric_limits<Int64>::max(),
        fmt::format("{}", std::numeric_limits<Int64>::max()));
    executeAndAssert<Int64>(
        func_name,
        std::numeric_limits<Int64>::min(),
        fmt::format("{}", std::numeric_limits<Int64>::min()));
}
CATCH

TEST_F(TestCastAsJson, CastRealAsJson)
try
{
    const String func_name = "cast_real_as_json";

    /// Float32
    executeAndAssert<Float32>(func_name, 0, "0");
    executeAndAssert<Float32>(func_name, 999.999f, "999.9990234375");
    executeAndAssert<Float32>(func_name, -999.999f, "-999.9990234375");
    executeAndAssert<Float32>(
        func_name,
        std::numeric_limits<Float32>::max(),
        fmt::format("{}", static_cast<Float64>(std::numeric_limits<Float32>::max())));
    executeAndAssert<Float32>(
        func_name,
        std::numeric_limits<Float32>::min(),
        fmt::format("{}", static_cast<Float64>(std::numeric_limits<Float32>::min())));

    /// Float64
    executeAndAssert<Float64>(func_name, 0, "0");
    executeAndAssert<Float64>(func_name, 999.999, "999.999");
    executeAndAssert<Float64>(func_name, -999.999, "-999.999");
    executeAndAssert<Float64>(
        func_name,
        std::numeric_limits<Float64>::max(),
        fmt::format("{}", std::numeric_limits<Float64>::max()));
    executeAndAssert<Float64>(
        func_name,
        std::numeric_limits<Float64>::min(),
        fmt::format("{}", std::numeric_limits<Float64>::min()));
}
CATCH

TEST_F(TestCastAsJson, CastDecimalAsJson)
try
{
    const String func_name = "cast_decimal_as_json";

    using DecimalField32 = DecimalField<Decimal32>;
    using DecimalField64 = DecimalField<Decimal64>;
    using DecimalField128 = DecimalField<Decimal128>;
    using DecimalField256 = DecimalField<Decimal256>;

    /// Decimal32
    executeAndAssert<Decimal32>(func_name, DecimalField32(1011, 1), "101.1");
    executeAndAssert<Decimal32>(func_name, DecimalField32(-1011, 1), "-101.1");
    executeAndAssert<Decimal32>(func_name, DecimalField32(9999, 1), "999.9");
    executeAndAssert<Decimal32>(func_name, DecimalField32(-9999, 1), "-999.9");

    /// Decimal64
    executeAndAssert<Decimal64>(func_name, DecimalField64(1011, 1), "101.1");
    executeAndAssert<Decimal64>(func_name, DecimalField64(-1011, 1), "-101.1");
    executeAndAssert<Decimal64>(func_name, DecimalField64(9999, 1), "999.9");
    executeAndAssert<Decimal64>(func_name, DecimalField64(-9999, 1), "-999.9");

    /// Decimal128
    executeAndAssert<Decimal128>(func_name, DecimalField128(1011, 1), "101.1");
    executeAndAssert<Decimal128>(func_name, DecimalField128(-1011, 1), "-101.1");
    executeAndAssert<Decimal128>(func_name, DecimalField128(9999, 1), "999.9");
    executeAndAssert<Decimal128>(func_name, DecimalField128(-9999, 1), "-999.9");

    /// Decimal256
    executeAndAssert<Decimal256>(func_name, DecimalField256(static_cast<Int256>(1011), 1), "101.1");
    executeAndAssert<Decimal256>(func_name, DecimalField256(static_cast<Int256>(-1011), 1), "-101.1");
    executeAndAssert<Decimal256>(func_name, DecimalField256(static_cast<Int256>(9999), 1), "999.9");
    executeAndAssert<Decimal256>(func_name, DecimalField256(static_cast<Int256>(-9999), 1), "-999.9");
}
CATCH

TEST_F(TestCastAsJson, CastDurationAsJson)
try
{
    ColumnWithTypeAndName input(
        // 22hour, 22min, 22s, 222ms
        createColumn<DataTypeMyDuration::FieldType>({(22 * 3600000 + 22 * 60000 + 22 * 1000 + 222) * 1000000L,
                                                     -1 * (22 * 3600000 + 22 * 60000 + 22 * 1000 + 222) * 1000000L})
            .column,
        std::make_shared<DataTypeMyDuration>(6),
        "");

    auto res = executeFunctionWithCast("cast_duration_as_json", {input});
    auto expect = createColumn<Nullable<String>>({"\"22:22:22.222000\"", "\"-22:22:22.222000\""});
    ASSERT_COLUMN_EQ(expect, res);
}
CATCH

} // namespace DB::tests
