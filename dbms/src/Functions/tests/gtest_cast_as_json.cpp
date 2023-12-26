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
#include <Common/Exception.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/Decode/JsonBinary.h>
#include <gtest/gtest.h>

#include <string>
#include <type_traits>
#include <vector>

namespace DB::tests
{
/**
  * Because most functions(except cast string as json) have 
  *    ```
  *    bool useDefaultImplementationForNulls() const override { return true; }
  *    bool useDefaultImplementationForConstants() const override { return true; }
  *    ```
  * there is no need to test const, null_value, and only null.
  *
  * CastIntAsJson, CastStringAsJson and CastDurationAsJson can only test the case where input_tidb_tp/output_tidb_tp is nullptr
  */
class TestCastAsJson : public DB::tests::FunctionTest
{
public:
    template <bool is_raw = false>
    ColumnWithTypeAndName executeFunctionWithCast(const String & func_name, const ColumnsWithTypeAndName & columns)
    {
        ColumnWithTypeAndName json_column;
        if constexpr (is_raw)
        {
            json_column = executeFunction(func_name, columns, nullptr, true);
        }
        else
        {
            json_column = executeFunction(func_name, columns);
        }
        // The `json_binary` should be cast as a string to improve readability.
        tipb::FieldType field_type;
        field_type.set_flen(-1);
        field_type.set_collate(TiDB::ITiDBCollator::BINARY);
        field_type.set_tp(TiDB::TypeString);
        return executeCastJsonAsStringFunction(json_column, field_type);
    }

    template <typename Input, bool is_raw = false>
    void executeAndAssert(const String & func_name, const Input & input, const String & expect)
    {
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({expect}),
            executeFunctionWithCast<is_raw>(func_name, {createColumn<Input>({input})}));
    }

    template <typename Input, bool is_raw = false>
    typename std::enable_if<IsDecimal<Input>, void>::type executeAndAssert(
        const String & func_name,
        const DecimalField<Input> & input,
        const String & expect)
    {
        auto meta = std::make_tuple(19, input.getScale());
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({expect}),
            executeFunctionWithCast<is_raw>(func_name, {createColumn<Input>(meta, {input})}));
    }

    template <typename IntType>
    typename std::enable_if<std::is_integral_v<IntType>, void>::type testForInt()
    {
        // Only raw function test is tested, so input_tidb_tp is always nullptr.
        String func_name = "cast_int_as_json";
        executeAndAssert<IntType, true>(func_name, 0, "0");
        executeAndAssert<IntType, true>(func_name, 99, "99");
        if constexpr (std::is_signed_v<IntType>)
        {
            executeAndAssert<IntType, true>(func_name, -99, "-99");
        }
        executeAndAssert<IntType, true>(
            func_name,
            std::numeric_limits<IntType>::max(),
            fmt::format("{}", std::numeric_limits<IntType>::max()));
        executeAndAssert<IntType, true>(
            func_name,
            std::numeric_limits<IntType>::min(),
            fmt::format("{}", std::numeric_limits<IntType>::min()));
        executeAndAssert<IntType, true>(
            func_name,
            std::numeric_limits<IntType>::lowest(),
            fmt::format("{}", std::numeric_limits<IntType>::lowest()));
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
    testForInt<UInt8>();
    testForInt<UInt16>();
    testForInt<UInt32>();
    testForInt<UInt64>();

    testForInt<Int8>();
    testForInt<Int16>();
    testForInt<Int32>();
    testForInt<Int64>();
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
    executeAndAssert<Float32>(
        func_name,
        std::numeric_limits<Float32>::lowest(),
        fmt::format("{}", static_cast<Float64>(std::numeric_limits<Float32>::lowest())));

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
    executeAndAssert<Float64>(
        func_name,
        std::numeric_limits<Float64>::lowest(),
        fmt::format("{}", std::numeric_limits<Float64>::lowest()));
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
    executeAndAssert(func_name, DecimalField32(1011, 1), "101.1");
    executeAndAssert(func_name, DecimalField32(-1011, 1), "-101.1");
    executeAndAssert(func_name, DecimalField32(9999, 1), "999.9");
    executeAndAssert(func_name, DecimalField32(-9999, 1), "-999.9");

    /// Decimal64
    executeAndAssert(func_name, DecimalField64(1011, 1), "101.1");
    executeAndAssert(func_name, DecimalField64(-1011, 1), "-101.1");
    executeAndAssert(func_name, DecimalField64(9999, 1), "999.9");
    executeAndAssert(func_name, DecimalField64(-9999, 1), "-999.9");

    /// Decimal128
    executeAndAssert(func_name, DecimalField128(1011, 1), "101.1");
    executeAndAssert(func_name, DecimalField128(-1011, 1), "-101.1");
    executeAndAssert(func_name, DecimalField128(9999, 1), "999.9");
    executeAndAssert(func_name, DecimalField128(-9999, 1), "-999.9");

    /// Decimal256
    executeAndAssert(func_name, DecimalField256(static_cast<Int256>(1011), 1), "101.1");
    executeAndAssert(func_name, DecimalField256(static_cast<Int256>(-1011), 1), "-101.1");
    executeAndAssert(func_name, DecimalField256(static_cast<Int256>(9999), 1), "999.9");
    executeAndAssert(func_name, DecimalField256(static_cast<Int256>(-9999), 1), "-999.9");
}
CATCH

TEST_F(TestCastAsJson, CastStringAsJson)
try
{
    // Only raw function test is tested, so output_tidb_tp is always nullptr and only the case of parsing json is tested here.
    // Because of `bool useDefaultImplementationForNulls() const override { return true; }`, null column is need to be tested here.

    const String func_name = "cast_string_as_json";

    /// case1 only null
    {
        ColumnWithTypeAndName only_null_const = createOnlyNullColumnConst(1);
        ColumnsWithTypeAndName input{only_null_const};
        auto res = executeFunction(func_name, input, nullptr, true);
        ASSERT_COLUMN_EQ(only_null_const, res);
    }

    /// case2 nullable column
    {
        ColumnWithTypeAndName nullable_column = createColumn<Nullable<String>>({{}, "[]"});
        auto res = executeFunctionWithCast<true>(func_name, {nullable_column});
        ASSERT_COLUMN_EQ(nullable_column, res);
    }
    // invalid json text.
    {
        ColumnWithTypeAndName nullable_column = createColumn<Nullable<String>>({""});
        ASSERT_THROW(executeFunctionWithCast<true>(func_name, {nullable_column}), Exception);
    }
    {
        ColumnWithTypeAndName nullable_column = createColumn<Nullable<String>>({"dsadhgashg"});
        ASSERT_THROW(executeFunctionWithCast<true>(func_name, {nullable_column}), Exception);
    }

    /// case3 not null
    // invalid json text
    {
        // empty document
        ColumnWithTypeAndName nullable_column = createColumn<Nullable<String>>({""});
        ASSERT_THROW(executeFunctionWithCast<true>(func_name, {nullable_column}), Exception);
    }
    {
        // invaild json
        ColumnWithTypeAndName nullable_column = createColumn<Nullable<String>>({"a"});
        ASSERT_THROW(executeFunctionWithCast<true>(func_name, {nullable_column}), Exception);
    }
    {
        // invaild json
        ColumnWithTypeAndName nullable_column = createColumn<Nullable<String>>({"{fds, 1}"});
        ASSERT_THROW(executeFunctionWithCast<true>(func_name, {nullable_column}), Exception);
    }
    {
        // too deep
        ColumnWithTypeAndName nullable_column = createColumn<Nullable<String>>(
            {"[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[["
             "[[[[[]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]"
             "]]]]]]]]]]"});
        ASSERT_THROW(executeFunctionWithCast<true>(func_name, {nullable_column}), Exception);
    }
    // valid json text
    // a. literal
    executeAndAssert<String, true>(func_name, "0", "0");
    executeAndAssert<String, true>(func_name, "1", "1");
    executeAndAssert<String, true>(func_name, "-1", "-1");
    executeAndAssert<String, true>(func_name, "1.11", "1.11");
    executeAndAssert<String, true>(func_name, "-1.11", "-1.11");
    executeAndAssert<String, true>(func_name, "\"a\"", "\"a\"");
    executeAndAssert<String, true>(func_name, "true", "true");
    executeAndAssert<String, true>(func_name, "false", "false");
    executeAndAssert<String, true>(func_name, "null", "null");
    // b. json array
    executeAndAssert<String, true>(func_name, "[]", "[]");
    executeAndAssert<String, true>(func_name, "[1, 1000, 2.22, \"a\", null]", "[1, 1000, 2.22, \"a\", null]");
    executeAndAssert<String, true>(
        func_name,
        R"([1, 1000, 2.22, "a", null, {"a":1.11}])",
        R"([1, 1000, 2.22, "a", null, {"a": 1.11}])");
    executeAndAssert<String, true>(
        func_name,
        "[[[[[[[[[[[[[[[[[[[[[]]]]]]]]]]]]]]]]]]]]]",
        "[[[[[[[[[[[[[[[[[[[[[]]]]]]]]]]]]]]]]]]]]]");
    // c. json object
    executeAndAssert<String, true>(func_name, "{}", "{}");
    executeAndAssert<String, true>(func_name, "{\"a\":1}", "{\"a\": 1}");
    executeAndAssert<String, true>(
        func_name,
        R"({"a":null,"b":1,"c":1.11,"d":[],"e":{}})",
        R"({"a": null, "b": 1, "c": 1.11, "d": [], "e": {}})");
    executeAndAssert<String, true>(
        func_name,
        R"({"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{}}}}}}}}}}})",
        R"({"a": {"a": {"a": {"a": {"a": {"a": {"a": {"a": {"a": {"a": {}}}}}}}}}}})");
}
CATCH

TEST_F(TestCastAsJson, CastTimeAsJson)
try
{
    static auto const datetime_type_ptr = std::make_shared<DataTypeMyDateTime>(6);
    static auto const date_type_ptr = std::make_shared<DataTypeMyDate>();

    // DataTypeMyDateTime
    // Only raw function test is tested, so input_tidb_tp is always nullptr and only the case of TiDB::TypeTimestamp is tested here.
    {
        auto data_col_ptr
            = createColumn<DataTypeMyDateTime::FieldType>(
                  {MyDateTime(2023, 1, 2, 3, 4, 5, 6).toPackedUInt(), MyDateTime(0, 0, 0, 0, 0, 0, 0).toPackedUInt()})
                  .column;
        ColumnWithTypeAndName input(data_col_ptr, datetime_type_ptr, "");
        auto res = executeFunctionWithCast<true>("cast_time_as_json", {input});
        auto expect
            = createColumn<Nullable<String>>({"\"2023-01-02 03:04:05.000006\"", "\"0000-00-00 00:00:00.000000\""});
        ASSERT_COLUMN_EQ(expect, res);
    }

    // DataTypeMyDate
    {
        auto data_col_ptr = createColumn<DataTypeMyDate::FieldType>(
                                {MyDate(2023, 12, 31).toPackedUInt(), MyDate(0, 0, 0).toPackedUInt()})
                                .column;
        ColumnWithTypeAndName input(data_col_ptr, date_type_ptr, "");
        auto res = executeFunctionWithCast("cast_time_as_json", {input});
        auto expect = createColumn<Nullable<String>>({"\"2023-12-31\"", "\"0000-00-00\""});
        ASSERT_COLUMN_EQ(expect, res);
    }
}
CATCH

TEST_F(TestCastAsJson, CastDurationAsJson)
try
{
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
    {
        ColumnWithTypeAndName input(
            // 22hour, 22min, 22s, 222ms
            createColumn<DataTypeMyDuration::FieldType>({(22 * 3600000 + 22 * 60000 + 22 * 1000 + 222) * 1000000L,
                                                         -1 * (22 * 3600000 + 22 * 60000 + 22 * 1000 + 222) * 1000000L})
                .column,
            std::make_shared<DataTypeMyDuration>(1),
            "");

        auto res = executeFunctionWithCast("cast_duration_as_json", {input});
        auto expect = createColumn<Nullable<String>>({"\"22:22:22.222000\"", "\"-22:22:22.222000\""});
        ASSERT_COLUMN_EQ(expect, res);
    }
}
CATCH

} // namespace DB::tests
