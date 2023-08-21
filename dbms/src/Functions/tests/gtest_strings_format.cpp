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

#include <DataTypes/DataTypeNullable.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

namespace DB::tests
{
class StringFormat : public DB::tests::FunctionTest
{
public:
    template <typename Decimal>
    void formatDecimalTestCase(int precision)
    {
        static const std::string func_name = "format";
        using Native = typename Decimal::NativeType;
        using FieldType = DecimalField<Decimal>;
        using NullableDecimal = Nullable<Decimal>;
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>(
                {"0.0000",
                 "-0.0120",
                 "0.0120",
                 "12,332.1000",
                 "12,332",
                 "12,332",
                 "12,332.300000000000000000000000000000",
                 "-12,332.30000",
                 "-1,000.0",
                 "-333.33",
                 {},
                 "99,999.9999000000",
                 "100,000.000",
                 "100,000"}),
            executeFunction(
                func_name,
                createColumn<NullableDecimal>(
                    std::make_tuple(precision, 4),
                    {FieldType(static_cast<Native>(0), 4),
                     FieldType(static_cast<Native>(-120), 4),
                     FieldType(static_cast<Native>(120), 4),
                     FieldType(static_cast<Native>(123321000), 4),
                     FieldType(static_cast<Native>(123322000), 4),
                     FieldType(static_cast<Native>(123323000), 4),
                     FieldType(static_cast<Native>(123323000), 4),
                     FieldType(static_cast<Native>(-123323000), 4),
                     FieldType(static_cast<Native>(-9999999), 4),
                     FieldType(static_cast<Native>(-3333330), 4),
                     FieldType(static_cast<Native>(0), 0),
                     FieldType(static_cast<Native>(999999999), 4),
                     FieldType(static_cast<Native>(999999999), 4),
                     FieldType(static_cast<Native>(999999999), 4)}),
                createColumn<Nullable<Int64>>({4, 4, 4, 4, 0, -1, 31, 5, 1, 2, {}, 10, 3, -5})));
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"12,332.100", "-12,332.300", "-1,000.000", "-333.333"}),
            executeFunction(
                func_name,
                createColumn<NullableDecimal>(
                    std::make_tuple(precision, 4),
                    {FieldType(static_cast<Native>(123321000), 4),
                     FieldType(static_cast<Native>(-123323000), 4),
                     FieldType(static_cast<Native>(-9999999), 4),
                     FieldType(static_cast<Native>(-3333330), 4)}),
                createConstColumn<Nullable<Int16>>(4, 3)));
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>(
                {"-999.9999",
                 "-1,000",
                 "-1,000",
                 "-999.999900000000000000000000000000",
                 "-999.99990",
                 "-1,000.0",
                 "-1,000.00"}),
            executeFunction(
                func_name,
                createConstColumn<NullableDecimal>(
                    std::make_tuple(precision, 4),
                    7,
                    FieldType(static_cast<Native>(-9999999), 4)),
                createColumn<Nullable<Int32>>({4, 0, -1, 31, 5, 1, 2})));
        ASSERT_COLUMN_EQ(
            createConstColumn<String>(1, "-1,000.000"),
            executeFunction(
                func_name,
                createConstColumn<NullableDecimal>(
                    std::make_tuple(precision, 4),
                    1,
                    FieldType(static_cast<Native>(-9999999), 4)),
                createConstColumn<Nullable<Int8>>(1, 3)));
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>(
                {"12,332.1000",
                 "12,332",
                 "12,332.300000000000000000000000000000",
                 "-12,332.30000",
                 "-1,000.0",
                 "-333.33",
                 {}}),
            executeFunction(
                func_name,
                createColumn<NullableDecimal>(
                    std::make_tuple(precision, 4),
                    {FieldType(static_cast<Native>(123321000), 4),
                     FieldType(static_cast<Native>(123323000), 4),
                     FieldType(static_cast<Native>(123323000), 4),
                     FieldType(static_cast<Native>(-123323000), 4),
                     FieldType(static_cast<Native>(-9999999), 4),
                     FieldType(static_cast<Native>(-3333330), 4),
                     FieldType(static_cast<Native>(0), 0)}),
                createColumn<Nullable<UInt64>>({4, 0, 31, 5, 1, 2, {}})));
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"12,332.100", "-12,332.300", "-1,000.000", "-333.333"}),
            executeFunction(
                func_name,
                createColumn<NullableDecimal>(
                    std::make_tuple(precision, 4),
                    {FieldType(static_cast<Native>(123321000), 4),
                     FieldType(static_cast<Native>(-123323000), 4),
                     FieldType(static_cast<Native>(-9999999), 4),
                     FieldType(static_cast<Native>(-3333330), 4)}),
                createConstColumn<Nullable<UInt16>>(4, 3)));
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>(
                {"-999.9999", "-1,000", "-999.999900000000000000000000000000", "-999.99990", "-1,000.0", "-1,000.00"}),
            executeFunction(
                func_name,
                createConstColumn<NullableDecimal>(
                    std::make_tuple(precision, 4),
                    6,
                    FieldType(static_cast<Native>(-9999999), 4)),
                createColumn<Nullable<UInt32>>({4, 0, 31, 5, 1, 2})));
        ASSERT_COLUMN_EQ(
            createConstColumn<String>(1, "-1,000.000"),
            executeFunction(
                func_name,
                createConstColumn<NullableDecimal>(
                    std::make_tuple(precision, 4),
                    1,
                    FieldType(static_cast<Native>(-9999999), 4)),
                createConstColumn<Nullable<UInt8>>(1, 3)));
    }

    template <typename Integer>
    void formatIntegerTestCase()
    {
        static const std::string func_name = "format";
        using NullableInteger = Nullable<Integer>;
        if constexpr (std::is_same_v<Integer, Int8> || std::is_same_v<Integer, UInt8>)
        {
            ASSERT_COLUMN_EQ(
                createColumn<Nullable<String>>({"0.0000", "10.0000", {}}),
                executeFunction(
                    func_name,
                    createColumn<NullableInteger>({0, 10, 10}),
                    createColumn<Nullable<Int64>>({4, 4, {}})));
            if constexpr (std::is_signed_v<Integer>)
            {
                ASSERT_COLUMN_EQ(
                    createColumn<Nullable<String>>({"0.0000", "-10.0000", {}}),
                    executeFunction(
                        func_name,
                        createColumn<NullableInteger>({-0, -10, -10}),
                        createColumn<Nullable<Int64>>({4, 4, {}})));
            }
        }
        else
        {
            ASSERT_COLUMN_EQ(
                createColumn<Nullable<String>>({"0.0000", "31,234.0000", {}}),
                executeFunction(
                    func_name,
                    createColumn<NullableInteger>({0, 31234, 10}),
                    createColumn<Nullable<Int64>>({4, 4, {}})));
            if constexpr (std::is_signed_v<Integer>)
            {
                ASSERT_COLUMN_EQ(
                    createColumn<Nullable<String>>({"0.0000", "-31,234.0000", {}}),
                    executeFunction(
                        func_name,
                        createColumn<NullableInteger>({-0, -31234, -31234}),
                        createColumn<Nullable<Int64>>({4, 4, {}})));
            }
        }
    }
};

TEST_F(StringFormat, FormatWithLocaleAllUnitTest)
try
{
    const std::string func_name = "formatWithLocale";
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>(
            {"0.0000",
             "-0.0120",
             "0.0120",
             "12,332.1235",
             "12,332.1235",
             "12,332.1235",
             "12,332.1235",
             "12,332.1235",
             {},
             {},
             {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Float64>>(
                {0,
                 -.012,
                 .012,
                 12332.123456,
                 12332.123456,
                 12332.123456,
                 12332.123456,
                 12332.123456,
                 12332.123456,
                 {},
                 {}}),
            createColumn<Nullable<Int64>>({4, 4, 4, 4, 4, 4, 4, 4, {}, 4, {}}),
            createColumn<Nullable<String>>(
                {"en_US", "en_US", "en_US", "en_US", "en_us", "EN_US", "xxx", {}, "xx1", "xx2", "xx3"})));

    auto gen_warning_str = [](const std::string & value) -> std::string {
        return fmt::format("Unknown locale: \'{}\'", value);
    };
    std::vector<std::string> expected_warnings{
        gen_warning_str("xxx"),
        gen_warning_str("NULL"),
        gen_warning_str("xx1"),
        gen_warning_str("xx2"),
        gen_warning_str("xx3")};
    std::vector<tipb::Error> actual_warnings;
    getDAGContext().consumeWarnings(actual_warnings);
    ASSERT_TRUE(expected_warnings.size() == actual_warnings.size());
    for (size_t i = 0; i < expected_warnings.size(); ++i)
    {
        auto actual_warning = actual_warnings[i];
        ASSERT_TRUE(actual_warning.has_msg() && actual_warning.msg() == expected_warnings[i]);
    }
}
CATCH


TEST_F(StringFormat, StringFormatAllUnitTest)
try
{
    const std::string func_name = "format";

    /// float64, int
    /// vector, vector
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>(
            {"12,332.1235",
             "12,332.1000",
             "12,332",
             "12,332",
             "12,332.300000000000000000000000000000",
             "-12,332.30000",
             "-1,000.0",
             "-333.33",
             {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Float64>>(
                {12332.123456, 12332.1, 12332.2, 12332.3, 12332.3, -12332.3, -999.9999, -333.333, 0}),
            createColumn<Nullable<Int64>>({4, 4, 0, -1, 31, 5, 1, 2, {}})));
    /// vector, const
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"12,332.123", "12,332.100", "-12,332.300", "-1,000.000", "-333.333"}),
        executeFunction(
            func_name,
            createColumn<Nullable<Float64>>({12332.123456, 12332.1, -12332.3, -999.9999, -333.333}),
            createConstColumn<Nullable<Int16>>(5, 3)));
    /// const, vector
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>(
            {"-999.9999",
             "-1,000",
             "-1,000",
             "-999.999900000000000000000000000000",
             "-999.99990",
             "-1,000.0",
             "-1,000.00"}),
        executeFunction(
            func_name,
            createConstColumn<Nullable<Float64>>(7, -999.9999),
            createColumn<Nullable<Int32>>({4, 0, -1, 31, 5, 1, 2})));
    /// const, const
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(1, "-1,000.000"),
        executeFunction(
            func_name,
            createConstColumn<Nullable<Float64>>(1, -999.9999),
            createConstColumn<Nullable<Int8>>(1, 3)));

    /// float64, uint
    /// vector, vector
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>(
            {"12,332.1235",
             "12,332.1000",
             "12,332",
             "12,332.300000000000000000000000000000",
             "-12,332.30000",
             "-1,000.0",
             "-333.33"}),
        executeFunction(
            func_name,
            createColumn<Nullable<Float64>>({12332.123456, 12332.1, 12332.2, 12332.3, -12332.3, -999.9999, -333.333}),
            createColumn<Nullable<UInt64>>({4, 4, 0, 31, 5, 1, 2})));
    /// vector, const
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"12,332.123", "12,332.100", "-12,332.300", "-1,000.000", "-333.333"}),
        executeFunction(
            func_name,
            createColumn<Nullable<Float64>>({12332.123456, 12332.1, -12332.3, -999.9999, -333.333}),
            createConstColumn<Nullable<UInt16>>(8, 3)));
    /// const, vector
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>(
            {"-999.9999", "-1,000", "-999.999900000000000000000000000000", "-999.99990", "-1,000.0", "-1,000.00"}),
        executeFunction(
            func_name,
            createConstColumn<Nullable<Float64>>(6, -999.9999),
            createColumn<Nullable<UInt32>>({4, 0, 31, 5, 1, 2})));
    /// const, const
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(1, "-1,000.000"),
        executeFunction(
            func_name,
            createConstColumn<Nullable<Float64>>(1, -999.9999),
            createConstColumn<Nullable<UInt8>>(1, 3)));

    /// float32, int
    /// const, const
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(1, "12.123"),
        executeFunction(
            func_name,
            createConstColumn<Nullable<Float32>>(1, 12.1235),
            createConstColumn<Nullable<UInt8>>(1, 3)));

    /// decimal
    formatDecimalTestCase<Decimal32>(9);
    formatDecimalTestCase<Decimal64>(18);
    formatDecimalTestCase<Decimal128>(38);
    formatDecimalTestCase<Decimal256>(65);

    /// int
    formatIntegerTestCase<Int8>();
    formatIntegerTestCase<Int16>();
    formatIntegerTestCase<Int32>();
    formatIntegerTestCase<Int64>();
    formatIntegerTestCase<UInt8>();
    formatIntegerTestCase<UInt16>();
    formatIntegerTestCase<UInt32>();
    formatIntegerTestCase<UInt64>();
}
CATCH

} // namespace DB::tests
