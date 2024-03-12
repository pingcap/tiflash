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

#include <Columns/ColumnsNumber.h>
#include <Common/Logger.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsTiDBConversion.h>
#include <TestUtils/FunctionTestUtils.h>
#include <common/logger_useful.h>
#include <common/types.h>
#include <gtest/gtest.h>

namespace DB::tests
{
namespace
{
const std::string func_name = "tidb_cast";
auto createCastTypeConstColumn(String str)
{
    return createConstColumn<String>(1, str);
}
class TestDivPrecisionIncre : public DB::tests::FunctionTest
{
public:
    void parseAndTestDecimalToFloat64(String input_decimal, double ref)
    {
        ASSERT_TRUE(!input_decimal.empty());
        size_t prec = input_decimal.length();
        size_t scale = 0;
        auto pos = input_decimal.find(".");
        if (pos != std::string::npos)
        {
            ASSERT_TRUE(input_decimal.length() >= pos + 1);
            scale = input_decimal.length() - pos - 1;
            --prec;
        }
        bool negative = input_decimal.starts_with("-");
        if (negative)
        {
            ASSERT_TRUE(prec >= 1);
            --prec;
        }
        ASSERT_TRUE(prec >= scale);
        ASSERT_COLUMN_EQ_V2(
            createColumn<Float64>({ref}),
            executeFunction(
                func_name,
                {createColumn<Decimal128>(std::make_tuple(prec, scale), {input_decimal}),
                 createCastTypeConstColumn("Float64")}));
        ASSERT_COLUMN_EQ_V2(
            createColumn<Float64>({ref}),
            executeFunction(
                func_name,
                {createColumn<Decimal256>(std::make_tuple(prec, scale), {input_decimal}),
                 createCastTypeConstColumn("Float64")}));
    }
};

using DecimalField32 = DecimalField<Decimal32>;
using DecimalField64 = DecimalField<Decimal64>;
using DecimalField128 = DecimalField<Decimal128>;
using DecimalField256 = DecimalField<Decimal256>;

TEST_F(TestDivPrecisionIncre, castDecimalAsRealBasic)
try
{
    /// null only cases
    ASSERT_COLUMN_EQ_V2(
        createColumn<Nullable<Float64>>({{}}),
        executeFunction(func_name, {createOnlyNullColumn(1), createCastTypeConstColumn("Nullable(Float64)")}));

    /// ColumnVector(const nullable)
    ASSERT_COLUMN_EQ_V2(
        createConstColumn<Nullable<Float64>>(1, {3.0}),
        executeFunction(
            func_name,
            {createConstColumn<Nullable<Decimal64>>(std::make_tuple(22, 1), 1, {"3.0"}),
             createCastTypeConstColumn("Nullable(Float64)")}));

    ASSERT_COLUMN_EQ_V2(
        createConstColumn<Nullable<Float64>>(1, {}),
        executeFunction(
            func_name,
            {createConstColumn<Nullable<Decimal64>>(std::make_tuple(22, 1), 1, std::nullopt),
             createCastTypeConstColumn("Nullable(Float64)")}));

    /// ColumnVector(const non-nullable)
    ASSERT_COLUMN_EQ_V2(
        createConstColumn<Float64>(1, {1230.345}),
        executeFunction(
            func_name,
            {createConstColumn<Decimal64>(std::make_tuple(22, 3), 1, {"1230.345"}),
             createCastTypeConstColumn("Float64")}));

    /// ColumnVector(nullable)
    ASSERT_COLUMN_EQ_V2(
        createColumn<Nullable<Float64>>({345.002, std::nullopt, 1.02}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Decimal64>>(std::make_tuple(22, 3), {"345.002", std::nullopt, "1.02"}),
             createCastTypeConstColumn("Nullable(Float64)")}));

    /// ColumnVector(non-nullable)
    ASSERT_COLUMN_EQ_V2(
        createColumn<Float64>({3.14159, 1.0234567}),
        executeFunction(
            func_name,
            {createColumn<Decimal64>(std::make_tuple(22, 7), {"3.14159", "1.0234567"}),
             createCastTypeConstColumn("Float64")}));

    /// Negative
    ASSERT_COLUMN_EQ_V2(
        createColumn<Float64>({-0.14159, -1.0234567}),
        executeFunction(
            func_name,
            {createColumn<Decimal64>(std::make_tuple(22, 7), {"-0.14159", "-1.0234567"}),
             createCastTypeConstColumn("Float64")}));

    /// Decimal32
    ASSERT_COLUMN_EQ_V2(
        createColumn<Float64>({0.141, 1.02}),
        executeFunction(
            func_name,
            {createColumn<Decimal32>(std::make_tuple(8, 3), {"0.141", "1.02"}), createCastTypeConstColumn("Float64")}));

    /// Decimal64
    ASSERT_COLUMN_EQ_V2(
        createColumn<Float64>({0.14159, 1.0234567}),
        executeFunction(
            func_name,
            {createColumn<Decimal64>(std::make_tuple(22, 7), {"0.14159", "1.0234567"}),
             createCastTypeConstColumn("Float64")}));

    /// Decimal128
    ASSERT_COLUMN_EQ_V2(
        createColumn<Float64>({0.141593452, 19387.023456789}),
        executeFunction(
            func_name,
            {createColumn<Decimal128>(std::make_tuple(25, 9), {"0.141593452", "19387.023456789"}),
             createCastTypeConstColumn("Float64")}));

    /// Decimal256
    ASSERT_COLUMN_EQ_V2(
        createColumn<Float64>({0.141593452, 193857.02356789}),
        executeFunction(
            func_name,
            {createColumn<Decimal256>(std::make_tuple(32, 9), {"0.141593452", "193857.02356789"}),
             createCastTypeConstColumn("Float64")}));
}
CATCH

TEST_F(TestDivPrecisionIncre, castDecimalAsRealNumeric)
try
{
    std::vector<std::pair<String, double>> test_data = {
        {"9007199254740993.0", 9007199254740992.0},
        {"9007199254740994.0", 9007199254740994.0},
        {"9007199254740995.0", 9007199254740996.0},
        {"9007199254740996.0", 9007199254740996.0},
        {"12345", 12345},
        {"123.45", 123.45},
        {"-123.45", -123.45},
        {"0.00012345000098765", 0.00012345000098765},
        {"1234500009876.5", 1234500009876.5},
        {"-9223372036854775807", -9223372036854775807.0},
        {"-9223372036854775808", -9223372036854775808.0},
        {"18446744073709551615", 18446744073709551615.0},
        {"123456789.987654321", 123456789.987654321},
        {"1", 1},
        {"+1", 1},
        {"100000000000000000000000", 1e+23},
        {"123456700", 1.234567e+08},
        {"99999999999999974834176", 9.999999999999997e+22},
        {"100000000000000000000001", 1.0000000000000001e+23},
        {"100000000000000008388608", 1.0000000000000001e+23},
        {"100000000000000016777215", 1.0000000000000001e+23},
        {"100000000000000016777216", 1.0000000000000003e+23},
        {"-1", -1},
        {"-0.1", -0.1},
        {"0", 0},
        {"22.222222222222222", 22.22222222222222},
        {"3.12415900000000035241", 3.124159000000000130370},
        {"3.12415900000000035242", 3.124159000000000574460},
        {"3.124158999999999908325", 3.124158999999999686281},
        {"3.124158999999999908326", 3.124159000000000130370},
        {"1090544144181609348671888949248", 1.0905441441816093e+30},
        {"1090544144181609348835077142190", 1.0905441441816094e+30},
    };
    for (const auto & data : test_data)
    {
        parseAndTestDecimalToFloat64(data.first, data.second);
    }
}
CATCH

} // namespace
} // namespace DB::tests
