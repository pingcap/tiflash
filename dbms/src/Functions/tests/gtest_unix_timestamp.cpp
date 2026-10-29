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

#include <Columns/ColumnConst.h>
#include <Common/Exception.h>
#include <Flash/Coprocessor/DefaultChunkCodec.h>
#include <Functions/FunctionsDateTime.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TimeZoneTestData.h>
#include <TiDB/Decode/TypeMapping.h>

#include <string>
#include <vector>

namespace DB
{
namespace tests
{
class TestUnixTimestamp : public DB::tests::FunctionTest
{
protected:
    String func_name_int = "tidbUnixTimeStampInt";
    String func_name_dec = "tidbUnixTimeStampDec";
};

TEST_F(TestUnixTimestamp, TestInputType)
try
{
    /// set timezone to UTC
    context->getTimezoneInfo().resetByTimezoneName("UTC");

    std::vector<DataTypeMyDate::FieldType> date_data{
        /// zero date
        MyDate(0, 0, 0).toPackedUInt(),
        /// date before 1970-01-01
        MyDate(1969, 1, 1).toPackedUInt(),
        MyDate(2020, 10, 10).toPackedUInt(),
        /// date after 2038-01-19
        MyDate(2050, 10, 10).toPackedUInt(),
    };
    std::vector<UInt64> date_result = {0, 0, 1602288000ull, 0};

    std::vector<DataTypeMyDateTime::FieldType> date_time_data{
        MyDateTime(0, 0, 0, 0, 0, 0, 0).toPackedUInt(),
        MyDateTime(1969, 1, 1, 11, 11, 11, 0).toPackedUInt(),
        MyDateTime(2020, 10, 10, 11, 11, 11, 0).toPackedUInt(),
        MyDateTime(2038, 1, 19, 3, 14, 7, 0).toPackedUInt(),
        MyDateTime(2038, 1, 19, 3, 14, 8, 0).toPackedUInt(),
    };
    std::vector<UInt64> date_time_int_result = {0, 0, 1602328271ull, 2147483647ull, 0};
    std::vector<String> date_time_decimal_result = {"0", "0", "1602328271", "2147483647", "0"};

    std::vector<DataTypeMyDateTime::FieldType> date_time_with_fsp_data{
        MyDateTime(0, 0, 0, 0, 0, 0, 0).toPackedUInt(),
        MyDateTime(1969, 12, 31, 23, 23, 23, 999999).toPackedUInt(),
        MyDateTime(1970, 1, 1, 0, 0, 0, 1).toPackedUInt(),
        MyDateTime(1970, 1, 01, 0, 0, 1, 1).toPackedUInt(),
        MyDateTime(2020, 10, 10, 11, 11, 11, 123456).toPackedUInt(),
        MyDateTime(2038, 1, 19, 3, 14, 7, 999999).toPackedUInt(),
        MyDateTime(2038, 1, 19, 3, 14, 8, 000000).toPackedUInt(),
    };
    std::vector<String> date_time_with_fsp_result
        = {"0.000000", "0.000000", "0.000000", "1.000001", "1602328271.123456", "2147483647.999999", "0.000000"};

    /// case 1, func(const)
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt64>(10, date_result[0]),
        executeFunction(func_name_int, createConstColumn<MyDate>(10, date_data[0])));
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt64>(10, date_time_int_result[0]),
        executeFunction(func_name_int, createConstColumn<MyDateTime>(std::make_tuple(0), 10, date_time_data[0])));
    ASSERT_COLUMN_EQ(
        createConstColumn<Decimal64>(std::make_tuple(12, 0), 10, date_time_decimal_result[0]),
        executeFunction(func_name_dec, createConstColumn<MyDateTime>(std::make_tuple(0), 10, date_time_data[0])));
    ASSERT_COLUMN_EQ(
        createConstColumn<Decimal64>(std::make_tuple(18, 6), 10, date_time_with_fsp_result[0]),
        executeFunction(
            func_name_dec,
            createConstColumn<MyDateTime>(std::make_tuple(6), 10, date_time_with_fsp_data[0])));
    /// case 2, func(nullable(not null const))
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt64>(10, date_result[0]),
        executeFunction(func_name_int, createConstColumn<Nullable<MyDate>>(10, date_data[0])));
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt64>(10, date_time_int_result[0]),
        executeFunction(
            func_name_int,
            createConstColumn<Nullable<MyDateTime>>(std::make_tuple(0), 10, date_time_data[0])));
    ASSERT_COLUMN_EQ(
        createConstColumn<Decimal64>(std::make_tuple(12, 0), 10, date_time_decimal_result[0]),
        executeFunction(
            func_name_dec,
            createConstColumn<Nullable<MyDateTime>>(std::make_tuple(0), 10, date_time_data[0])));
    ASSERT_COLUMN_EQ(
        createConstColumn<Decimal64>(std::make_tuple(18, 6), 10, date_time_with_fsp_result[0]),
        executeFunction(
            func_name_dec,
            createConstColumn<Nullable<MyDateTime>>(std::make_tuple(6), 10, date_time_with_fsp_data[0])));
    /// case 3, func(nullable(null const))
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<UInt64>>(10, {}),
        executeFunction(func_name_int, createConstColumn<Nullable<MyDate>>(10, {})));
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<UInt64>>(10, {}),
        executeFunction(func_name_int, createConstColumn<Nullable<MyDateTime>>(std::make_tuple(0), 10, {})));
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<Decimal64>>(std::make_tuple(12, 0), 10, {}, "", 0),
        executeFunction(func_name_dec, createConstColumn<Nullable<MyDateTime>>(std::make_tuple(0), 10, {})));
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<Decimal64>>(std::make_tuple(18, 6), 10, {}, "", 0),
        executeFunction(func_name_dec, createConstColumn<Nullable<MyDateTime>>(std::make_tuple(6), 10, {})));
    /// case 4, func(null_constant)
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<Null>>(10, {}),
        executeFunction(func_name_int, createConstColumn<Nullable<Null>>(10, {})));
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<Null>>(10, {}),
        executeFunction(func_name_dec, createConstColumn<Nullable<Null>>(10, {})));
    /// case 5, func(vector)
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>(date_result),
        executeFunction(func_name_int, createColumn<MyDate>(date_data)));
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>(date_time_int_result),
        executeFunction(func_name_int, createColumn<MyDateTime>(std::make_tuple(0), date_time_data)));
    ASSERT_COLUMN_EQ(
        createColumn<Decimal64>(std::make_tuple(12, 0), date_time_decimal_result),
        executeFunction(func_name_dec, createColumn<MyDateTime>(std::make_tuple(0), date_time_data)));
    /// unix_timestamp('1970-01-01 00:00:00.000001') returns 0.000001 in TiDB, but both TiFlash and MySQL returns 0.000000
    /// TiDB should follow MySQL/TiFlash's behavior. https://github.com/pingcap/tidb/issues/32197
    ASSERT_COLUMN_EQ(
        createColumn<Decimal64>(std::make_tuple(18, 6), date_time_with_fsp_result),
        executeFunction(func_name_dec, createColumn<MyDateTime>(std::make_tuple(6), date_time_with_fsp_data)));
    /// case 6, func(nullable(vector))
    std::vector<Int32> null_map = {0, 1, 0, 1, 0, 1, 0, 1};
    ASSERT_COLUMN_EQ(
        createNullableColumn<UInt64>(date_result, null_map),
        executeFunction(func_name_int, createNullableColumn<MyDate>(date_data, null_map)));
    ASSERT_COLUMN_EQ(
        createNullableColumn<UInt64>(date_time_int_result, null_map),
        executeFunction(func_name_int, createNullableColumn<MyDateTime>(std::make_tuple(0), date_time_data, null_map)));
    ASSERT_COLUMN_EQ(
        createNullableColumn<Decimal64>(std::make_tuple(12, 0), date_time_decimal_result, null_map),
        executeFunction(func_name_dec, createNullableColumn<MyDateTime>(std::make_tuple(0), date_time_data, null_map)));
    ASSERT_COLUMN_EQ(
        createNullableColumn<Decimal64>(std::make_tuple(18, 6), date_time_with_fsp_result, null_map),
        executeFunction(
            func_name_dec,
            createNullableColumn<MyDateTime>(std::make_tuple(6), date_time_with_fsp_data, null_map)));
}
CATCH
TEST_F(TestUnixTimestamp, TestTimezone)
try
{
    /// name based timezone
    context->getTimezoneInfo().resetByTimezoneName("Asia/Shanghai");
    std::vector<DataTypeMyDateTime::FieldType> date_time_data{
        /// min-max valid timestamp
        MyDateTime(1970, 1, 1, 8, 0, 0, 0).toPackedUInt(),
        MyDateTime(1970, 1, 1, 8, 0, 1, 0).toPackedUInt(),
        MyDateTime(2038, 1, 19, 11, 14, 7, 0).toPackedUInt(),
        MyDateTime(2038, 1, 19, 11, 14, 8, 0).toPackedUInt(),
        /// timestamp with dts
        /// according to https://www.timeanddate.com/time/change/china/shanghai?year=1990
        /// When local standard time was about to reach
        /// Sunday, 15 April 1990, 02:00:00 clocks were turned forward 1 hour to
        /// Sunday, 15 April 1990, 03:00:00 local daylight time instead.
        MyDateTime(1990, 4, 15, 1, 59, 59, 0).toPackedUInt(),
        MyDateTime(1990, 4, 15, 3, 0, 0, 0).toPackedUInt(),
        /// When local daylight time was about to reach
        /// Sunday, 16 September 1990, 02:00:00 clocks were turned backward 1 hour to
        /// Sunday, 16 September 1990, 01:00:00 local standard time instead.
        MyDateTime(1990, 9, 16, 0, 59, 59, 0).toPackedUInt(),
        MyDateTime(1990, 9, 16, 1, 0, 0, 0).toPackedUInt(),
        MyDateTime(1990, 9, 16, 1, 59, 59, 0).toPackedUInt(),
        MyDateTime(1990, 9, 16, 2, 0, 0, 0).toPackedUInt(),
    };
    std::vector<UInt64> date_time_int_result{
        0,
        1,
        2147483647ull,
        0,
        640115999ull,
        640116000ull,
        653414399ull,
        653418000ull,
        653421599ull,
        653421600ull};
    std::vector<String> date_time_decimal_result{
        "0",
        "1",
        "2147483647",
        "0",
        "640115999",
        "640116000",
        "653414399",
        "653418000",
        "653421599",
        "653421600"};
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>(date_time_int_result),
        executeFunction(func_name_int, createColumn<MyDateTime>(date_time_data)));
    ASSERT_COLUMN_EQ(
        createColumn<Decimal64>(std::make_tuple(12, 0), date_time_decimal_result),
        executeFunction(func_name_dec, createColumn<MyDateTime>(date_time_data)));

    context->getTimezoneInfo().resetByTimezoneName("America/Santiago");
    date_time_data = {
        /// min-max valid timestamp
        MyDateTime(1969, 12, 31, 21, 0, 0, 0).toPackedUInt(),
        MyDateTime(1969, 12, 31, 21, 0, 1, 0).toPackedUInt(),
        MyDateTime(2038, 1, 19, 0, 14, 7, 0).toPackedUInt(),
        MyDateTime(2038, 1, 19, 0, 14, 8, 0).toPackedUInt(),
        /// according to https://www.timeanddate.com/time/change/chile/santiago?year=2022
        /// When local daylight time is about to reach
        /// Sunday, 3 April 2022, 00:00:00 clocks are turned backward 1 hour to
        /// Saturday, 2 April 2022, 23:00:00 local standard time instead.
        MyDateTime(2022, 4, 2, 22, 59, 59, 0).toPackedUInt(),
        /// Match TiDB's choice of the earlier occurrence in this timezone.
        MyDateTime(2022, 4, 2, 23, 0, 0, 0).toPackedUInt(),
        MyDateTime(2022, 4, 2, 23, 59, 59, 0).toPackedUInt(),
        MyDateTime(2022, 4, 3, 0, 0, 0, 0).toPackedUInt(),
        /// When local standard time is about to reach
        /// Sunday, 5 September 2021, 00:00:00 clocks are turned forward 1 hour to
        /// Sunday, 5 September 2021, 01:00:00 local daylight time instead.
        MyDateTime(2021, 9, 4, 23, 59, 59, 0).toPackedUInt(),
        MyDateTime(2021, 9, 5, 1, 0, 0, 0).toPackedUInt(),
    };
    date_time_int_result
        = {0,
           1,
           2147483647ull,
           0,
           1648951199ull,
           1648951200ull,
           1648954799ull,
           1648958400ull,
           1630814399ull,
           1630814400ull};
    date_time_decimal_result
        = {"0",
           "1",
           "2147483647",
           "0",
           "1648951199",
           "1648951200",
           "1648954799",
           "1648958400",
           "1630814399",
           "1630814400"};
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>(date_time_int_result),
        executeFunction(func_name_int, createColumn<MyDateTime>(date_time_data)));
    ASSERT_COLUMN_EQ(
        createColumn<Decimal64>(std::make_tuple(12, 0), date_time_decimal_result),
        executeFunction(func_name_dec, createColumn<MyDateTime>(date_time_data)));

    /// offset based timezone
    context->getTimezoneInfo().resetByTimezoneOffset(28800);
    date_time_data = {
        /// min-max valid timestamp
        MyDateTime(1970, 1, 1, 8, 0, 0, 0).toPackedUInt(),
        MyDateTime(1970, 1, 1, 8, 0, 1, 0).toPackedUInt(),
        MyDateTime(2038, 1, 19, 11, 14, 7, 0).toPackedUInt(),
        MyDateTime(2038, 1, 19, 11, 14, 8, 0).toPackedUInt(),
    };
    date_time_int_result = {0, 1, 2147483647ull, 0};
    date_time_decimal_result = {"0", "1", "2147483647", "0"};
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>(date_time_int_result),
        executeFunction(func_name_int, createColumn<MyDateTime>(date_time_data)));
    ASSERT_COLUMN_EQ(
        createColumn<Decimal64>(std::make_tuple(12, 0), date_time_decimal_result),
        executeFunction(func_name_dec, createColumn<MyDateTime>(date_time_data)));

    context->getTimezoneInfo().resetByTimezoneOffset(-10800);
    date_time_data = {
        /// min-max valid timestamp
        MyDateTime(1969, 12, 31, 21, 0, 0, 0).toPackedUInt(),
        MyDateTime(1969, 12, 31, 21, 0, 1, 0).toPackedUInt(),
        MyDateTime(2038, 1, 19, 0, 14, 7, 0).toPackedUInt(),
        MyDateTime(2038, 1, 19, 0, 14, 8, 0).toPackedUInt(),
    };
    date_time_int_result = {0, 1, 2147483647ull, 0};
    date_time_decimal_result = {"0", "1", "2147483647", "0"};
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>(date_time_int_result),
        executeFunction(func_name_int, createColumn<MyDateTime>(date_time_data)));
    ASSERT_COLUMN_EQ(
        createColumn<Decimal64>(std::make_tuple(12, 0), date_time_decimal_result),
        executeFunction(func_name_dec, createColumn<MyDateTime>(date_time_data)));
}
CATCH

TEST_F(TestUnixTimestamp, DST)
try
{
    context->getTimezoneInfo().resetByTimezoneName(timeZoneTestName("America/New_York"));
    const auto input = createColumn<MyDateTime>(
        std::make_tuple(6),
        {MyDateTime(2026, 11, 1, 1, 30, 0, 123456).toPackedUInt(),
         MyDateTime(2026, 3, 8, 2, 30, 0, 123456).toPackedUInt()});
    ASSERT_COLUMN_EQ(createColumn<UInt64>({1793511000, 1772953200}), executeFunction(func_name_int, input));
    ASSERT_COLUMN_EQ(
        createColumn<Decimal64>(std::make_tuple(18, 6), {"1793511000.123456", "0.000000"}),
        executeFunction(func_name_dec, input));
}
CATCH

TEST_F(TestUnixTimestamp, PreciseDSTTransitions)
try
{
    for (const auto & [name, midnight] : {
             std::pair{"America/St_Johns", UInt64{1268537400}},
             std::pair{"America/Goose_Bay", UInt64{1268539200}},
         })
    {
        SCOPED_TRACE(name);
        context->getTimezoneInfo().resetByTimezoneName(timeZoneTestName(name));
        const auto input = createColumn<MyDateTime>(
            std::make_tuple(6),
            {MyDateTime(2010, 3, 14, 0, 0, 0, 0).toPackedUInt(),
             MyDateTime(2010, 3, 14, 0, 0, 30, 123456).toPackedUInt(),
             MyDateTime(2010, 3, 14, 0, 0, 59, 999999).toPackedUInt(),
             MyDateTime(2010, 3, 14, 0, 1, 0, 0).toPackedUInt(),
             MyDateTime(2010, 3, 14, 1, 0, 59, 999999).toPackedUInt(),
             MyDateTime(2010, 3, 14, 1, 1, 0, 0).toPackedUInt()});
        ASSERT_COLUMN_EQ(
            createColumn<UInt64>({midnight, midnight + 30, midnight + 59, midnight + 60, midnight + 60, midnight + 60}),
            executeFunction(func_name_int, input));
        ASSERT_COLUMN_EQ(
            createColumn<Decimal64>(
                std::make_tuple(18, 6),
                {fmt::format("{}.000000", midnight),
                 fmt::format("{}.123456", midnight + 30),
                 fmt::format("{}.999999", midnight + 59),
                 "0.000000",
                 "0.000000",
                 fmt::format("{}.000000", midnight + 60)}),
            executeFunction(func_name_dec, input));
    }
}
CATCH

TEST_F(TestUnixTimestamp, HistoricalSecondLevelOffset)
try
{
    context->getTimezoneInfo().resetByTimezoneName(timeZoneTestName("Africa/Monrovia"));
    const auto input
        = createColumn<MyDateTime>(std::make_tuple(6), {MyDateTime(1970, 7, 15, 11, 15, 0, 123456).toPackedUInt()});
    ASSERT_COLUMN_EQ(createColumn<UInt64>({16891170}), executeFunction(func_name_int, input));
    ASSERT_COLUMN_EQ(
        createColumn<Decimal64>(std::make_tuple(18, 6), {"16891170.123456"}),
        executeFunction(func_name_dec, input));
}
CATCH

TEST_F(TestUnixTimestamp, AdjustedGoTimeMicrosecondThreshold)
try
{
    context->getTimezoneInfo().resetByTimezoneName(timeZoneTestName("Antarctica/Vostok"));
    const auto input = createColumn<MyDateTime>(
        std::make_tuple(6),
        {MyDateTime(1994, 11, 1, 3, 59, 59, 999999).toPackedUInt(),
         MyDateTime(1994, 11, 1, 4, 0, 0, 0).toPackedUInt(),
         MyDateTime(1994, 11, 1, 4, 0, 0, 1).toPackedUInt(),
         MyDateTime(1994, 11, 1, 4, 0, 1, 0).toPackedUInt()});
    ASSERT_COLUMN_EQ(createColumn<UInt64>({783648000, 783648000, 0, 0}), executeFunction(func_name_int, input));
    ASSERT_COLUMN_EQ(
        createColumn<Decimal64>(std::make_tuple(18, 6), {"0.000000", "0.000000", "0.000000", "0.000000"}),
        executeFunction(func_name_dec, input));
}
CATCH

TEST_F(FunctionTest, TimeZoneRowOutputCompatibility)
try
{
    struct Case
    {
        String timezone;
        MyDateTime stored;
        MyDateTime expected_output;
    };
    // TypeDefault cannot retain the original instant when DateLUT's forward
    // projection loses information. Preserve its existing inverse and fold choice.
    for (const auto & test : {
             Case{"Africa/Monrovia", {1972, 1, 7, 0, 45, 0, 123456}, {1972, 1, 7, 0, 44, 30, 123456}},
             Case{"America/New_York", {2026, 11, 1, 5, 30, 0, 123456}, {2026, 11, 1, 6, 30, 0, 123456}},
             Case{"America/New_York", {2026, 11, 1, 6, 30, 0, 123456}, {2026, 11, 1, 6, 30, 0, 123456}},
         })
    {
        SCOPED_TRACE(test.timezone + " " + test.stored.toString(6));
        const auto zone = createConstColumn<String>(1, timeZoneTestName(test.timezone));
        const auto local = executeFunction(
            "ConvertTimeZoneFromUTC",
            {createColumn<MyDateTime>(std::make_tuple(6), {test.stored.toPackedUInt()}), zone},
            nullptr,
            true);
        auto output = executeFunction("ConvertTimeZoneToUTC", {local, zone}, nullptr, true);
        tipb::FieldType field_type;
        field_type.set_tp(TiDB::TypeTimestamp);
        field_type.set_decimal(6);
        field_type.set_flag(TiDB::ColumnFlagNotNull);
        const std::vector<tipb::FieldType> fields{field_type};
        DefaultChunkCodec codec;
        auto stream = codec.newCodecStream(fields);
        Block block({output});
        stream->encode(block, 0, 1);
        const auto decoded = codec.decode(stream->getString(), {{"ts", TiDB::fieldTypeToColumnInfo(field_type)}});
        ASSERT_EQ(decoded.rows(), 1);
        EXPECT_EQ(decoded.getByPosition(0).column->getUInt(0), test.expected_output.toPackedUInt());
    }
}
CATCH

TEST_F(TestUnixTimestamp, DateLineOffset)
try
{
    struct Case
    {
        String timezone;
        UInt64 expected_int;
        String expected_decimal;
    };
    for (const auto & test : {
             Case{"Pacific/Apia", 1772406000, "1772406000.123456"},
             Case{"Pacific/Kwajalein", 1772409600, "1772409600.123456"},
         })
    {
        SCOPED_TRACE(test.timezone);
        context->getTimezoneInfo().resetByTimezoneName(timeZoneTestName(test.timezone));
        const auto input
            = createColumn<MyDateTime>(std::make_tuple(6), {MyDateTime(2026, 3, 2, 12, 0, 0, 123456).toPackedUInt()});
        ASSERT_COLUMN_EQ(createColumn<UInt64>({test.expected_int}), executeFunction(func_name_int, input));
        ASSERT_COLUMN_EQ(
            createColumn<Decimal64>(std::make_tuple(18, 6), {test.expected_decimal}),
            executeFunction(func_name_dec, input));
    }
}
CATCH
} // namespace tests
} // namespace DB
