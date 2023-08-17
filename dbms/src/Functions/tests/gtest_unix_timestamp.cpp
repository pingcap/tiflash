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
#include <Functions/FunctionsDateTime.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

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
        /// due to https://github.com/pingcap/tidb/issues/32251, TiDB will return 1648951200 for `2022-04-02 23:00:00` while TiFlash return 1648954800
        MyDateTime(2022, 4, 2, 23, 0, 0, 0).toPackedUInt(),
        /// due to https://github.com/pingcap/tidb/issues/32251, TiDB will return 1648954799 for `2022-04-02 23:59:59` while TiFlash return 1648958399
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
           1648954800ull,
           1648958399ull,
           1648958400ull,
           1630814399ull,
           1630814400ull};
    date_time_decimal_result
        = {"0",
           "1",
           "2147483647",
           "0",
           "1648951199",
           "1648954800",
           "1648958399",
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
} // namespace tests
} // namespace DB
