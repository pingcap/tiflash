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
class TestFromUnixTime : public DB::tests::FunctionTest
{
protected:
    String func_name = "fromUnixTime";
};

TEST_F(TestFromUnixTime, TestInputPattern)
try
{
    /// set timezone to UTC
    context->getTimezoneInfo().resetByTimezoneName("UTC");
    std::vector<String> decimal_20_0_data{"-1", "0", "1602328271", "2147483647", "2147483648"};
    InferredDataVector<MyDateTime> decimal_20_0_date_time_result{
        MyDateTime(0, 0, 0, 0, 0, 0, 0).toPackedUInt(),
        MyDateTime(1970, 1, 1, 0, 0, 0, 0).toPackedUInt(),
        MyDateTime(2020, 10, 10, 11, 11, 11, 0).toPackedUInt(),
        MyDateTime(2038, 1, 19, 3, 14, 7, 0).toPackedUInt(),
        MyDateTime(2038, 1, 19, 3, 14, 8, 0).toPackedUInt(),
    };
    /// currently, the second argument of from_unixtime can only be constant
    String format_data = "%Y-%m-%d %H:%i:%s.%f";
    std::vector<String> decimal_20_0_string_result{
        "0000-00-00 00:00:00.000000",
        "1970-01-01 00:00:00.000000",
        "2020-10-10 11:11:11.000000",
        "2038-01-19 03:14:07.000000",
        "2038-01-19 03:14:08.000000"};
    std::vector<Int32> null_map{1, 0, 0, 0, 1};
    std::vector<Int32> null_map_for_nullable_vector_input{1, 0, 1, 0, 1};

    ColumnsWithTypeAndName first_const_arguments = {
        /// constant
        createConstColumn<Decimal128>(std::make_tuple(20, 0), 5, decimal_20_0_data[1]),
        /// nullable(not null constant)
        createConstColumn<Nullable<Decimal128>>(std::make_tuple(20, 0), 5, decimal_20_0_data[1]),
        /// nullable(null constant)
        createConstColumn<Nullable<Decimal128>>(std::make_tuple(20, 0), 5, {}, "", 0),
        /// onlyNull constant
        createConstColumn<Nullable<Null>>(5, {})};
    ColumnsWithTypeAndName second_const_arguments
        = {createConstColumn<String>(5, format_data),
           createConstColumn<Nullable<String>>(5, format_data),
           createConstColumn<Nullable<String>>(5, {}),
           createConstColumn<Nullable<Null>>(5, {})};
    auto null_datetime_result = createConstColumn<Nullable<MyDateTime>>(5, {});
    auto not_null_datetime_result = createConstColumn<Nullable<MyDateTime>>(5, decimal_20_0_date_time_result[1]);
    auto only_null_result = createConstColumn<Nullable<Null>>(5, {});
    /// case 1 func(const)
    for (auto & col : first_const_arguments)
    {
        auto result_col = null_datetime_result;
        if (col.type->onlyNull())
            result_col = only_null_result;
        else if (!col.column->isNullAt(0))
            result_col = not_null_datetime_result;
        ASSERT_COLUMN_EQ(result_col, executeFunction(func_name, col));
    }
    /// case 2 func(const, const)
    auto null_string_result = createConstColumn<Nullable<String>>(5, {});
    auto not_null_string_result = createConstColumn<Nullable<String>>(5, decimal_20_0_string_result[1]);
    for (auto & col1 : first_const_arguments)
    {
        for (auto & col2 : second_const_arguments)
        {
            auto result_col = null_string_result;
            if (col1.type->onlyNull() || col2.type->onlyNull())
                result_col = only_null_result;
            else if (!col1.column->isNullAt(0) && !col2.column->isNullAt(0))
                result_col = not_null_string_result;
            ASSERT_COLUMN_EQ(result_col, executeFunction(func_name, col1, col2));
        }
    }
    /// case 3, func(vector)
    ASSERT_COLUMN_EQ(
        createNullableColumn<MyDateTime>(decimal_20_0_date_time_result, null_map),
        executeFunction(func_name, createColumn<Decimal128>(std::make_tuple(20, 0), decimal_20_0_data)));
    /// case 4, func(vector, const)
    auto not_all_null_string_result_vector = createNullableColumn<String>(decimal_20_0_string_result, null_map);
    for (auto & format_col : second_const_arguments)
    {
        auto result_col = null_string_result;
        if (format_col.type->onlyNull())
            result_col = only_null_result;
        else if (!format_col.column->isNullAt(0))
            result_col = not_all_null_string_result_vector;
        ASSERT_COLUMN_EQ(
            result_col,
            executeFunction(
                func_name,
                createColumn<Decimal128>(std::make_tuple(20, 0), decimal_20_0_data),
                format_col));
    }
    /// case 5, func(nullable(vector))
    ASSERT_COLUMN_EQ(
        createNullableColumn<MyDateTime>(decimal_20_0_date_time_result, null_map_for_nullable_vector_input),
        executeFunction(
            func_name,
            createNullableColumn<Decimal128>(
                std::make_tuple(20, 0),
                decimal_20_0_data,
                null_map_for_nullable_vector_input)));
    /// case 6, func(nullable(vector), const)
    not_all_null_string_result_vector
        = createNullableColumn<String>(decimal_20_0_string_result, null_map_for_nullable_vector_input);
    for (auto & format_col : second_const_arguments)
    {
        auto result_col = null_string_result;
        if (format_col.type->onlyNull())
            result_col = only_null_result;
        else if (!format_col.column->isNullAt(0))
            result_col = not_all_null_string_result_vector;
        ASSERT_COLUMN_EQ(
            result_col,
            executeFunction(
                func_name,
                createNullableColumn<Decimal128>(
                    std::make_tuple(20, 0),
                    decimal_20_0_data,
                    null_map_for_nullable_vector_input),
                format_col));
    }
}
CATCH
TEST_F(TestFromUnixTime, TestInputType)
try
{
    /// set timezone to UTC
    context->getTimezoneInfo().resetByTimezoneName("UTC");
    std::vector<String> decimal_20_3_data{"-1.000", "0.123", "1602328271.124", "2147483647.123", "2147483648.123"};
    InferredDataVector<MyDateTime> decimal_20_3_date_time_result{
        MyDateTime(0, 0, 0, 0, 0, 0, 0).toPackedUInt(),
        MyDateTime(1970, 1, 1, 0, 0, 0, 123000).toPackedUInt(),
        MyDateTime(2020, 10, 10, 11, 11, 11, 124000).toPackedUInt(),
        MyDateTime(2038, 1, 19, 3, 14, 7, 123000).toPackedUInt(),
        MyDateTime(2038, 1, 19, 3, 14, 8, 123000).toPackedUInt(),
    };
    String format_data = "%Y-%m-%d %H:%i:%s.%f";
    std::vector<String> decimal_20_3_string_result{
        "0000-00-00 00:00:00.000000",
        "1970-01-01 00:00:00.123000",
        "2020-10-10 11:11:11.124000",
        "2038-01-19 03:14:07.123000",
        "2038-01-19 03:14:08.123000"};
    std::vector<Int32> null_map{1, 0, 0, 0, 1};

    ASSERT_COLUMN_EQ(
        createNullableColumn<MyDateTime>(std::make_tuple(3), decimal_20_3_date_time_result, null_map),
        executeFunction(func_name, createColumn<Decimal128>(std::make_tuple(20, 3), decimal_20_3_data)));
    ASSERT_COLUMN_EQ(
        createNullableColumn<String>(decimal_20_3_string_result, null_map),
        executeFunction(
            func_name,
            createColumn<Decimal128>(std::make_tuple(20, 3), decimal_20_3_data),
            createConstColumn<String>(5, format_data)));

    std::vector<String>
        decimal_20_6_data{"-1.000000", "0.123456", "1602328271.123456", "2147483647.123456", "2147483648.123456"};
    InferredDataVector<MyDateTime> decimal_20_6_date_time_result{
        MyDateTime(0, 0, 0, 0, 0, 0, 0).toPackedUInt(),
        MyDateTime(1970, 1, 1, 0, 0, 0, 123456).toPackedUInt(),
        MyDateTime(2020, 10, 10, 11, 11, 11, 123456).toPackedUInt(),
        MyDateTime(2038, 1, 19, 3, 14, 7, 123456).toPackedUInt(),
        MyDateTime(2038, 1, 19, 3, 14, 8, 123456).toPackedUInt(),
    };
    std::vector<String> decimal_20_6_string_result{
        "0000-00-00 00:00:00.000000",
        "1970-01-01 00:00:00.123456",
        "2020-10-10 11:11:11.123456",
        "2038-01-19 03:14:07.123456",
        "2038-01-19 03:14:08.123456"};

    ASSERT_COLUMN_EQ(
        createNullableColumn<MyDateTime>(std::make_tuple(6), decimal_20_6_date_time_result, null_map),
        executeFunction(func_name, createColumn<Decimal128>(std::make_tuple(20, 6), decimal_20_6_data)));
    ASSERT_COLUMN_EQ(
        createNullableColumn<String>(decimal_20_6_string_result, null_map),
        executeFunction(
            func_name,
            createColumn<Decimal128>(std::make_tuple(20, 6), decimal_20_6_data),
            createConstColumn<String>(5, format_data)));

    ASSERT_COLUMN_EQ(
        createNullableColumn<MyDateTime>(std::make_tuple(6), decimal_20_6_date_time_result, null_map),
        executeFunction(func_name, createColumn<Decimal256>(std::make_tuple(40, 6), decimal_20_6_data)));
    ASSERT_COLUMN_EQ(
        createNullableColumn<String>(decimal_20_6_string_result, null_map),
        executeFunction(
            func_name,
            createColumn<Decimal256>(std::make_tuple(40, 6), decimal_20_6_data),
            createConstColumn<String>(5, format_data)));

    std::vector<String> decimal_18_0_data{"-1", "0", "1602328271", "2147483647", "2147483648"};
    InferredDataVector<MyDateTime> decimal_18_0_date_time_result{
        MyDateTime(0, 0, 0, 0, 0, 0, 0).toPackedUInt(),
        MyDateTime(1970, 1, 1, 0, 0, 0, 0).toPackedUInt(),
        MyDateTime(2020, 10, 10, 11, 11, 11, 0).toPackedUInt(),
        MyDateTime(2038, 1, 19, 3, 14, 7, 0).toPackedUInt(),
        MyDateTime(2038, 1, 19, 3, 14, 8, 0).toPackedUInt(),
    };
    std::vector<String> decimal_18_0_string_result{
        "0000-00-00 00:00:00.000000",
        "1970-01-01 00:00:00.000000",
        "2020-10-10 11:11:11.000000",
        "2038-01-19 03:14:07.000000",
        "2038-01-19 03:14:08.000000"};
    ASSERT_COLUMN_EQ(
        createNullableColumn<MyDateTime>(std::make_tuple(0), decimal_18_0_date_time_result, null_map),
        executeFunction(func_name, createColumn<Decimal64>(std::make_tuple(18, 0), decimal_18_0_data)));
    ASSERT_COLUMN_EQ(
        createNullableColumn<String>(decimal_18_0_string_result, null_map),
        executeFunction(
            func_name,
            createColumn<Decimal64>(std::make_tuple(18, 0), decimal_18_0_data),
            createConstColumn<String>(5, format_data)));

    std::vector<String> decimal_8_0_data{"-1", "0", "16023282", "21474836"};
    InferredDataVector<MyDateTime> decimal_8_0_date_time_result{
        MyDateTime(0, 0, 0, 0, 0, 0, 0).toPackedUInt(),
        MyDateTime(1970, 1, 1, 0, 0, 0, 0).toPackedUInt(),
        MyDateTime(1970, 7, 5, 10, 54, 42, 0).toPackedUInt(),
        MyDateTime(1970, 9, 6, 13, 13, 56, 0).toPackedUInt(),
    };
    std::vector<String> decimal_8_0_string_result{
        "0000-00-00 00:00:00.000000",
        "1970-01-01 00:00:00.000000",
        "1970-07-05 10:54:42.000000",
        "1970-09-06 13:13:56.000000"};
    ASSERT_COLUMN_EQ(
        createNullableColumn<MyDateTime>(std::make_tuple(0), decimal_8_0_date_time_result, null_map),
        executeFunction(func_name, createColumn<Decimal32>(std::make_tuple(8, 0), decimal_8_0_data)));
    ASSERT_COLUMN_EQ(
        createNullableColumn<String>(decimal_8_0_string_result, null_map),
        executeFunction(
            func_name,
            createColumn<Decimal32>(std::make_tuple(8, 0), decimal_8_0_data),
            createConstColumn<String>(5, format_data)));
}
CATCH
TEST_F(TestFromUnixTime, TestTimezone)
try
{
    context->getTimezoneInfo().resetByTimezoneName("Asia/Shanghai");
    std::vector<String>
        decimal_20_0_data{"-1", "0", "640115999", "640116000", "653414400", "653418000", "2147483647", "2147483648"};
    InferredDataVector<MyDateTime> decimal_20_0_date_time_result{
        MyDateTime(0, 0, 0, 0, 0, 0, 0).toPackedUInt(),
        MyDateTime(1970, 1, 1, 8, 0, 0, 0).toPackedUInt(),
        MyDateTime(1990, 4, 15, 1, 59, 59, 0).toPackedUInt(),
        MyDateTime(1990, 4, 15, 3, 0, 0, 0).toPackedUInt(),
        MyDateTime(1990, 9, 16, 1, 0, 0, 0).toPackedUInt(),
        MyDateTime(1990, 9, 16, 1, 0, 0, 0).toPackedUInt(),
        MyDateTime(2038, 1, 19, 11, 14, 7, 0).toPackedUInt(),
        MyDateTime(2038, 1, 19, 11, 14, 8, 0).toPackedUInt(),
    };
    /// currently, the second argument of from_unixtime can only be constant
    String format_data = "%Y-%m-%d %H:%i:%s.%f";
    std::vector<String> decimal_20_0_string_result{
        "0000-00-00 00:00:00.000000",
        "1970-01-01 08:00:00.000000",
        "1990-04-15 01:59:59.000000",
        "1990-04-15 03:00:00.000000",
        "1990-09-16 01:00:00.000000",
        "1990-09-16 01:00:00.000000",
        "2038-01-19 11:14:07.000000",
        "2038-01-19 11:14:08.000000"};
    std::vector<Int32> null_map{1, 0, 0, 0, 0, 0, 0, 1};

    ASSERT_COLUMN_EQ(
        createNullableColumn<MyDateTime>(std::make_tuple(0), decimal_20_0_date_time_result, null_map),
        executeFunction(func_name, createColumn<Decimal128>(std::make_tuple(20, 0), decimal_20_0_data)));
    ASSERT_COLUMN_EQ(
        createNullableColumn<String>(decimal_20_0_string_result, null_map),
        executeFunction(
            func_name,
            createColumn<Decimal128>(std::make_tuple(20, 0), decimal_20_0_data),
            createConstColumn<String>(5, format_data)));

    context->getTimezoneInfo().resetByTimezoneOffset(28800);
    decimal_20_0_date_time_result[3] = MyDateTime(1990, 4, 15, 2, 0, 0, 0).toPackedUInt();
    decimal_20_0_string_result[3] = "1990-04-15 02:00:00.000000";
    decimal_20_0_date_time_result[4] = MyDateTime(1990, 9, 16, 0, 0, 0, 0).toPackedUInt();
    decimal_20_0_string_result[4] = "1990-09-16 00:00:00.000000";

    ASSERT_COLUMN_EQ(
        createNullableColumn<MyDateTime>(std::make_tuple(0), decimal_20_0_date_time_result, null_map),
        executeFunction(func_name, createColumn<Decimal128>(std::make_tuple(20, 0), decimal_20_0_data)));
    ASSERT_COLUMN_EQ(
        createNullableColumn<String>(decimal_20_0_string_result, null_map),
        executeFunction(
            func_name,
            createColumn<Decimal128>(std::make_tuple(20, 0), decimal_20_0_data),
            createConstColumn<String>(5, format_data)));

    context->getTimezoneInfo().resetByTimezoneName("America/Santiago");
    decimal_20_0_data = {"-1", "0", "1648951200", "1648954800", "1630814399", "1630814400", "2147483647", "2147483648"};
    decimal_20_0_date_time_result = {
        MyDateTime(0, 0, 0, 0, 0, 0, 0).toPackedUInt(),
        MyDateTime(1969, 12, 31, 21, 0, 0, 0).toPackedUInt(),
        MyDateTime(2022, 4, 2, 23, 0, 0, 0).toPackedUInt(),
        MyDateTime(2022, 4, 2, 23, 0, 0, 0).toPackedUInt(),
        MyDateTime(2021, 9, 4, 23, 59, 59, 0).toPackedUInt(),
        MyDateTime(2021, 9, 5, 1, 0, 0, 0).toPackedUInt(),
        MyDateTime(2038, 1, 19, 0, 14, 7, 0).toPackedUInt(),
        MyDateTime(2038, 1, 19, 0, 14, 8, 0).toPackedUInt(),
    };
    decimal_20_0_string_result
        = {"0000-00-00 00:00:00.000000",
           "1969-12-31 21:00:00.000000",
           "2022-04-02 23:00:00.000000",
           "2022-04-02 23:00:00.000000",
           "2021-09-04 23:59:59.000000",
           "2021-09-05 01:00:00.000000",
           "2038-01-19 00:14:07.000000",
           "2038-01-19 00:14:08.000000"};

    ASSERT_COLUMN_EQ(
        createNullableColumn<MyDateTime>(std::make_tuple(0), decimal_20_0_date_time_result, null_map),
        executeFunction(func_name, createColumn<Decimal128>(std::make_tuple(20, 0), decimal_20_0_data)));
    ASSERT_COLUMN_EQ(
        createNullableColumn<String>(decimal_20_0_string_result, null_map),
        executeFunction(
            func_name,
            createColumn<Decimal128>(std::make_tuple(20, 0), decimal_20_0_data),
            createConstColumn<String>(5, format_data)));

    context->getTimezoneInfo().resetByTimezoneOffset(-10800);
    decimal_20_0_date_time_result[3] = MyDateTime(2022, 4, 3, 0, 0, 0, 0).toPackedUInt();
    decimal_20_0_string_result[3] = "2022-04-03 00:00:00.000000";
    decimal_20_0_date_time_result[4] = MyDateTime(2021, 9, 5, 0, 59, 59, 0).toPackedUInt();
    decimal_20_0_string_result[4] = "2021-09-05 00:59:59.000000";

    ASSERT_COLUMN_EQ(
        createNullableColumn<MyDateTime>(std::make_tuple(0), decimal_20_0_date_time_result, null_map),
        executeFunction(func_name, createColumn<Decimal128>(std::make_tuple(20, 0), decimal_20_0_data)));
    ASSERT_COLUMN_EQ(
        createNullableColumn<String>(decimal_20_0_string_result, null_map),
        executeFunction(
            func_name,
            createColumn<Decimal128>(std::make_tuple(20, 0), decimal_20_0_data),
            createConstColumn<String>(5, format_data)));
}
CATCH
} // namespace tests
} // namespace DB
