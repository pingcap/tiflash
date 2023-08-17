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

#include <Common/MyTime.h>
#include <Functions/FunctionFactory.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

namespace DB::tests
{
class TestDateTimeToString : public DB::tests::FunctionTest
{
};

TEST_F(TestDateTimeToString, TestToDateName)
try
{
    /// toDayName(nullable(vector))
    const String func_name = "toDayName";
    static auto const nullable_datetime_type_ptr = makeNullable(std::make_shared<DataTypeMyDateTime>(6));
    static auto const datetime_type_ptr = std::make_shared<DataTypeMyDateTime>(6);
    static auto const date_type_ptr = std::make_shared<DataTypeMyDate>();
    auto data_col_ptr = createColumn<Nullable<DataTypeMyDateTime::FieldType>>(
                            {
                                MyDateTime(2022, 1, 0, 0, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2022, 0, 1, 0, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2022, 0, 0, 0, 0, 0, 0).toPackedUInt(),
                                MyDateTime(0, 0, 0, 0, 0, 0, 0).toPackedUInt(), // Zero time
                                {}, // Null
                                MyDateTime(0000, 12, 1, 23, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2022, 2, 21, 9, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2022, 2, 22, 10, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2022, 2, 23, 8, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2022, 2, 24, 7, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2022, 2, 25, 11, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2022, 2, 26, 20, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2022, 2, 27, 18, 0, 0, 0).toPackedUInt(),
                            })
                            .column;
    auto input_col = ColumnWithTypeAndName(data_col_ptr, nullable_datetime_type_ptr, "input");
    auto output_col = createColumn<Nullable<String>>(
        {{}, {}, {}, {}, {}, "Friday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    /// toDayName(vector)
    data_col_ptr
        = createColumn<DataTypeMyDateTime::FieldType>({
                                                          MyDateTime(2022, 1, 0, 0, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(2022, 0, 1, 0, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(2022, 0, 0, 0, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(0, 0, 0, 0, 0, 0, 0).toPackedUInt(), // Zero time
                                                          MyDateTime(0000, 12, 1, 23, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(2022, 2, 21, 9, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(2022, 2, 22, 10, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(2022, 2, 23, 8, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(2022, 2, 24, 7, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(2022, 2, 25, 11, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(2022, 2, 26, 20, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(2022, 2, 27, 18, 0, 0, 0).toPackedUInt(),
                                                      })
              .column;
    input_col = ColumnWithTypeAndName(data_col_ptr, datetime_type_ptr, "input");
    output_col = createColumn<Nullable<String>>(
        {{}, {}, {}, {}, "Friday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    /// toDayName(const)
    ColumnsWithTypeAndName const_arguments = {
        /// constant
        ColumnWithTypeAndName(
            createConstColumn<DataTypeMyDateTime::FieldType>(5, MyDateTime(2022, 2, 21, 9, 0, 0, 0).toPackedUInt())
                .column,
            datetime_type_ptr,
            "input"),
        /// nullable(not null constant)
        ColumnWithTypeAndName(
            createConstColumn<Nullable<DataTypeMyDateTime::FieldType>>(
                5,
                MyDateTime(2022, 2, 21, 9, 0, 0, 0).toPackedUInt())
                .column,
            nullable_datetime_type_ptr,
            "input"),
        /// nullable(null constant)
        ColumnWithTypeAndName(
            createConstColumn<Nullable<DataTypeMyDateTime::FieldType>>(5, {}).column,
            nullable_datetime_type_ptr,
            "input")};

    auto null_datename_result = createConstColumn<Nullable<String>>(5, {});
    auto not_null_datename_result = createConstColumn<Nullable<String>>(5, "Monday");
    for (auto & col : const_arguments)
    {
        auto result_col = null_datename_result;
        if (!col.column->isNullAt(0))
            result_col = not_null_datename_result;
        ASSERT_COLUMN_EQ(result_col, executeFunction(func_name, col));
    }

    /// toDayName(vector) for MyDate type
    data_col_ptr = createColumn<DataTypeMyDate::FieldType>({
                                                               MyDate(2022, 1, 0).toPackedUInt(),
                                                               MyDate(2022, 0, 1).toPackedUInt(),
                                                               MyDate(2022, 0, 0).toPackedUInt(),
                                                               MyDate(0, 0, 0).toPackedUInt(), // Zero time
                                                               MyDate(0000, 12, 1).toPackedUInt(),
                                                               MyDate(2022, 2, 21).toPackedUInt(),
                                                               MyDate(2022, 2, 22).toPackedUInt(),
                                                               MyDate(2022, 2, 23).toPackedUInt(),
                                                               MyDate(2022, 2, 24).toPackedUInt(),
                                                               MyDate(2022, 2, 25).toPackedUInt(),
                                                               MyDate(2022, 2, 26).toPackedUInt(),
                                                               MyDate(2022, 2, 27).toPackedUInt(),
                                                           })
                       .column;
    input_col = ColumnWithTypeAndName(data_col_ptr, date_type_ptr, "input");
    output_col = createColumn<Nullable<String>>(
        {{}, {}, {}, {}, "Friday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));
}
CATCH

TEST_F(TestDateTimeToString, TestToMonthName)
try
{
    /// toMonthName(nullable(vector))
    const String func_name = "toMonthName";
    static auto const nullable_datetime_type_ptr = makeNullable(std::make_shared<DataTypeMyDateTime>(6));
    static auto const datetime_type_ptr = std::make_shared<DataTypeMyDateTime>(6);
    static auto const date_type_ptr = std::make_shared<DataTypeMyDate>();
    auto data_col_ptr = createColumn<Nullable<DataTypeMyDateTime::FieldType>>(
                            {
                                MyDateTime(2022, 0, 1, 0, 0, 0, 0).toPackedUInt(),
                                MyDateTime(0, 0, 0, 0, 0, 0, 0).toPackedUInt(), // Zero time
                                {}, // Null
                                MyDateTime(0000, 1, 1, 3, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2020, 2, 21, 4, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2021, 3, 22, 7, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2022, 4, 23, 0, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2023, 5, 24, 0, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2024, 6, 25, 10, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2025, 7, 26, 0, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2022, 8, 27, 0, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2022, 9, 0, 0, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2022, 10, 1, 0, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2022, 11, 10, 0, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2022, 12, 8, 0, 0, 0, 0).toPackedUInt(),
                            })
                            .column;
    auto input_col = ColumnWithTypeAndName(data_col_ptr, nullable_datetime_type_ptr, "input");
    auto output_col = createColumn<Nullable<String>>(
        {{},
         {},
         {},
         "January",
         "February",
         "March",
         "April",
         "May",
         "June",
         "July",
         "August",
         "September",
         "October",
         "November",
         "December"});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    /// toMonthName(vector)
    data_col_ptr
        = createColumn<DataTypeMyDateTime::FieldType>({
                                                          MyDateTime(2022, 0, 1, 0, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(0, 0, 0, 0, 0, 0, 0).toPackedUInt(), // Zero time
                                                          MyDateTime(0000, 1, 1, 3, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(2020, 2, 21, 4, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(2021, 3, 22, 7, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(2022, 4, 23, 0, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(2023, 5, 24, 0, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(2024, 6, 25, 10, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(2025, 7, 26, 0, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(2022, 8, 27, 0, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(2022, 9, 0, 0, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(2022, 10, 1, 0, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(2022, 11, 10, 0, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(2022, 12, 8, 0, 0, 0, 0).toPackedUInt(),
                                                      })
              .column;
    input_col = ColumnWithTypeAndName(data_col_ptr, datetime_type_ptr, "input");
    output_col = createColumn<Nullable<String>>(
        {{},
         {},
         "January",
         "February",
         "March",
         "April",
         "May",
         "June",
         "July",
         "August",
         "September",
         "October",
         "November",
         "December"});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    /// toMonthName(const)
    ColumnsWithTypeAndName const_arguments = {
        /// constant
        ColumnWithTypeAndName(
            createConstColumn<DataTypeMyDateTime::FieldType>(5, MyDateTime(2022, 2, 21, 9, 0, 0, 0).toPackedUInt())
                .column,
            datetime_type_ptr,
            "input"),
        /// nullable(not null constant)
        ColumnWithTypeAndName(
            createConstColumn<Nullable<DataTypeMyDateTime::FieldType>>(
                5,
                MyDateTime(2022, 2, 21, 9, 0, 0, 0).toPackedUInt())
                .column,
            nullable_datetime_type_ptr,
            "input"),
        /// nullable(null constant)
        ColumnWithTypeAndName(
            createConstColumn<Nullable<DataTypeMyDateTime::FieldType>>(5, {}).column,
            nullable_datetime_type_ptr,
            "input")};

    auto null_monthname_result = createConstColumn<Nullable<String>>(5, {});
    auto not_null_monthname_result = createConstColumn<Nullable<String>>(5, "February");
    for (auto & col : const_arguments)
    {
        auto result_col = null_monthname_result;
        if (!col.column->isNullAt(0))
            result_col = not_null_monthname_result;
        ASSERT_COLUMN_EQ(result_col, executeFunction(func_name, col));
    }

    /// toMonthName(vector) for MyDate type
    data_col_ptr = createColumn<DataTypeMyDate::FieldType>({
                                                               MyDate(2022, 0, 1).toPackedUInt(),
                                                               MyDate(0, 0, 0).toPackedUInt(), // Zero time
                                                               MyDate(0000, 1, 1).toPackedUInt(),
                                                               MyDate(2020, 2, 21).toPackedUInt(),
                                                               MyDate(2021, 3, 22).toPackedUInt(),
                                                               MyDate(2022, 4, 23).toPackedUInt(),
                                                               MyDate(2023, 5, 24).toPackedUInt(),
                                                               MyDate(2024, 6, 25).toPackedUInt(),
                                                               MyDate(2025, 7, 26).toPackedUInt(),
                                                               MyDate(2022, 8, 27).toPackedUInt(),
                                                               MyDate(2022, 9, 0).toPackedUInt(),
                                                               MyDate(2022, 10, 1).toPackedUInt(),
                                                               MyDate(2022, 11, 10).toPackedUInt(),
                                                               MyDate(2022, 12, 8).toPackedUInt(),
                                                           })
                       .column;
    input_col = ColumnWithTypeAndName(data_col_ptr, date_type_ptr, "input");
    output_col = createColumn<Nullable<String>>(
        {{},
         {},
         "January",
         "February",
         "March",
         "April",
         "May",
         "June",
         "July",
         "August",
         "September",
         "October",
         "November",
         "December"});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));
}
CATCH

} // namespace DB::tests
