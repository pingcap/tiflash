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
class TestDayOfWeekYear : public DB::tests::FunctionTest
{
};

TEST_F(TestDayOfWeekYear, TestDayOfWeek)
try
{
    auto & dag_context = getDAGContext();
    UInt64 ori_flags = dag_context.getFlags();
    dag_context.addFlag(TiDBSQLFlags::TRUNCATE_AS_WARNING);
    /// ColumnVector(nullable)
    const String func_name = "tidbDayOfWeek";
    static auto const nullable_datetime_type_ptr = makeNullable(std::make_shared<DataTypeMyDateTime>(6));
    static auto const datetime_type_ptr = std::make_shared<DataTypeMyDateTime>(6);
    static auto const date_type_ptr = std::make_shared<DataTypeMyDate>();
    auto data_col_ptr = createColumn<Nullable<DataTypeMyDateTime::FieldType>>(
                            {
                                {}, // Null
                                // FIXME: https://github.com/pingcap/tiflash/issues/4186
                                // MyDateTime(2022, 12, 0, 1, 1, 1, 1).toPackedUInt(),
                                // MyDateTime(2022, 13, 31, 1, 1, 1, 1).toPackedUInt(),
                                MyDateTime(0, 0, 0, 0, 0, 0, 0).toPackedUInt(),
                                MyDateTime(0, 1, 1, 0, 0, 0, 0).toPackedUInt(),
                                MyDateTime(1969, 1, 2, 1, 1, 1, 1).toPackedUInt(),
                                MyDateTime(2022, 3, 13, 6, 7, 8, 9).toPackedUInt(),
                                MyDateTime(2022, 3, 14, 9, 8, 7, 6).toPackedUInt(),
                                MyDateTime(2022, 3, 15, 0, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2022, 3, 16, 1, 2, 3, 4).toPackedUInt(),
                                MyDateTime(2022, 3, 17, 4, 3, 2, 1).toPackedUInt(),
                                MyDateTime(2022, 3, 18, 0, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2022, 3, 19, 1, 1, 1, 1).toPackedUInt(),
                            })
                            .column;
    auto input_col = ColumnWithTypeAndName(data_col_ptr, nullable_datetime_type_ptr, "input");
    auto output_col = createColumn<Nullable<UInt16>>({{}, {}, 1, 5, 1, 2, 3, 4, 5, 6, 7});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    /// ColumnVector(non-null)
    data_col_ptr = createColumn<DataTypeMyDateTime::FieldType>({
                                                                   MyDateTime(0, 0, 0, 0, 0, 0, 0).toPackedUInt(),
                                                                   MyDateTime(1969, 1, 2, 1, 1, 1, 1).toPackedUInt(),
                                                                   MyDateTime(2022, 3, 13, 6, 7, 8, 9).toPackedUInt(),
                                                                   MyDateTime(2022, 3, 14, 9, 8, 7, 6).toPackedUInt(),
                                                                   MyDateTime(2022, 3, 15, 0, 0, 0, 0).toPackedUInt(),
                                                                   MyDateTime(2022, 3, 16, 1, 2, 3, 4).toPackedUInt(),
                                                                   MyDateTime(2022, 3, 17, 4, 3, 2, 1).toPackedUInt(),
                                                                   MyDateTime(2022, 3, 18, 0, 0, 0, 0).toPackedUInt(),
                                                                   MyDateTime(2022, 3, 19, 1, 1, 1, 1).toPackedUInt(),
                                                               })
                       .column;
    input_col = ColumnWithTypeAndName(data_col_ptr, datetime_type_ptr, "input");
    output_col = createColumn<Nullable<UInt16>>({{}, 5, 1, 2, 3, 4, 5, 6, 7});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    /// ColumnConst(non-null)
    input_col = ColumnWithTypeAndName(
        createConstColumn<DataTypeMyDateTime::FieldType>(1, MyDateTime(2022, 3, 19, 1, 1, 1, 1).toPackedUInt()).column,
        datetime_type_ptr,
        "input");
    output_col = createConstColumn<Nullable<UInt16>>(1, {7});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    /// ColumnConst(nullable)
    input_col = ColumnWithTypeAndName(
        createConstColumn<Nullable<DataTypeMyDateTime::FieldType>>(
            1,
            MyDateTime(2022, 3, 19, 1, 1, 1, 1).toPackedUInt())
            .column,
        nullable_datetime_type_ptr,
        "input");
    output_col = createConstColumn<Nullable<UInt16>>(1, {7});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    /// ColumnConst(nullable(null))
    input_col = ColumnWithTypeAndName(
        createConstColumn<Nullable<DataTypeMyDateTime::FieldType>>(1, {}).column,
        nullable_datetime_type_ptr,
        "input");
    output_col = createConstColumn<Nullable<UInt16>>(1, {});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    /// MyDate ColumnVector(non-null)
    data_col_ptr = createColumn<DataTypeMyDate::FieldType>({
                                                               MyDate(1969, 1, 2).toPackedUInt(),
                                                               MyDate(2022, 3, 13).toPackedUInt(),
                                                               MyDate(2022, 3, 14).toPackedUInt(),
                                                               MyDate(2022, 3, 15).toPackedUInt(),
                                                               MyDate(2022, 3, 16).toPackedUInt(),
                                                               MyDate(2022, 3, 17).toPackedUInt(),
                                                               MyDate(2022, 3, 18).toPackedUInt(),
                                                               MyDate(2022, 3, 19).toPackedUInt(),
                                                           })
                       .column;
    input_col = ColumnWithTypeAndName(data_col_ptr, date_type_ptr, "input");
    output_col = createColumn<Nullable<UInt16>>({5, 1, 2, 3, 4, 5, 6, 7});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));
    dag_context.setFlags(ori_flags);
}
CATCH

TEST_F(TestDayOfWeekYear, TestDayOfYear)
try
{
    auto & dag_context = getDAGContext();
    UInt64 ori_flags = dag_context.getFlags();
    dag_context.addFlag(TiDBSQLFlags::TRUNCATE_AS_WARNING);
    /// ColumnVector(nullable)
    const String func_name = "tidbDayOfYear";
    static auto const nullable_datetime_type_ptr = makeNullable(std::make_shared<DataTypeMyDateTime>(6));
    static auto const datetime_type_ptr = std::make_shared<DataTypeMyDateTime>(6);
    static auto const date_type_ptr = std::make_shared<DataTypeMyDate>();
    auto data_col_ptr = createColumn<Nullable<DataTypeMyDateTime::FieldType>>(
                            {
                                {}, // Null
                                // FIXME: https://github.com/pingcap/tiflash/issues/4186
                                // MyDateTime(2022, 12, 0, 1, 1, 1, 1).toPackedUInt(),
                                // MyDateTime(2022, 13, 31, 1, 1, 1, 1).toPackedUInt(),
                                MyDateTime(0, 0, 0, 0, 0, 0, 0).toPackedUInt(),
                                MyDateTime(0, 1, 1, 0, 0, 0, 0).toPackedUInt(),
                                MyDateTime(1969, 1, 2, 1, 1, 1, 1).toPackedUInt(),
                                MyDateTime(2022, 3, 13, 6, 7, 8, 9).toPackedUInt(),
                                MyDateTime(2022, 3, 14, 9, 8, 7, 6).toPackedUInt(),
                                MyDateTime(2022, 3, 15, 0, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2022, 3, 16, 1, 2, 3, 4).toPackedUInt(),
                                MyDateTime(2022, 3, 17, 4, 3, 2, 1).toPackedUInt(),
                                MyDateTime(2022, 3, 18, 0, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2022, 3, 19, 1, 1, 1, 1).toPackedUInt(),
                                MyDateTime(1900, 12, 31, 1, 1, 1, 1).toPackedUInt(),
                                MyDateTime(2020, 12, 31, 1, 1, 1, 1).toPackedUInt(),
                                MyDateTime(2022, 12, 31, 1, 1, 1, 1).toPackedUInt(),
                            })
                            .column;
    auto input_col = ColumnWithTypeAndName(data_col_ptr, nullable_datetime_type_ptr, "input");
    auto output_col = createColumn<Nullable<UInt16>>({{}, {}, 1, 2, 72, 73, 74, 75, 76, 77, 78, 365, 366, 365});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    /// ColumnVector(non-null)
    data_col_ptr = createColumn<DataTypeMyDateTime::FieldType>({
                                                                   MyDateTime(0, 0, 0, 0, 0, 0, 0).toPackedUInt(),
                                                                   MyDateTime(1969, 1, 2, 1, 1, 1, 1).toPackedUInt(),
                                                                   MyDateTime(2022, 3, 13, 6, 7, 8, 9).toPackedUInt(),
                                                                   MyDateTime(2022, 3, 14, 9, 8, 7, 6).toPackedUInt(),
                                                                   MyDateTime(2022, 3, 15, 0, 0, 0, 0).toPackedUInt(),
                                                                   MyDateTime(2022, 3, 16, 1, 2, 3, 4).toPackedUInt(),
                                                                   MyDateTime(2022, 3, 17, 4, 3, 2, 1).toPackedUInt(),
                                                                   MyDateTime(2022, 3, 18, 0, 0, 0, 0).toPackedUInt(),
                                                                   MyDateTime(2022, 3, 19, 1, 1, 1, 1).toPackedUInt(),
                                                                   MyDateTime(1900, 12, 31, 1, 1, 1, 1).toPackedUInt(),
                                                                   MyDateTime(2020, 12, 31, 1, 1, 1, 1).toPackedUInt(),
                                                                   MyDateTime(2022, 12, 31, 1, 1, 1, 1).toPackedUInt(),
                                                               })
                       .column;
    input_col = ColumnWithTypeAndName(data_col_ptr, datetime_type_ptr, "input");
    output_col = createColumn<Nullable<UInt16>>({{}, 2, 72, 73, 74, 75, 76, 77, 78, 365, 366, 365});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    /// ColumnConst(non-null)
    input_col = ColumnWithTypeAndName(
        createConstColumn<DataTypeMyDateTime::FieldType>(1, MyDateTime(2022, 3, 19, 1, 1, 1, 1).toPackedUInt()).column,
        datetime_type_ptr,
        "input");
    output_col = createConstColumn<Nullable<UInt16>>(1, {78});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    /// ColumnConst(nullable)
    input_col = ColumnWithTypeAndName(
        createConstColumn<Nullable<DataTypeMyDateTime::FieldType>>(
            1,
            MyDateTime(2022, 3, 19, 1, 1, 1, 1).toPackedUInt())
            .column,
        nullable_datetime_type_ptr,
        "input");
    output_col = createConstColumn<Nullable<UInt16>>(1, {78});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    /// ColumnConst(nullable(null))
    input_col = ColumnWithTypeAndName(
        createConstColumn<Nullable<DataTypeMyDateTime::FieldType>>(1, {}).column,
        nullable_datetime_type_ptr,
        "input");
    output_col = createConstColumn<Nullable<UInt16>>(1, {});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    /// MyDate ColumnVector(non-null)
    data_col_ptr = createColumn<DataTypeMyDate::FieldType>({
                                                               MyDate(1969, 1, 2).toPackedUInt(),
                                                               MyDate(2022, 3, 13).toPackedUInt(),
                                                               MyDate(2022, 3, 14).toPackedUInt(),
                                                               MyDate(2022, 3, 15).toPackedUInt(),
                                                               MyDate(2022, 3, 16).toPackedUInt(),
                                                               MyDate(2022, 3, 17).toPackedUInt(),
                                                               MyDate(2022, 3, 18).toPackedUInt(),
                                                               MyDate(2022, 3, 19).toPackedUInt(),
                                                               MyDate(1900, 12, 31).toPackedUInt(),
                                                               MyDate(2020, 12, 31).toPackedUInt(),
                                                               MyDate(2022, 12, 31).toPackedUInt(),
                                                           })
                       .column;
    input_col = ColumnWithTypeAndName(data_col_ptr, date_type_ptr, "input");
    output_col = createColumn<Nullable<UInt16>>({2, 72, 73, 74, 75, 76, 77, 78, 365, 366, 365});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));
    dag_context.setFlags(ori_flags);
}
CATCH

} // namespace DB::tests
