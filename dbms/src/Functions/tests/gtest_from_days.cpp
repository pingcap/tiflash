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
class TestFromDays : public DB::tests::FunctionTest
{
};

TEST_F(TestFromDays, TestAll)
try
{
    auto & dag_context = getDAGContext();
    UInt64 ori_flags = dag_context.getFlags();
    dag_context.addFlag(TiDBSQLFlags::TRUNCATE_AS_WARNING);
    /// ColumnVector(nullable)
    const String func_name = "tidbFromDays";
    static auto const nullable_date_type_ptr = makeNullable(std::make_shared<DataTypeMyDate>());
    auto data_col_ptr = createColumn<Nullable<DataTypeMyDate::FieldType>>({
                                                                              {}, // Null
                                                                              0, // Zero date
                                                                              MyDate(1969, 1, 2).toPackedUInt(),
                                                                              MyDate(2000, 12, 31).toPackedUInt(),
                                                                              MyDate(2022, 3, 13).toPackedUInt(),
                                                                          })
                            .column;
    auto output_col = ColumnWithTypeAndName(data_col_ptr, nullable_date_type_ptr, "input");
    auto input_col = createColumn<Nullable<UInt32>>({{}, 1, 719164, 730850, 738592});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    /// ColumnVector(non-null)
    data_col_ptr = createColumn<Nullable<DataTypeMyDateTime::FieldType>>({
                                                                             MyDate(0, 0, 0).toPackedUInt(),
                                                                             MyDate(1969, 1, 2).toPackedUInt(),
                                                                             MyDate(2000, 12, 31).toPackedUInt(),
                                                                             MyDate(2022, 3, 13).toPackedUInt(),
                                                                         })
                       .column;
    output_col = ColumnWithTypeAndName(data_col_ptr, nullable_date_type_ptr, "input");
    input_col = createColumn<UInt32>({1, 719164, 730850, 738592});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    /// ColumnConst(non-null)
    output_col = ColumnWithTypeAndName(
        createConstColumn<Nullable<DataTypeMyDate::FieldType>>(1, MyDate(2022, 3, 13).toPackedUInt()).column,
        nullable_date_type_ptr,
        "input");
    input_col = createConstColumn<UInt32>(1, {738592});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    /// ColumnConst(nullable)
    output_col = ColumnWithTypeAndName(
        createConstColumn<Nullable<DataTypeMyDate::FieldType>>(1, MyDate(2022, 3, 13).toPackedUInt()).column,
        nullable_date_type_ptr,
        "input");
    input_col = createConstColumn<Nullable<UInt32>>(1, {738592});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    /// ColumnConst(nullable(null))
    output_col = ColumnWithTypeAndName(
        createConstColumn<Nullable<DataTypeMyDate::FieldType>>(1, {}).column,
        nullable_date_type_ptr,
        "input");
    input_col = createConstColumn<Nullable<UInt32>>(1, {});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));
    dag_context.setFlags(ori_flags);
}
CATCH

TEST_F(TestFromDays, ConversionFromDaysAndDate)
try
{
    ASSERT_EQ(calcDayNum(9999, 12, 31), 3652424);
    for (int i = 366; i <= 3652424; ++i)
    {
        MyDateTime tmp(0);
        fromDayNum(tmp, i);
        int calced_daynum = calcDayNum(tmp.year, tmp.month, tmp.day);
        ASSERT_EQ(i, calced_daynum);
    }
}
CATCH

TEST_F(TestFromDays, ConversionFromDateAndDays)
try
{
    auto is_valid_date = [](int year, int month, int day) {
        bool is_leap_year = year % 400 == 0 || (year % 4 == 0 && year % 100 != 0);
        constexpr int max_day_for_year[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        constexpr int max_day_for_leap_year[] = {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        if (is_leap_year)
        {
            return max_day_for_leap_year[month - 1] >= day;
        }
        else
        {
            return max_day_for_year[month - 1] >= day;
        }
    };

    for (int year = 1; year <= 2037; year++)
        for (int month = 1; month <= 12; month++)
            for (int day = 1; day <= 31; day++)
            {
                if (is_valid_date(year, month, day))
                {
                    auto day_num = calcDayNum(year, month, day);
                    MyDateTime t(0);
                    fromDayNum(t, day_num);
                    ASSERT_EQ(t.year, year);
                    ASSERT_EQ(t.month, month);
                    ASSERT_EQ(t.day, day);
                }
            }
}
CATCH

} // namespace DB::tests
