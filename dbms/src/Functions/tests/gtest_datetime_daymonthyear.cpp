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
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsDateTime.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

namespace DB
{
namespace tests
{
class TestDateTimeDayMonthYear : public DB::tests::FunctionTest
{
protected:
    template <bool isconst, bool nullable>
    void testForDateTime(UInt16 year, UInt8 month, UInt8 day)
    {
        testFor<isconst, nullable, DataTypeMyDateTime, MyDateTime>(
            year,
            month,
            day,
            MyDateTime{year, month, day, 0, 0, 0, 0});
    }

    template <bool isconst, bool nullable>
    void testForDate(UInt16 year, UInt8 month, UInt8 day)
    {
        testFor<isconst, nullable, DataTypeMyDate, MyDate>(year, month, day, MyDate{year, month, day});
    }

    template <bool isconst, bool nullable>
    void testEmptyForDateTime()
    {
        testEmptyFor<isconst, nullable, DataTypeMyDateTime>();
    }

    template <bool isconst, bool nullable>
    void testEmptyForDate()
    {
        testEmptyFor<isconst, nullable, DataTypeMyDate>();
    }

    void assertDayMonthYear(
        const ColumnWithTypeAndName & column,
        const ColumnWithTypeAndName & result_day,
        const ColumnWithTypeAndName & result_month,
        const ColumnWithTypeAndName & result_year)
    {
        ASSERT_COLUMN_EQ(result_day, executeFunction(ToDayOfMonthImpl::name, column));
        ASSERT_COLUMN_EQ(result_month, executeFunction(ToMonthImpl::name, column));
        ASSERT_COLUMN_EQ(result_year, executeFunction(ToYearImpl::name, column));
    }

    template <bool isconst, bool nullable, typename DT, typename V>
    void testFor(UInt16 year, UInt8 month, UInt8 day, V raw_input)
    {
        ColumnWithTypeAndName column;
        ColumnWithTypeAndName result_year, result_month, result_day;
        if constexpr (isconst)
        {
            if constexpr (nullable)
            {
                column = ColumnWithTypeAndName(
                    createConstColumn<Nullable<typename DT::FieldType>>(1, {raw_input.toPackedUInt()}).column,
                    makeNullable(std::make_shared<DT>()),
                    "result");
                result_day = createConstColumn<UInt8>(1, {day});
                result_month = createConstColumn<UInt8>(1, {month});
                result_year = createConstColumn<UInt16>(1, {year});
            }
            else
            {
                column = ColumnWithTypeAndName(
                    createConstColumn<typename DT::FieldType>(1, {raw_input.toPackedUInt()}).column,
                    std::make_shared<DT>(),
                    "result");
                result_day = createConstColumn<UInt8>(1, {day});
                result_month = createConstColumn<UInt8>(1, {month});
                result_year = createConstColumn<UInt16>(1, {year});
            }
        }
        else
        {
            if constexpr (nullable)
            {
                column = ColumnWithTypeAndName(
                    createColumn<Nullable<typename DT::FieldType>>({raw_input.toPackedUInt()}).column,
                    makeNullable(std::make_shared<DT>()),
                    "result");
                result_day = createColumn<Nullable<UInt8>>({day});
                result_month = createColumn<Nullable<UInt8>>({month});
                result_year = createColumn<Nullable<UInt16>>({year});
            }
            else
            {
                column = ColumnWithTypeAndName(
                    createColumn<typename DT::FieldType>({raw_input.toPackedUInt()}).column,
                    std::make_shared<DT>(),
                    "result");
                result_day = createColumn<UInt8>({day});
                result_month = createColumn<UInt8>({month});
                result_year = createColumn<UInt16>({year});
            }
        }


        assertDayMonthYear(column, result_day, result_month, result_year);
    }


    template <bool isconst, bool nullable, typename DT>
    void testEmptyFor()
    {
        ColumnWithTypeAndName column;
        ColumnWithTypeAndName result_year, result_month, result_day;
        if constexpr (isconst)
        {
            if constexpr (nullable)
            {
                column = ColumnWithTypeAndName(
                    createConstColumn<Nullable<typename DT::FieldType>>(1, {}).column,
                    makeNullable(std::make_shared<DT>()),
                    "result");
                result_day = createConstColumn<Nullable<UInt8>>(1, {});
                result_month = createConstColumn<Nullable<UInt8>>(1, {});
                result_year = createConstColumn<Nullable<UInt16>>(1, {});
            }
            else
            {
                column = ColumnWithTypeAndName(
                    createConstColumn<typename DT::FieldType>(1, {}).column,
                    std::make_shared<DT>(),
                    "result");
                result_day = createConstColumn<UInt8>(1, {});
                result_month = createConstColumn<UInt8>(1, {});
                result_year = createConstColumn<UInt16>(1, {});
            }
        }
        else
        {
            if constexpr (nullable)
            {
                column = ColumnWithTypeAndName(
                    createColumn<Nullable<typename DT::FieldType>>({{}}).column,
                    makeNullable(std::make_shared<DT>()),
                    "result");
                result_day = createColumn<Nullable<UInt8>>({{}});
                result_month = createColumn<Nullable<UInt8>>({{}});
                result_year = createColumn<Nullable<UInt16>>({{}});
            }
            else
            {
                column = ColumnWithTypeAndName(
                    createColumn<typename DT::FieldType>({{}}).column,
                    std::make_shared<DT>(),
                    "result");
                result_day = createColumn<UInt8>({{}});
                result_month = createColumn<UInt8>({{}});
                result_year = createColumn<UInt16>({{}});
            }
        }

        assertDayMonthYear(column, result_day, result_month, result_year);
    }
};

TEST_F(TestDateTimeDayMonthYear, dayMonthYearTest)
try
{
    //NULL cases
    ASSERT_COLUMN_EQ(createOnlyNullColumnConst(5), executeFunction(ToDayOfMonthImpl::name, createOnlyNullColumn(5)));
    ASSERT_COLUMN_EQ(createOnlyNullColumnConst(5), executeFunction(ToMonthImpl::name, createOnlyNullColumn(5)));
    ASSERT_COLUMN_EQ(createOnlyNullColumnConst(5), executeFunction(ToYearImpl::name, createOnlyNullColumn(5)));
    ASSERT_COLUMN_EQ(
        createOnlyNullColumnConst(5),
        executeFunction(ToDayOfMonthImpl::name, createOnlyNullColumnConst(5)));
    ASSERT_COLUMN_EQ(createOnlyNullColumnConst(5), executeFunction(ToMonthImpl::name, createOnlyNullColumnConst(5)));
    ASSERT_COLUMN_EQ(createOnlyNullColumnConst(5), executeFunction(ToYearImpl::name, createOnlyNullColumnConst(5)));

    testEmptyForDate<true, true>();
    testEmptyForDate<true, false>();
    testEmptyForDate<false, true>();
    testEmptyForDate<false, true>();

    testEmptyForDateTime<true, true>();
    testEmptyForDateTime<true, false>();
    testEmptyForDateTime<false, true>();
    testEmptyForDateTime<false, false>();


    // Scan day, month and year. There is no matter if the Date/DateTime is invalid,
    // since you can just ignore the invalid cases,
    // and we ensure function extracts correct member info from MyDateTime.
    UInt8 corner_days[] = {1, 28, 29, 30, 31};
    for (UInt16 year = 1990; year <= 2030; year++)
    {
        for (UInt8 month = 1; month <= 12; month++)
        {
            for (UInt8 day : corner_days)
            {
                //DateTime const nullable
                testForDateTime<true, true>(year, month, day);
                //DateTime const non-nullable
                testForDateTime<true, false>(year, month, day);
                //DateTime non-const nullable
                testForDateTime<false, true>(year, month, day);
                //DateTime non-const non-nullable
                testForDateTime<false, false>(year, month, day);

                //Date const nullable
                testForDate<true, true>(year, month, day);
                //Date const non-nullable
                testForDate<true, false>(year, month, day);
                //Date non-const nullable
                testForDate<false, true>(year, month, day);
                //Date non-const non-nullable
                testForDate<false, false>(year, month, day);
            }
        }
    }

    //scan a whole year to scan days.
    for (UInt16 year = 2000; year <= 2000; year++)
    {
        for (UInt8 month = 1; month <= 12; month++)
        {
            for (UInt8 day = 1; day < 31; day++)
            {
                //DateTime const nullable
                testForDateTime<true, true>(year, month, day);
                //DateTime const non-nullable
                testForDateTime<true, false>(year, month, day);
                //DateTime non-const nullable
                testForDateTime<false, true>(year, month, day);
                //DateTime non-const non-nullable
                testForDateTime<false, false>(year, month, day);

                //Date const nullable
                testForDate<true, true>(year, month, day);
                //Date const non-nullable
                testForDate<true, false>(year, month, day);
                //Date non-const nullable
                testForDate<false, true>(year, month, day);
                //Date non-const non-nullable
                testForDate<false, false>(year, month, day);
            }
        }
    }
}
CATCH

} // namespace tests
} // namespace DB