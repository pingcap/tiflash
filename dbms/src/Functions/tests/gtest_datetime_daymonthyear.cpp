#include <Columns/ColumnConst.h>
#include <Common/Exception.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsDateTime.h>
#include <Interpreters/Context.h>
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
    template <bool isconst, bool nullable, typename T>
    void testForDateTime(UInt16 year, UInt8 month, UInt8 day)
    {
        ColumnWithTypeAndName column;
        ColumnWithTypeAndName result_year, result_month, result_day;
        if constexpr (isconst)
        {
            if constexpr (nullable)
            {
                column = ColumnWithTypeAndName(
                    createConstColumn<T>(1,
                                         {MyDateTime{year, month, day, 0, 0, 0, 0}.toPackedUInt()})
                        .column,
                    makeNullable(std::make_shared<DataTypeMyDateTime>(0)),
                    "result");
                result_day = createConstColumn<Nullable<UInt8>>(1, {day});
                result_month = createConstColumn<Nullable<UInt8>>(1, {month});
                result_year = createConstColumn<Nullable<UInt16>>(1, {year});
            }
            else
            {
                column = ColumnWithTypeAndName(
                    createConstColumn<T>(1,
                                         {MyDateTime{year, month, day, 0, 0, 0, 0}.toPackedUInt()})
                        .column,
                    std::make_shared<DataTypeMyDateTime>(0),
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
                    createColumn<T>(
                        {MyDateTime{year, month, day, 0, 0, 0, 0}.toPackedUInt()})
                        .column,
                    makeNullable(std::make_shared<DataTypeMyDateTime>(0)),
                    "result");
                result_day = createColumn<Nullable<UInt8>>({day});
                result_month = createColumn<Nullable<UInt8>>({month});
                result_year = createColumn<Nullable<UInt16>>({year});
            }
            else
            {
                column = ColumnWithTypeAndName(
                    createColumn<T>(
                        {MyDateTime{year, month, day, 0, 0, 0, 0}.toPackedUInt()})
                        .column,
                    std::make_shared<DataTypeMyDateTime>(0),
                    "result");
                result_day = createColumn<UInt8>({day});
                result_month = createColumn<UInt8>({month});
                result_year = createColumn<UInt16>({year});
            }
        }


        ASSERT_COLUMN_EQ(
            result_day,
            executeFunction(ToDayOfMonthImpl::name, column));
        ASSERT_COLUMN_EQ(
            result_month,
            executeFunction(ToMonthImpl::name, column));
        ASSERT_COLUMN_EQ(
            result_year,
            executeFunction(ToYearImpl::name, column));
    }
};

TEST_F(TestDateTimeDayMonthYear, dayMonthYearTest)
try
{
    for (UInt16 year = 1990; year <= 2050; year++)
    {
        for (UInt8 month = 1; month < 12; month++)
        {
            for (UInt8 day = 1; day < 31; day++)
            {
                // There is no matter if the Date/DateTime is invalid,
                // since you can just ignore the invalid cases,
                // and we just ensure function can extract correct member info from MyDateTime.

                //const nullable
                testForDateTime<false, true, Nullable<DataTypeMyDateTime::FieldType>>(year, month, day);
                //const non-nullable
                testForDateTime<false, false, DataTypeMyDateTime::FieldType>(year, month, day);
                //non-const nullable
                testForDateTime<true, true, Nullable<DataTypeMyDateTime::FieldType>>(year, month, day);
                //non-const non-nullable
                testForDateTime<true, false, DataTypeMyDateTime::FieldType>(year, month, day);
            }
        }
    }
}
CATCH

} // namespace tests
} // namespace DB