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
    template <bool isconst, bool nullable>
    void testForDateTime(UInt16 year, UInt8 month, UInt8 day)
    {
        testFor<isconst, nullable, DataTypeMyDateTime, MyDateTime>(year, month, day, MyDateTime{year, month, day, 0, 0, 0, 0});
    }

    template <bool isconst, bool nullable>
    void testForDate(UInt16 year, UInt8 month, UInt8 day)
    {
        testFor<isconst, nullable, DataTypeMyDate, MyDate>(year, month, day, MyDate{year, month, day});
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
                    createConstColumn<Nullable<typename DT::FieldType>>(1,
                                                                        {raw_input.toPackedUInt()})
                        .column,
                    makeNullable(std::make_shared<DT>()),
                    "result");
                result_day = createConstColumn<Nullable<UInt8>>(1, {day});
                result_month = createConstColumn<Nullable<UInt8>>(1, {month});
                result_year = createConstColumn<Nullable<UInt16>>(1, {year});
            }
            else
            {
                column = ColumnWithTypeAndName(
                    createConstColumn<typename DT::FieldType>(1,
                                                              {raw_input.toPackedUInt()})
                        .column,
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
                    createColumn<Nullable<typename DT::FieldType>>(
                        {raw_input.toPackedUInt()})
                        .column,
                    makeNullable(std::make_shared<DT>()),
                    "result");
                result_day = createColumn<Nullable<UInt8>>({day});
                result_month = createColumn<Nullable<UInt8>>({month});
                result_year = createColumn<Nullable<UInt16>>({year});
            }
            else
            {
                column = ColumnWithTypeAndName(
                    createColumn<typename DT::FieldType>(
                        {raw_input.toPackedUInt()})
                        .column,
                    std::make_shared<DT>(),
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
                // and we ensure function extracts correct member info from MyDateTime.

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