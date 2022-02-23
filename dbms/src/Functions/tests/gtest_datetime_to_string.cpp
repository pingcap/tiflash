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
    const String func_name = "toDayName";
    static const auto data_type_ptr = makeNullable(std::make_shared<DataTypeMyDateTime>(6));
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
    ColumnWithTypeAndName input_col(data_col_ptr, data_type_ptr, "input");
    auto output_col = createColumn<Nullable<String>>({{}, {}, {}, {}, {}, "Friday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));
}
CATCH

TEST_F(TestDateTimeToString, TestToMonthName)
try
{
    const String func_name = "toMonthName";
    static const auto data_type_ptr = makeNullable(std::make_shared<DataTypeMyDateTime>(6));
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
    ColumnWithTypeAndName input_col(data_col_ptr, data_type_ptr, "input");
    auto output_col = createColumn<Nullable<String>>({{}, {}, {}, "January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));
}
CATCH

} // namespace DB::tests
