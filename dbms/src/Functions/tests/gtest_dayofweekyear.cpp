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
    /// ColumnVector(nullable)
    const String func_name = "toDayOfWeek";
    static auto const nullable_datetime_type_ptr = makeNullable(std::make_shared<DataTypeMyDateTime>(6));
    static auto const datetime_type_ptr = std::make_shared<DataTypeMyDateTime>(6);
    static auto const date_type_ptr = std::make_shared<DataTypeMyDate>();
    auto data_col_ptr = createColumn<Nullable<DataTypeMyDateTime::FieldType>>(
                            {
                                {}, // Null
                                // FIXME: https://github.com/pingcap/tiflash/issues/4186
                                // MyDateTime(2022, 12, 0, 1, 1, 1, 1).toPackedUInt(),
                                // MyDateTime(2022, 13, 31, 1, 1, 1, 1).toPackedUInt(),
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
    auto output_col = createColumn<Nullable<UInt8>>({{}, 5, 1, 2, 3, 4, 5, 6, 7});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    /// ColumnVector(non-null)
    data_col_ptr = createColumn<DataTypeMyDateTime::FieldType>(
                       {
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
    output_col = createColumn<UInt8>({5, 1, 2, 3, 4, 5, 6, 7});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    /// ColumnConst(non-null)
    input_col = ColumnWithTypeAndName(createConstColumn<DataTypeMyDateTime::FieldType>(1, MyDateTime(2022, 3, 19, 1, 1, 1, 1).toPackedUInt()).column, datetime_type_ptr, "input");
    output_col = createConstColumn<UInt8>(1, {7});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    /// ColumnConst(nullable)
    input_col = ColumnWithTypeAndName(createConstColumn<Nullable<DataTypeMyDateTime::FieldType>>(1, MyDateTime(2022, 3, 19, 1, 1, 1, 1).toPackedUInt()).column, nullable_datetime_type_ptr, "input");
    output_col = createConstColumn<Nullable<UInt8>>(1, {7});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    /// ColumnConst(nullable(null))
    input_col = ColumnWithTypeAndName(createConstColumn<Nullable<DataTypeMyDateTime::FieldType>>(1, {}).column, nullable_datetime_type_ptr, "input");
    output_col = createConstColumn<Nullable<UInt8>>(1, {});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));
}
CATCH

TEST_F(TestDayOfWeekYear, TestDayOfYear)
try
{
    /// ColumnVector(nullable)
    const String func_name = "toDayOfYear";
    static auto const nullable_datetime_type_ptr = makeNullable(std::make_shared<DataTypeMyDateTime>(6));
    static auto const datetime_type_ptr = std::make_shared<DataTypeMyDateTime>(6);
    static auto const date_type_ptr = std::make_shared<DataTypeMyDate>();
    auto data_col_ptr = createColumn<Nullable<DataTypeMyDateTime::FieldType>>(
                            {
                                {}, // Null
                                // FIXME: https://github.com/pingcap/tiflash/issues/4186
                                // MyDateTime(2022, 12, 0, 1, 1, 1, 1).toPackedUInt(),
                                // MyDateTime(2022, 13, 31, 1, 1, 1, 1).toPackedUInt(),
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
    auto output_col = createColumn<Nullable<UInt16>>({{}, 2, 72, 73, 74, 75, 76, 77, 78, 365, 366, 365});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    /// ColumnVector(non-null)
    data_col_ptr = createColumn<DataTypeMyDateTime::FieldType>(
                       {
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
    output_col = createColumn<UInt16>({2, 72, 73, 74, 75, 76, 77, 78, 365, 366, 365});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    /// ColumnConst(non-null)
    input_col = ColumnWithTypeAndName(createConstColumn<DataTypeMyDateTime::FieldType>(1, MyDateTime(2022, 3, 19, 1, 1, 1, 1).toPackedUInt()).column, datetime_type_ptr, "input");
    output_col = createConstColumn<UInt16>(1, {78});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    /// ColumnConst(nullable)
    input_col = ColumnWithTypeAndName(createConstColumn<Nullable<DataTypeMyDateTime::FieldType>>(1, MyDateTime(2022, 3, 19, 1, 1, 1, 1).toPackedUInt()).column, nullable_datetime_type_ptr, "input");
    output_col = createConstColumn<Nullable<UInt16>>(1, {78});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    /// ColumnConst(nullable(null))
    input_col = ColumnWithTypeAndName(createConstColumn<Nullable<DataTypeMyDateTime::FieldType>>(1, {}).column, nullable_datetime_type_ptr, "input");
    output_col = createConstColumn<Nullable<UInt16>>(1, {});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));
}
CATCH

} // namespace DB::tests
