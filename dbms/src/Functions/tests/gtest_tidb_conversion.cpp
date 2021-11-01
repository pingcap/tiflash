#include "Columns/ColumnsNumber.h"
#include "Core/ColumnWithTypeAndName.h"
#include "DataTypes/DataTypeMyDateTime.h"
#include "DataTypes/DataTypeNullable.h"
#include "DataTypes/DataTypesNumber.h"
#include "Functions/FunctionFactory.h"
#include "Functions/FunctionHelpers.h"
#include "TestUtils/FunctionTestUtils.h"
#include "common/types.h"
#include "gtest/gtest.h"

namespace DB
{
namespace tests
{
class TestTidbConversion : public DB::tests::FunctionTest
{
public:
    static auto getDatetimeColumn(bool single_field = false)
    {
        MyDateTime datetime(2021, 10, 26, 16, 8, 59, 0);
        MyDateTime datetime_frac(2021, 10, 26, 16, 8, 59, 123456);

        auto col_datetime = ColumnUInt64::create();
        col_datetime->insert(Field(datetime.toPackedUInt()));
        if (!single_field)
            col_datetime->insert(Field(datetime_frac.toPackedUInt()));
        return col_datetime;
    }
};

TEST_F(TestTidbConversion, castTimestampAsReal)
try
{
    static const std::string func_name = "tidb_cast";
    static const auto data_type_ptr = std::make_shared<DataTypeMyDateTime>(6);
    static const Float64 datetime_float = 20211026160859;
    static const Float64 datetime_frac_float = 20211026160859.125;

    // cast datetime to float
    auto col_datetime1 = getDatetimeColumn();
    auto ctn_datetime1 = ColumnWithTypeAndName(std::move(col_datetime1), data_type_ptr, "datetime");
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({datetime_float, datetime_frac_float}),
        executeFunction(func_name,
                        {ctn_datetime1,
                         createConstColumn<String>(1, "Float64")}));

    // cast datetime to nullable float
    auto col_datetime2 = getDatetimeColumn();
    auto ctn_datetime2 = ColumnWithTypeAndName(std::move(col_datetime2), data_type_ptr, "datetime");
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({datetime_float, datetime_frac_float}),
        executeFunction(func_name,
                        {ctn_datetime2,
                         createConstColumn<String>(1, "Nullable(Float64)")}));

    // cast nullable datetime to nullable float
    auto col_datetime3 = getDatetimeColumn();
    auto datetime3_null_map = ColumnUInt8::create(2, 0);
    datetime3_null_map->getData()[1] = 1;
    auto col_datetime3_nullable = ColumnNullable::create(std::move(col_datetime3), std::move(datetime3_null_map));
    auto ctn_datetime3_nullable = ColumnWithTypeAndName(std::move(col_datetime3_nullable), makeNullable(data_type_ptr), "datetime");
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({datetime_float, {}}),
        executeFunction(func_name,
                        {ctn_datetime3_nullable,
                         createConstColumn<String>(1, "Nullable(Float64)")}));

    // cast const datetime to float
    auto col_datetime4_const = ColumnConst::create(getDatetimeColumn(true), 1);
    auto ctn_datetime4_const = ColumnWithTypeAndName(std::move(col_datetime4_const), data_type_ptr, "datetime");
    ASSERT_COLUMN_EQ(
        createConstColumn<Float64>(1, datetime_float),
        executeFunction(func_name,
                        {ctn_datetime4_const,
                         createConstColumn<String>(1, "Float64")}));

    // cast nullable const datetime to float
    auto col_datetime5 = getDatetimeColumn(true);
    auto datetime5_null_map = ColumnUInt8::create(1, 0);
    auto col_datetime5_nullable = ColumnNullable::create(std::move(col_datetime5), std::move(datetime5_null_map));
    auto col_datetime5_nullable_const = ColumnConst::create(std::move(col_datetime5_nullable), 1);
    auto ctn_datetime5_nullable_const = ColumnWithTypeAndName(std::move(col_datetime5_nullable_const), makeNullable(data_type_ptr), "datetime");
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<Float64>>(1, datetime_float),
        executeFunction(func_name,
                        {ctn_datetime5_nullable_const,
                         createConstColumn<String>(1, "Nullable(Float64)")}));
}
CATCH

} // namespace tests
} // namespace DB
