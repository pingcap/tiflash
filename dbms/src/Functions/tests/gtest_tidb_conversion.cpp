#include "Columns/ColumnsNumber.h"
#include "Core/ColumnWithTypeAndName.h"
#include "DataTypes/DataTypeMyDateTime.h"
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
};

TEST_F(TestTidbConversion, castTimestampAsReal)
try
{
    static const std::string func_name = "tidb_cast";

    MyDateTime datetime(2021, 10, 26, 16, 8, 59, 0);
    MyDateTime datetime_frac(2021, 10, 26, 16, 8, 59, 123456);
    auto col_datetime = ColumnUInt64::create();
    col_datetime->insert(Field(datetime.toPackedUInt()));
    col_datetime->insert(Field(datetime_frac.toPackedUInt()));
    auto ctn_datetime = ColumnWithTypeAndName(std::move(col_datetime), std::make_shared<DataTypeMyDateTime>(6), "datetime");
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({20211026160859, 20211026160859.125}),
        executeFunction(func_name,
                        {ctn_datetime,
                         createConstColumn<String>(1, "Nullable(Float64)")}));
}
CATCH

} // namespace tests
} // namespace DB
