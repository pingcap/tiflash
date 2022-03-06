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

class TestDayOfMonth : public DB::tests::FunctionTest
{
};

TEST_F(TestDayOfMonth, BasicTest)
try
{
    const String func_name = "tidbDayOfMonth";

    // nullable column test
    ASSERT_COLUMN_EQ(
            createColumn<Nullable<Int64>>({10, {}}),
            executeFunction(func_name,
                {createColumn<Nullable<MyDate>>({MyDate(2020, 10, 10).toPackedUInt(), {}})}));

    ASSERT_COLUMN_EQ(
            createColumn<Nullable<Int64>>({10, {}}),
            executeFunction(func_name,
                {createColumn<Nullable<MyDateTime>>({MyDateTime(2020, 10, 10, 10, 10, 10, 0).toPackedUInt(), {}})}));

    // not-null column test
    ASSERT_COLUMN_EQ(
            createColumn<Int64>({10}),
            executeFunction(func_name,
                {createColumn<MyDate>({MyDate(2020, 10, 10).toPackedUInt()})}));

    ASSERT_COLUMN_EQ(
            createColumn<Int64>({10}),
            executeFunction(func_name,
                {createColumn<MyDateTime>({MyDateTime(2020, 10, 10, 10, 10, 10, 0).toPackedUInt()})}));

    // const test
    ASSERT_COLUMN_EQ(
            createConstColumn<Int64>(1, 10),
            executeFunction(func_name, {createConstColumn<MyDate>(1, {MyDate(2020, 10, 10).toPackedUInt()})}));

    ASSERT_COLUMN_EQ(
            createConstColumn<Int64>(1, 10),
            executeFunction(func_name, {createConstColumn<MyDateTime>(1, {MyDateTime(2020, 10, 10, 10, 10, 10, 0).toPackedUInt()})}));

    // null test
    ASSERT_COLUMN_EQ(
        createOnlyNullColumnConst(5),
        executeFunction(func_name, createOnlyNullColumn(5)));
}
CATCH

} // namespace tests
} // namespace DB
