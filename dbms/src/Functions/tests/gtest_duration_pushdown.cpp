#include <Common/MyDuration.h>
#include <DataTypes/DataTypeMyDuration.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/registerFunctions.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

namespace DB
{
namespace tests
{
class DurationPushDown : public DB::tests::FunctionTest
{
};

TEST_F(DurationPushDown, durationPushDownTest)
try
{
    ColumnWithTypeAndName result_col(
        createColumn<Nullable<DataTypeMyDuration::FieldType>>({-1, 0, 1, {}, INT64_MAX, INT64_MIN, (838 * 3600 + 59 * 60 + 59) * 1000000000L, -(838 * 3600 + 59 * 60 + 59) * 1000000000L}).column,
        makeNullable(std::make_shared<DataTypeMyDuration>(1)),
        "result");
    ASSERT_COLUMN_EQ(
        result_col,
        executeFunction(
            "FunctionConvertDurationFromNanos",
            createColumn<Nullable<Int64>>({-1, 0, 1, {}, INT64_MAX, INT64_MIN, (838 * 3600 + 59 * 60 + 59) * 1000000000L, -(838 * 3600 + 59 * 60 + 59) * 1000000000L}),
            createConstColumn<Int64>(8, 1)));

    ColumnWithTypeAndName result_col2(
        createConstColumn<DataTypeMyDuration::FieldType>(3, 3).column,
        std::make_shared<DataTypeMyDuration>(2),
        "result2");
    ASSERT_COLUMN_EQ(
        result_col2,
        executeFunction(
            "FunctionConvertDurationFromNanos",
            createConstColumn<Int64>(3, 3),
            createConstColumn<Int64>(3, 2)));
}
CATCH

TEST_F(DurationPushDown, hourPushDownTest)
try
{
    ColumnWithTypeAndName input(
        createColumn<Nullable<DataTypeMyDuration::FieldType>>({(838 * 3600 + 59 * 60 + 59) * 1000000000L + 999999000L, -(838 * 3600 + 59 * 60 + 59) * 1000000000L - 123456000L}).column,
        makeNullable(std::make_shared<DataTypeMyDuration>(6)),
        "result");

    auto hour_output = createColumn<Nullable<Int64>>({838, 838});
    auto minute_output = createColumn<Nullable<Int64>>({59, 59});
    auto second_output = createColumn<Nullable<Int64>>({59, 59});
    auto microsecond_output = createColumn<Nullable<Int64>>({999999, 123456});
    ASSERT_COLUMN_EQ(hour_output, executeFunction("hour", input));
    ASSERT_COLUMN_EQ(minute_output, executeFunction("minute", input));
    ASSERT_COLUMN_EQ(second_output, executeFunction("second", input));
    ASSERT_COLUMN_EQ(microsecond_output, executeFunction("microSecond", input));
}
CATCH
} // namespace tests
} // namespace DB