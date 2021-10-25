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
        createColumn<Nullable<DataTypeMyDuration::FieldType>>({-1, 0, 1, {}, INT64_MAX, INT64_MIN, (838 * 3600 + 59 * 60 + 59) * NANOS_PER_SECOND, -(838 * 3600 + 59 * 60 + 59) * NANOS_PER_SECOND}).column,
        makeNullable(std::make_shared<DataTypeMyDuration>(1)),
        "result");
    ASSERT_COLUMN_EQ(
        result_col,
        executeFunction(
            "FunctionConvertDurationFromNanos",
            createColumn<Nullable<Int64>>({-1, 0, 1, {}, INT64_MAX, INT64_MIN, (838 * 3600 + 59 * 60 + 59) * NANOS_PER_SECOND, -(838 * 3600 + 59 * 60 + 59) * NANOS_PER_SECOND}),
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
} // namespace tests
} // namespace DB