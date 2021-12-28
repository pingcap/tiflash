#include <optional>

#include "DataTypes/DataTypeMyDuration.h"
#include "DataTypes/DataTypeNullable.h"
#include "TestUtils/FunctionTestUtils.h"
#include "common/types.h"
#include "gtest/gtest.h"

namespace DB::tests
{
namespace
{
class TestTimestampDiff : public DB::tests::FunctionTest
{
};

const std::string func_name = "tidbTimestampDiff";

#define ASSERT_TIMESTAMP_DIFF(unit, t1, t2, result) \
    ASSERT_COLUMN_EQ(result, executeFunction(func_name, {createConstColumn<String>((t1).column->size(), unit), t1, t2}))


TEST_F(TestTimestampDiff, nullOnly)
try
{
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<Int64>>(1, std::nullopt),
        executeFunction(func_name,
                        {createConstColumn<String>(1, "year"),
                         createOnlyNullColumnConst(1),
                         createDateTimeColumnNullable({{{2020, 1, 1, 0, 0, 0, 0}}}, 6)}));
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<Int64>>(1, std::nullopt),
        executeFunction(func_name,
                        {createConstColumn<String>(1, "year"),
                         createDateTimeColumnNullable({{{2020, 1, 1, 0, 0, 0, 0}}}, 6),
                         createOnlyNullColumnConst(1)}));
}
CATCH

TEST_F(TestTimestampDiff, constVector)
try
{
    ASSERT_TIMESTAMP_DIFF("year",
                          createDateTimeColumnConst(2, {2021, 1, 1, 0, 0, 0, 0}, 6),
                          createDateTimeColumnNullable({{{2022, 1, 1, 0, 0, 0, 0}}, {{2020, 1, 1, 0, 0, 0, 0}}}, 6),
                          createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF("year",
                          createDateTimeColumnNullable({{{2022, 1, 1, 0, 0, 0, 0}}, {{2020, 1, 1, 0, 0, 0, 0}}}, 6),
                          createDateTimeColumnConst(2, {2021, 1, 1, 0, 0, 0, 0}, 6),
                          createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMP_DIFF("quarter",
                          createDateTimeColumnConst(2, {2021, 1, 1, 0, 0, 0, 0}, 6),
                          createDateTimeColumnNullable({{{2021, 4, 1, 0, 0, 0, 0}}, {{2020, 10, 1, 0, 0, 0, 0}}}, 6),
                          createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF("quarter",
                          createDateTimeColumnNullable({{{2021, 4, 1, 0, 0, 0, 0}}, {{2020, 10, 1, 0, 0, 0, 0}}}, 6),
                          createDateTimeColumnConst(2, {2021, 1, 1, 0, 0, 0, 0}, 6),
                          createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMP_DIFF("month",
                          createDateTimeColumnConst(2, {2021, 1, 1, 0, 0, 0, 0}, 6),
                          createDateTimeColumnNullable({{{2021, 2, 1, 0, 0, 0, 0}}, {{2020, 12, 1, 0, 0, 0, 0}}}, 6),
                          createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF("month",
                          createDateTimeColumnNullable({{{2021, 2, 1, 0, 0, 0, 0}}, {{2020, 12, 1, 0, 0, 0, 0}}}, 6),
                          createDateTimeColumnConst(2, {2021, 1, 1, 0, 0, 0, 0}, 6),
                          createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMP_DIFF("week",
                          createDateTimeColumnConst(2, {2021, 2, 28, 0, 0, 0, 0}, 6),
                          createDateTimeColumnNullable({{{2021, 3, 7, 0, 0, 0, 0}}, {{2021, 2, 21, 0, 0, 0, 0}}}, 6),
                          createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF("week",
                          createDateTimeColumnNullable({{{2021, 3, 7, 0, 0, 0, 0}}, {{2021, 2, 21, 0, 0, 0, 0}}}, 6),
                          createDateTimeColumnConst(2, {2021, 2, 28, 0, 0, 0, 0}, 6),
                          createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMP_DIFF("day",
                          createDateTimeColumnConst(2, {2021, 2, 28, 0, 0, 0, 0}, 6),
                          createDateTimeColumnNullable({{{2021, 3, 1, 0, 0, 0, 0}}, {{2021, 2, 27, 0, 0, 0, 0}}}, 6),
                          createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF("day",
                          createDateTimeColumnNullable({{{2021, 3, 1, 0, 0, 0, 0}}, {{2021, 2, 27, 0, 0, 0, 0}}}, 6),
                          createDateTimeColumnConst(2, {2021, 2, 28, 0, 0, 0, 0}, 6),
                          createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMP_DIFF("hour",
                          createDateTimeColumnConst(2, {2021, 1, 1, 23, 0, 0, 0}, 6),
                          createDateTimeColumnNullable({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 22, 0, 0, 0}}}, 6),
                          createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF("hour",
                          createDateTimeColumnNullable({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 22, 0, 0, 0}}}, 6),
                          createDateTimeColumnConst(2, {2021, 1, 1, 23, 0, 0, 0}, 6),
                          createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMP_DIFF("minute",
                          createDateTimeColumnConst(2, {2021, 1, 1, 23, 59, 0, 0}, 6),
                          createDateTimeColumnNullable({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 58, 0, 0}}}, 6),
                          createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF("minute",
                          createDateTimeColumnNullable({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 58, 0, 0}}}, 6),
                          createDateTimeColumnConst(2, {2021, 1, 1, 23, 59, 0, 0}, 6),
                          createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMP_DIFF("second",
                          createDateTimeColumnConst(2, {2021, 1, 1, 23, 59, 59, 0}, 6),
                          createDateTimeColumnNullable({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 59, 58, 0}}}, 6),
                          createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF("second",
                          createDateTimeColumnNullable({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 59, 58, 0}}}, 6),
                          createDateTimeColumnConst(2, {2021, 1, 1, 23, 59, 59, 0}, 6),
                          createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMP_DIFF("microsecond",
                          createDateTimeColumnConst(2, {2021, 1, 1, 23, 59, 59, 999999}, 6),
                          createDateTimeColumnNullable({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 59, 59, 999998}}}, 6),
                          createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF("microsecond",
                          createDateTimeColumnNullable({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 59, 59, 999998}}}, 6),
                          createDateTimeColumnConst(2, {2021, 1, 1, 23, 59, 59, 999999}, 6),
                          createColumn<Nullable<Int64>>({-1, 1}));
}
CATCH

TEST_F(TestTimestampDiff, vectorVector)
try
{
    ASSERT_TIMESTAMP_DIFF("year",
                          createDateTimeColumnNullable({{{2021, 1, 1, 0, 0, 0, 0}}, {{2021, 1, 1, 0, 0, 0, 0}}}, 6),
                          createDateTimeColumnNullable({{{2022, 1, 1, 0, 0, 0, 0}}, {{2020, 1, 1, 0, 0, 0, 0}}}, 6),
                          createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF("year",
                          createDateTimeColumnNullable({{{2022, 1, 1, 0, 0, 0, 0}}, {{2020, 1, 1, 0, 0, 0, 0}}}, 6),
                          createDateTimeColumnNullable({{{2021, 1, 1, 0, 0, 0, 0}}, {{2021, 1, 1, 0, 0, 0, 0}}}, 6),
                          createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMP_DIFF("quarter",
                          createDateTimeColumnNullable({{{2021, 1, 1, 0, 0, 0, 0}}, {{2021, 1, 1, 0, 0, 0, 0}}}, 6),
                          createDateTimeColumnNullable({{{2021, 4, 1, 0, 0, 0, 0}}, {{2020, 10, 1, 0, 0, 0, 0}}}, 6),
                          createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF("quarter",
                          createDateTimeColumnNullable({{{2021, 4, 1, 0, 0, 0, 0}}, {{2020, 10, 1, 0, 0, 0, 0}}}, 6),
                          createDateTimeColumnNullable({{{2021, 1, 1, 0, 0, 0, 0}}, {{2021, 1, 1, 0, 0, 0, 0}}}, 6),
                          createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMP_DIFF("month",
                          createDateTimeColumnNullable({{{2021, 1, 1, 0, 0, 0, 0}}, {{2021, 1, 1, 0, 0, 0, 0}}}, 6),
                          createDateTimeColumnNullable({{{2021, 2, 1, 0, 0, 0, 0}}, {{2020, 12, 1, 0, 0, 0, 0}}}, 6),
                          createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF("month",
                          createDateTimeColumnNullable({{{2021, 2, 1, 0, 0, 0, 0}}, {{2020, 12, 1, 0, 0, 0, 0}}}, 6),
                          createDateTimeColumnNullable({{{2021, 1, 1, 0, 0, 0, 0}}, {{2021, 1, 1, 0, 0, 0, 0}}}, 6),
                          createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMP_DIFF("week",
                          createDateTimeColumnNullable({{{2021, 2, 28, 0, 0, 0, 0}}, {{2021, 2, 28, 0, 0, 0, 0}}}, 6),
                          createDateTimeColumnNullable({{{2021, 3, 7, 0, 0, 0, 0}}, {{2021, 2, 21, 0, 0, 0, 0}}}, 6),
                          createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF("week",
                          createDateTimeColumnNullable({{{2021, 3, 7, 0, 0, 0, 0}}, {{2021, 2, 21, 0, 0, 0, 0}}}, 6),
                          createDateTimeColumnNullable({{{2021, 2, 28, 0, 0, 0, 0}}, {{2021, 2, 28, 0, 0, 0, 0}}}, 6),
                          createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMP_DIFF("day",
                          createDateTimeColumnNullable({{{2021, 2, 28, 0, 0, 0, 0}}, {{2021, 2, 28, 0, 0, 0, 0}}}, 6),
                          createDateTimeColumnNullable({{{2021, 3, 1, 0, 0, 0, 0}}, {{2021, 2, 27, 0, 0, 0, 0}}}, 6),
                          createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF("day",
                          createDateTimeColumnNullable({{{2021, 3, 1, 0, 0, 0, 0}}, {{2021, 2, 27, 0, 0, 0, 0}}}, 6),
                          createDateTimeColumnNullable({{{2021, 2, 28, 0, 0, 0, 0}}, {{2021, 2, 28, 0, 0, 0, 0}}}, 6),
                          createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMP_DIFF("hour",
                          createDateTimeColumnNullable({{{2021, 1, 1, 23, 0, 0, 0}}, {{2021, 1, 1, 23, 0, 0, 0}}}, 6),
                          createDateTimeColumnNullable({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 22, 0, 0, 0}}}, 6),
                          createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF("hour",
                          createDateTimeColumnNullable({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 22, 0, 0, 0}}}, 6),
                          createDateTimeColumnNullable({{{2021, 1, 1, 23, 0, 0, 0}}, {{2021, 1, 1, 23, 0, 0, 0}}}, 6),
                          createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMP_DIFF("minute",
                          createDateTimeColumnNullable({{{2021, 1, 1, 23, 59, 0, 0}}, {{2021, 1, 1, 23, 59, 0, 0}}}, 6),
                          createDateTimeColumnNullable({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 58, 0, 0}}}, 6),
                          createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF("minute",
                          createDateTimeColumnNullable({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 58, 0, 0}}}, 6),
                          createDateTimeColumnNullable({{{2021, 1, 1, 23, 59, 0, 0}}, {{2021, 1, 1, 23, 59, 0, 0}}}, 6),
                          createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMP_DIFF("second",
                          createDateTimeColumnNullable({{{2021, 1, 1, 23, 59, 59, 0}}, {{2021, 1, 1, 23, 59, 59, 0}}}, 6),
                          createDateTimeColumnNullable({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 59, 58, 0}}}, 6),
                          createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF("second",
                          createDateTimeColumnNullable({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 59, 58, 0}}}, 6),
                          createDateTimeColumnNullable({{{2021, 1, 1, 23, 59, 59, 0}}, {{2021, 1, 1, 23, 59, 59, 0}}}, 6),
                          createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMP_DIFF("microsecond",
                          createDateTimeColumnNullable({{{2021, 1, 1, 23, 59, 59, 999999}}, {{2021, 1, 1, 23, 59, 59, 999999}}}, 6),
                          createDateTimeColumnNullable({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 59, 59, 999998}}}, 6),
                          createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF("microsecond",
                          createDateTimeColumnNullable({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 59, 59, 999998}}}, 6),
                          createDateTimeColumnNullable({{{2021, 1, 1, 23, 59, 59, 999999}}, {{2021, 1, 1, 23, 59, 59, 999999}}}, 6),
                          createColumn<Nullable<Int64>>({-1, 1}));
}
CATCH

} // namespace
} // namespace DB::tests
