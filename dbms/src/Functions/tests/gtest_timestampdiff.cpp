#include "Columns/ColumnsNumber.h"
#include "Core/ColumnWithTypeAndName.h"
#include "DataTypes/DataTypeMyDateTime.h"
#include "DataTypes/DataTypeMyDuration.h"
#include "DataTypes/DataTypeNullable.h"
#include "DataTypes/DataTypesNumber.h"
#include "Functions/FunctionHelpers.h"
#include "TestUtils/FunctionTestUtils.h"
#include "common/types.h"
#include "gtest/gtest.h"

namespace DB::tests
{
namespace
{
class TestTimestampdiff : public DB::tests::FunctionTest
{
};

const std::string func_name = "tidbTimestampDiff";

#define ASSERT_TIMESTAMPDIFF(unit, t1, t2, result) \
    ASSERT_COLUMN_EQ(result, executeFunction(func_name, {createConstColumn<String>(t1.column->size(), unit), t1, t2}))

TEST_F(TestTimestampdiff, constVector)
try
{
    ASSERT_TIMESTAMPDIFF("year",
                         createDateTimeColumnConst(2, {2021, 1, 1, 0, 0, 0, 0}, 6),
                         createDateTimeColumnNullable({{{2022, 1, 1, 0, 0, 0, 0}}, {{2020, 1, 1, 0, 0, 0, 0}}}, 6),
                         createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMPDIFF("year",
                         createDateTimeColumnNullable({{{2022, 1, 1, 0, 0, 0, 0}}, {{2020, 1, 1, 0, 0, 0, 0}}}, 6),
                         createDateTimeColumnConst(2, {2021, 1, 1, 0, 0, 0, 0}, 6),
                         createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMPDIFF("quarter",
                         createDateTimeColumnConst(2, {2021, 1, 1, 0, 0, 0, 0}, 6),
                         createDateTimeColumnNullable({{{2021, 4, 1, 0, 0, 0, 0}}, {{2020, 10, 1, 0, 0, 0, 0}}}, 6),
                         createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMPDIFF("quarter",
                         createDateTimeColumnNullable({{{2021, 4, 1, 0, 0, 0, 0}}, {{2020, 10, 1, 0, 0, 0, 0}}}, 6),
                         createDateTimeColumnConst(2, {2021, 1, 1, 0, 0, 0, 0}, 6),
                         createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMPDIFF("month",
                         createDateTimeColumnConst(2, {2021, 1, 1, 0, 0, 0, 0}, 6),
                         createDateTimeColumnNullable({{{2021, 2, 1, 0, 0, 0, 0}}, {{2020, 12, 1, 0, 0, 0, 0}}}, 6),
                         createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMPDIFF("month",
                         createDateTimeColumnNullable({{{2021, 2, 1, 0, 0, 0, 0}}, {{2020, 12, 1, 0, 0, 0, 0}}}, 6),
                         createDateTimeColumnConst(2, {2021, 1, 1, 0, 0, 0, 0}, 6),
                         createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMPDIFF("week",
                         createDateTimeColumnConst(2, {2021, 2, 28, 0, 0, 0, 0}, 6),
                         createDateTimeColumnNullable({{{2021, 3, 7, 0, 0, 0, 0}}, {{2021, 2, 21, 0, 0, 0, 0}}}, 6),
                         createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMPDIFF("week",
                         createDateTimeColumnNullable({{{2021, 3, 7, 0, 0, 0, 0}}, {{2021, 2, 21, 0, 0, 0, 0}}}, 6),
                         createDateTimeColumnConst(2, {2021, 2, 28, 0, 0, 0, 0}, 6),
                         createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMPDIFF("day",
                         createDateTimeColumnConst(2, {2021, 2, 28, 0, 0, 0, 0}, 6),
                         createDateTimeColumnNullable({{{2021, 3, 1, 0, 0, 0, 0}}, {{2021, 2, 27, 0, 0, 0, 0}}}, 6),
                         createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMPDIFF("day",
                         createDateTimeColumnNullable({{{2021, 3, 1, 0, 0, 0, 0}}, {{2021, 2, 27, 0, 0, 0, 0}}}, 6),
                         createDateTimeColumnConst(2, {2021, 2, 28, 0, 0, 0, 0}, 6),
                         createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMPDIFF("hour",
                         createDateTimeColumnConst(2, {2021, 1, 1, 23, 0, 0, 0}, 6),
                         createDateTimeColumnNullable({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 22, 0, 0, 0}}}, 6),
                         createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMPDIFF("hour",
                         createDateTimeColumnNullable({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 22, 0, 0, 0}}}, 6),
                         createDateTimeColumnConst(2, {2021, 1, 1, 23, 0, 0, 0}, 6),
                         createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMPDIFF("minute",
                         createDateTimeColumnConst(2, {2021, 1, 1, 23, 59, 0, 0}, 6),
                         createDateTimeColumnNullable({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 58, 0, 0}}}, 6),
                         createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMPDIFF("minute",
                         createDateTimeColumnNullable({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 58, 0, 0}}}, 6),
                         createDateTimeColumnConst(2, {2021, 1, 1, 23, 59, 0, 0}, 6),
                         createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMPDIFF("second",
                         createDateTimeColumnConst(2, {2021, 1, 1, 23, 59, 59, 0}, 6),
                         createDateTimeColumnNullable({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 59, 58, 0}}}, 6),
                         createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMPDIFF("second",
                         createDateTimeColumnNullable({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 59, 58, 0}}}, 6),
                         createDateTimeColumnConst(2, {2021, 1, 1, 23, 59, 59, 0}, 6),
                         createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMPDIFF("microsecond",
                         createDateTimeColumnConst(2, {2021, 1, 1, 23, 59, 59, 999999}, 6),
                         createDateTimeColumnNullable({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 59, 59, 999998}}}, 6),
                         createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMPDIFF("microsecond",
                         createDateTimeColumnNullable({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 59, 59, 999998}}}, 6),
                         createDateTimeColumnConst(2, {2021, 1, 1, 23, 59, 59, 999999}, 6),
                         createColumn<Nullable<Int64>>({-1, 1}));
}
CATCH

TEST_F(TestTimestampdiff, vectorVector)
try
{
    ASSERT_TIMESTAMPDIFF("year",
                         createDateTimeColumnNullable({{{2021, 1, 1, 0, 0, 0, 0}}, {{2021, 1, 1, 0, 0, 0, 0}}}, 6),
                         createDateTimeColumnNullable({{{2022, 1, 1, 0, 0, 0, 0}}, {{2020, 1, 1, 0, 0, 0, 0}}}, 6),
                         createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMPDIFF("year",
                         createDateTimeColumnNullable({{{2022, 1, 1, 0, 0, 0, 0}}, {{2020, 1, 1, 0, 0, 0, 0}}}, 6),
                         createDateTimeColumnNullable({{{2021, 1, 1, 0, 0, 0, 0}}, {{2021, 1, 1, 0, 0, 0, 0}}}, 6),
                         createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMPDIFF("quarter",
                         createDateTimeColumnNullable({{{2021, 1, 1, 0, 0, 0, 0}}, {{2021, 1, 1, 0, 0, 0, 0}}}, 6),
                         createDateTimeColumnNullable({{{2021, 4, 1, 0, 0, 0, 0}}, {{2020, 10, 1, 0, 0, 0, 0}}}, 6),
                         createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMPDIFF("quarter",
                         createDateTimeColumnNullable({{{2021, 4, 1, 0, 0, 0, 0}}, {{2020, 10, 1, 0, 0, 0, 0}}}, 6),
                         createDateTimeColumnNullable({{{2021, 1, 1, 0, 0, 0, 0}}, {{2021, 1, 1, 0, 0, 0, 0}}}, 6),
                         createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMPDIFF("month",
                         createDateTimeColumnNullable({{{2021, 1, 1, 0, 0, 0, 0}}, {{2021, 1, 1, 0, 0, 0, 0}}}, 6),
                         createDateTimeColumnNullable({{{2021, 2, 1, 0, 0, 0, 0}}, {{2020, 12, 1, 0, 0, 0, 0}}}, 6),
                         createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMPDIFF("month",
                         createDateTimeColumnNullable({{{2021, 2, 1, 0, 0, 0, 0}}, {{2020, 12, 1, 0, 0, 0, 0}}}, 6),
                         createDateTimeColumnNullable({{{2021, 1, 1, 0, 0, 0, 0}}, {{2021, 1, 1, 0, 0, 0, 0}}}, 6),
                         createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMPDIFF("week",
                         createDateTimeColumnNullable({{{2021, 2, 28, 0, 0, 0, 0}}, {{2021, 2, 28, 0, 0, 0, 0}}}, 6),
                         createDateTimeColumnNullable({{{2021, 3, 7, 0, 0, 0, 0}}, {{2021, 2, 21, 0, 0, 0, 0}}}, 6),
                         createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMPDIFF("week",
                         createDateTimeColumnNullable({{{2021, 3, 7, 0, 0, 0, 0}}, {{2021, 2, 21, 0, 0, 0, 0}}}, 6),
                         createDateTimeColumnNullable({{{2021, 2, 28, 0, 0, 0, 0}}, {{2021, 2, 28, 0, 0, 0, 0}}}, 6),
                         createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMPDIFF("day",
                         createDateTimeColumnNullable({{{2021, 2, 28, 0, 0, 0, 0}}, {{2021, 2, 28, 0, 0, 0, 0}}}, 6),
                         createDateTimeColumnNullable({{{2021, 3, 1, 0, 0, 0, 0}}, {{2021, 2, 27, 0, 0, 0, 0}}}, 6),
                         createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMPDIFF("day",
                         createDateTimeColumnNullable({{{2021, 3, 1, 0, 0, 0, 0}}, {{2021, 2, 27, 0, 0, 0, 0}}}, 6),
                         createDateTimeColumnNullable({{{2021, 2, 28, 0, 0, 0, 0}}, {{2021, 2, 28, 0, 0, 0, 0}}}, 6),
                         createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMPDIFF("hour",
                         createDateTimeColumnNullable({{{2021, 1, 1, 23, 0, 0, 0}}, {{2021, 1, 1, 23, 0, 0, 0}}}, 6),
                         createDateTimeColumnNullable({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 22, 0, 0, 0}}}, 6),
                         createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMPDIFF("hour",
                         createDateTimeColumnNullable({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 22, 0, 0, 0}}}, 6),
                         createDateTimeColumnNullable({{{2021, 1, 1, 23, 0, 0, 0}}, {{2021, 1, 1, 23, 0, 0, 0}}}, 6),
                         createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMPDIFF("minute",
                         createDateTimeColumnNullable({{{2021, 1, 1, 23, 59, 0, 0}}, {{2021, 1, 1, 23, 59, 0, 0}}}, 6),
                         createDateTimeColumnNullable({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 58, 0, 0}}}, 6),
                         createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMPDIFF("minute",
                         createDateTimeColumnNullable({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 58, 0, 0}}}, 6),
                         createDateTimeColumnNullable({{{2021, 1, 1, 23, 59, 0, 0}}, {{2021, 1, 1, 23, 59, 0, 0}}}, 6),
                         createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMPDIFF("second",
                         createDateTimeColumnNullable({{{2021, 1, 1, 23, 59, 59, 0}}, {{2021, 1, 1, 23, 59, 59, 0}}}, 6),
                         createDateTimeColumnNullable({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 59, 58, 0}}}, 6),
                         createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMPDIFF("second",
                         createDateTimeColumnNullable({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 59, 58, 0}}}, 6),
                         createDateTimeColumnNullable({{{2021, 1, 1, 23, 59, 59, 0}}, {{2021, 1, 1, 23, 59, 59, 0}}}, 6),
                         createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMPDIFF("microsecond",
                         createDateTimeColumnNullable({{{2021, 1, 1, 23, 59, 59, 999999}}, {{2021, 1, 1, 23, 59, 59, 999999}}}, 6),
                         createDateTimeColumnNullable({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 59, 59, 999998}}}, 6),
                         createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMPDIFF("microsecond",
                         createDateTimeColumnNullable({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 59, 59, 999998}}}, 6),
                         createDateTimeColumnNullable({{{2021, 1, 1, 23, 59, 59, 999999}}, {{2021, 1, 1, 23, 59, 59, 999999}}}, 6),
                         createColumn<Nullable<Int64>>({-1, 1}));
}
CATCH

} // namespace
} // namespace DB::tests
