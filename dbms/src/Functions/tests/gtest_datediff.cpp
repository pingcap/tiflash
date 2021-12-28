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
class TestDateDiff : public DB::tests::FunctionTest
{
};

const std::string func_name = "tidbDateDiff";


TEST_F(TestDateDiff, constVector)
try
{
    ASSERT_COLUMN_EQ(createColumn<Nullable<Int64>>({1, -1}),
                     executeFunction(func_name,
                                     {createDateTimeColumnConst(2, {2021, 1, 1, 23, 59, 59, 999999}, 6),
                                      createDateTimeColumnNullable({{{2020, 12, 31, 0, 0, 0, 0}}, {{2021, 1, 2, 0, 0, 0, 0}}}, 6)}));
    ASSERT_COLUMN_EQ(createColumn<Nullable<Int64>>({-1, 1}),
                     executeFunction(func_name,
                                     {createDateTimeColumnNullable({{{2020, 12, 31, 0, 0, 0, 0}}, {{2021, 1, 2, 0, 0, 0, 0}}}, 6),
                                      createDateTimeColumnConst(2, {2021, 1, 1, 23, 59, 59, 999999}, 6)}));
}
CATCH

TEST_F(TestDateDiff, vectorVector)
try
{
    ASSERT_COLUMN_EQ(createColumn<Nullable<Int64>>({1, -1}),
                     executeFunction(func_name,
                                     {createDateTimeColumnNullable({{{2021, 1, 1, 23, 59, 59, 999999}}, {{2021, 1, 1, 23, 59, 59, 999999}}}, 6),
                                      createDateTimeColumnNullable({{{2020, 12, 31, 0, 0, 0, 0}}, {{2021, 1, 2, 0, 0, 0, 0}}}, 6)}));
}
CATCH

TEST_F(TestDateDiff, onlyNull)
try
{
    ASSERT_COLUMN_EQ(createConstColumn<Nullable<Int64>>(2, {}),
                     executeFunction(func_name,
                                     {createOnlyNullColumn(2),
                                      createDateTimeColumnNullable({{{2020, 12, 31, 0, 0, 0, 0}}, {{2021, 1, 2, 0, 0, 0, 0}}}, 6)}));
    ASSERT_COLUMN_EQ(createConstColumn<Nullable<Int64>>(2, {}),
                     executeFunction(func_name,
                                     {createDateTimeColumnNullable({{{2020, 12, 31, 0, 0, 0, 0}}, {{2021, 1, 2, 0, 0, 0, 0}}}, 6),
                                      createOnlyNullColumn(2)}));
}
CATCH

} // namespace
} // namespace DB::tests
