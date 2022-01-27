#include "Columns/ColumnsNumber.h"
#include "Common/Exception.h"
#include "Core/ColumnWithTypeAndName.h"
#include "DataTypes/DataTypeMyDateTime.h"
#include "DataTypes/DataTypeMyDuration.h"
#include "DataTypes/DataTypeNullable.h"
#include "DataTypes/DataTypesNumber.h"
#include "Functions/FunctionHelpers.h"
#include "TestUtils/FunctionTestUtils.h"
#include "TestUtils/TiFlashTestException.h"
#include "common/types.h"
#include "gtest/gtest.h"

namespace DB::tests
{
namespace
{
class TestToMyDate : public DB::tests::FunctionTest
{
};

const std::string func_name = "toMyDate";


TEST_F(TestToMyDate, all)
try
{
    ASSERT_COLUMN_EQ(createDateColumnNullable({{{2021, 12, 31}}, {}}),
                     executeFunction(func_name,
                                     {createDateTimeColumnNullable({{{2021, 12, 31, 0, 0, 0, 0}}, {}}, 6)}));
    ASSERT_THROW(
        executeFunction(func_name, {createColumn<Int64>({1})}),
        Exception);
}
CATCH


} // namespace
} // namespace DB::tests
