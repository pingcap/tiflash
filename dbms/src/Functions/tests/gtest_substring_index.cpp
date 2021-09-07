#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsString.h>
#include <Functions/registerFunctions.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <Poco/Types.h>

#pragma GCC diagnostic pop

namespace DB
{
namespace tests
{
class SubStringIndex : public ::testing::Test
{
protected:
    static void SetUpTestCase()
    {
        try
        {
            registerFunctions();
        }
        catch (DB::Exception &)
        {
            // Maybe another test has already registed, ignore exception here.
        }
    }
};

// test string and fixed string
TEST_F(SubStringIndex, str_Test)
try
{
    // Test string
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"www.pingcap", "pingcap.com", "", "www.pingcap.com"}),
        executeFunction(
            "substring_index",
            createColumn<Nullable<String>>(
                {"www.pingcap.com", "www.pingcap.com", "www.pingcap.com", "www.pingcap.com"}),
            createColumn<Nullable<String>>({".", ".", ".", "."}),
            createColumn<Nullable<Int64>>({2, -2, 0, 10})));
    // Test Null
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({{}, {}, {}, "www.pingcap.com"}),
        executeFunction(
            "substring_index",
            createColumn<Nullable<String>>(
                {{}, "www.pingcap.com", "www.pingcap.com", "www.pingcap.com"}),
            createColumn<Nullable<String>>({".", {}, ".", "."}),
            createColumn<Nullable<Int64>>({2, -2, {}, 10})));
}
CATCH

} // namespace tests
} // namespace DB