#include <DataTypes/DataTypeNullable.h>
#include <Functions/registerFunctions.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

namespace DB
{
namespace tests
{


class StringFormat : public ::testing::Test
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


TEST_F(StringFormat, string_format_all_unit_Test)
try
{
    std::string func_name = "format";

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"12,332.1235", "12,332.1000", "12,332", "12,332", "12,332.300000000000000000000000000000", "-12,332.30000", "-1,000.0", "-333.33"}),
        executeFunction(
            func_name,
            createColumn<Nullable<Float64>>({12332.123456, 12332.1, 12332.2, 12332.3, 12332.3, -12332.3, -999.9999, -333.333}),
            createColumn<Nullable<Int64>>({4, 4, 0, -1, 31, 5, 1, 2})));
}
CATCH

} // namespace tests
} // namespace DB
