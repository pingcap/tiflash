#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/registerFunctions.h>
#include <Interpreters/Context.h>
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

class TestInetAtonNtoa: public TiFlashTestBase
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

TEST_F(TestInetAtonNtoa, InetAton)
try
{
    const String func_name = "tiDBIPv4StringToNum";

    // empty column
    EXECUTE_UNARY_FUNCTION_AND_CHECK(
        func_name,
        String,
        UInt64,
        makeNullableDataType<DataTypeString>(),
        makeNullableDataType<DataTypeUInt32>(),
        {},
        {});

    // const null-only column
    EXECUTE_UNARY_FUNCTION_AND_CHECK(
        func_name,
        String,
        UInt64,
        makeNullableDataType<DataTypeString>(),
        makeNullableDataType<DataTypeUInt32>(),
        {{}},
        {{}});

    // const non-null column
    EXECUTE_UNARY_FUNCTION_AND_CHECK(
        func_name,
        String,
        UInt64,
        makeNullableDataType<DataTypeString>(),
        makeNullableDataType<DataTypeUInt32>(),
        {"0.0.0.1"},
        {1});

    // normal valid cases
    EXECUTE_UNARY_FUNCTION_AND_CHECK(
        func_name,
        String,
        UInt64,
        makeNullableDataType<DataTypeString>(),
        makeNullableDataType<DataTypeUInt32>(),
        {"1.2.3.4", "0.1.0.1", "0.255.0.255", "0000.1.2.3", "00000.0000.0000.000", "1.0.1.0", "111.0.21.012"},
        {16909060, 65537, 16711935, 66051, 0, 16777472, 1862276364});

    // valid but weird cases
    EXECUTE_UNARY_FUNCTION_AND_CHECK(
        func_name,
        String,
        UInt64,
        makeNullableDataType<DataTypeString>(),
        makeNullableDataType<DataTypeUInt32>(),
        {"255", ".255", "..255", "...255", "..255.255", ".255.255", ".255..255", "1", "1.2", "1.2.3"},
        {255, 255, 255, 255, 65535, 16711935, 16711935, 1, 16777218, 16908291});

    // invalid cases
    EXECUTE_UNARY_FUNCTION_AND_CHECK(
        func_name,
        String,
        UInt64,
        makeNullableDataType<DataTypeString>(),
        makeNullableDataType<DataTypeUInt32>(),
        {{}, "", ".", "....255", "...255.255", ".255...255", ".255.255.", "1.0.a", "1.a", "a.1"},
        {{}, {}, {}, {}, {}, {}, {}, {}, {}, {}});
}
CATCH

} // namespace tests
} // namespace DB


