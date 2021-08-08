#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/registerFunctions.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <random>
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

class TestInetAtonNtoa: public ::testing::Test
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

#if 0
TEST_F(TestInetAtonNtoa, InetAton)
try
{
    const String func_name = "tiDBIPv4StringToNum";

    // empty column
    EXECUTE_UNARY_FUNCTION_AND_CHECK(
        func_name,
        makeNullableDataType<DataTypeString>(),
        makeNullableDataType<DataTypeUInt32>(),
        DataVectorString{},
        DataVectorUInt64{});

    // const null-only column
    EXECUTE_UNARY_FUNCTION_AND_CHECK(
        func_name,
        makeNullableDataType<DataTypeString>(),
        makeNullableDataType<DataTypeUInt32>(),
        DataVectorString{{}},
        DataVectorUInt64{{}});

    // const non-null column
    EXECUTE_UNARY_FUNCTION_AND_CHECK(
        func_name,
        makeNullableDataType<DataTypeString>(),
        makeNullableDataType<DataTypeUInt32>(),
        DataVectorString{"0.0.0.1"},
        DataVectorUInt64{1});

    // normal valid cases
    EXECUTE_UNARY_FUNCTION_AND_CHECK(
        func_name,
        makeNullableDataType<DataTypeString>(),
        makeNullableDataType<DataTypeUInt32>(),
        DataVectorString{"1.2.3.4", "0.1.0.1", "0.255.0.255", "0000.1.2.3", "00000.0000.0000.000", "1.0.1.0", "111.0.21.012"},
        DataVectorUInt64{16909060, 65537, 16711935, 66051, 0, 16777472, 1862276364});

    // valid but weird cases
    EXECUTE_UNARY_FUNCTION_AND_CHECK(
        func_name,
        makeNullableDataType<DataTypeString>(),
        makeNullableDataType<DataTypeUInt32>(),
        DataVectorString{"255", ".255", "..255", "...255", "..255.255", ".255.255", ".255..255", "1", "1.2", "1.2.3"},
        DataVectorUInt64{255, 255, 255, 255, 65535, 16711935, 16711935, 1, 16777218, 16908291});

    // invalid cases
    EXECUTE_UNARY_FUNCTION_AND_CHECK(
        func_name,
        makeNullableDataType<DataTypeString>(),
        makeNullableDataType<DataTypeUInt32>(),
        DataVectorString{{}, "", ".", "....255", "...255.255", ".255...255", ".255.255.", "1.0.a", "1.a", "a.1"},
        DataVectorUInt64{{}, {}, {}, {}, {}, {}, {}, {}, {}, {}});
}
CATCH

TEST_F(TestInetAtonNtoa, InetNtoa)
try
{
    const String func_name = "IPv4NumToString";

    // empty column
    EXECUTE_UNARY_FUNCTION_AND_CHECK(
        func_name,
        makeNullableDataType<DataTypeUInt32>(),
        makeNullableDataType<DataTypeString>(),
        DataVectorUInt64{},
        DataVectorString{});

    // const null-only column
    EXECUTE_UNARY_FUNCTION_AND_CHECK(
        func_name,
        makeNullableDataType<DataTypeUInt32>(),
        makeNullableDataType<DataTypeString>(),
        DataVectorUInt64{{}},
        DataVectorString{{}});

    // const non-null column
    EXECUTE_UNARY_FUNCTION_AND_CHECK(
        func_name,
        makeNullableDataType<DataTypeUInt32>(),
        makeNullableDataType<DataTypeString>(),
        DataVectorUInt64{1},
        DataVectorString{"0.0.0.1"});

    // normal cases
    EXECUTE_UNARY_FUNCTION_AND_CHECK(
        func_name,
        makeNullableDataType<DataTypeUInt32>(),
        makeNullableDataType<DataTypeString>(),
        DataVectorUInt64{16909060, 65537, 16711935, 66051, 0, 16777472, 1862276364},
        DataVectorString{"1.2.3.4", "0.1.0.1", "0.255.0.255", "0.1.2.3", "0.0.0.0", "1.0.1.0", "111.0.21.12"});
}
CATCH

TEST_F(TestInetAtonNtoa, InetNtoaReversible)
try
{
    const String aton = "tiDBIPv4StringToNum";
    const String ntoa = "IPv4NumToString";

    std::random_device rd;
    std::mt19937 mt(rd());
    std::uniform_int_distribution<DB::UInt32> dist;

    DataVectorUInt64 num_vec;
    for (size_t i = 0; i < 10000; ++i)
    {
        num_vec.emplace_back(dist(mt));
    }

    auto num_data_type = makeNullableDataType<DataTypeUInt32>();
    auto num_column = makeColumnWithTypeAndName("num", num_vec.size(), num_data_type, num_vec);
    auto str_column = executeFunction(ntoa, {num_column});
    auto num_column_2 = executeFunction(aton, {str_column});
    assertColumnEqual(num_column, num_column_2);
}
CATCH
#endif

} // namespace tests
} // namespace DB

