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

TEST_F(TestInetAtonNtoa, InetAton)
try
{
    const String func_name = "tiDBIPv4StringToNum";

    // empty column
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt32>>({}),
        executeFunction(func_name, createColumn<String>({})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt32>>({}),
        executeFunction(func_name, createColumn<Nullable<String>>({})));

    // const null-only column
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<UInt32>>(1, {}),
        executeFunction(func_name, createConstColumn<Nullable<String>>(1, {})));

    // const non-null column
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<UInt32>>(1, 1),
        executeFunction(func_name, createConstColumn<Nullable<String>>(1, "0.0.0.1")));

    // normal valid cases
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt32>>({16909060, 65537, 16711935, 66051, 0, 16777472, 1862276364}),
        executeFunction(func_name, createColumn<Nullable<String>>(
            {"1.2.3.4", "0.1.0.1", "0.255.0.255", "0000.1.2.3", "00000.0000.0000.000", "1.0.1.0", "111.0.21.012"})));

    // valid but weird cases
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt32>>({255, 255, 255, 255, 65535, 16711935, 16711935, 1, 16777218, 16908291}),
        executeFunction(func_name, createColumn<Nullable<String>>(
            {"255", ".255", "..255", "...255", "..255.255", ".255.255", ".255..255", "1", "1.2", "1.2.3"})));

    // invalid cases
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt32>>({{}, {}, {}, {}, {}, {}, {}, {}, {}, {}}),
        executeFunction(func_name, createColumn<Nullable<String>>(
            {{}, "", ".", "....255", "...255.255", ".255...255", ".255.255.", "1.0.a", "1.a", "a.1"})));
}
CATCH

TEST_F(TestInetAtonNtoa, InetNtoa)
try
{
    const String func_name = "IPv4NumToString";

    // empty column
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({}),
        executeFunction(func_name, createColumn<Nullable<UInt32>>({})));

    ASSERT_COLUMN_EQ(
        createColumn<String>({}),
        executeFunction(func_name, createColumn<UInt32>({})));

    // const null-only column
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(1, {}),
        executeFunction(func_name, createConstColumn<Nullable<UInt32>>(1, {})));

    // const non-null column
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(1, "0.0.0.1"),
        executeFunction(func_name, createConstColumn<Nullable<UInt32>>(1, 1)));

    // normal cases
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"1.2.3.4", "0.1.0.1", "0.255.0.255", "0.1.2.3", "0.0.0.0", "1.0.1.0", "111.0.21.12"}),
        executeFunction(func_name, createColumn<Nullable<UInt32>>(
            {16909060, 65537, 16711935, 66051, 0, 16777472, 1862276364})));
}
CATCH

TEST_F(TestInetAtonNtoa, InetNtoaReversible)
try
{
    const String aton = "tiDBIPv4StringToNum";
    const String ntoa = "IPv4NumToString";

    std::random_device rd;
    std::mt19937 mt(rd());
    std::uniform_int_distribution<UInt32> dist;

    InferredDataVector<UInt32> num_vec;
    for (size_t i = 0; i < 10000; ++i)
    {
        num_vec.emplace_back(dist(mt));
    }

    auto num_data_type = makeDataType<Nullable<UInt32>>();
    ColumnWithTypeAndName num_column(makeColumn<UInt32>(num_data_type, num_vec), num_data_type, "num");
    auto str_column = executeFunction(ntoa, num_column);
    auto num_column_2 = executeFunction(aton, str_column);
    ASSERT_COLUMN_EQ(num_column, num_column_2);
}
CATCH

} // namespace tests
} // namespace DB

